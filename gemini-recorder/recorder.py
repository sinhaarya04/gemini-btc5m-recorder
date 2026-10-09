"""Public Gemini BTC05M recorder. Contains no order-entry or trading code."""
from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import random
import shutil
import signal
import sqlite3
import time
from urllib.parse import quote
import uuid

import httpx
from websockets.asyncio.client import connect

API = "https://api.gemini.com/v1/prediction-markets/events"
WS = "wss://ws.gemini.com?snapshot=-1"
LOG = logging.getLogger("recorder")
TERMINAL = {"settled", "invalid"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def atomic_json(path, obj):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as f:
        json.dump(obj, f, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())
    temporary.replace(path)


class Spool:
    """Append locally before upload; retain originals until confirmed storage success."""

    def __init__(self, root, rotate_seconds=60, max_bytes=8_000_000):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.rotate_seconds = rotate_seconds
        self.max_bytes = max_bytes
        self.session = uuid.uuid4().hex
        self.file = None
        self.path = None
        self.started = 0
        self.bytes = 0
        self.counts = Counter()
        self.total = 0
        self.recovered = 0
        self.truncated_bytes = 0
        # Single process owns this directory (enforced by flock in main).
        for path in self.root.glob("*.open"):
            self.recover(path)

    def recover(self, path):
        raw = path.read_bytes()
        end = raw.rfind(b"\n") + 1
        self.truncated_bytes += len(raw) - end
        with path.open("r+b") as f:
            f.truncate(end)
        path.rename(path.with_suffix(".pending"))
        self.recovered += 1

    def write(self, kind, payload, **context):
        now = time.monotonic()
        if self.file and (now - self.started >= self.rotate_seconds or self.bytes >= self.max_bytes):
            self.seal()
        if self.file is None:
            if shutil.disk_usage(self.root).free < 200_000_000:
                raise OSError("Local data disk nearly full; preserving pending recordings")
            name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex
            self.path = self.root / (name + ".open")
            self.file = self.path.open("ab")
            self.started = now
            self.bytes = 0
        record = {
            "schema_version": 1, "received_at_ns": time.time_ns(),
            "received_monotonic_ns": time.monotonic_ns(), "session_id": self.session,
            "kind": kind, **context, "payload": payload,
        }
        line = (json.dumps(record, separators=(",", ":")) + "\n").encode()
        self.file.write(line)
        self.file.flush()
        self.bytes += len(line)
        self.total += 1
        self.counts[kind] += 1

    def sync(self):
        if self.file:
            self.file.flush()
            os.fsync(self.file.fileno())

    def seal(self):
        if self.file:
            self.sync()
            self.file.close()
            self.file = None
            self.path.rename(self.path.with_suffix(".pending"))

    def rotate_if_due(self):
        if self.file and time.monotonic() - self.started >= self.rotate_seconds:
            self.seal()


def compress_pending(path):
    destination = path.with_suffix(".jsonl.gz")
    temporary = path.with_suffix(".gz.part")
    with path.open("rb") as source, temporary.open("wb") as out:
        with gzip.GzipFile(fileobj=out, mode="wb", mtime=0) as zipped:
            shutil.copyfileobj(source, zipped)
        out.flush()
        os.fsync(out.fileno())
    temporary.replace(destination)
    path.unlink()
    return destination


class Storage:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL", "").rstrip("/")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
        self.bucket = os.getenv("SUPABASE_BUCKET", "gemini-btc5m-raw")
        self.enabled = bool(self.url and self.key)
        self.last_upload = None
        self.files_uploaded = 0
        self.bytes_uploaded = 0
        self.last_error = None

    def headers(self):
        return {"apikey": self.key, "Authorization": "Bearer " + self.key}

    async def upload(self, client, path):
        # Unique immutable names make retry after timeout safe.
        date = path.name[:8]
        key = f"btc5m/{date[:4]}-{date[4:6]}-{date[6:8]}/{path.name}"
        url = self.url + "/storage/v1/object/" + quote(self.bucket) + "/" + key
        data = await asyncio.to_thread(path.read_bytes)
        response = await client.post(url, content=data, headers={
            **self.headers(), "Content-Type": "application/gzip", "x-upsert": "false",
        })
        duplicate = response.status_code in (400, 409) and (
            "Duplicate" in response.text or "already exists" in response.text
        )
        if duplicate:
            existing = await client.get(url, headers=self.headers())
            existing.raise_for_status()
            if hashlib.sha256(existing.content).digest() != hashlib.sha256(data).digest():
                raise RuntimeError("Remote object checksum mismatch; retaining local file")
        else:
            response.raise_for_status()
        self.last_upload = utc_now()
        self.files_uploaded += 1
        self.bytes_uploaded += len(data)
        self.last_error = None
        path.unlink()
        LOG.info("uploaded file=%s bytes=%d", key, len(data))


class BookSequence:
    def __init__(self):
        self.last = None

    def accept(self, message):
        first, last = int(message["U"]), int(message["u"])
        if first > last:
            raise ValueError("invalid sequence range")
        if self.last is None:
            self.last = last
            return "snapshot"
        if last <= self.last:
            return "duplicate"
        if first > self.last + 1:
            raise ValueError(f"book sequence gap: previous={self.last} next={first}")
        self.last = last
        return "delta"


def message_kind(message):
    if "id" in message:
        return "reply"
    if message.get("e"):
        return message["e"]
    if "b" in message and "B" in message:
        return "bookTicker"
    if "t" in message and "p" in message and "q" in message:
        return "trade"
    return "unknown"


class Recorder:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.spool = Spool(self.root / "spool", float(os.getenv("ROTATE_SECONDS", "60")))
        self.storage = Storage()
        self.db = sqlite3.connect(self.root / "catalog.sqlite")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS events (
            ticker TEXT PRIMARY KEY, expiry REAL NOT NULL, terminal INTEGER NOT NULL,
            checked REAL NOT NULL, body TEXT NOT NULL)""")
        self.db.commit()
        self.stop = asyncio.Event()
        self.streams = {}
        self.connections = {}
        self.last_discovery = None
        self.last_reference = None
        self.started_at = utc_now()
        self.gaps = 0
        self.errors = 0
        self.settled = 0
        self.last_error = None
        self.terms_saved = set()

    def error(self, operation, exc):
        # Never print HTTP request headers or environment variables.
        self.errors += 1
        detail = type(exc).__name__
        if isinstance(exc, httpx.HTTPStatusError):
            detail += ": HTTP " + str(exc.response.status_code)
        self.last_error = {"time": utc_now(), "operation": operation, "type": detail}
        LOG.warning("operation=%s error=%s", operation, detail)
        self.spool.write("collector_error", self.last_error)

    async def pause(self, seconds):
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.stop.wait(), seconds)

    def save_event(self, event, source):
        ticker = event["ticker"]
        if not ticker.upper().startswith("BTC05M"):
            return
        terminal = event.get("status", "").lower() in TERMINAL
        old = self.db.execute("SELECT terminal FROM events WHERE ticker=?", (ticker,)).fetchone()
        if terminal and (old is None or not old[0]):
            self.settled += 1
        # Store an as-observed snapshot before updating the convenience catalog.
        self.spool.write("event_metadata", event, endpoint=source, event_ticker=ticker)
        self.db.execute("""INSERT INTO events VALUES(?,?,?,?,?) ON CONFLICT(ticker)
            DO UPDATE SET expiry=excluded.expiry, terminal=excluded.terminal,
            checked=excluded.checked, body=excluded.body""", (
            ticker, timestamp(event["expiryDate"]), int(terminal), time.time(), json.dumps(event)))
        self.db.commit()

    async def archive_terms(self, client, url):
        if not url or url in self.terms_saved:
            return
        # Metadata isn't trusted as an arbitrary download destination.
        if not url.startswith("https://assets.gemini.com/predictions/terms_and_conditions/"):
            self.spool.write("unarchived_terms_url", {"url": url})
            return
        response = await client.get(url)
        response.raise_for_status()
        import base64
        self.spool.write("contract_rules", {
            "url": url, "sha256": hashlib.sha256(response.content).hexdigest(),
            "content_type": response.headers.get("content-type"),
            "base64": base64.b64encode(response.content).decode(),
        })
        self.terms_saved.add(url)

    async def discover(self, client):
        events = []
        offset = 0
        while True:
            response = await client.get(API, params={
                "category": "crypto", "status": "active", "limit": 500, "offset": offset})
            response.raise_for_status()
            body = response.json()
            page = body["data"]
            events.extend(e for e in page if e.get("series", "").upper() == "BTC05M")
            offset += len(page)
            if not page or offset >= body["pagination"]["total"]:
                break
            if offset >= 10000:
                raise RuntimeError("Discovery exceeded pagination safety bound")
        wanted = {}
        now = time.time()
        for event in events:
            self.save_event(event, API)
            source = event.get("sourceDetails", {})
            if source.get("agency") and source.get("index"):
                index_stream = source["agency"] + ":" + source["index"] + "@indexPrice"
                wanted[index_stream] = [index_stream]
            for contract in event.get("contracts", []):
                expiry = timestamp(contract["expiryDate"])
                start = timestamp(contract.get("effectiveDate") or contract["strike"]["availableAt"])
                symbol = contract["instrumentSymbol"]
                if expiry > now - 30 and start <= now + 600:
                    wanted[symbol.lower()] = [symbol + x for x in ("@depth@100ms", "@bookTicker", "@trade")]
                await self.archive_terms(client, contract.get("termsAndConditionsUrl"))
        wanted["contractStatus"] = ["contractStatus"]
        for key in list(self.streams):
            if key not in wanted:
                self.streams[key].cancel()
                await asyncio.gather(self.streams.pop(key), return_exceptions=True)
                self.connections.pop(key, None)
        for key, subscriptions in wanted.items():
            if key not in self.streams or self.streams[key].done():
                self.streams[key] = asyncio.create_task(self.stream(key, subscriptions), name=key)
        self.last_discovery = utc_now()

    async def settle_pending(self, client):
        pending = self.db.execute("""SELECT ticker FROM events WHERE terminal=0
            AND expiry<? AND checked<? ORDER BY checked LIMIT 10""",
            (time.time() - 5, time.time() - 30)).fetchall()
        for (ticker,) in pending:
            endpoint = API + "/" + quote(ticker)
            # Mark attempts to prevent one stale event starving other pending events.
            self.db.execute("UPDATE events SET checked=? WHERE ticker=?", (time.time(), ticker))
            self.db.commit()
            try:
                response = await client.get(endpoint)
                response.raise_for_status()
                self.save_event(response.json(), endpoint)
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                self.error("settlement_lookup", exc)

    async def stream(self, key, subscriptions):
        delay = 1
        while not self.stop.is_set():
            connection_id = uuid.uuid4().hex
            sequence = BookSequence()
            connected = time.monotonic()
            is_reference = "@indexPrice" in key
            try:
                self.spool.write("connection_start", {"subscriptions": subscriptions}, connection_id=connection_id)
                async with connect(WS, ping_interval=20, ping_timeout=20, open_timeout=20,
                                   close_timeout=5, max_size=8_000_000, max_queue=1024) as ws:
                    await ws.send(json.dumps({"id": "subscribe", "method": "SUBSCRIBE", "params": subscriptions}))
                    self.connections[key] = {"connected": True, "connection_id": connection_id,
                                             "last_message": None, "acknowledged": False}
                    last_data = time.monotonic()
                    snapshot_received = False
                    while not self.stop.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), 30)
                        except asyncio.TimeoutError:
                            if is_reference or (key.startswith("gemi-") and not snapshot_received):
                                raise TimeoutError("Required stream is stale or missing initial book snapshot")
                            continue  # Quiet future contracts are healthy if protocol ping/pong succeeds.
                        message = json.loads(raw)
                        kind = message_kind(message)
                        extra = {}
                        if kind == "reply":
                            self.spool.write("ws_reply", message, connection_id=connection_id, stream=key)
                            if message.get("status") != 200:
                                raise RuntimeError("Subscription rejected")
                            self.connections[key]["acknowledged"] = True
                            continue
                        if kind == "contractStatus" and "btc05m" not in message.get("s", "").lower():
                            continue
                        if kind == "depthUpdate":
                            try:
                                disposition = sequence.accept(message)
                            except ValueError:
                                self.spool.write("invalid_book_update", message, connection_id=connection_id, stream=key)
                                raise
                            extra["book_update_type"] = disposition
                            snapshot_received = True
                        if kind == "thirdPartyPrice":
                            self.last_reference = utc_now()
                        self.spool.write(kind, message, connection_id=connection_id, stream=key, **extra)
                        self.connections[key]["last_message"] = utc_now()
                        last_data = time.monotonic()
                        if last_data - connected > 30:
                            delay = 1
            except asyncio.CancelledError:
                self.spool.write("connection_end", {"reason": "contract_rotation_or_shutdown"},
                                 connection_id=connection_id, stream=key)
                raise
            except Exception as exc:
                self.gaps += 1
                self.error("websocket_" + key, exc)
                self.spool.write("data_gap", {
                    "reason": type(exc).__name__, "detail": str(exc)[:250],
                    "recovery": "reconnect_with_full_snapshot; lost updates are not backfilled",
                }, connection_id=connection_id, stream=key)
                self.connections[key] = {"connected": False, "connection_id": connection_id}
                await self.pause(delay + random.random())
                delay = min(delay * 2, 30)

    async def metadata_loop(self):
        async with httpx.AsyncClient(timeout=20, follow_redirects=False) as client:
            while not self.stop.is_set():
                try:
                    await self.discover(client)
                    await self.settle_pending(client)
                except Exception as exc:
                    self.error("metadata", exc)
                await self.pause(float(os.getenv("DISCOVERY_SECONDS", "15")))

    async def upload_loop(self):
        async with httpx.AsyncClient(timeout=60) as client:
            while not self.stop.is_set():
                try:
                    self.spool.rotate_if_due()
                    self.spool.sync()
                    for path in sorted(self.spool.root.glob("*.pending")):
                        await asyncio.to_thread(compress_pending, path)
                    if self.storage.enabled:
                        for path in sorted(self.spool.root.glob("*.jsonl.gz")):
                            await self.storage.upload(client, path)
                except Exception as exc:
                    self.storage.last_error = {"time": utc_now(), "type": type(exc).__name__}
                    self.error("storage", exc)
                    await self.pause(10)
                await self.pause(2)

    def status(self):
        files = list(self.spool.root.iterdir())
        pending = self.db.execute("SELECT count(*) FROM events WHERE terminal=0 AND expiry<?", (time.time(),)).fetchone()[0]
        return {
            "mode": "public_data_only", "started_at": self.started_at, "updated_at": utc_now(),
            "session_id": self.spool.session, "last_discovery": self.last_discovery,
            "last_reference_price": self.last_reference, "records": self.spool.total,
            "counts": dict(self.spool.counts), "connections": self.connections,
            "gap_count": self.gaps, "error_count": self.errors, "last_error": self.last_error,
            "settled_events_seen": self.settled, "pending_settlements": pending,
            "storage_enabled": self.storage.enabled, "last_upload": self.storage.last_upload,
            "uploaded_files": self.storage.files_uploaded, "uploaded_bytes": self.storage.bytes_uploaded,
            "storage_error": self.storage.last_error, "local_files": len(files),
            "local_bytes": sum(p.stat().st_size for p in files if p.exists()),
            "disk_free_bytes": shutil.disk_usage(self.root).free,
        }

    async def health_loop(self):
        while not self.stop.is_set():
            status = self.status()
            atomic_json(self.root / "status.json", status)
            self.spool.write("collector_status", status)
            LOG.info("records=%d uploads=%d streams=%d gaps=%d pending_settlements=%d",
                     status["records"], status["uploaded_files"], len(self.streams), self.gaps,
                     status["pending_settlements"])
            await self.pause(30)

    async def run(self):
        self.spool.write("collector_start", {
            "version": "1.0.0", "storage_enabled": self.storage.enabled,
            "recovered_files": self.spool.recovered, "discarded_partial_bytes": self.spool.truncated_bytes,
            "timestamps": "receive times in ns; raw exchange E preserved (units vary by stream)",
        })
        tasks = [asyncio.create_task(fn()) for fn in (self.metadata_loop, self.upload_loop, self.health_loop)]
        stopper = asyncio.create_task(self.stop.wait())
        try:
            finished, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
            # A crashed worker must stop the process so Fly's restart policy can recover.
            for task in finished:
                if task is not stopper:
                    task.result()
                    raise RuntimeError("Recorder worker stopped unexpectedly")
        finally:
            self.stop.set()
            stopper.cancel()
            for task in tasks + list(self.streams.values()):
                task.cancel()
            await asyncio.gather(*tasks, *self.streams.values(), stopper, return_exceptions=True)
            self.spool.write("collector_stop", {"reason": "shutdown"})
            self.spool.seal()
            atomic_json(self.root / "status.json", self.status())
            self.db.close()


async def main():
    import fcntl
    root = Path(os.getenv("DATA_DIR", "data"))
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / "recorder.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    recorder = Recorder(root)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, recorder.stop.set)
    await recorder.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(main())
