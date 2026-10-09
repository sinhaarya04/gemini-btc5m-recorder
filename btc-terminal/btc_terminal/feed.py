from __future__ import annotations

import asyncio
import contextlib
import json
import time

import httpx
from websockets.asyncio.client import connect

from .market import MarketState, select_contract

API = "https://api.gemini.com/v1/prediction-markets/events"
WS = "wss://ws.gemini.com?snapshot=-1"


class GeminiFeed:
    def __init__(self, state: MarketState):
        self.state = state
        self.stream_task: asyncio.Task | None = None
        self.wakeup = asyncio.Event()

    async def discover(self, client: httpx.AsyncClient) -> list[dict]:
        events, offset = [], 0
        while offset < 10000:
            response = await client.get(API, params={"category": "crypto", "status": "active",
                                                   "limit": 500, "offset": offset})
            response.raise_for_status()
            body = response.json()
            page = body["data"]
            events.extend(page)
            offset += len(page)
            if not page or offset >= body["pagination"]["total"]:
                return events
        raise RuntimeError("Event list exceeded pagination limit")

    async def cancel_stream(self) -> None:
        if self.stream_task:
            self.stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.stream_task
            self.stream_task = None

    def reconnect(self) -> None:
        if self.stream_task:
            self.stream_task.cancel()
        self.state.book.reset()
        self.state.connected = False
        self.state.status = "Reconnecting…"
        self.state.revision += 1
        self.wakeup.set()

    async def run(self) -> None:
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                while True:
                    self.wakeup.clear()
                    try:
                        events = await self.discover(client)
                        contract = select_contract(events, time.time())
                        self.state.discovery_error = ""
                        if contract:
                            previous = self.state.contract
                            changed = previous is None or (
                                previous.symbol, previous.reference_stream
                            ) != (contract.symbol, contract.reference_stream)
                            if changed:
                                await self.cancel_stream()
                                self.state.switch(contract)
                            else:
                                # An absent REST strike must not overwrite a known status-stream strike.
                                contract.strike = contract.strike if contract.strike is not None else previous.strike
                                self.state.contract = contract
                            if self.stream_task is None or self.stream_task.done():
                                await self.cancel_stream()
                                self.stream_task = asyncio.create_task(self.stream())
                        else:
                            await self.cancel_stream()
                            self.state.connected = False
                            self.state.book.reset()
                            self.state.status = "No active BTC five-minute contract · retrying"
                            self.state.revision += 1
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        detail = f"HTTP {exc.response.status_code}" if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
                        self.state.discovery_error = f"Market discovery: {detail} · retrying"
                    delay = 10.0
                    if self.state.contract:
                        remaining = self.state.contract.expiry - time.time()
                        delay = min(delay, max(1.0, remaining + 0.15))
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self.wakeup.wait(), delay)
        finally:
            await self.cancel_stream()

    async def stream(self) -> None:
        contract = self.state.contract
        subscriptions = [contract.symbol + "@depth@100ms", contract.symbol + "@trade",
                         contract.reference_stream, "contractStatus"]
        delay = 1
        try:
            while time.time() < contract.expiry:
                self.state.book.reset()
                self.state.acknowledged = False
                self.state.reference_at = 0
                self.state.status = "Connecting · requesting full snapshot"
                self.state.revision += 1
                started = time.monotonic()
                try:
                    async with connect(WS, ping_interval=10, ping_timeout=10, open_timeout=12,
                                       close_timeout=2, max_size=8_000_000, max_queue=1024) as ws:
                        await ws.send(json.dumps({"id": "btc", "method": "SUBSCRIBE", "params": subscriptions}))
                        self.state.connected = True
                        while time.time() < contract.expiry:
                            raw = await asyncio.wait_for(ws.recv(), timeout=15)
                            self.state.ingest(json.loads(raw))
                            self.state.latency_ms = ws.latency * 1000
                            elapsed = time.monotonic() - started
                            if self.state.book.sequence is None and elapsed > 15:
                                raise TimeoutError("Book snapshot missing")
                            if time.monotonic() - max(self.state.reference_at, started) > 15:
                                raise TimeoutError("BTC reference feed stale")
                            self.state.status = "Connected · full depth"
                            if elapsed > 30:
                                delay = 1
                    self.wakeup.set()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.state.connected = False
                    self.state.book.reset()
                    self.state.revision += 1
                    self.state.reconnects += 1
                    self.state.status = f"{type(exc).__name__} · reconnecting in {delay}s"
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 15)
        finally:
            self.state.connected = False
            self.state.book.reset()
            self.state.revision += 1
