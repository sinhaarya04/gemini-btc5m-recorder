import asyncio
import gzip
import json
from pathlib import Path
import sys

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from recorder import BookSequence, Recorder, Spool, Storage, compress_pending, message_kind


def test_book_snapshot_delta_duplicate_and_gap():
    sequence = BookSequence()
    assert sequence.accept({"U": 100, "u": 100}) == "snapshot"
    assert sequence.accept({"U": 101, "u": 103}) == "delta"
    assert sequence.accept({"U": 101, "u": 103}) == "duplicate"
    with pytest.raises(ValueError, match="gap"):
        sequence.accept({"U": 105, "u": 105})
    assert sequence.last == 103
    assert BookSequence().accept({"U": 200, "u": 200}) == "snapshot"


def test_crash_recovery_preserves_complete_records(tmp_path):
    (tmp_path / "crashed.open").write_bytes(b'{"good":1}\n{"incomplete":')
    spool = Spool(tmp_path)
    assert spool.recovered == 1
    assert spool.truncated_bytes == len(b'{"incomplete":')
    path = compress_pending(tmp_path / "crashed.pending")
    assert gzip.decompress(path.read_bytes()) == b'{"good":1}\n'


def test_spool_keeps_exchange_precision_and_timestamps(tmp_path):
    spool = Spool(tmp_path)
    spool.write("depthUpdate", {"E": 1789967605997838588, "p": "0.0100"}, book_update_type="snapshot")
    spool.seal()
    path = compress_pending(next(tmp_path.glob("*.pending")))
    row = json.loads(gzip.decompress(path.read_bytes()))
    assert row["payload"]["E"] == 1789967605997838588
    assert row["payload"]["p"] == "0.0100"
    assert isinstance(row["received_at_ns"], int)
    assert row["book_update_type"] == "snapshot"


def test_failed_upload_keeps_local_recording(tmp_path, monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-secret")
    path = tmp_path / "20260921T000000-test.jsonl.gz"
    path.write_bytes(gzip.compress(b'{}\n'))

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(503))) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await Storage().upload(client, path)
        assert path.exists()
    asyncio.run(run())


@pytest.mark.parametrize("same", [True, False])
def test_duplicate_upload_verified_before_deleting(tmp_path, monkeypatch, same):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-secret")
    path = tmp_path / "20260921T000000-test.jsonl.gz"
    data = gzip.compress(b'{}\n')
    path.write_bytes(data)
    def serve(request):
        if request.method == "POST":
            return httpx.Response(409, json={"error": "Duplicate"})
        return httpx.Response(200, content=data if same else b"different")
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as client:
            if same:
                await Storage().upload(client, path)
                assert not path.exists()
            else:
                with pytest.raises(RuntimeError, match="checksum"):
                    await Storage().upload(client, path)
                assert path.exists()
    asyncio.run(run())


def test_trade_ticker_reference_classification():
    assert message_kind({"E": 1, "s": "x", "t": 123, "p": "0.5", "q": "10", "m": True}) == "trade"
    assert message_kind({"E": 1, "s": "x", "b": "0.5", "B": "10"}) == "bookTicker"
    assert message_kind({"e": "thirdPartyPrice", "E": 1789967604}) == "thirdPartyPrice"


def test_failed_worker_terminates_for_restart(tmp_path, monkeypatch):
    async def broken(self):
        raise RuntimeError("worker failed")
    monkeypatch.setattr(Recorder, "metadata_loop", broken)
    async def run():
        recorder = Recorder(tmp_path)
        with pytest.raises(RuntimeError, match="worker failed"):
            await asyncio.wait_for(recorder.run(), 3)
        assert recorder.stop.is_set()
        assert list((tmp_path / "spool").glob("*.pending"))
    asyncio.run(run())
