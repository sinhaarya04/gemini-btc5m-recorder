# Gemini BTC five-minute recorder

This worker records public Gemini BTC05M prediction-market data continuously. It does not place orders and has no Gemini trading credentials.

## Deployment

- Fly app: `gemini-btc5m-recorder-aryan`
- Region: `iad` (Virginia)
- Machine: `859160f4419e58`, one shared CPU, 2 GB RAM
- Encrypted persistent volume: `recorder_data`, 10 GB
- Supabase organization: `pmarkets`
- Supabase project: `btc-data` (`vhfqgyzrmbhsvtpynypi`), dedicated to this recorder. On October 10, 2026 the archive moved here from `dtgciwhecaqwnddzepiz` in the `E[X]` organization; all 28,278 earlier files were copied and size-verified
- Private Storage bucket: `gemini-btc5m-raw`
- Production files: `btc5m/YYYY-MM-DD/*.jsonl.gz`
- Local validation recordings, if uploaded: `validation/YYYY-MM-DD/*.jsonl.gz`

[Fly monitoring](https://fly.io/apps/gemini-btc5m-recorder-aryan/monitoring) · [Supabase project](https://supabase.com/dashboard/project/vhfqgyzrmbhsvtpynypi/storage/buckets)

Fly Secrets holds the Supabase URL and backend credential. No credential is embedded in the source or container image. The bucket is private. Existing apps, buckets and database tables were not modified. In this first version, metadata and outcomes are included in the raw files; an on-volume SQLite catalog tracks pending settlements. No Supabase database tables are required.

## What gets recorded

- Full initial price-level order-book snapshots followed by Gemini's 100 ms batched differential updates.
- Best bid/ask prices and quantities, market trades, and the contract's own reference-price stream.
- Contract metadata, starting strike, expiry, official outcomes, observed settlement prices and contract status changes.
- A copy of the published contract-rules PDF, with its SHA-256 hash.
- Connection IDs, local receipt timestamps, session IDs, health samples and explicitly recorded feed gaps.

The reference provider is discovered from each event's `sourceDetails`. The live contracts tested on September 21, 2026 used `Kaiko:GRR-KAIKO_RFR_BTCUSD_60S@indexPrice`; it is not hardcoded. The worker follows the current round and upcoming rounds that start within ten minutes, so recording begins before each contract window.

Each compressed file contains newline-delimited JSON envelopes:

```json
{"schema_version":1,"received_at_ns":1789967605997838588,"received_monotonic_ns":12345,"session_id":"...","kind":"depthUpdate","connection_id":"...","stream":"gemi-btc05m...-up","book_update_type":"snapshot","payload":{"e":"depthUpdate","E":1789967605997838588,"s":"gemi-btc05m...-up","U":100,"u":100,"b":[["0.50","10"]],"a":[["0.52","5"]]}}
```

`received_at_ns` is UTC Unix nanoseconds on the recording machine. `received_monotonic_ns` is only comparable inside the same process/session. Exchange timestamps remain unchanged: their units vary by feed (the tested reference-price `E` was seconds, book/trade `E` nanoseconds, and contract-status `E` milliseconds). Prices and sizes remain strings to preserve precision.

For `depthUpdate`, `book_update_type` is `snapshot`, `delta` or `duplicate`. Start a book from each connection's snapshot, replace quantities at changed levels, delete zero-quantity levels, and ignore duplicates. On a detected sequence gap, discard the previous reconstructed book and begin again from the new connection's snapshot. Snapshots restore current state; missing historical changes cannot be recovered.

## Persistence and recovery

Data is written to the volume immediately, synced approximately every two seconds, and rotated every sixty seconds or eight million uncompressed bytes. Sealed files are compressed and uploaded. Local files are deleted only after successful upload; duplicate upload responses require matching downloaded checksums first. Upload failures retain the backlog. An interrupted final line is discarded during recovery and its byte count recorded.

The process restarts automatically on exit. There is no HTTP service, public listening port, or scale-to-zero setting. The worker reconnects with backoff and re-discovers contracts every fifteen seconds. Pending outcomes survive restarts in SQLite and are revisited until settled or invalid.

A single machine can still have outages, and the newest unuploaded data depends on its volume. Deployments and restarts can interrupt collection. Do not treat an absence of explicit gap records as proof that the history is perfectly complete: inspect session boundaries, timestamps and coverage as well. No external alerting service is configured in this first version.

## Operations

Run these commands from a terminal logged into the same Fly account:

```sh
fly logs --app gemini-btc5m-recorder-aryan
fly ssh console --app gemini-btc5m-recorder-aryan --command 'cat /data/status.json'
fly machine status 859160f4419e58 --app gemini-btc5m-recorder-aryan
```

`status.json` updates every thirty seconds. Check fresh discovery/reference times, connected/acknowledged streams, growing record counts, recent uploads and storage errors. Counters reset at process startup; raw archives retain earlier sessions. A quiet contract can legitimately have no trades.

To stop collection while retaining the recorded data:

```sh
fly machine stop 859160f4419e58 --app gemini-btc5m-recorder-aryan
```

To resume:

```sh
fly machine start 859160f4419e58 --app gemini-btc5m-recorder-aryan
```

For an intentional code update, run from this directory:

```sh
fly deploy --remote-only --ha=false --no-public-ips
```

Keep one machine for this configuration: two writers must not share the same spool/catalog. Additional recorders need separate volumes and deduplication during research.

## Local use and validation

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt pytest
.venv/bin/python -m pytest tests -q
DATA_DIR=./data .venv/bin/python recorder.py
```

Without Supabase environment variables, local mode records to disk only. Cloud upload requires `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` and optionally `SUPABASE_BUCKET` in the environment. Keep secrets out of source control and terminal output.

Tests cover book gap detection, crash recovery, exchange timestamp precision, failed-upload retention, checksum verification for upload retries, and message classification. See `verification.json` for the most recent live object readback check.

This is a recording dataset, not a validated trading strategy. Before model training, reconstruct books, join official outcomes by contract, exclude incomplete coverage, and split contracts chronologically. Keep the `validation/` prefix out of production datasets unless explicitly deduplicated.

## API references

- [Gemini prediction-market streams](https://developer.gemini.com/trading/websocket/streams)
- [Gemini event discovery](https://developer.gemini.com/rest-api/prediction-markets/events/list-events)
- [Gemini event outcomes](https://developer.gemini.com/rest-api/prediction-markets/events/get-event)
- [Fly persistent volumes](https://fly.io/docs/volumes/overview/)
