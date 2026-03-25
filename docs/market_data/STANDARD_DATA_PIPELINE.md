# Standard Data Pipeline Blueprint (Market Data)

This document defines a reusable pipeline pattern for datasets in `market_data`, aligned with the current OHLCV architecture:

`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

It is intended to be copied for new datasets (funding, open interest, trades, snapshots) with minimal shape changes.

---

## 1) Design goals

- Single canonical ingest path for both initialization and incremental updates.
- Idempotent writes keyed by dataset natural key.
- Restart-safe progress tracking with cursor/watermark semantics.
- Operationally simple scheduling with bounded retries and clear logs.
- Dataset-specific parsing, shared job/runtime mechanics.

---

## 2) Standard components

### 2.1 Config split

- **Macro/env**: DB URLs, provider base URLs, API credentials (if needed), deployment-specific endpoints.
- **Micro/code constants**: symbols, intervals, lookback windows, chunk size, scheduler cadence, retry defaults.

### 2.2 Provider

- Dataset-facing provider protocol (read-only).
- Venue implementation handles request params, timeout, retry/backoff, and rate limiting.
- Shared limiter per provider instance so all jobs for the same provider respect one request budget.

### 2.3 Validation and parse

- Normalize raw payload into one internal model type.
- Validate row shape, timestamp semantics, monotonicity/order assumptions, and field invariants.
- Raise parse/validation errors so caller retry policy can decide whether to retry or give up.

### 2.4 Storage

- Upsert on natural key: `INSERT ... ON CONFLICT ... DO UPDATE`.
- Keep write functions transaction-agnostic (caller controls `commit` boundaries).
- Add window/query helpers needed by correction and repair jobs.

### 2.5 Cursor / watermark

- Maintain explicit cursor table per dataset key-space (or shared generic cursor table if stable).
- Cursor advances only after successful durable chunk commit.
- Startup uses `max(cursor, max(stored_time))` to recover safely from partial runs.

---

## 3) Runtime modes

## 3.1 Initialize / backfill mode

Use when dataset is empty, partially populated, or requires historical replay.

### Ordered flow

1. Load settings and build provider + DB connection.
2. Enumerate configured series (`symbol`, `interval`, or dataset equivalent).
3. Resolve range start:
   - default: after `max(cursor, max(stored_time))`
   - cold start: `now - INITIAL_BACKFILL_DAYS`
   - optional no-watermark mode: force policy horizon replay
4. Fetch forward in chunked pages from provider.
5. Parse + validate each page into internal rows.
6. Upsert rows and advance cursor (same transaction scope).
7. Commit once per chunk (checkpoint).
8. Continue until page empty or end range reached.
9. Emit per-series summary: rows upserted, chunks, retry/give-up count.

### Backfill options (recommended)

- `--no-watermark`: ignore cursor for start-point calculation.
- `--skip-existing`: detect gaps in policy window and refill only missing spans.
- `--once`: run ingest pass and exit.

## 3.2 Real-time scheduled mode

Use for continuous maintenance after initial history exists.

### Ordered flow

1. Scheduler starts and triggers first run immediately.
2. **Ingest job** runs on short cadence:
   - incremental catch-up from cursor/watermark
   - closed rows only (for candle-like datasets)
3. **Correct-window job** runs on medium cadence:
   - refetch recent window
   - compare against stored values
   - log drift and upsert corrected rows
4. **Repair-gap job** (optional/long cadence):
   - detect missing spans over policy window
   - targeted refetch + upsert
5. Each step is wrapped in exception handling so scheduler continues on failure.
6. Next run times are UTC-aligned after each completion.

---

## 4) Retry and failure policy

- Retry transient HTTP/network failures with exponential backoff.
- Treat obvious client errors (for example HTTP 400 invalid symbol) as non-retryable.
- Parse/shape failures should be visible in logs and counted; retry policy may treat them as transient for provider instability.
- Commit per chunk so completed progress survives process restart.
- Never rely on in-memory state as the only progress source.

---

## 5) Data quality controls

- Batch integrity checks (ordering, duplicates, monotonic timestamps).
- Span/coverage checks for requested windows (head slack/tail shortfall diagnostics).
- Drift tracking during correction window.
- Gap detection against expected grid inside a fixed policy window.
- Freshness checks: latest stored timestamp lag by series.

---

## 6) Observability and operations

- Structured logs per job step:
  - series processed
  - rows fetched/upserted
  - retries/give-ups
  - drift rows
  - gap spans repaired
- Health signals:
  - scheduler loop alive
  - cursor advancing
  - ingest staleness alerts
- Operational commands:
  - one-shot run (`--once`)
  - one-shot with repair
  - full backfill script

---

## 7) Testing blueprint

### Unit tests

- Parser/model validation (good rows, malformed rows, edge timestamps).
- Provider request construction and retry behavior.
- Storage idempotency (`ON CONFLICT`) and cursor updates.
- Gap detection and window math.

### Job-level tests

- Cold start backfill (empty table).
- Incremental resume from cursor.
- No-op when fully up to date.
- Partial failures with chunk checkpoint recovery.
- Correct-window drift detection/upsert.
- Repair-gap targeted refill semantics.

### Integration tests

- End-to-end one-shot run writes expected rows.
- Scheduler cadence executes jobs in expected order.
- Restart continues from persisted watermark.

---

## 8) Delivery checklist for new dataset

- [ ] Define natural key, table schema, indexes, migration.
- [ ] Add micro constants and required macro env fields.
- [ ] Implement internal model + parser + validation tests.
- [ ] Implement provider protocol + venue adapter + retry/limiter behavior.
- [ ] Implement storage upsert + cursor helpers + tests.
- [ ] Implement ingest job (backfill + incremental).
- [ ] Implement correct-window job.
- [ ] Implement repair-gap job (or explicitly defer).
- [ ] Wire jobs into scheduler runner.
- [ ] Add docs/runbook and update service README.
- [ ] Add unit/job/integration test coverage.

---

## 9) Reference mapping to current OHLCV implementation

- Scheduler and job orchestration: `market_data/main.py`
- Ingest/backfill mechanics: `market_data/jobs/ingest_ohlcv.py`
- Chunk paging helpers: `market_data/jobs/common.py`
- Drift correction: `market_data/jobs/correct_window.py`
- Gap detect/repair: `market_data/jobs/repair_gap.py`
- Provider/retry/limiter behavior: `market_data/providers/binance_spot.py`
- Validation and parsing: `market_data/validation.py`, `market_data/schemas.py`
- Upsert and cursor persistence: `market_data/storage.py`
- Tunables and settings: `market_data/config.py`

---

## 10) Reference mapping — open interest (`open_interest`)

- Scheduler wiring: `market_data/main.py` (`ingest_open_interest`, `correct_window_open_interest`, `repair_gap_open_interest`)
- Ingest / backfill mechanics: `market_data/jobs/ingest_open_interest.py`
- Chunk paging: `market_data/jobs/common.py` (`iter_open_interest_batches_forward`, `floor_align_ms_to_interval`)
- Drift correction: `market_data/jobs/correct_window_open_interest.py`
- Gap detect/repair: `market_data/jobs/repair_gap_open_interest.py`
- Provider: `market_data/providers/binance_perps.py` (`fetch_open_interest_hist`)
- Validation / model: `market_data/validation.py`, `market_data/schemas.py` (`OpenInterestPoint`)
- Storage / cursor: `market_data/storage.py`
- One-shot backfill CLI: `scripts/backfill_open_interest.py`
- Tunables: `OPEN_INTEREST_*` in `market_data/config.py`
