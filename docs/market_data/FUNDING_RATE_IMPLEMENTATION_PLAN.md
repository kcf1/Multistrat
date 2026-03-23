# Funding Rate Implementation Plan (Postgres, OHLCV-Aligned)

This plan follows `docs/market_data/STANDARD_DATA_PIPELINE.md` and mirrors the OHLCV runtime pattern:
`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

---

## 1) Design goals

- Ship funding history ingestion with one canonical path for backfill + incremental updates.
- Keep writes idempotent on a stable natural key.
- Make progress restart-safe with durable cursor checkpoints.
- Reuse OHLCV job/scheduler behavior and observability style.
- Keep spot OHLCV provider separate from perps provider.

---

## 2) Standard components (funding-specific)

### 2.1 Config split

- **Macro/env**
  - `MARKET_DATA_DATABASE_URL` (or `DATABASE_URL` fallback)
  - `MARKET_DATA_BINANCE_PERPS_BASE_URL` (new, service-isolated perps endpoint)
- **Micro/code constants** in `market_data/config.py`
  - `FUNDING_SYMBOLS`
  - `FUNDING_INITIAL_BACKFILL_DAYS`
  - `FUNDING_FETCH_CHUNK_LIMIT`
  - `FUNDING_CORRECT_WINDOW_POINTS`
  - `FUNDING_SCHEDULER_INGEST_INTERVAL_SECONDS`
  - `FUNDING_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`
  - `FUNDING_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS`
  - funding retry/backoff constants

### 2.2 Provider

- Keep existing OHLCV spot provider unchanged (`KlinesProvider` + `binance_spot`).
- Add perps provider contract and adapter:
  - protocol: `PerpsMarketDataProvider.fetch_funding_rates(...)`
  - implementation: `market_data/providers/binance_perps.py`
- Use one shared `ProviderRateLimiter` per perps provider instance.

### 2.3 Validation and parse

- Add `FundingRatePoint` in `market_data/schemas.py`.
- Parse Binance funding payloads into canonical model.
- Validate UTC timestamps, numeric fields, symbol normalization, and order assumptions.
- Surface parse errors for retry/give-up policy.

### 2.4 Storage

- Add table `funding_rate`:
  - `exchange` TEXT
  - `symbol` TEXT
  - `funding_time` TIMESTAMPTZ
  - `funding_rate` NUMERIC(18,10)
  - `mark_price` NUMERIC(20,10) NULL
  - `sample_ts` TIMESTAMPTZ
  - `ingested_at` TIMESTAMPTZ DEFAULT now()
- Natural key: `(exchange, symbol, funding_time)`.
- Read index: `(symbol, funding_time DESC)`.
- Upsert with `ON CONFLICT ... DO UPDATE`.

### 2.5 Cursor / watermark

- Add explicit funding cursor helpers (`get`, `upsert`, `max_stored_time`).
- Cursor advances only after chunk commit.
- Start-point recovery: `max(cursor, max(funding_time))`.

---

## 3) Runtime modes

## 3.1 Initialize / backfill mode

### Ordered flow

1. Load settings, build provider, open DB connection.
2. Enumerate `FUNDING_SYMBOLS`.
3. Resolve start from `max(cursor, max(stored_time))`; cold start from `now - FUNDING_INITIAL_BACKFILL_DAYS`.
4. Fetch forward in chunked pages from funding endpoint.
5. Parse + validate into `FundingRatePoint`.
6. Upsert rows + update cursor in same transaction scope.
7. Commit per chunk.
8. Continue until end range or empty page.
9. Emit per-symbol result summary (rows/chunks/retries/give-ups).

### Backfill options

- `--no-watermark`
- `--skip-existing`
- `--once`

## 3.2 Real-time scheduled mode

1. Scheduler starts, first run immediate.
2. `ingest_funding_rate` on short cadence.
3. `correct_window_funding_rate` on medium cadence.
4. `repair_gap_funding_rate` on optional long cadence.
5. Each job failure is isolated; scheduler continues.
6. Next runs remain UTC-aligned.

---

## 4) Retry and failure policy

- Retry transient HTTP/network issues with exponential backoff.
- Do not retry obvious client input errors (for example HTTP 400 invalid symbol).
- Track parse failures and give-ups in logs and run summaries.
- Commit per chunk to preserve progress on crash/restart.

---

## 5) Data quality controls

- Batch ordering and duplicate-key diagnostics.
- Coverage checks for requested range spans.
- Drift counts in correct-window runs.
- Gap-span metrics in repair runs.
- Freshness checks on latest funding time per symbol.

---

## 6) Observability and operations

- Structured job logs:
  - symbols processed
  - rows fetched/upserted
  - retries/give-ups
  - drift rows
  - repaired spans
- Health checks:
  - scheduler liveness
  - funding cursor advancement
  - staleness alerting
- Operational commands:
  - one-shot run
  - one-shot with repair
  - full backfill

---

## 7) Testing blueprint

### Unit tests

- Parser/model validity and malformed payload handling.
- Provider URL params, limiter order, retry/backoff, 400 no-retry behavior.
- Storage idempotency and cursor update semantics.
- Gap/window helper logic.

### Job-level tests

- Cold start backfill.
- Incremental resume from cursor.
- No-op when up to date.
- Partial failure with chunk checkpoint recovery.
- Correct-window drift upsert behavior.
- Repair-gap targeted refill behavior.

### Integration tests

- End-to-end one-shot writes expected funding rows.
- Scheduler executes funding jobs in expected cadence/order.
- Restart resumes from persisted watermark.

---

## 8) Delivery checklist

- [ ] Add migration for `funding_rate` table and indexes.
- [ ] Add funding config constants and perps base URL setting.
- [ ] Add `FundingRatePoint` model + parser + tests.
- [ ] Add perps provider protocol + Binance perps adapter.
- [ ] Add funding upsert + cursor helpers + tests.
- [ ] Implement `ingest_funding_rate`.
- [ ] Implement `correct_window_funding_rate`.
- [ ] Implement `repair_gap_funding_rate` (or explicitly defer).
- [ ] Wire funding jobs into `market_data/main.py`.
- [ ] Update docs and README.
- [ ] Add unit/job/integration coverage.

---

## 9) Reference mapping to OHLCV code

- Scheduler: `market_data/main.py`
- Ingest baseline: `market_data/jobs/ingest_ohlcv.py`
- Correct-window baseline: `market_data/jobs/correct_window.py`
- Repair baseline: `market_data/jobs/repair_gap.py`
- Provider baseline: `market_data/providers/binance_spot.py`
- Validation baseline: `market_data/validation.py`, `market_data/schemas.py`
- Storage baseline: `market_data/storage.py`
- Config baseline: `market_data/config.py`

---

## 10) Implementation list (execution order)

### Phase A - Foundations

1. Add migration for `funding_rate` table + indexes.
2. Add funding constants in `market_data/config.py`.
3. Add `MARKET_DATA_BINANCE_PERPS_BASE_URL` in settings + docs.

### Phase B - Provider and parsing

4. Add `FundingRatePoint` model in `market_data/schemas.py`.
5. Add funding payload parser in `market_data/validation.py`.
6. Define perps provider protocol.
7. Implement `market_data/providers/binance_perps.py` with limiter/retry policy.

### Phase C - Storage and cursor

8. Add funding upsert function(s).
9. Add funding cursor helpers.
10. Add range/window query helpers for correction and repair jobs.

### Phase D - Jobs and scheduler

11. Implement `ingest_funding_rate`.
12. Implement `correct_window_funding_rate`.
13. Implement `repair_gap_funding_rate`.
14. Register funding jobs in `market_data/main.py`.

### Phase E - Verification and rollout

15. Add unit and job tests.
16. Add scheduler integration smoke test.
17. Update README + runbook docs.
18. Run one-shot smoke in dev and verify cursor advancement + freshness.

---

## Follow-on note

Open interest should reuse the same perps provider module and limiter budget, but be implemented as a separate dataset plan using the same blueprint.
