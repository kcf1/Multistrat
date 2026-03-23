# Open Interest Implementation Plan (Binance Open-Interest-Statistics, Historical)

This plan follows `docs/market_data/STANDARD_DATA_PIPELINE.md` and mirrors the OHLCV/basis runtime shape:
`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

Scope here is Binance historical open interest from the open-interest-statistics endpoint, not locally derived metrics.

---

## 1) Design goals

- Ingest official Binance open interest history as the source of truth.
- Ship hourly snapshots from Binance endpoint `period=1h`.
- Keep writes idempotent on a stable natural key.
- Make ingest restart-safe with durable cursor checkpoints.
- Reuse existing market_data scheduler/observability patterns.

---

## 2) Binance source contract (open-interest-statistics)

Planned source endpoint:

- `GET /futures/data/openInterestHist`

Expected request fields:

- `symbol` (for example `BTCUSDT`)
- `contractType` (`ALL`, `CURRENT_QUARTER`, `NEXT_QUARTER`, `PERPETUAL`)
- `period` (`1h` for this tranche)
- optional pagination controls (`startTime`, `endTime`, `limit`)

Expected response fields (dataset payload):

- `symbol`
- `contractType`
- `sumOpenInterest`
- `sumOpenInterestValue`
- `CMCCirculatingSupply`
- `timestamp`

Key constraints to encode in plan:

- Source retention is limited (Binance docs indicate latest ~30 days).
- Backfill horizon must be treated as request intent; actual returned history is provider-capped.

---

## 3) Standard components (open-interest-specific)

### 3.1 Config split

- **Macro/env**
  - `MARKET_DATA_DATABASE_URL` (or `DATABASE_URL` fallback)
  - `MARKET_DATA_BINANCE_PERPS_BASE_URL`
- **Micro/code constants** in `market_data/config.py`
  - `OPEN_INTEREST_SYMBOLS`
  - `OPEN_INTEREST_CONTRACT_TYPES`
  - `OPEN_INTEREST_PERIODS` (initially `("1h",)`)
  - `OPEN_INTEREST_INITIAL_BACKFILL_DAYS`
  - `OPEN_INTEREST_FETCH_CHUNK_LIMIT`
  - `OPEN_INTEREST_CORRECT_WINDOW_POINTS`
  - `OPEN_INTEREST_SCHEDULER_INGEST_INTERVAL_SECONDS`
  - `OPEN_INTEREST_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`
  - `OPEN_INTEREST_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS`
  - open-interest retry/backoff constants

### 3.2 Provider

- Extend perps market-data provider contract:
  - `fetch_open_interest_hist(...) -> list[OpenInterestPoint]`
- Implement Binance adapter in `market_data/providers/binance_perps.py`.
- Use one shared `ProviderRateLimiter` per provider instance across basis/funding/open-interest calls.
- Preserve no-retry behavior for obvious client errors (invalid symbol/params).

### 3.3 Validation and parse

- Add `OpenInterestPoint` model in `market_data/schemas.py`.
- Parse payload to typed model:
  - uppercase `symbol`
  - strict `contract_type`
  - UTC-aware `timestamp` mapped to `sample_time`
  - decimals for `sumOpenInterest`, `sumOpenInterestValue`, and `CMCCirculatingSupply`
- Validate ordering and duplicate timestamp handling per series key `(symbol, contract_type, period)`.

### 3.4 Storage

- Add table `open_interest`:
  - `symbol` TEXT
  - `contract_type` TEXT
  - `period` TEXT
  - `sample_time` TIMESTAMPTZ
  - `sum_open_interest` NUMERIC(36,18)
  - `sum_open_interest_value` NUMERIC(36,18)
  - `cmc_circulating_supply` NUMERIC(36,18) NULL
  - `ingested_at` TIMESTAMPTZ DEFAULT now()
- Natural key: `(symbol, contract_type, period, sample_time)`.
- Read index: `(symbol, contract_type, period, sample_time DESC)`.
- Upsert semantics: `ON CONFLICT ... DO UPDATE`.

### 3.5 Cursor / watermark

- Add cursor helpers for open-interest series key:
  - `get_open_interest_cursor(...)`
  - `upsert_open_interest_cursor(...)`
  - `max_open_interest_sample_time(...)`
- Start-point recovery:
  - `max(cursor, max(sample_time))`
- Cursor rows keyed by `(symbol, contract_type, period)`.
- Cursor advances only after successful per-chunk commit.

---

## 4) Runtime modes

## 4.1 Initialize / backfill mode

### Ordered flow

1. Load settings, build perps provider, open DB connection.
2. Enumerate series by `(symbol, contract_type, period)`.
3. Resolve start:
   - default from `max(cursor, max(sample_time))`
   - cold start from `now - OPEN_INTEREST_INITIAL_BACKFILL_DAYS`
4. Fetch chunked pages from `openInterestHist`.
5. Parse + validate into `OpenInterestPoint`.
6. Upsert rows + update cursor in same transaction scope.
7. Commit per chunk (checkpoint).
8. Continue until end range reached or page empty.
9. Emit per-series run summary (rows/chunks/retries/give-ups).

### Backfill options

- `--no-watermark`
- `--skip-existing`
- `--once`

## 4.2 Real-time scheduled mode

1. Scheduler starts with immediate first run.
2. `ingest_open_interest` runs on short cadence.
3. `correct_window_open_interest` runs on medium cadence.
4. `repair_gap_open_interest` runs on optional long cadence.
5. Job failures are isolated; loop continues.
6. Subsequent runs are UTC-aligned.

---

## 5) Retry and failure policy

- Retry transient transport/server errors with exponential backoff.
- Do not retry clear client errors (invalid params/symbol contract).
- Log parse failures and provider give-ups with `(symbol, contract_type, period)` and time range.
- Commit per chunk so partial progress persists across crashes.
- Respect source retention limits (if request asks older than available window, clamp + log once per series).

---

## 6) Data quality controls

- Ordering/duplicate checks by `(symbol, contract_type, period, sample_time)`.
- Span checks for requested windows (head slack/tail shortfall diagnostics).
- Drift checks in correction window for OI fields.
- Gap detection over policy window and targeted repair.
- Freshness monitoring of latest `sample_time` per series.

---

## 7) Observability and operations

- Structured logs per job:
  - series processed (`symbol`, `contract_type`, `period`)
  - rows fetched/upserted
  - retries/give-ups
  - corrected rows
  - repaired gap spans
- Health:
  - scheduler liveness
  - cursor advancement
  - staleness alerts per `(symbol, contract_type, period)`
- Ops modes:
  - one-shot ingest
  - one-shot ingest + repair
  - standalone backfill script

---

## 8) Testing blueprint

### Unit tests

- `OpenInterestPoint` parser/model validity and malformed payload handling.
- Provider request params and retry behavior.
- Storage upsert idempotency and cursor semantics.
- Gap/span helper math.

### Job-level tests

- Cold start backfill.
- Incremental resume from cursor.
- Up-to-date no-op behavior.
- Chunk checkpoint recovery after partial failure.
- Correct-window drift upsert behavior.
- Gap-repair targeted refill behavior.

### Integration tests

- One-shot ingest writes expected open-interest rows.
- Scheduler triggers open-interest jobs at configured cadence.
- Restart resumes from persisted watermark.

---

## 9) Delivery checklist

- [x] Add migration for `open_interest` table and indexes.
- [x] Add open-interest config constants in `market_data/config.py`.
- [x] Add/confirm perps base URL setting and docs.
- [x] Add `OpenInterestPoint` model + parser + tests.
- [x] Add/extend perps provider with `fetch_open_interest_hist`.
- [x] Add open-interest upsert + cursor helpers + tests.
- [x] Implement `ingest_open_interest`.
- [x] Implement `correct_window_open_interest`.
- [x] Implement `repair_gap_open_interest`.
- [x] Wire open-interest jobs into `market_data/main.py`.
- [x] Add one-shot/backfill CLI for open interest.
- [ ] Update README and dataset docs.
- [ ] Add unit/job/integration coverage.

---

## 10) Implementation list (execution order)

### Phase A - Foundations

1. Add Alembic migration for `open_interest`.
2. Add open-interest constants and settings wiring in `market_data/config.py`.
3. Ensure perps endpoint setting exists and is documented.

### Phase B - Provider and parsing

4. Add `OpenInterestPoint` model in `market_data/schemas.py`.
5. Add payload parser/validation in `market_data/validation.py`.
6. Extend perps provider protocol and implement Binance adapter method for open-interest-statistics.

### Phase C - Storage and cursor

7. Add open-interest upsert storage function.
8. Add open-interest cursor helpers and max-time helper.
9. Add query helpers used by correction/repair jobs.

### Phase D - Jobs and scheduler

10. Implement `ingest_open_interest`.
11. Implement `correct_window_open_interest`.
12. Implement `repair_gap_open_interest`.
13. Register open-interest jobs in `market_data/main.py`.
14. Add open-interest backfill script.

### Phase E - Verification and rollout

15. Add unit tests for parser/provider/storage.
16. Add job-level tests and scheduler smoke tests.
17. Run local one-shot smoke and verify cursor progression.
18. Finalize docs and operational runbook.

---

## 11) Explicit non-goals for this tranche

- Do not derive open interest from other datasets in this phase.
- Do not add websocket-based OI stream in first cut.
- Do not couple OI ingestion with strategy computations in the ingest service.

Follow-on work can add computed indicators and joins to basis/funding in dedicated analytics tables.
