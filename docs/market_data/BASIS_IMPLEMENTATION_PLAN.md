# Basis First Implementation Plan (Binance-Provided, Not Locally Computed)

This plan follows `docs/market_data/STANDARD_DATA_PIPELINE.md` and mirrors the OHLCV/funding runtime shape:
`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

Scope here is **Binance-provided basis data** from futures market data endpoints, not basis derived from mark/index klines in local code.

---

## 1) Design goals

- Ingest official Binance basis series as the source of truth.
- Ship hourly snapshots using Binance `period=1h` directly.
- Keep writes idempotent on a stable natural key.
- Make ingest restart-safe with durable cursor checkpoints.
- Reuse existing market_data scheduler/observability patterns.

---

## 2) Binance source contract (official basis)

Planned source endpoint:

- `GET /futures/data/basis`

Expected request fields:

- `pair` (for example `BTCUSDT`)
- `contractType` (`PERPETUAL`, `CURRENT_QUARTER`, `NEXT_QUARTER`)
- `period` (`1h` for this tranche)
- optional pagination controls (`startTime`, `endTime`, `limit`)

Expected response fields (dataset payload):

- `pair`
- `contractType`
- `timestamp`
- `basis`
- `basisRate`
- `annualizedBasisRate`
- `futuresPrice`
- `indexPrice`

Key constraints to encode in plan:

- This endpoint is historical-window constrained (Binance docs indicate limited retention).
- Pull cadence and backfill windows should respect source availability limits.

---

## 3) Standard components (basis-specific)

### 3.1 Config split

- **Macro/env**
  - `MARKET_DATA_DATABASE_URL` (or `DATABASE_URL` fallback)
  - `MARKET_DATA_BINANCE_PERPS_BASE_URL` (new or reused perps base URL for basis/funding family)
- **Micro/code constants** in `market_data/config.py`
  - `BASIS_PAIRS`
  - `BASIS_CONTRACT_TYPES`
  - `BASIS_PERIODS` (initially `("1h",)`)
  - `BASIS_INITIAL_BACKFILL_DAYS`
  - `BASIS_FETCH_CHUNK_LIMIT`
  - `BASIS_CORRECT_WINDOW_POINTS`
  - `BASIS_SCHEDULER_INGEST_INTERVAL_SECONDS`
  - `BASIS_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`
  - `BASIS_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS`
  - basis retry/backoff constants

### 3.2 Provider

- Add perps market-data provider contract method:
  - `fetch_basis(...) -> list[BasisPoint]`
- Implement Binance adapter in `market_data/providers/binance_perps.py`.
- Use one shared `ProviderRateLimiter` per provider instance across basis/funding requests.
- Preserve no-retry semantics for clear client-side errors.

### 3.3 Validation and parse

- Add `BasisPoint` model in `market_data/schemas.py` with canonical normalized fields.
- Parse Binance payload to typed model:
  - uppercase `pair`
  - strict `contract_type`
  - UTC-aware `timestamp` mapped to `sample_time`
  - decimals for rates/prices
- Validate ordering and duplicate timestamp handling per series key
  `(pair, contract_type, period)`.

### 3.4 Storage

- Add table `basis_rate` with Binance-returned fields plus OHLCV-style ingest metadata:
  - `pair` TEXT
  - `contract_type` TEXT
  - `period` TEXT
  - `sample_time` TIMESTAMPTZ
  - `basis` NUMERIC(28,12)
  - `basis_rate` NUMERIC(18,10)
  - `annualized_basis_rate` NUMERIC(18,10)
  - `futures_price` NUMERIC(20,10)
  - `index_price` NUMERIC(20,10)
  - `ingested_at` TIMESTAMPTZ DEFAULT now()
- Natural key: `(pair, contract_type, period, sample_time)`.
- Read index: `(pair, contract_type, period, sample_time DESC)`.
- Upsert semantics: `ON CONFLICT ... DO UPDATE`.
- Drop non-essential metadata fields (for example `exchange`, `source_ts`) in v1.

### 3.5 Cursor / watermark

- Add cursor helpers for basis series key:
  - `get_basis_cursor(...)`
  - `upsert_basis_cursor(...)`
  - `max_basis_sample_time(...)`
- Start-point recovery:
  - `max(cursor, max(sample_time))`
- Cursor rows are keyed by `(pair, contract_type, period)`.
- Cursor advances only after successful per-chunk commit.

---

## 4) Runtime modes

## 4.1 Initialize / backfill mode

### Ordered flow

1. Load settings, build perps provider, open DB connection.
2. Enumerate series by `(pair, contract_type, period)`.
3. Resolve start:
   - default from `max(cursor, max(sample_time))`
   - cold start from `now - BASIS_INITIAL_BACKFILL_DAYS`
4. Fetch chunked basis pages from Binance official endpoint.
5. Parse + validate into `BasisPoint`.
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
2. `ingest_basis_rate` runs on short cadence.
3. `correct_window_basis_rate` runs on medium cadence.
4. `repair_gap_basis_rate` runs on optional long cadence.
5. Job failures are isolated; loop continues.
6. Subsequent runs are UTC-aligned.

---

## 5) Retry and failure policy

- Retry transient transport/server errors with exponential backoff.
- Do not retry clear client errors (invalid params/symbol contract).
- Log parse failures and provider give-ups with `(pair, contract_type, period)` and time range.
- Commit per chunk so partial progress persists across crashes.
- Respect endpoint retention constraints: if requested start is older than source retention, clamp and log once per series.

---

## 6) Data quality controls

- Ordering/duplicate checks by `(pair, contract_type, period, sample_time)`.
- Span checks for requested windows (head slack/tail shortfall diagnostics).
- Drift checks in correction window for basis fields.
- Gap detection over policy window and targeted repair.
- Freshness monitoring of latest `sample_time` per series.

---

## 7) Observability and operations

- Structured logs per job:
  - series processed (`pair`, `contract_type`, `period`)
  - rows fetched/upserted
  - retries/give-ups
  - corrected rows
  - repaired gap spans
- Health:
  - scheduler liveness
  - cursor advancement
  - staleness alerts per `(pair, contract_type, period)`
- Ops modes:
  - one-shot ingest
  - one-shot ingest + repair
  - standalone backfill script

---

## 8) Testing blueprint

### Unit tests

- `BasisPoint` parser/model validity and malformed payload handling.
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

- One-shot ingest writes expected basis rows.
- Scheduler triggers basis jobs at configured cadence.
- Restart resumes from persisted watermark.

---

## 9) Delivery checklist

- [ ] Add migration for `basis_rate` table and indexes.
- [ ] Add basis config constants in `market_data/config.py`.
- [ ] Add/confirm perps base URL setting and docs.
- [ ] Add `BasisPoint` model + parser + tests.
- [ ] Add/extend perps provider with `fetch_basis`.
- [ ] Add basis upsert + cursor helpers + tests.
- [ ] Implement `ingest_basis_rate`.
- [ ] Implement `correct_window_basis_rate`.
- [ ] Implement `repair_gap_basis_rate` (or explicitly defer).
- [ ] Wire basis jobs into `market_data/main.py`.
- [ ] Add one-shot/backfill CLI for basis.
- [ ] Update README and dataset docs.
- [ ] Add unit/job/integration coverage.

---

## 10) Implementation list (execution order)

### Phase A - Foundations

1. Add Alembic migration for `basis_rate`.
2. Add basis constants and settings wiring in `market_data/config.py`.
3. Ensure perps endpoint setting exists and is documented.

### Phase B - Provider and parsing

4. Add `BasisPoint` model in `market_data/schemas.py`.
5. Add basis payload parser/validation in `market_data/validation.py`.
6. Extend perps provider protocol and implement Binance adapter method for basis.

### Phase C - Storage and cursor

7. Add basis upsert storage function.
8. Add basis cursor helpers and max-time helpers.
9. Add query helpers used by correction/repair jobs.

### Phase D - Jobs and scheduler

10. Implement `ingest_basis_rate`.
11. Implement `correct_window_basis_rate`.
12. Implement `repair_gap_basis_rate`.
13. Register basis jobs in `market_data/main.py`.
14. Add basis backfill script.

### Phase E - Verification and rollout

15. Add unit tests for parser/provider/storage.
16. Add job-level tests and scheduler smoke tests.
17. Run local one-shot smoke and verify cursor progression.
18. Finalize docs and operational runbook.

---

## 11) Explicit non-goals for this tranche

- Do not compute basis locally from mark/index klines.
- Do not replace official funding-rate ingestion with derived basis math.
- Do not add websocket-based basis feed in first cut.

Follow-on work can add computed analytics tables, but this plan keeps the primary dataset strictly vendor-provided.
