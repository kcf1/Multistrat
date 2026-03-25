# Funding Rate Implementation Plan (Binance USD-M `fundingRate`, Historical)

This plan follows `docs/market_data/STANDARD_DATA_PIPELINE.md` and mirrors the OHLCV/basis/open-interest runtime shape:
`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

Scope here is Binance USD-M **funding rate history** from the public futures market-data endpoint, not locally computed funding or premium index math.

---

## 1) Design goals

- Ingest official Binance funding settlements as the source of truth (per symbol, per `fundingTime`).
- Support long backfills where the venue allows (this endpoint is **not** the same retention family as `/futures/data/*`; tune cold-start days in code constants, not the 27-day limited-retention helper unless Binance documents a hard cap).
- Keep writes idempotent on a stable natural key.
- Make ingest restart-safe with durable cursor checkpoints.
- Reuse existing market_data scheduler/observability patterns and the **same** `BinancePerpsMarketDataProvider` + shared `ProviderRateLimiter` as basis/open interest.

---

## 2) Binance source contract (funding rate history)

Planned source endpoint (USD-M futures, same REST host as basis/OI today — `MARKET_DATA_BINANCE_PERPS_BASE_URL`, default `https://fapi.binance.com`):

- `GET /fapi/v1/fundingRate`

Expected request fields:

- `symbol` (for example `BTCUSDT`) — **required for our jobs** (we enumerate symbols; omitting `symbol` returns a global recent window, which is not the series model we use)
- optional pagination / window: `startTime`, `endTime` (milliseconds, inclusive)
- `limit` (default 100, **max 1000** per request)

Expected response fields (dataset payload, per row):

- `symbol`
- `fundingTime`
- `fundingRate`
- `markPrice` (present in current API docs; treat as optional when parsing for forward compatibility)

Key constraints to encode in plan:

- Responses are **ascending** by `fundingTime`; chunk ingest should advance cursors forward in time.
- If only `startTime`/`endTime` span more than `limit` events, the venue returns from `startTime` up to `limit` rows — paginate by moving `startTime` past the last seen `fundingTime`.
- Shares IP rate-weight budget with other USD-M market endpoints (coordinate with the shared per-provider limiter).
- If Binance documents or enforces a maximum history depth, clamp cold-start / `--no-watermark` ranges similarly to open interest (log once per series).

---

## 3) Standard components (funding-specific)

### 3.1 Config split

- **Macro/env**
  - `MARKET_DATA_DATABASE_URL` (or `DATABASE_URL` fallback)
  - `MARKET_DATA_BINANCE_PERPS_BASE_URL`
- **Micro/code constants** in `market_data/config.py`
  - `FUNDING_SYMBOLS` (typically aligned with `DATA_COLLECTION_SYMBOLS` / perp universe)
  - `FUNDING_INITIAL_BACKFILL_DAYS` (cold start; **not** tied to `BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS` unless we discover a matching venue cap)
  - `FUNDING_FETCH_CHUNK_LIMIT` (≤ 1000)
  - `FUNDING_CORRECT_WINDOW_POINTS` (re-fetch the last *N* funding settlements per symbol for drift correction)
  - `FUNDING_SCHEDULER_INGEST_INTERVAL_SECONDS`
  - `FUNDING_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`
  - `FUNDING_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS`
  - funding-specific retry/backoff constants (or shared perps fetch knobs if we consolidate with basis/OI)

### 3.2 Provider

- Extend perps market-data provider contract:
  - `fetch_funding_rates(...) -> list[FundingRatePoint]`
- Implement in `market_data/providers/binance_perps.py`.
- Use one shared `ProviderRateLimiter` per provider instance across basis/funding/open-interest calls.
- Preserve no-retry behavior for obvious client errors (HTTP 400 / invalid symbol).

### 3.3 Validation and parse

- Add `FundingRatePoint` model in `market_data/schemas.py`.
- Parse payload to typed model:
  - uppercase `symbol`
  - UTC-aware `fundingTime` mapped to `funding_time`
  - decimals for `fundingRate` and optional `markPrice`
- Validate ordering and duplicate `fundingTime` handling per series key `(symbol)`.

### 3.4 Storage

- Add table `funding_rate` with Binance-returned fields plus ingest metadata:
  - `symbol` TEXT
  - `funding_time` TIMESTAMPTZ
  - `funding_rate` NUMERIC(18,10)
  - `mark_price` NUMERIC(20,10) NULL
  - `ingested_at` TIMESTAMPTZ DEFAULT now()
- Natural key: `(symbol, funding_time)` (single-venue v1; add `exchange` later if multi-venue ingestion is introduced).
- Read index: `(symbol, funding_time DESC)`.
- Upsert semantics: `ON CONFLICT ... DO UPDATE`.
- Omit redundant columns such as a separate “sample_ts” if it duplicates `funding_time`.

### 3.5 Cursor / watermark

- Add cursor helpers for funding series key:
  - `get_funding_cursor(...)`
  - `upsert_funding_cursor(...)`
  - `max_funding_time(...)` (or equivalent max stored settlement time)
- Start-point recovery:
  - `max(cursor, max(funding_time))`
- Cursor rows keyed by `symbol`.
- Cursor advances only after successful per-chunk commit.

---

## 4) Runtime modes

## 4.1 Initialize / backfill mode

### Ordered flow

1. Load settings, build perps provider, open DB connection.
2. Enumerate series by `symbol`.
3. Resolve start:
   - default from `max(cursor, max(funding_time))`
   - cold start from `now - FUNDING_INITIAL_BACKFILL_DAYS`
4. Fetch chunked funding pages from `/fapi/v1/fundingRate`.
5. Parse + validate into `FundingRatePoint`.
6. Upsert rows + update cursor in same transaction scope.
7. Commit per chunk (checkpoint).
8. Continue until end range reached or page empty / underfilled per pagination rules.
9. Emit per-series run summary (rows/chunks/retries/give-ups).

### Backfill options

- `--no-watermark`
- `--skip-existing`
- `--once`

## 4.2 Real-time scheduled mode

1. Scheduler starts with immediate first run.
2. `ingest_funding_rate` runs on short cadence.
3. `correct_window_funding_rate` runs on medium cadence.
4. `repair_gap_funding_rate` runs on optional long cadence.
5. Job failures are isolated; loop continues.
6. Subsequent runs are UTC-aligned.

---

## 5) Retry and failure policy

- Retry transient transport/server errors with exponential backoff.
- Do not retry clear client errors (invalid params/symbol).
- Log parse failures and provider give-ups with `symbol` and time range.
- Commit per chunk so partial progress persists across crashes.
- If requested history exceeds venue availability, clamp + log once per series (same pattern as other datasets).

---

## 6) Data quality controls

- Ordering/duplicate checks by `(symbol, funding_time)`.
- Span checks for requested windows (head slack / tail shortfall diagnostics where applicable).
- Drift checks in correction window for `funding_rate` / `mark_price`.
- Gap detection over a policy window and targeted repair (funding intervals are symbol-dependent; gap logic should use time thresholds, not assume fixed bar length like OHLCV).
- Freshness monitoring of latest `funding_time` per symbol.

---

## 7) Observability and operations

- Structured logs per job:
  - series processed (`symbol`)
  - rows fetched/upserted
  - retries/give-ups
  - corrected rows
  - repaired gap spans
- Health:
  - scheduler liveness
  - cursor advancement
  - staleness alerts per `symbol`
- Ops modes:
  - one-shot ingest
  - one-shot ingest + repair
  - standalone backfill script (e.g. `scripts/backfill_funding_rate.py`)

---

## 8) Testing blueprint

### Unit tests

- `FundingRatePoint` parser/model validity and malformed payload handling.
- Provider request params, pagination, limiter interaction, and retry behavior.
- Storage upsert idempotency and cursor semantics.
- Gap/window helper math for irregular funding cadence.

### Job-level tests

- Cold start backfill.
- Incremental resume from cursor.
- Up-to-date no-op behavior.
- Chunk checkpoint recovery after partial failure.
- Correct-window drift upsert behavior.
- Gap-repair targeted refill behavior.

### Integration tests

- One-shot ingest writes expected funding rows.
- Scheduler triggers funding jobs at configured cadence.
- Restart resumes from persisted watermark.

---

## 9) Delivery checklist

- [ ] Add migration for `funding_rate` table, cursor table (if separate), and indexes.
- [ ] Add funding config constants in `market_data/config.py`.
- [ ] Add/confirm perps base URL setting and docs (already shared with basis/OI).
- [ ] Add `FundingRatePoint` model + parser + tests.
- [ ] Add/extend perps provider with `fetch_funding_rates`.
- [ ] Add funding upsert + cursor helpers + tests.
- [ ] Implement `ingest_funding_rate`.
- [ ] Implement `correct_window_funding_rate`.
- [ ] Implement `repair_gap_funding_rate`.
- [ ] Wire funding jobs into `market_data/main.py`.
- [ ] Add one-shot/backfill CLI for funding.
- [ ] Update README and dataset docs.
- [ ] Add unit/job/integration coverage (automated tests under `market_data/tests/`; live DB E2E optional operator step — mirror §12 in open-interest plan).

---

## 10) Implementation list (execution order)

### Phase A - Foundations

1. Add Alembic migration for `funding_rate` (+ `funding_rate_cursor` if not using a generic cursor table).
2. Add funding constants and settings wiring in `market_data/config.py`.
3. Ensure perps endpoint setting exists and is documented (`MARKET_DATA_BINANCE_PERPS_BASE_URL`).

### Phase B - Provider and parsing

4. Add `FundingRatePoint` model in `market_data/schemas.py`.
5. Add funding payload parser/validation in `market_data/validation.py`.
6. Extend perps provider protocol and implement Binance adapter method for `/fapi/v1/fundingRate` (pagination + limiter).

### Phase C - Storage and cursor

7. Add funding upsert storage function.
8. Add funding cursor helpers and max-time helper.
9. Add query helpers used by correction/repair jobs.

### Phase D - Jobs and scheduler

10. Implement `ingest_funding_rate`.
11. Implement `correct_window_funding_rate`.
12. Implement `repair_gap_funding_rate`.
13. Register funding jobs in `market_data/main.py`.
14. Add funding backfill script.

### Phase E - Verification and rollout

15. Add unit tests for parser/provider/storage.
16. Add job-level tests and scheduler smoke tests.
17. Run local one-shot smoke and verify cursor progression.
18. Finalize docs and operational runbook (§12).

---

## 11) Explicit non-goals for this tranche

- Do not compute funding from mark/index prices in this phase (that belongs in analytics, not the canonical funding table).
- Do not conflate this dataset with Binance **premium index** or **mark price** klines unless stored as separate series.
- Do not add websocket-based funding stream in first cut.
- Do not couple funding ingestion with strategy computations in the ingest service.

Follow-on work can add joins to basis/open interest in dedicated analytics tables or views.

---

## 12) Operational runbook (smoke & verification)

### Preconditions

- Postgres reachable; migrations applied (`alembic upgrade head`) including `funding_rate` and funding cursor storage.
- Env: `MARKET_DATA_DATABASE_URL` or `DATABASE_URL`; optional `MARKET_DATA_BINANCE_PERPS_BASE_URL` for testnet/mainnet.

### Automated tests

```bash
python -m pytest market_data/tests/ -q
```

### One-shot service run (no daemon)

```bash
python -m market_data.main --once
```

With a policy-window gap repair pass (once funding jobs exist, include them in the same `--with-repair` behavior as other datasets).

### Large / policy backfill CLI

```bash
python scripts/backfill_funding_rate.py
python scripts/backfill_funding_rate.py --no-watermark
python scripts/backfill_funding_rate.py --no-watermark --skip-existing
```

### Verify cursor / watermark (examples)

Adjust table/column names to match the chosen migration.

```sql
SELECT symbol, last_sample_time
  FROM funding_rate_cursor
 ORDER BY symbol;

SELECT symbol, max(funding_time) AS newest
  FROM funding_rate
 GROUP BY symbol
 ORDER BY symbol;
```

After ingest, `funding_rate_cursor.last_sample_time` and `max(funding_time)` should advance toward the latest settlement per symbol (same cursor column name pattern as `basis_cursor` / `open_interest_cursor`).

### Long-running scheduler

```bash
python -m market_data.main
```

Tune cadence via `FUNDING_SCHEDULER_*` in `market_data/config.py`. Set `FUNDING_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS` to a positive value to enable scheduled gap repair (default `0` = off, consistent with basis/OI).

---

## Reference (code parallels)

- Scheduler: `market_data/main.py`
- Ingest pattern: `market_data/jobs/ingest_basis_rate.py` / `ingest_open_interest.py`
- Correct-window pattern: `market_data/jobs/correct_window_basis_rate.py` / `correct_window_open_interest.py`
- Repair pattern: `market_data/jobs/repair_gap_basis_rate.py` / `repair_gap_open_interest.py`
- Perps provider: `market_data/providers/binance_perps.py`
- Validation: `market_data/validation.py`, `market_data/schemas.py`
- Storage: `market_data/storage.py`
- Config: `market_data/config.py`
