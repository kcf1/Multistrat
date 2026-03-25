# Top Trader Long/Short Implementation Plan (Binance-Provided, Historical)

This plan follows `docs/market_data/STANDARD_DATA_PIPELINE.md` and mirrors the OHLCV/basis/open-interest/taker runtime shape:
`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

Scope here is **Binance-provided top trader long/short ratio positions data** from the official REST endpoint, not locally computed analytics.

Reference (Binance docs): [Top Trader Long/Short Position Ratio](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Top-Trader-Long-Short-Ratio).

---
## 1) Design goals

- Ingest official Binance “top trader long/short ratio (positions)” as the source of truth.
- Ship snapshots for configured futures `period` values (time-buckets chosen by our constants).
- Keep writes idempotent on a stable natural key.
- Make ingest restart-safe with durable cursor checkpoints.
- Reuse existing market_data scheduler/observability patterns.

---
## 2) Binance source contract (official top trader long/short ratio positions)

Planned source endpoint:

- `GET /futures/data/topLongShortPositionRatio`

Expected request fields:

- `symbol` (STRING, mandatory)
- `period` (ENUM, mandatory): `"5m","15m","30m","1h","2h","4h","6h","12h","1d"`
- `limit` (LONG, optional): default `30`, max `500`
- optional pagination controls:
  - `startTime` (LONG, ms epoch)
  - `endTime` (LONG, ms epoch)

Key constraints to encode in plan (from Binance docs):

- If `startTime` and `endTime` are not sent, the most recent data is returned.
- Only the data of the latest 30 days is available.
- Request weight is `0`.
- Binance IP rate limit is documented as `1000 requests/5min`.

Expected response fields (dataset payload, per row):

- `longShortRatio` (string/decimal numeric): long/short position ratio of top traders
- `longAccount` (string/decimal numeric): long positions ratio of top traders
- `shortAccount` (string/decimal numeric): short positions ratio of top traders
- `timestamp` (ms epoch as string/int): sample time

---
## 3) Standard components (top-trader-long-short-specific)

### 3.1 Config split

- **Macro/env**
  - `MARKET_DATA_DATABASE_URL` (or `DATABASE_URL` fallback)
  - `MARKET_DATA_BINANCE_PERPS_BASE_URL` (USD-M futures REST base)

- **Micro/code constants** in `market_data/config.py`
  - `TOP_TRADER_LONG_SHORT_SYMBOLS`
  - `TOP_TRADER_LONG_SHORT_PERIODS` (initially choose a subset; e.g. `("1h",)`; can later expand to full Binance enum list)
  - `TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS`
  - `TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT` (<= `500`)
  - `TOP_TRADER_LONG_SHORT_CORRECT_WINDOW_POINTS`
  - `TOP_TRADER_LONG_SHORT_SCHEDULER_INGEST_INTERVAL_SECONDS`
  - `TOP_TRADER_LONG_SHORT_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`
  - `TOP_TRADER_LONG_SHORT_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS`
  - retry/backoff constants (align with basis/open-interest/taker conventions)

Retention/backfill horizon:
- Use `BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS` (**27**) as a buffer to stay within Binance’s “latest ~30 days available” contract.

---
### 3.2 Provider

- Extend the perps provider protocol with:
  - `fetch_top_trader_long_short_position_ratio(symbol, period, start_time_ms, end_time_ms, limit) -> list[TopTraderLongShortPoint]`

- Implement the Binance adapter method in `market_data/providers/binance_perps.py`:
  - Add endpoint path constant:
    - `TOP_TRADER_LONG_SHORT_POSITION_RATIO_PATH = "/futures/data/topLongShortPositionRatio"`
  - Validate `limit` in `1..500` (Binance cap).
  - Build request params:
    - `symbol` (uppercase)
    - `period` (trimmed; exact Binance enum string)
    - `startTime` and `endTime` when provided
    - `limit`
  - Use the existing shared `ProviderRateLimiter` per provider instance.
  - HTTP `400` is treated as non-retryable and returns an empty page (consistent with existing basis/OI/taker behavior).
  - Retry transient HTTP/network failures with exponential backoff up to a configured max attempts.

Pagination strategy (important for chunk paging):
- This endpoint is also a “limited retention historical window” style endpoint (like `openInterestHist` and `takerlongshortRatio`).
- When both `startTime` and `endTime` are sent, we must assume the response is effectively capped and can return the “latest N rows within the window”.
- Therefore implement **backward paging** in `market_data/jobs/common.py` using the same approach as:
  - `iter_open_interest_batches_forward` (backward endTime paging, then yield oldest-first), and
  - `iter_taker_buy_sell_volume_batches_forward`.

Implementation checkpoint:
- If an integration test shows the endpoint does not behave like `openInterestHist`/`takerlongshortRatio` (i.e., pagination direction differs), update only the paging helper while keeping storage/job semantics identical.

---
### 3.3 Validation and parse

- Add `TopTraderLongShortPoint` model in `market_data/schemas.py`:
  - `symbol: str`
  - `period: str`
  - `sample_time: datetime` (UTC-aware)
  - `long_short_ratio: Decimal` (from `longShortRatio`)
  - `long_account_ratio: Decimal` (from `longAccount`)
  - `short_account_ratio: Decimal` (from `shortAccount`)

- Parse payload to typed model:
  - normalize `symbol` to uppercase
  - strict/trim `period`
  - parse `timestamp` ms into UTC-aware `sample_time`
  - decimals for the three ratio fields

- Validate batch integrity:
  - ordering/non-decreasing `sample_time` per `(symbol, period)` batch
  - duplicates are handled deterministically at the upsert layer (but parsing should still reject malformed row objects)

- Add parser entrypoints in `market_data/validation.py`, following the existing naming style:
  - `process_binance_top_trader_long_short_position_ratio_payload(...)`

---
### 3.4 Storage

- Add table `top_trader_long_short` (name can follow repo conventions; this plan uses `top_trader_long_short`):
  - `symbol` TEXT
  - `period` TEXT
  - `sample_time` TIMESTAMPTZ
  - `long_short_position_ratio` NUMERIC(18,10)
  - `long_account_ratio` NUMERIC(18,10)
  - `short_account_ratio` NUMERIC(18,10)
  - `ingested_at` TIMESTAMPTZ DEFAULT now()

- Natural key: `(symbol, period, sample_time)`
- Read index: `(symbol, period, sample_time DESC)` (or ASC, depending on existing style; at minimum include sample_time)
- Upsert semantics: `INSERT ... ON CONFLICT ... DO UPDATE` updating ratio fields + `ingested_at = now()`
- Omit non-essential vendor metadata in v1 (consistent with basis/open-interest/taker).

---
### 3.5 Cursor / watermark

- Add cursor table `top_trader_long_short_cursor`:
  - `symbol` TEXT
  - `period` TEXT
  - `last_sample_time` TIMESTAMPTZ (timezone-aware; UTC recommended)
  - `updated_at` TIMESTAMPTZ

- Cursor rows keyed by `(symbol, period)`.
- Start-point recovery:
  - `start_ms = max(cursor.last_sample_time, max(sample_time in table))` (by timestamp)
  - then `start_ms = open_time_plus_interval_ms(ref, pd_ms)` to avoid re-fetching the last committed bucket (same semantics as taker/open-interest ingests).
- Cursor advances only after successful per-chunk commit.

---
## 4) Runtime modes

### 4.1 Initialize / backfill mode

Ordered flow:

1. Load settings, build perps provider, open DB connection.
2. Enumerate series by `(symbol, period)`.
3. Resolve start:
   - default from `max(cursor, max(stored_sample_time))`
   - cold start from `now - TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS`
4. Fetch chunked pages from `/futures/data/topLongShortPositionRatio`.
5. Parse + validate into `TopTraderLongShortPoint`.
6. Upsert rows + update cursor in the same transaction scope.
7. Commit per chunk (checkpoint).
8. Continue until end range reached or the venue returns an empty page.
9. Emit per-series run summary (rows/chunks/retries/give-ups).

Backfill options:

- `--no-watermark`
- `--skip-existing`
- `--once`

---
### 4.2 Real-time scheduled mode

1. Scheduler starts with immediate first run.
2. `ingest_top_trader_long_short` runs on short cadence.
3. `correct_window_top_trader_long_short` runs on medium cadence for drift checks.
4. `repair_gap_top_trader_long_short` runs on optional long cadence.
5. Each job failure is isolated; scheduler continues.
6. Subsequent runs remain UTC-aligned.

---
## 5) Retry and failure policy

- Retry transient transport/server errors with exponential backoff.
- Do not retry clear client errors (invalid params/symbol/period); HTTP `400` => return `[]`.
- Log parse failures and provider give-ups with `(symbol, period)` and time range.
- Commit per chunk so partial progress persists across crashes.
- Respect source availability constraints:
  - in practice, the `INITIAL_BACKFILL_DAYS = 27` buffer keeps requests inside the valid retention window
  - additionally, if a request still hits retention edge cases, clamp and log once per series (operator-visible).

---
## 6) Data quality controls

- Ordering/duplicate checks by `(symbol, period, sample_time)`.
- Span checks for requested windows:
  - detect head slack / tail shortfall to separate “missing due to retention” vs “missing due to ingest errors”.
- Drift checks in correction window:
  - compare latest N vendor values (ratios) and count drift rows
  - upsert corrected values (same as taker/open-interest correction jobs)
- Gap detection over the expected time grid:
  - snapshots are aligned to `period` buckets, so missing buckets are detectable via time-delta threshold logic.

---
## 7) Observability and operations

- Structured logs per job:
  - series processed (`symbol`, `period`)
  - rows fetched/upserted
  - retries/give-ups
  - drift rows
  - repaired gap spans
- Health:
  - scheduler liveness
  - cursor advancement
  - staleness alerts per `(symbol, period)` (optional, but consistent with other dataset families)
- Ops modes:
  - one-shot ingest
  - one-shot ingest + repair
  - standalone backfill script

---
## 8) Testing blueprint

### Unit tests

- `TopTraderLongShortPoint` parser/model validity and malformed payload handling.
- Provider request params and retry behavior:
  - symbol + period required
  - `limit` clamping/validation <= 500
  - pagination uses expected `startTime`/`endTime` params
  - HTTP 400 no-retry => empty page
- Storage upsert idempotency and cursor semantics.
- Backward paging helper correctness:
  - iterate yields oldest-first across multiple pages
  - mirrors taker paging behavior tested in `test_common.py`
- Gap detection and window math for time-grid based snapshots.

### Job-level tests

- Cold start backfill within the limited “latest 30 days” range.
- Incremental resume from cursor.
- Up-to-date no-op behavior.
- Partial failure with chunk checkpoint recovery.
- Correct-window drift upsert behavior.
- Repair-gap targeted refill behavior (within retention constraints).

### Integration tests

- One-shot ingest writes expected top trader long/short rows.
- Scheduler triggers top-trader jobs at configured cadence/order.
- Restart resumes from persisted watermark.

---
## 9) Delivery checklist

- [ ] Add Alembic migration for `top_trader_long_short` table + cursor table.
- [ ] Add top-trader-long-short config constants in `market_data/config.py`.
- [ ] Extend perps provider protocol + Binance adapter method for `/futures/data/topLongShortPositionRatio`.
- [ ] Add `TopTraderLongShortPoint` model + parser in `market_data/schemas.py` / `market_data/validation.py`.
- [ ] Add top-trader-long-short upsert storage + cursor helpers + tests in `market_data/storage.py`.
- [ ] Implement `ingest_top_trader_long_short`.
- [ ] Implement `correct_window_top_trader_long_short`.
- [ ] Implement `repair_gap_top_trader_long_short` (policy-window detect + refetch).
- [ ] Wire top-trader jobs into `market_data/main.py` scheduler loop + `--once` flow.
- [ ] Add one-shot/backfill CLI/script (e.g. `scripts/backfill_top_trader_long_short.py`).
- [ ] Update README and dataset docs where appropriate.
- [ ] Add unit/job/integration coverage under `market_data/tests/`.

---
## 10) Implementation list (execution order)

### Phase A - Foundations

1. Add Alembic migration for `top_trader_long_short` (+ cursor table).
2. Add constants and settings wiring in `market_data/config.py`.
3. Ensure perps endpoint base URL setting exists and is documented.

### Phase B - Provider and parsing

4. Add `TopTraderLongShortPoint` in `market_data/schemas.py`.
5. Add payload parser/validation in `market_data/validation.py`.
6. Extend perps provider protocol.
7. Implement Binance adapter method in `market_data/providers/binance_perps.py`:
   - `/futures/data/topLongShortPositionRatio`
   - request params + limiter + retry semantics + HTTP 400 no-retry

### Phase C - Storage and cursor

8. Add top-trader upsert storage function for ratio rows.
9. Add cursor helpers and max-time helper.
10. Add query helpers used by correction/repair jobs.

### Phase D - Jobs and scheduler

11. Add backward paging helper in `market_data/jobs/common.py`.
12. Implement `ingest_top_trader_long_short`.
13. Implement `correct_window_top_trader_long_short`.
14. Implement `repair_gap_top_trader_long_short`.
15. Register jobs in `market_data/main.py`.
16. Add backfill script/CLI.

### Phase E - Verification and rollout

17. Add unit tests for parser/provider/storage/paging.
18. Add job-level tests and scheduler smoke tests.
19. Run local one-shot smoke; verify cursor progression and that retention clamping works.
20. Finalize docs and operational runbook.

---
## 11) Explicit non-goals for this tranche

- Do not compute top trader ratios locally from trades/open interest/basis/etc.
- Do not add websocket-based top trader feed in first cut.
- Do not add joins/analytics across basis/funding/OI in this ingest service.
- Do not model contract types that are not part of this endpoint’s request contract.

