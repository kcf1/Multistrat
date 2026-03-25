# Taker Buy/Sell Volume Implementation Plan (Binance USD-M `takerlongshortRatio`, Historical)

This plan follows `docs/market_data/STANDARD_DATA_PIPELINE.md` and mirrors the OHLCV/basis/open-interest runtime shape:
`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

Scope: Binance USD-M futures "Taker Buy/Sell Volume" time-series from the public REST endpoint (historical + incremental). This dataset is based on the Binance `takerlongshortRatio` response, not on derived local analytics.

Reference: Binance Open Platform docs for `Taker Buy/Sell Volume` (endpoint + fields). See:
https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume

---

## 1) Design goals

- Ingest official Binance taker buy/sell volume settlements as the source of truth for the chosen `period`.
- Ship provider snapshots for configured `period` values (5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d).
- Keep writes idempotent on a stable natural key (symbol + period + timestamp).
- Make ingest restart-safe with durable cursor checkpoints.
- Reuse existing market_data scheduler/observability patterns.

---

## 2) Binance source contract

Planned source endpoint:

- `GET /futures/data/takerlongshortRatio`

Expected request fields (Binance docs):

- `symbol` (STRING, mandatory)
- `period` (ENUM, mandatory): `"5m","15m","30m","1h","2h","4h","6h","12h","1d"`
- optional `startTime` / `endTime` (ms, inclusive)
- optional `limit` (LONG, default 30, max 500)

Key constraints to encode in plan (from Binance docs):

- If `startTime` and `endTime` are not sent, the most recent data is returned.
- Only the data of the latest 30 days is available.
- Binance IP rate limit is documented as `1000 requests/5min`.

Link for the full contract and example payload:
https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume

Expected response fields (dataset payload, per row):

- `buySellRatio` (string/decimal numeric)
- `buyVol` (string/decimal numeric)
- `sellVol` (string/decimal numeric)
- `timestamp` (ms epoch as string/int)

---

## 3) Standard components (taker-buy-sell-volume-specific)

### 3.1 Config split

- **Macro/env**
  - `MARKET_DATA_DATABASE_URL` (or `DATABASE_URL` fallback)
  - `MARKET_DATA_BINANCE_PERPS_BASE_URL` (USD-M REST base)

- **Micro/code constants** in `market_data/config.py`
  - `TAKER_BUYSELL_VOLUME_SYMBOLS` (default: `DATA_COLLECTION_SYMBOLS`-style universe)
  - `TAKER_BUYSELL_VOLUME_PERIODS` (initially choose a subset; e.g. `("1h",)` or mirror full enum list)
  - `TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS` (**default near 27** to stay inside "latest 30 days" availability window)
  - `TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT` (<= 500)
  - `TAKER_BUYSELL_VOLUME_CORRECT_WINDOW_POINTS` (re-fetch last N points for drift checks)
  - `TAKER_BUYSELL_VOLUME_SCHEDULER_INGEST_INTERVAL_SECONDS`
  - `TAKER_BUYSELL_VOLUME_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`
  - `TAKER_BUYSELL_VOLUME_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS`
  - retry/backoff constants (align with basis/OI conventions)

---

### 3.2 Provider

- Extend perps provider protocol with:
  - `fetch_taker_buy_sell_volume(symbol, period, start_time_ms, end_time_ms, limit) -> list[TakerBuySellVolumePoint]`

- Implement Binance adapter in `market_data/providers/binance_perps.py`:
  - Use the existing `ProviderRateLimiter` shared by this provider instance.
  - Apply the same "HTTP 400 = non-retryable" behavior as basis/OI.
  - Handle pagination by advancing `startTime`/`endTime` using the last received `timestamp` (cursor progression).

---

### 3.3 Validation and parse

- Add `TakerBuySellVolumePoint` model in `market_data/schemas.py`.
- Parse payload to typed model:
  - normalize `symbol` to uppercase
  - parse `timestamp` (ms) into UTC-aware `sample_time` (`TIMESTAMPTZ` in storage)
  - decimals for:
    - `buy_sell_ratio`
    - `buy_vol`
    - `sell_vol`

- Validate ordering and duplicates:
  - enforce monotonically non-decreasing `sample_time` per `(symbol, period)`
  - handle duplicate timestamps deterministically in upsert layer

---

### 3.4 Storage

- Add table `taker_buy_sell_volume` (name can follow your repo naming conventions):
  - `symbol` TEXT
  - `period` TEXT
  - `sample_time` TIMESTAMPTZ
  - `buy_sell_ratio` NUMERIC(18,10)
  - `buy_vol` NUMERIC(30,14)
  - `sell_vol` NUMERIC(30,14)
  - `ingested_at` TIMESTAMPTZ DEFAULT now()

- Natural key: `(symbol, period, sample_time)`.
- Read index: `(symbol, period, sample_time DESC)`.
- Upsert semantics: `INSERT ... ON CONFLICT ... DO UPDATE`.

Notes:
- Omit non-essential vendor metadata; keep v1 schema minimal and OHLCV-like.

---

### 3.5 Cursor / watermark

- Add cursor helpers for taker-buy-sell-volume series key:
  - `get_taker_buy_sell_volume_cursor(symbol, period)`
  - `upsert_taker_buy_sell_volume_cursor(symbol, period, last_sample_time)`
  - `max_taker_buy_sell_volume_sample_time(...)` helper if needed

- Start-point recovery:
  - `start = max(cursor.last_sample_time, max(sample_time_in_table))`

- Cursor advances only after successful per-chunk commit (same semantics as basis/OI ingests).

---

## 4) Runtime modes

### 4.1 Initialize / backfill mode

Ordered flow:

1. Load settings, build perps provider, open DB connection.
2. Enumerate series by `(symbol, period)`.
3. Resolve start:
   - default from `max(cursor, max(stored_time))`
   - cold start from `now - TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS`
4. Fetch forward in chunked pages from `/futures/data/takerlongshortRatio`.
5. Parse + validate each page into `TakerBuySellVolumePoint`.
6. Upsert rows + update cursor in the same transaction scope.
7. Commit per chunk (checkpoint).
8. Continue until end range reached or the venue returns fewer than expected rows / empty page.
9. Emit per-series run summary (rows/chunks/retries/give-ups).

Backfill options:
- `--no-watermark`
- `--skip-existing`
- `--once`

Retention handling:
- Since Binance documents "only the latest 30 days is available", clamp any request start older than the practical source window and log once per series when clamping occurs.

---

### 4.2 Real-time scheduled mode

1. Scheduler starts with immediate first run.
2. `ingest_taker_buy_sell_volume` runs on short cadence (aligned to configured period).
3. `correct_window_taker_buy_sell_volume` runs on medium cadence for drift checks.
4. `repair_gap_taker_buy_sell_volume` runs on optional long cadence.
5. Each job failure is isolated; scheduler continues.
6. Subsequent runs remain UTC-aligned.

---

## 5) Retry and failure policy

- Retry transient HTTP/network/server errors with exponential backoff.
- Do not retry clear client errors (HTTP 400 invalid params/symbol/period).
- Log parse failures and provider give-ups with `(symbol, period)` and time range.
- Commit per chunk so partial progress persists across crashes.
- If the requested window exceeds venue availability, clamp and log once per series (consistent with open interest / basis plans).

---

## 6) Data quality controls

- Ordering/duplicate checks by `(symbol, period, sample_time)`.
- Span checks:
  - verify coverage requested time window against persisted data bounds
  - detect head slack / tail shortfall to separate "missing due to retention" vs "missing due to ingest errors"
- Drift checks in correction window:
  - compare latest N vendor values (buy/sell ratio and volumes) and quantify changes
- Gap detection:
  - funding-style irregularity does not apply here; still, time buckets come from Binance `period`, so gap logic should be time-grid based on the selected `period` length.

---

## 7) Observability and operations

- Structured logs per job:
  - series processed (`symbol`, `period`)
  - rows fetched/upserted
  - retries/give-ups
  - corrected rows / drift counts
  - repaired gap spans
- Health:
  - scheduler liveness
  - cursor advancement
  - staleness alerts per `(symbol, period)`
- Ops modes:
  - one-shot ingest
  - one-shot ingest + repair
  - standalone backfill script

---

## 8) Testing blueprint

### Unit tests

- `TakerBuySellVolumePoint` parser/model validity and malformed payload handling.
- Provider request params:
  - symbol + period required
  - `limit` clamping to <= 500
  - pagination behavior (start/end advancement)
  - shared limiter usage
- Provider retry behavior:
  - HTTP 400 returns `[]` (or give-up) without retry
  - transient failures retry up to max attempts
- Storage upsert idempotency and cursor semantics.
- Gap/window helper math for the selected `period` (time-grid alignment).

### Job-level tests

- Cold start backfill within the limited "latest 30 days" range.
- Incremental resume from cursor.
- Up-to-date no-op behavior.
- Partial failure with chunk checkpoint recovery.
- Correct-window drift upsert behavior.
- Repair-gap targeted refill behavior (within retention constraints).

### Integration tests

- One-shot ingest writes expected taker volume rows.
- Scheduler triggers taker volume jobs at configured cadence/order.
- Restart resumes from persisted watermark.

---

## 9) Delivery checklist

- [x] Add Alembic migration for `taker_buy_sell_volume` table + cursor table.
- [x] Add taker-buy-sell-volume config constants in `market_data/config.py`.
- [x] Confirm perps REST base URL setting exists and is documented (`MARKET_DATA_BINANCE_PERPS_BASE_URL`).
- [x] Add `TakerBuySellVolumePoint` model + parser + tests.
- [x] Add perps provider protocol + Binance adapter method for `/futures/data/takerlongshortRatio`.
- [x] Add taker-buy-sell-volume upsert storage + cursor helpers + tests.
- [ ] Implement `ingest_taker_buy_sell_volume`.
- [ ] Implement `correct_window_taker_buy_sell_volume`.
- [ ] Implement `repair_gap_taker_buy_sell_volume` (or explicitly defer with scheduler repair disabled).
- [ ] Wire taker jobs into `market_data/main.py`.
- [ ] Add one-shot/backfill CLI/script for this dataset.
- [ ] Update README and dataset docs.
- [ ] Add unit/job/integration coverage under `market_data/tests/`.

---

## 10) Implementation list (execution order)

### Phase A - Foundations

1. Add Alembic migration for `taker_buy_sell_volume` (+ cursor storage).
2. Add constants and settings wiring in `market_data/config.py`.
3. Ensure perps endpoint base URL is present in settings/docs.

### Phase B - Provider and parsing

4. Add `TakerBuySellVolumePoint` in `market_data/schemas.py`.
5. Add payload parser/validation in `market_data/validation.py`.
6. Extend perps provider protocol.
7. Implement Binance adapter method in `market_data/providers/binance_perps.py`:
   - `/futures/data/takerlongshortRatio`
   - pagination + limiter + retry semantics.

### Phase C - Storage and cursor

8. Add taker-buy-sell-volume upsert storage function.
9. Add cursor helpers and max-time helper.
10. Add query helpers used by correction/repair jobs.

### Phase D - Jobs and scheduler

11. Implement `ingest_taker_buy_sell_volume`.
12. Implement `correct_window_taker_buy_sell_volume`.
13. Implement `repair_gap_taker_buy_sell_volume`.
14. Register jobs in `market_data/main.py`.
15. Add backfill script/CLI.

### Phase E - Verification and rollout

16. Add unit tests for parser/provider/storage.
17. Add job-level tests and scheduler smoke tests.
18. Run local one-shot smoke; verify cursor progression and that clamping works inside the "latest 30 days" availability.
19. Finalize docs/runbook.

---

## 11) Explicit non-goals for this tranche

- Do not derive taker buy/sell volume from trades or other datasets in this phase.
- Do not add websocket streams in first cut.
- Do not join with strategy computations in ingest jobs.

Follow-on work can add analytics tables (e.g., correlation to basis/funding/OI) in dedicated modules.

