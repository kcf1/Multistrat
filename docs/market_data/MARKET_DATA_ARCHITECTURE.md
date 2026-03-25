# Market Data Architecture

Single reference for the **`market_data`** Python package: layers, runtime workflow, logical schemas, validation, Postgres usage, and configuration. For the reusable ingest pattern (config → provider → validation → storage → cursor → jobs → scheduler), see **STANDARD_DATA_PIPELINE.md**.

---

## 1. Purpose and boundaries

**market_data** pulls public Binance REST data (spot klines and USD-M futures analytics endpoints), validates it, and upserts into **Postgres**. It uses **service-isolated** settings: macro URLs and DB via env (see §8); symbol lists, intervals, scheduler cadence, chunk sizes, and default REST hosts live as **code constants** in `market_data/config.py` and `market_data/universe.py` (per `.cursor/rules/env-and-config.mdc`).

---

## 2. Layered architecture

| Layer | Role | Main modules |
|--------|------|----------------|
| Entry / scheduling | Long-running loop or one-shot | `market_data/main.py` |
| Config | Macro (DB, base URLs) vs micro (symbols, intervals, limits, cadence) | `market_data/config.py`, `market_data/universe.py` |
| Providers | HTTP → parsed rows | `market_data/providers/base.py` (Protocols), `binance_spot.py`, `binance_perps.py` |
| Schemas | Pydantic rows + vendor parsers | `market_data/schemas.py` |
| Batch validation | Shape, monotonicity, klines overlap/span rules | `market_data/validation.py` |
| Persistence | Upserts, cursors, window helpers | `market_data/storage.py` |
| Jobs | Ingest, drift correction, gap repair | `market_data/jobs/*.py` |
| Shared job utilities | Paging, time helpers | `market_data/jobs/common.py` |
| Rate limiting | Optional min interval per provider instance | `market_data/rate_limit.py` |
| Time grid | Binance interval strings → ms, floor alignment | `market_data/intervals.py` |

**Data flow:** scheduler → `load_settings()` → job → provider (HTTP + retry) → validation → storage upsert (+ cursor commit for incremental ingest).

---

## 3. Workflow

### 3.1 Long-running service

Command: `python -m market_data.main`

- **First run:** each scheduled step runs **immediately** on startup.
- **Later runs:** next deadlines are **UTC-aligned** to Unix-epoch multiples of the configured period (e.g. 300s → :00, :05, :10 UTC).
- **Order within each loop tick:**
  1. **All ingests** (in this order): OHLCV → basis rate → open interest → top trader long/short → taker buy/sell volume.
  2. **Per-dataset drift and gap repair** (same order): for OHLCV, then basis, then open interest, then top trader, then taker — run that dataset’s **`correct_window`** when due, then its **`repair_gap`** when due. Gap repair runs only if that dataset’s `*_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS` is **greater than zero** (defaults are **0 = disabled**).

Steps log aggregate counts (e.g. bars/rows upserted, drift rows, gaps repaired). Failures in one step are logged and do not stop the loop (per-step `try/except` in `main.py`).

### 3.2 One-shot mode

- `python -m market_data.main --once` — same ingest order as §3.1, then the same **per-dataset** sequence: `correct_window` and (only with `--with-repair`) `repair_gap`, in OHLCV → basis → open interest → top trader → taker order, then exit.
- `--with-repair` — after each dataset’s `correct_window`, runs one policy-window `repair_gap` pass for that family when supported.

### 3.3 Per-dataset job roles

| Job type | Typical behavior |
|----------|------------------|
| **ingest_*** | Catch up from **cursor** and/or `MAX(open_time)` or `MAX(sample_time)`, or cold-start horizon from config; **commits per chunk**; OHLCV updates `ingestion_cursor` to the maximum bar time successfully written in that chunk. |
| **correct_window_*** | Re-fetch the last **N** bars/points, compare to DB (OHLCV compares **O, H, L, C** only), upsert corrections; counts **drift** rows. |
| **repair_gap_*** | Within a **policy time window**, detect missing grid spans (threshold ~ `gap_multiple` × bar/sample period), refetch and upsert; **does not** advance the main ingest cursor (historical fill; see `market_data/jobs/repair_gap.py` for OHLCV). |

### 3.4 Provider and pagination notes

- Spot klines: forward paging from `startTime` toward `endTime` with chunk limit (Binance cap 1000).
- **Futures basis** (`/futures/data/basis`): **forward** paging from `startTime` toward `endTime`, same idea as klines (`iter_basis_batches_forward` in `market_data/jobs/common.py`).
- **Futures endpoints** `openInterestHist`, `takerlongshortRatio`, and `topLongShortPositionRatio`: when both `startTime` and `endTime` are set, the API returns the **latest** `limit` rows in the window (not the earliest from `startTime`). `common.py` uses **backward paging** with a stack, then yields batches **oldest-first** for stable cursors and ingest.

---

## 4. Universe and series dimensions

- **Source list:** `DATA_COLLECTION_BASE_ASSETS` in `market_data/universe.py` → `DATA_COLLECTION_SYMBOLS` as `{BASE}USDT` (spot-style symbol strings).
- **OHLCV:** `OHLCV_SYMBOLS` matches that tuple; intervals from `OHLCV_INTERVALS` (e.g. `1h` only today).
- **Futures analytics** (basis, open interest, taker volume, top trader ratios): same symbol list where applicable, plus Binance parameters such as **`contract_type`** (e.g. `PERPETUAL`) and **`period`** (e.g. `1h`), per `config.py`.

---

## 5. Logical dataset schema (application models)

Immutable Pydantic models and `parse_binance_*` helpers live in `market_data/schemas.py`. Persisted columns match `market_data/storage.py` upsert SQL (Postgres DDL may live elsewhere; **storage** is the authoritative column list for this package).

### 5.1 OHLCV — `OhlcvBar` / table `ohlcv`

**Natural key:** `(symbol, interval, open_time)`

**Fields:** `open`, `high`, `low`, `close`, `volume`; optional `quote_volume`, `trades`, `close_time`. Table also tracks `ingested_at` on upsert.

### 5.2 Basis — `BasisPoint` / table `basis_rate`

**Natural key:** `(pair, contract_type, period, sample_time)`

**Fields:** `basis`, `basis_rate`, `futures_price`, `index_price`

### 5.3 Open interest — `OpenInterestPoint` / table `open_interest`

**Natural key:** `(symbol, contract_type, period, sample_time)`

**Fields:** `sum_open_interest`, `sum_open_interest_value`; optional `cmc_circulating_supply`

### 5.4 Taker buy/sell volume — `TakerBuySellVolumePoint` / table `taker_buy_sell_volume`

**Natural key:** `(symbol, period, sample_time)`

**Fields:** `buy_sell_ratio`, `buy_vol`, `sell_vol`

### 5.5 Top trader long/short — `TopTraderLongShortPoint` / table `top_trader_long_short`

**Natural key:** `(symbol, period, sample_time)`

**Model fields:** `long_short_ratio`, `long_account_ratio`, `short_account_ratio` map to DB columns `long_short_position_ratio`, `long_account_ratio`, `short_account_ratio` in `storage.py`.

### 5.6 Cursors (high-water marks)

| Table | Key | Value |
|--------|-----|--------|
| `ingestion_cursor` | `(symbol, interval)` | `last_open_time` (inclusive high-water semantics per module docstring) |
| `basis_cursor` | `(pair, contract_type, period)` | `last_sample_time` |
| `open_interest_cursor` | `(symbol, contract_type, period)` | `last_sample_time` |
| `taker_buy_sell_volume_cursor` | `(symbol, period)` | `last_sample_time` |
| `top_trader_long_short_cursor` | `(symbol, period)` | `last_sample_time` |

Storage helpers **do not** `commit`; callers commit after batches.

---

## 6. Validation rules

### 6.1 Row-level (Pydantic, `schemas.py`)

Examples:

- **OHLCV:** `high >= low`, `high` bounds `open`/`close`, `low` bounds `open`/`close`, `volume >= 0`, non-negative optional `quote_volume` / `trades`, `close_time >= open_time` when set.
- **Basis:** `futures_price > 0`, `index_price > 0`.
- **Open interest:** non-negative sums; optional supply non-negative when set.
- **Taker:** non-negative volumes and ratio.
- **Top trader:** ratio fields non-negative.

Identifiers normalized (e.g. uppercase symbols/pairs, stripped periods).

### 6.2 Batch-level (`validation.py`)

- **Klines:** Response must be a JSON array; each row a list; per-row parse; **strictly increasing** `open_time`, **no duplicates**; **no interior overlap** — consecutive `open_time` step must not be shorter than about `interval_ms * OHLCV_KLINES_GRID_MIN_STEP_RATIO` (longer gaps allowed — venue omits candles). Large implied gaps **warn** (`OHLCV_KLINES_WARN_OPEN_TIME_GAP_BARS`). With `startTime`, `endTime`, and `request_limit`, optional span checks: **head slack** and **tail shortfall** log as **warnings**; other span failures may raise.
- **Basis / open interest / taker / top trader:** JSON array of objects; open interest, taker, and top-trader batches are **sorted by `sample_time`** after parse; require **strictly increasing** timestamps and **no duplicate** `sample_time` in the batch.

**Note:** `OHLCV_KLINES_GRID_MAX_STEP_RATIO` in `config.py` is currently **unused** by `validation.py` (only the min-step ratio drives overlap detection).

---

## 7. System parameters (micro constants)

Defined in `market_data/config.py` unless noted.

**Cross-cutting**

- `MARKET_DATA_MIN_REQUEST_INTERVAL_SEC` — optional minimum seconds between requests per provider instance (`None` = no limit).
- `DEFAULT_BINANCE_REST_URL` / `DEFAULT_BINANCE_PERPS_REST_URL` — used when env overrides are unset (perps must not fall back to spot host).

**OHLCV (representative)**

- `OHLCV_INTERVALS`, `OHLCV_SYMBOLS`, `OHLCV_INITIAL_BACKFILL_DAYS`, `OHLCV_KLINES_CHUNK_LIMIT`, fetch retry (`OHLCV_KLINES_FETCH_MAX_ATTEMPTS`, `OHLCV_KLINES_FETCH_RETRY_BASE_SLEEP_SEC`), grid/span/warning thresholds, `OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS`, `OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`, `OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS`, `OHLCV_CORRECT_WINDOW_BARS`, `OHLCV_SKIP_EXISTING_GAP_MULTIPLE` (used with gap detection when resume rules require it).

**Futures limited retention**

- `BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS` (27-day buffer for venue ~30d windows) and backward-compatible alias `BINANCE_FUTURES_30D_DATASET_BACKFILL_DAYS`. Used for basis, open interest, taker, top-trader cold starts.

**Per-dataset blocks** (basis, open interest, taker, top trader): symbols/pairs, periods, contract types where needed, `*_INITIAL_BACKFILL_DAYS`, `*_FETCH_CHUNK_LIMIT` (500), `*_CORRECT_WINDOW_POINTS` (48), `*_SCHEDULER_*`, `*_FETCH_MAX_ATTEMPTS`, `*_FETCH_RETRY_BASE_SLEEP_SEC`.

---

## 8. Macro environment (`MarketDataSettings`)

Loaded from `.env` via pydantic-settings (`market_data/config.py`):

| Env variable | Purpose |
|----------------|---------|
| `MARKET_DATA_DATABASE_URL` or `DATABASE_URL` | Postgres (**required**). If both are set, **`MARKET_DATA_DATABASE_URL` wins**. |
| `MARKET_DATA_BINANCE_BASE_URL` | Spot REST base (optional) |
| `MARKET_DATA_BINANCE_PERPS_BASE_URL` | USD-M futures public REST base (optional) |

Computed properties: `binance_rest_url`, `binance_perps_rest_url`; `symbols` / `intervals` expose OHLCV tuple constants.

Document **macro** vars in `.env.example`; keep tunables in `config.py` per project rules.

---

## 9. Tests and scripts

- **Tests:** `market_data/tests/` (schemas, validation, storage, jobs, providers, config).
- **Scripts:** repository `scripts/` for backfills and one-off repairs (e.g. OHLCV / taker); see comments in `config.py` and implementation plans under `docs/market_data/`.

---

## 10. Related docs

- **STANDARD_DATA_PIPELINE.md** — reusable pipeline blueprint for new datasets.
- **DATASET_INGESTION_STEPS.md** — step checklist for new feeds.
- Per-dataset plans: `BASIS_IMPLEMENTATION_PLAN.md`, `OPEN_INTEREST_IMPLEMENTATION_PLAN.md`, `TAKER_BUYSELL_VOLUME_IMPLEMENTATION_PLAN.md`, `TOP_TRADER_LONG_SHORT_IMPLEMENTATION_PLAN.md`, etc.
- Phase plans: **docs/PHASE4_DETAILED_PLAN.md** (market data phase references).
