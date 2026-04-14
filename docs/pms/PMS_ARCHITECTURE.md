# PMS Architecture: Data Flow, Interfaces & Design

Single reference for the **Portfolio Management System (PMS)**: data flow, Postgres interfaces, and alignment with OMS. PMS maintains a **granular `positions` table** in Postgres at **asset grain** (per broker, account, book, and asset). It **does not** read OMS Redis positions. **PnL, margin, and Redis publishing are not implemented** in the current process loop; valuation today is **USD numeraire** via `usd_price` on each row and a **generated** `usd_notional`. See **docs/pms/POSITION_VALUATION_SCHEMA_PLAN.md** and **docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md** for the valuation refactor; **docs/PHASE2_DETAILED_PLAN.md** §8 for historical Phase 2 context. **Postgres layout** (schemas, `search_path`, cross-schema reads): **docs/POSTGRES_SCHEMA_GROUPING_PLAN.md**.

---

## 1. High-Level Data Flow

```
┌──────────────┐
│     OMS      │  • Order sync → Postgres **oms.orders**
└──────┬───────┘  • Account sync → **oms.accounts**, **oms.balances** (when sync_balances=True)
       │          • **oms.balance_changes** (from balanceUpdate; deposits/withdrawals/transfers)
       │          • **oms.symbols** (from exchangeInfo at startup)
       │          • Produces oms_fills (Redis stream) — PMS does not consume it
       ▼
┌───────────────┐
│ Postgres      │     (PMS does not use a fills table)
│ oms.orders    │     ← executed fills → split into base/quote legs (needs symbols)
│ oms.balance_changes│ ← net deltas merged into asset positions (core path)
│ oms.symbols   │     ← required to map order symbol → (base_asset, quote_asset)
│ pms.assets    │     ← usd_price / usd_symbol for USD valuation (PMS reads)
│ oms.accounts  │     (optional; PMS loop does not require it today)
│ oms.balances  │     (OMS sync; query_balances() exists — not used in main tick)
└───────┬───────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                        PMS                                   │
│  - Read: oms.orders, oms.symbols, oms.balance_changes, pms.assets │
│  - Derive: open_qty per (broker, account_id, book, asset)    │
│  - Enrich: usd_price (stables from assets; optional feed)    │
│  - Write: pms.positions (open_qty, position_side, usd_price) │
│  - Skips: Redis, balances in loop, mark provider on tick    │
└─────────────────────────────────────────────────────────────┘
```

- **Upstream:** OMS syncs **`oms.orders`**, **`oms.accounts`**, optional **`oms.balances`**, **`oms.balance_changes`**, and **`oms.symbols`**. PMS does **not** read OMS Redis positions.
- **PMS:** Each tick **recomputes** positions from **`oms.orders`** (as two asset legs) **plus** **`oms.balance_changes`** nets, then enriches **USD** using **`pms.assets`** (and optional asset price feed). Writes **`pms.positions`** only; **`pgconn.configure_for_pms`** sets home schema for unqualified PMS-writes. **No** `pnl:*` / `margin:*` Redis keys and **no** realized/unrealized PnL or margin computation in code today.

---

## 2. Core behavior (current implementation)

| # | Feature | Description |
|---|--------|-------------|
| 1 | **Derive positions from orders + balance_changes** | Read orders with `LOWER(TRIM(status)) IN ('partially_filled','filled')` and `executed_qty > 0`. Use **`symbols`** to split each order into **base** and **quote** legs (BUY/SELL with `executed_qty`, `price`, and `binance_cumulative_quote_qty` when present). Add **net** amounts from **`balance_changes`** (`deposit`, `withdrawal`, `adjustment`, `transfer`) per `(broker, account_id, book, asset)`. **Grain:** `(broker, account_id, book, asset)`. **open_qty** signed; **position_side** `long` \| `short` \| `flat`. FIFO is used internally for leg cost when building legs from orders; **entry_avg is not persisted** on `positions`. |
| 2 | **USD valuation on the row** | After derivation, `enrich_positions_with_usd_from_assets` sets **`usd_price`** from **`assets.usd_price`** (e.g. stables = 1). Optional **asset price feed** (`pms/asset_price_feed.py`, source from **code constants** in `pms/config.py`) may refresh **`assets.usd_price`** before each tick when enabled. **`MarkPriceProvider` is constructed in `main` but not used inside `run_one_tick`** (no per-tick mark fetch for unrealized PnL). |
| 3 | **Granular store** | Table **`positions`**: one row per `(broker, account_id, book, asset)` with **open_qty**, **position_side**, **usd_price**, **updated_at**. PostgreSQL may define **`usd_notional`** as `GENERATED ALWAYS AS (open_qty * usd_price) STORED`. |
| 4 | **Timer loop** | `python -m pms.main` → `run_pms_loop()` (or `--once` for a single tick). Interval from **`PMS_TICK_INTERVAL_SECONDS`** (default **10**). **Redis is skipped.** |

**Legacy / tests only:** `derive_positions_from_orders()` in `pms/reads.py` aggregates net qty per **trading symbol** (no leg split). The **production path** is **`derive_positions_from_orders_and_balance_changes()`** only.

**Optional / not wired today:**

| Item | Notes |
|------|--------|
| **balances** in tick | `query_balances()` exists; main loop does **not** call it. Broker cash remains in OMS **`balances`**; PMS does not merge balances into `positions` in the current loop. |
| **Redis** `pnl:{account_id}`, `margin:{account_id}` | Not implemented. |
| **Snapshots** | `pnl_snapshots` / `margin_snapshots` not implemented. |
| **Repairs / rebuild interval** | `PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS` is defined in settings but **unused**; each tick already recomputes from DB state. |
| **`PMS_MARK_PRICE_SOURCE` = redis / market_data** | `get_mark_price_provider()` currently supports **`binance`** and empty/`none`/`fake`; other values **raise**. |

---

## 3. PMS role (as implemented)

- **Read** from Postgres: **orders**, **symbols**, **balance_changes**, **assets** (and optionally **balances** via helper, not used in loop).
- **Derive** asset-level **open_qty** and **position_side**; merge **balance_changes** into the same grain as order-derived exposure.
- **Enrich** **`usd_price`** from **assets** (stables-first); optional Binance-backed **asset** price feed updates **assets** before ticks.
- **Write** **`positions`** via UPSERT on **`(broker, account_id, book, asset)`**.
- **Does not:** consume OMS Redis positions, compute PnL/margin, or write Redis.

---

## 4. Data flows (OMS-aligned)

| Pattern | OMS | PMS |
|--------|-----|-----|
| **Event-driven** | Fill callback → sync order, `oms_fills` stream | Timer-driven only; **no** fill stream consumer. |
| **Periodic sync** | Terminal order sync | Periodic tick: re-read Postgres → derive → write `positions`. |
| **Triggered** | On terminal status | `python -m pms.main --once` for one-shot refresh. |
| **Repairs** | Binance payload → order columns | Full **recompute from orders + balance_changes** each tick; no separate repair module. |

**PMS loop (conceptual):** orders + balance_changes + symbols → derive asset positions → enrich **usd_price** from **assets** → write **positions**.

---

## 5. Redis

- **Reads:** None for PMS today (Postgres only for the service).
- **Writes:** None. **`REDIS_URL`** may appear in settings for future use but **is not used** by `pms.main` / `pms.loop`.

Planned Admin/UI keys (`pnl:{account_id}`, `margin:{account_id}`) remain **documentation-only** until implemented.

---

## 6. Granular store and `positions` table

- **Grain:** **`(broker, account_id, book, asset)`** — asset codes (e.g. `BTC`, `USDT`), not trading pair symbols.
- **Derivation:** Orders contribute **two** legs per fill (base and quote); **`symbols`** must list the pair so PMS can map `orders.symbol` → `(base_asset, quote_asset)`; orders with unknown symbols are **skipped** (logged).
- **Grouping** (by book, asset, broker, etc.) is done in SQL or application code over this table.
- **PMS writes** the table directly; SQL VIEWs on top are optional.

**Comparison to orders:**

| | Orders (OMS) | `positions` (PMS) |
|--|--------------|-------------------|
| **Grain** | One row per order | One row per `(broker, account_id, book, asset)` |
| **Content** | Order fields, fills as `executed_qty` / `price` | Aggregated **open_qty**, **position_side**, **usd_price** |
| **Purpose** | Audit / execution truth | Fast read of net exposure + USD snapshot |

### 6.1 `positions` write schema (current)

Implemented in **`pms/granular_store.py`** (`write_pms_positions`). **UPSERT** conflict target: **`(broker, account_id, book, asset)`** (unique index **`uq_positions_broker_account_book_asset`**).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **id** | BIGINT (PK, autoincrement) | NOT NULL | Surrogate key (historical migrations). |
| **broker** | TEXT | NOT NULL | Broker name (e.g. `binance`); part of grain. |
| **account_id** | TEXT | NOT NULL | Broker account id (matches `orders.account_id`). |
| **book** | TEXT | NOT NULL | Strategy/book (default `''`). |
| **asset** | TEXT | NOT NULL | Asset code (e.g. `BTC`, `USDT`). |
| **open_qty** | NUMERIC(36,18) | NOT NULL | Signed qty; positive = long, negative = short. |
| **position_side** | TEXT | NOT NULL | `long` \| `short` \| `flat`. |
| **usd_price** | NUMERIC(36,18) | NULL | Per-unit USD price used for valuation (from enrichment). |
| **usd_notional** | NUMERIC(36,18) | NULL | May be a **generated** column: `open_qty * usd_price` (see migration `u1v2w3x4y5z6`). |
| **updated_at** | TIMESTAMPTZ | NULL | Last PMS write time. |

**Removed in later migrations** (do not expect these on `positions`): **`realized_pnl`** (dropped in **`i9j0k1l2m3n4` — historical**), **`entry_avg`**, **`mark_price`**, **`notional`**, **`unrealized_pnl`** (removed in **`s9t0u1v2w3x4`** per **POSITION_VALUATION_SCHEMA_PLAN**). Symbol column was renamed to **`asset`** (`m3n4o5p6q7r8`).

**Indexes (typical):**

- **UNIQUE** `(broker, account_id, book, asset)` — UPSERT target.
- **INDEX** `(account_id)` — `ix_positions_account_id` (name may vary by migration history).
- **INDEX** `(asset)` — e.g. `ix_positions_asset` (renamed from `ix_positions_symbol`).

---

## 7. Reference data, cash, and capital

- **`symbols`:** Effectively **required** for order-based legs: without a row for `orders.symbol`, that order does not contribute to positions.
- **`assets`:** **Required for USD enrichment**; stables are seeded (e.g. `pms/asset_init.py` at startup). *Roadmap:* non-stables may use **`usd_symbol`** + mark provider (not wired in the tick today).
- **`balances`:** OMS-owned broker balances; **not read in the PMS tick**. For **total capital** views, combine **`positions`** (with `usd_price` / `usd_notional`) with **`balances`** in reporting or a future PMS extension — not implemented inside `run_one_tick`.
- **`balance_changes`:** **Used** in the core derivation path to adjust **open_qty** by deposits/withdrawals/adjustments/transfers (not “audit-only” in code).

---

## 8. Postgres schema (reads and writes)

PMS **home** schema is **`pms`** (`configure_for_pms`: unqualified **`assets`**, **`positions`** in writes). **Reads** from OMS and market data use **qualified** names in SQL (**`oms.orders`**, **`oms.symbols`**, **`market_data.ohlcv`**, …). See **docs/POSTGRES_SCHEMA_GROUPING_PLAN.md** §7–§8.

### 8.1 Reads

#### `oms.orders`

Primary source for **executed** exposure (after leg split). Implemented in **`pms/reads.py`** — `query_orders_for_positions`.

| Column (subset) | PMS usage |
|-----------------|-----------|
| **internal_id** | Logging when symbol missing from `symbols`. |
| **account_id**, **book**, **broker** | Grain and routing. |
| **symbol**, **side** | Leg split via `symbols`. |
| **executed_qty** | Exposed as `executed_quantity`. |
| **price** | Average fill price for base leg. |
| **binance_cumulative_quote_qty** | Preferred quote leg notional when present. |
| **created_at** | Ordering (FIFO for internal lot logic). |
| **status** | Filter: partially_filled / filled (case-insensitive trim). |

Full OMS column list: **docs/oms/OMS_ORDERS_DB_FIELDS.md**.

#### `oms.balance_changes`

Aggregated by **`pms/reads.py`** — `query_balance_changes_net_by_account_book_asset`. **`account_id`** is **TEXT** (broker id), aligned with **`oms.orders.account_id`**.

#### `oms.symbols`

**symbol → (base_asset, quote_asset)** via `query_symbol_map`.

#### `pms.assets`

**asset → usd_price, usd_symbol** via `query_assets_usd_config`.

#### `oms.accounts` / `oms.balances`

**accounts:** exists for OMS; PMS can use **`account_id`** (TEXT) consistently with orders. **balances:** `query_balances(pg, account_ids)` takes **`oms.accounts.id` (bigint)**; **not** invoked from **`run_one_tick`**.

### 8.2 Writes

- **`pms.positions`** — see **§6.1**.
- **Redis / snapshot tables** — not implemented.

---

## 9. Main loop and processing

**Entry:** **`pms/main.py`** → **`run_pms_loop()`** or **`run_one_tick()`** (`--once`).

1. **Bootstrap:** **`PmsSettings`**, optional **`init_assets_stables`**, build **`MarkPriceProvider`** (unused in tick today), optional **asset price feed** provider from **`get_asset_price_provider`**.
2. **Each tick:**
   - Optional **`pre_tick_callback`**: asset price feed step (**`run_asset_price_feed_step`**).
   - **`derive_positions_from_orders_and_balance_changes`**
   - **`enrich_positions_with_usd_from_assets`**
   - **`write_pms_positions`**
3. **No** Redis, **no** PnL/margin math, **no** call to **`enrich_positions_with_mark_prices`** in **`run_one_tick`**.

**Implementation:** **`pms/loop.py`**.

---

## 10. Configuration

| Variable / constant | Purpose |
|---------------------|--------|
| **DATABASE_URL** | Postgres (required to run). |
| **PMS_TICK_INTERVAL_SECONDS** | Seconds between ticks (default **10**). |
| **PMS_MARK_PRICE_SOURCE** | **`binance`** or empty/`none`/`fake` for provider factory; **`redis` / `market_data` not implemented** in factory. |
| **BINANCE_BASE_URL** (settings field **`binance_base_url`**) | Optional REST base for Binance mark provider. |
| **`binance_price_feed_base_url`** | Optional base for **asset** price feed HTTP calls. |
| **REDIS_URL** | Present on **`PmsSettings`**; **unused** by current PMS process. |
| **PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS** | **Defined but unused.** |
| **`ASSET_PRICE_FEED_SOURCE`**, **`ASSET_PRICE_FEED_ASSETS`**, **`ASSET_PRICE_FEED_INTERVAL_SECONDS`** | In **`pms/config.py`** as **module constants** (not env); drive optional feed. See **docs/pms/ASSET_PRICE_FEED_PLAN.md**. |

Per **`.cursor/rules/env-and-config.mdc`**, prefer **env** for infra URLs; keep feed asset lists / toggles in **code** unless you deliberately move them to settings later.

---

## 11. Mark price vs asset price

- **`pms/mark_price.py`:** **`MarkPriceProvider`** + **`BinanceMarkPriceProvider`** + **`MarkPricesResult`** (Pydantic). Intended for **pair/symbol** marks (e.g. future unrealized PnL per instrument).
- **Current tick:** does **not** invoke **`get_mark_prices`** on this path; **`loop.enrich_positions_with_mark_prices`** exists but is **not** called from **`run_one_tick`**.
- **Asset feed:** updates **`assets.usd_price`** for configured base assets (see **`asset_price_feed.py`**), separate from **`MarkPriceProvider`**.

**Phase 4:** Redis/DB-backed provider can implement the same **`MarkPriceProvider`** interface; **wiring into `run_one_tick`** would be explicit future work.

---

## 12. Pydantic (usage in repo)

| Area | Implementation |
|------|------------------|
| **Config** | **`pms/config.py`** — **`PmsSettings`** (`pydantic-settings`). |
| **Mark price** | **`MarkPricesResult`**, **`BinanceTickerPriceItem`** in **`pms/schemas_pydantic.py`**. |
| **Derived rows** | **`DerivedPosition`**, **`OrderRow`** in **`schemas_pydantic.py`** (`usd_notional` computed field). |

Snapshot / Redis payload models are **not** present (nothing writes them).

---

## 13. File map (actual)

| Area | File(s) |
|------|--------|
| Entry | **`pms/main.py`** |
| Loop | **`pms/loop.py`** |
| Postgres reads + derivation | **`pms/reads.py`** |
| Postgres writes | **`pms/granular_store.py`** |
| Settings | **`pms/config.py`** |
| Mark price (symbol-level API) | **`pms/mark_price.py`** |
| Asset USD feed | **`pms/asset_price_feed.py`**, **`pms/asset_init.py`**, **`pms/asset_price_providers/`** |
| Models | **`pms/schemas_pydantic.py`** |
| Logging | **`pms/log.py`** |
| Tests | **`pms/tests/`** |

---

## 14. Account data and session

- **`accounts`** table: OMS-owned; PMS keys everything by **TEXT `account_id`** in **`orders`** / **`positions`**.
- No user **session** in PMS; it is a background worker.

---

## 15. References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS data flow, sync, Redis/Postgres.
- **docs/oms/BALANCE_CHANGES_HISTORY.md** — `balance_changes` semantics.
- **docs/pms/POSITION_VALUATION_SCHEMA_PLAN.md** — USD single-numeraire, dropped columns.
- **docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md** — asset grain, broker column.
- **docs/pms/ASSET_PRICE_FEED_PLAN.md** — feed behavior.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 PMS (may describe earlier symbol-grain targets; this doc reflects **current** code).
