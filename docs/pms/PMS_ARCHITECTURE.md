# PMS Architecture: Data Flow, Interfaces & Design

Single reference for the **Portfolio Management System (PMS)**: data flow, Redis/Postgres interfaces, and alignment with OMS. PMS is the **source of truth** for PnL and margin; it reads from OMS **Postgres only** (orders, accounts, balances). **PMS does not read OMS Redis positions** (that path is dummy). Positions used by PMS are **derived from the orders table only** (no fills table; filter orders by partial/fully filled status). Follows the same patterns as **OMS** (periodic sync, event-driven updates, data fixes). See **docs/PHASE2_DETAILED_PLAN.md** §8 for the PMS role.

---

## 1. High-Level Data Flow

```
┌──────────────┐
│     OMS      │  • Order sync → Postgres orders
└──────┬───────┘  • Account sync → Postgres accounts, balances
       │          • Produces oms_fills (Redis stream)
       │          • (OMS Redis positions exist but PMS does not read them — dummy)
       ▼
┌───────────────┐
│ Postgres      │     (PMS does not use a fills table)
│ orders        │
│ accounts      │
│ balances      │
│ balance_changes│
└───────┬───────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                        PMS                                   │
│  - Read: orders, balances (PG only; no fills table)           │
│  - Positions: derived from orders (filter partial/fully filled)│
│  - Compute: realized PnL (orders), unrealized (mark × open qty)│
│  - Write: pnl_snapshots / margin_snapshots, Redis cache       │
└─────────────────────────────────────────────────────────────┘
```

- **Upstream:** OMS syncs **orders** and **accounts/balances** to Postgres. OMS may keep positions in Redis internally; **PMS does not read OMS Redis positions** (that path is dummy). OMS produces **oms_fills**; PMS does **not** consume a fills table — **orders are the primary data source**.
- **PMS:** Reads **Postgres only** (orders, balances). **Positions** are **derived from the orders table only** (filter by status: partially_filled, filled), not from OMS Redis or a fills table. PMS aggregates PnL and margin and writes snapshots and cache. **PMS is the source of truth for PnL and margin.**

---

## 2. Core vs optional

**Core** (minimal PMS — implement these first):

| # | Feature | Description |
|---|--------|-------------|
| 1 | **Derive positions from orders** | Read **orders only** from Postgres (no fills table); filter by status **partially_filled**, **filled**. **Signed approach:** net quantity per (account_id, book, symbol) = sum(executed_qty for BUY) − sum(executed_qty for SELL). Output: **open_qty** (signed: positive = long, negative = short), **position_side** ('long' | 'short' | 'flat'), **entry_avg** (cost basis of the **open** quantity only — FIFO/cost-basis so flattened chunks are excluded). No OMS Redis. |
| 2 | **Compute PnL** | **Realized** from executed order rows (the part that has been closed/flattened). **Unrealized** = (mark_price − entry_avg) × **open_qty** (signed); **only when open_qty ≠ 0** (flattened positions have no unrealized — PnL already realized). One mark price source (e.g. Binance) is enough for core. |
| 3 | **One granular position store** | One table (**positions**): one row per (account_id, book, symbol) with **open_qty** (signed), **position_side** ('long' | 'short' | 'flat'), entry_avg, mark_price, notional, unrealized_pnl. Updated each run. |
| 4 | **Write PnL/margin for consumers** | After each run: update granular table; write Redis (e.g. `pnl:{account_id}`, `margin:{account_id}`) for Admin/UI. |
| 5 | **Timer loop** | Periodically: read orders (and balances if needed for margin) → derive positions → get mark prices → compute PnL/margin → write table + Redis. |

**Minimal reads for core:** `orders`, `balances` (and `accounts` to know which accounts to run). **No fills table:** orders are the primary data source. Optional for core: `symbols`, allocations.

**Optional** (add when needed):

- **Symbols table** — Reference (base/quote, validation); can use convention or config in core.
- **book_allocations** (or equivalent) — Capital by book; assume single book or simple rule in core.
- **pnl_snapshots / margin_snapshots** — Time-series tables; core only needs current state in granular table + Redis.
- **book_cash ledger** — Internal cash-by-book; only if splitting cash by book without external allocations.
- **Repairs / rebuild-from-orders** — Periodic full rebuild and drift detection; core can do simple “recompute from orders” each tick.
- **Multiple mark price sources** — Phase 2: one (e.g. Binance); Phase 4: add Redis/DB; switch via config.
- **SQL VIEWs** on PMS tables — For grouping by symbol, book/symbol, broker/symbol; core uses one granular table, grouping on request in code or ad-hoc queries.

---

## 3. PMS Role (Phase 2)

From **docs/PHASE2_DETAILED_PLAN.md** §8 (see **§2 Core vs optional** above for minimal scope):

- **Periodically** (e.g. every few seconds) read **balances** from Postgres and **derive positions** from the **orders** table only (filter: status partially_filled / filled; no fills table). **PMS does not read OMS Redis positions** (that is dummy).
- Compute **realized PnL** (from executed order rows), **unrealized PnL** (mark price × open qty only when open_qty > 0; flattened positions have no unrealized), and **margin** (for futures).
- Write results to Postgres (`pnl_snapshots` or similar) and/or to Redis (e.g. `pnl:{account_id}`, `margin:{account_id}`) for Admin/UI.

**Using the orders table for positions:**

- **Source for positions:** PMS **derives** positions from the **orders** table only (filter orders by status: **partially_filled**, **filled**; no fills table). **Signed** net quantity per (account_id, book, symbol): open_qty = sum(BUY executed_qty) − sum(SELL executed_qty); **position_side** = 'long' | 'short' | 'flat' from sign of open_qty; **entry_avg** = cost basis of the **open** quantity (FIFO; flattened exposure excluded). PMS does **not** read positions from OMS Redis (dummy).
- **Reconciliation / audit:** Order-derived positions are the canonical view; optional reconciliation can compare with other sources (e.g. a future Postgres positions table) or balance_changes.
- **Consistency:** Orders table is the audit trail (OMS sync); positions derived from orders are the source of truth for PMS. **PMS is the source of truth for PnL and margin.**

---

## 4. Data Flows (OMS-Aligned)

Same conceptual patterns as OMS:

| Pattern | OMS | PMS |
|--------|-----|-----|
| **Event-driven** | Fill callback → sync_one_order, produce oms_fills | PMS can react to Redis cache updates or run on interval (no direct stream consumer in minimal design). |
| **Periodic sync** | sync_terminal_orders every N seconds | Periodic read of balances from Postgres; **derive positions from orders** (PMS does not read OMS Redis positions). |
| **Triggered sync** | On terminal status → sync_one_order | On-demand or timer: recompute positions from orders. |
| **Data fixes** | run_all_repairs (Binance payload → order columns) | **Repairs:** Rebuild positions from orders; fix/flag positions or fills when drift detected. |

See **docs/pms/PMS_DATA_FLOWS.md** for detailed flow descriptions.

---

## 5. Redis Key Layout (PMS)

**Reads:** PMS reads **Postgres only** (orders, balances, optional fills). **PMS does not read OMS Redis positions or balances** (that path is dummy). Positions are derived from the orders table.

**Writes** (PMS):

| Key pattern | Type | Purpose |
|-------------|------|--------|
| `pnl:{account_id}` | Hash / JSON | Realized PnL, unrealized PnL, timestamp. |
| `margin:{account_id}` | Hash / JSON | Total margin, available balance (PMS-owned). |

Document exact value format in PMS code for PnL/margin keys.

---

## 6. Granular Store, Views, and Tables (PMS Writes Tables Only)

**One granular store, grouping on request:**

- PMS maintains **one granular store** at a fixed grain: **(account_id, book, symbol)**. One row per (account, book, symbol) with **open_qty** (signed: positive = long, negative = short), **position_side** ('long' | 'short' | 'flat' — net long/short indicator), entry_avg, mark_price, notional, unrealized_pnl. This is **derived from orders** (which have `book`) and updated by PMS after each calculation cycle.
- **Different views** (by symbol, by book/symbol, by broker/symbol) are **computed on request** — either by SQL `GROUP BY` on the granular table(s), or by the PMS/API reading the granular table and returning grouped results. No need to pre-materialize each view unless one is very hot.
- **PMS writes tables only** (no SQL VIEWs as the primary output). Full valuation needs mark price (external), so PMS must compute and write. Optional: define DB VIEWs **on top of** PMS-written tables for simple groupings (e.g. `valuation_by_symbol`).

**How this differs from the orders table:**

| | Orders table (OMS) | Granular store (PMS) |
|--|--------------------|----------------------|
| **Grain** | One row per order | One row per (account_id, book, symbol) |
| **Content** | Order fields (qty, price, status, book, broker, …) | Aggregated state (open_qty signed, position_side, entry_avg, mark_price, notional, unrealized_pnl) |
| **Purpose** | Audit trail, source of truth for “what happened” | Current state for fast reads and on-demand grouping |

Orders = ledger; granular store = materialized position/snapshot written by PMS after calculations.

### 6.1 Positions table (PMS write schema)

PMS **writes** the **positions** table. Grain: **(account_id, book, symbol)**. One row per (account, book, symbol); UPSERT by unique (account_id, book, symbol). Implemented in `pms/granular_store.py` (`write_pms_positions`).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| **id** | BIGSERIAL | NOT NULL | Primary key (surrogate). |
| **account_id** | TEXT | NOT NULL | Broker account id (same semantics as `orders.account_id`). |
| **book** | TEXT | NOT NULL | Strategy/book identifier (default `''`). |
| **symbol** | TEXT | NOT NULL | Trading symbol (e.g. BTCUSDT). |
| **open_qty** | NUMERIC(36,18) | NOT NULL | Signed net quantity: positive = long, negative = short; default 0. |
| **position_side** | TEXT | NOT NULL | `'long'` \| `'short'` \| `'flat'`; default `'flat'`. |
| **entry_avg** | NUMERIC(36,18) | NULL | Cost basis of **open** quantity only (FIFO). |
| **mark_price** | NUMERIC(36,18) | NULL | Mark price used to compute unrealized_pnl (from mark price provider). |
| **notional** | NUMERIC(36,18) | NULL | Position notional value (e.g. open_qty × mark_price). |
| **unrealized_pnl** | NUMERIC(36,18) | NOT NULL | Unrealized PnL when open_qty ≠ 0; default 0. |
| **updated_at** | TIMESTAMPTZ | NULL | Last update timestamp. |

**Indexes:**

- **UNIQUE** `uq_positions_account_book_symbol` on `(account_id, book, symbol)` — used for UPSERT conflict target.
- **INDEX** `ix_positions_account_id` on `(account_id)`.
- **INDEX** `ix_positions_symbol` on `(symbol)`.

**Note:** There is no `realized_pnl` column (dropped in migration i9j0k1l2m3n4). Realized PnL is not stored in this table in the current implementation.

---

## 7. Reference Data: Symbols and Allocations (optional for core)

**Symbol properties (reference table):**

- **Symbols table** (e.g. `symbols` or `instruments`): maps symbol → base_asset, quote_asset, tick_size, lot_size, min_qty, product_type (spot/futures), broker, etc. Used for base/quote derivation, validation, and display. Populated by sync from broker (e.g. exchange info) or config; **PMS only reads** it. See **docs/pms/PMS_DATA_MODEL.md** (symbol vs asset).

**Cash vs positions (keep separate):**

- **Positions** = open exposure in a **trading pair** (symbol). Stored in PMS granular table; grain (account_id, book, symbol) with **open_qty** signed and **position_side** ('long' | 'short' | 'flat'). Do **not** put cash (asset balances) into the positions table.
- **Cash** = holdings per **asset** (e.g. USDT, BTC). Stays in OMS **balances** table. PMS reads it for margin and for capital-by-book. A combined “holdings” view (positions + cash) can be derived in a VIEW or API, not by mixing concepts in one table.

**Capital by book:**

- **capital(book) = asset_value(book) + cash(book)**. Asset value by book = sum of position values (qty × mark price) for that book — requires **book** in the granular store grain. Cash by book is not provided by the broker (balances are per account/asset). Two options:
  - **Allocation:** Split account cash across books by a rule (e.g. shares or fixed amounts). Allocations are **managed outside** the system (admin, config, another service). The system is **provided with** allocations via a defined **interface** (table, API, or file) and uses them as **read-only input**.
  - **Internal book_cash ledger:** Table (account_id, book, asset) → balance, updated by PMS on each fill (e.g. book A sells BTC → credit USDT for book A). Initial state from allocation or config.
- **Allocations:** Managed outside; system reads via interface (e.g. table `book_allocations`, or config API, or file). PMS does not decide allocation percentages or amounts; it only consumes them.

---

## 8. Postgres Schema (Reads & Writes)

PMS does **not** own the read tables; they are populated by OMS (or reference data). PMS **owns** the **positions** table (see §6.1 for full write schema).

### 8.1 Read schema (tables PMS reads)

#### orders (OMS-owned; primary data source for positions)

PMS derives positions **only** from this table (no fills table). Filter: `status IN ('partially_filled', 'filled')` and `COALESCE(executed_qty, 0) > 0`. Order by `created_at ASC` for FIFO. Implemented in `pms/reads.py` (`query_orders_for_positions`).

| Column | Type | PMS usage |
|--------|------|-----------|
| **account_id** | TEXT | Broker account id; grain for position derivation. |
| **book** | TEXT | Strategy/book; grain for position derivation. |
| **symbol** | TEXT | Trading symbol; grain for position derivation. |
| **side** | TEXT | BUY or SELL; sign of executed_qty for net. |
| **executed_qty** | NUMERIC(36,18) | Used as executed quantity for position (exposed as `executed_quantity` in reads). |
| **price** | NUMERIC(36,18) | Executed (average fill) price; used for FIFO entry_avg. |
| **created_at** | TIMESTAMPTZ | Order time; used for FIFO ordering. |
| **status** | TEXT | Filter: `partially_filled`, `filled` only. |

Other columns (internal_id, broker, broker_order_id, order_type, quantity, limit_price, etc.) exist but are not used by PMS for position derivation. See **docs/oms/OMS_ORDERS_DB_FIELDS.md** for full OMS schema.

#### accounts (OMS-owned)

Used to know which accounts exist; optional for scoping. **Note:** `accounts.id` is BIGINT (PK); `accounts.account_id` is TEXT (broker account id). Balances table uses `account_id` as FK to `accounts.id` (bigint).

| Column | Type | PMS usage |
|--------|------|-----------|
| **id** | BIGINT | Primary key; used when querying balances by account (balances.account_id → accounts.id). |
| **account_id** | TEXT | Broker account id; matches `orders.account_id` and `positions.account_id`. |
| **name** | TEXT | Display name. |
| **broker** | TEXT | e.g. `binance`. |
| **config** | JSONB | Optional config. |

#### balances (OMS-owned)

Current balance per account/asset. **Cash** stays here; not in positions table. PMS can read for margin or capital; current implementation derives positions from **orders only**. Implemented in `pms/reads.py` (`query_balances`). **Note:** `balances.account_id` is FK to `accounts.id` (BIGINT), not the broker account_id string.

| Column | Type | PMS usage |
|--------|------|-----------|
| **account_id** | BIGINT | FK to accounts.id (use accounts.id when filtering by account). |
| **asset** | TEXT | Asset symbol (e.g. USDT, BTC). |
| **available** | NUMERIC(36,18) | Available balance. |
| **locked** | NUMERIC(36,18) | Locked balance. |
| **updated_at** | TIMESTAMPTZ | Last update. |

#### Optional read tables

- **symbols** (reference) — **(Optional)** symbol, base_asset, quote_asset, tick_size, lot_size, etc. PMS reads for base/quote and validation. Not implemented in current codebase.
- **book_allocations** — **(Optional)** (account_id, book, asset, amount_or_share). Provided via interface; managed outside; PMS reads only. Not implemented.

### 8.2 Write schema (tables PMS writes)

- **positions** — Full column list and indexes in **§6.1**. Grain (account_id, book, symbol); UPSERT by unique (account_id, book, symbol). **Core.**
- **Redis** `pnl:{account_id}`, `margin:{account_id}` — **(Optional / not yet implemented)** Intended for Admin/UI; current PMS does not write these keys.
- **pnl_snapshots** — **(Optional)** account_id, book (optional), realized_pnl, unrealized_pnl, mark_price_used, timestamp. Not implemented.
- **margin_snapshots** — **(Optional)** account_id, total_margin, available_balance, timestamp. Not implemented.
- **book_cash** — **(Optional)** If using internal ledger for cash by book: (account_id, book, asset) → balance. Not implemented.

---

## 9. Main Loop and Processing (PMS)

**Entrypoint:** e.g. `pms/main.py` → `run_pms_loop()`.

1. **Bootstrap:** Redis client, Postgres connection (or callable), config (interval, which accounts).
2. **Loop (each iteration or timer):**
   - **Read:** Balances from Postgres; **derive positions** from **orders** table only (filter partial/fully filled; no fills table).
   - **Calculate:** Realized PnL (from executed order rows), unrealized PnL (mark × open_qty when open_qty ≠ 0; signed open_qty), margin.
   - **Optional:** Periodic rebuild from orders; compare to other sources or run **repairs** if drift (flag or correct).
   - **Write (core):** Update granular position table (`positions`: signed open_qty, position_side); update Redis (`pnl:{account_id}`, `margin:{account_id}`). **Optional:** Postgres `pnl_snapshots` / `margin_snapshots`.
3. **Optional:** Periodic repairs — rebuild-from-orders, diff, fix/flag (see **docs/pms/PMS_DATA_FLOWS.md**).

Blocking vs polling: PMS is **timer-driven** (no Redis stream consumer in minimal design). For event-driven behavior later, could subscribe to Redis keyspace or a small “tick” stream on fill.

---

## 10. Configuration (Environment)

| Variable | Purpose |
|----------|--------|
| REDIS_URL | Redis connection; PMS writes PnL/margin keys only. |
| DATABASE_URL | Postgres; read orders, balances (no fills table); write snapshots. |
| PMS_TICK_INTERVAL_SECONDS | How often to run read → derive → calculate → write (e.g. 5). |
| PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS | How often to rebuild positions from orders and run repairs (e.g. 60 or 300). |
| PMS_MARK_PRICE_SOURCE | `binance` (Phase 2: wrap Binance REST/WS) or `redis` / `market_data` (Phase 4: read from Redis/DB fed by Market Data service). Chooses which **mark price provider** implementation to use. |

---

## 11. Mark Price (Unrealized PnL) — Interface and Implementations

**Approach:** Define a **mark price provider interface** first; PMS depends only on this contract. Implementations are swappable via config.

- **Interface (contract):** e.g. `get_mark_prices(symbols: list[str]) -> dict[str, Decimal]` (or similar). Used by PMS when computing unrealized PnL. Unit tests can use a fake implementation.
- **Phase 2:** Implement the interface by **wrapping Binance** (REST or WebSocket) — e.g. Binance mark/last price per symbol. PMS gets real unrealized PnL without a separate Market Data service.
- **Phase 4:** Add a second implementation that **reads from Redis or DB** (fed by the Market Data service). Wire it via the same interface; switch with `PMS_MARK_PRICE_SOURCE` (e.g. `binance` vs `redis` or `market_data`). No change to PMS calculation logic.

**Config:** `PMS_MARK_PRICE_SOURCE` selects the implementation (`binance` now; `redis`/`market_data` in Phase 4). If unset or unsupported, unrealized PnL can be zero.

**File map:** e.g. `pms/mark_price.py` — protocol/interface + Binance implementation; Phase 4 adds Redis/DB implementation in same module or `pms/mark_price_redis.py`.

**Pydantic:** Use Pydantic for (1) **mark price provider** — parse Binance mark/last-price API response with a Pydantic model; optional return model for `get_mark_prices` (e.g. `MarkPricesResult`) for consistent typing. (2) **Phase 4** Redis/DB payloads from Market Data can be validated with Pydantic before use.

---

## 12. Pydantic (recommended usage)

Align with OMS, which uses Pydantic for stream messages and broker events. Recommended in PMS:

| Area | Use Pydantic for |
|------|------------------|
| **Config** | `pms/config.py` — `BaseSettings` (or similar) for env: `PMS_TICK_INTERVAL_SECONDS`, `PMS_MARK_PRICE_SOURCE`, `DATABASE_URL`, `REDIS_URL`. Validates and types config at startup. |
| **Mark price** | Binance API response (mark/last price) — parse with a Pydantic model. Optional: return type of mark price provider as a model (e.g. `MarkPricesResult(symbols: dict[str, Decimal])`) for tests and Phase 4 impl. |
| **Internal shapes** | Order-derived position, PnL snapshot, margin snapshot — use Pydantic models (e.g. `DerivedPosition`, `PnlSnapshot`) so calculations and snapshot writes use validated structures; avoids ad-hoc dicts. |
| **Snapshot writes** | Structure written to Redis (`pnl:{account_id}`, `margin:{account_id}`) and to Postgres `pnl_snapshots` / `margin_snapshots` — validate with Pydantic before write. |
| **Read from Postgres** | Optional: map DB rows to Pydantic models (e.g. order row → `OrderRow`) for type-safe use in derivation and calculations. |

PMS can depend on the same `pydantic>=2.0.0` as OMS (root or shared requirements).

---

## 13. File Map (Concise)

| Area | Files (suggested) |
|------|-------------------|
| Entry & loop | `pms/main.py` |
| Read positions/balances | `pms/read_store.py` or `storage/` |
| Rebuild from orders | `pms/rebuild_from_orders.py` |
| Granular position store | `positions` table (grain: account_id, book, symbol; open_qty signed, position_side) |
| PnL / margin calculation | `pms/calculations.py` |
| Repairs (position drift) | `pms/repair.py` |
| Redis cache read/write | `pms/cache.py` (PMS writes PnL/margin keys only; does not read OMS Redis) |
| Mark price provider | `pms/mark_price.py` (interface + Binance impl); Phase 4: Redis/DB impl |
| Snapshots write | `pms/snapshots.py` |
| Logging | `pms/log.py` |

---

## 14. Account Data and Session Dependency

Whether **account data management** (or session) must be built **before** PMS:

- **Accounts table:** Already part of Phase 2 OMS schema (§3.1). PMS only needs **account_id** to scope reads/writes (e.g. which accounts to compute PnL for). No separate “account service” is required for Phase 2; a **default account** or env-based account list is enough.
- **Session:** No session is required to run PMS. It is a background loop that runs per process; “session” in the sense of user login is an Admin/Phase 3 concern. If “session” means “which broker/account is active,” that is already encoded in **account_id** and the **accounts** table; PMS can iterate over all accounts or a configured subset.

**Recommendation:** You do **not** need to build a dedicated account data management service or session layer beforehand. Ensure the **accounts** table exists (OMS schema 12.2.1) and at least one row (e.g. seed default account); PMS can then run and scope by account_id. See **docs/pms/PMS_ACCOUNT_AND_SESSION.md** for details.

---

## 15. References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS data flow, sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §3 Postgres schema, §8 PMS, §12.3 PMS task checklist.
- **docs/pms/PMS_DATA_MODEL.md** — Symbol vs asset, building positions, reconciliation.
- **docs/oms/OMS_BLOCKING_EXPLAINED.md** — Event-driven vs polling (PMS uses timer-based polling unless extended).
