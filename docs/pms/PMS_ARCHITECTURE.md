# PMS Architecture: Data Flow, Interfaces & Design

Single reference for the **Portfolio Management System (PMS)**: data flow, Postgres interfaces, and alignment with OMS. PMS is the **source of truth** for PnL and margin. It reads **orders** (and **balances** when OMS syncs) from Postgres; **derives positions** from orders; **cash** comes from the broker-backed **balances** table (OMS syncs when `sync_balances=True`). PMS does **not** read OMS Redis positions. See **docs/PHASE2_DETAILED_PLAN.md** §8.

---

## 1. High-Level Data Flow

```
┌──────────────┐
│     OMS      │  • Order sync → Postgres orders
└──────┬───────┘  • Account sync → Postgres accounts, balances (when sync_balances=True)
       │          • balance_changes (from balanceUpdate only; history/audit)
       │          • symbols (from exchangeInfo at startup)
       │          • Produces oms_fills (Redis stream)
       ▼
┌───────────────┐
│ Postgres      │     (PMS does not use a fills table)
│ orders        │     ← primary source for position derivation
│ accounts      │
│ balances      │     ← cash (when OMS syncs)
│ balance_changes│     (deposit/withdrawal history; optional for PMS)
│ symbols       │     (OMS-managed; optional for PMS)
└───────┬───────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                        PMS                                   │
│  - Read: orders (required), balances (cash when synced)      │
│  - Positions: derived from orders (filter partial/fully filled)│
│  - Cash: from balances table (broker-backed)                 │
│  - Compute: PnL, unrealized (mark × open qty)                 │
│  - Write: positions table                                     │
└─────────────────────────────────────────────────────────────┘
```

- **Upstream:** OMS syncs **orders** and **accounts** to Postgres; optionally **balances** when `sync_balances=True`. Writes **balance_changes** from balanceUpdate (deposit/withdrawal/transfer). **Symbols** table populated by OMS (e.g. exchangeInfo at startup). PMS does **not** read OMS Redis positions.
- **PMS:** Reads **orders** (and **balances** when synced). **Positions** derived from **orders**; **cash** from **balances** table. Writes **positions** only. **PMS is the source of truth for PnL and margin.**

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

**Minimal reads for core:** `orders` (required), `balances` (for cash when OMS syncs). **No fills table:** orders are the primary data source. **Cash** = **balances** table (broker-backed; OMS syncs when `sync_balances=True`). **balance_changes** = history/audit only; **symbols** = optional reference (OMS-managed).

**Optional** (add when needed):

- **symbols** — OMS-managed; PMS may read for display/validation.
- **balance_changes** — History of deposits/withdrawals; optional for reporting.
- **pnl_snapshots / margin_snapshots** — Time-series; core uses positions table + optional Redis.
- **Repairs / rebuild-from-orders** — Periodic full rebuild and drift detection; core can do simple “recompute from orders” each tick.
- **Multiple mark price sources** — Phase 2: one (e.g. Binance); Phase 4: add Redis/DB; switch via config.
- **SQL VIEWs** on PMS tables — For grouping by symbol, book/symbol, broker/symbol; core uses one granular table, grouping on request in code or ad-hoc queries.

---

## 3. PMS Role (Phase 2)

From **docs/PHASE2_DETAILED_PLAN.md** §8 (see **§2 Core vs optional** above for minimal scope):

- **Periodically** read **orders** (and **balances** when OMS syncs) from Postgres; **derive positions** from orders (filter: partially_filled / filled). **Cash** from **balances** table. PMS does not read OMS Redis positions.
- Compute **realized PnL** (from order rows), **unrealized PnL** (mark × open qty when open_qty ≠ 0), and **margin** (for futures).
- Write **positions** table; optionally Redis (`pnl:{account_id}`, `margin:{account_id}`) for Admin/UI.

**Using the orders table for positions:**

- **Source for positions:** PMS **derives** positions from the **orders** table only (filter orders by status: **partially_filled**, **filled**; no fills table). **Signed** net quantity per (account_id, book, symbol): open_qty = sum(BUY executed_qty) − sum(SELL executed_qty); **position_side** = 'long' | 'short' | 'flat' from sign of open_qty; **entry_avg** = cost basis of the **open** quantity (FIFO; flattened exposure excluded). PMS does **not** read positions from OMS Redis (dummy).
- **Reconciliation / audit:** Order-derived positions are the canonical view; optional reconciliation can compare with other sources (e.g. a future Postgres positions table) or balance_changes.
- **Consistency:** Orders table is the audit trail (OMS sync); positions and cash derived/built from orders + balance_changes are the source of truth for PMS. **PMS is the source of truth for PnL, margin, and cash.**

---

## 4. Data Flows (OMS-Aligned)

Same conceptual patterns as OMS:

| Pattern | OMS | PMS |
|--------|-----|-----|
| **Event-driven** | Fill callback → sync_one_order, produce oms_fills | PMS can react to Redis cache updates or run on interval (no direct stream consumer in minimal design). |
| **Periodic sync** | sync_terminal_orders every N seconds | Periodic read of orders (and balances when synced); **derive positions**; cash from **balances**. |
| **Triggered sync** | On terminal status → sync_one_order | On-demand or timer: recompute positions from orders. |
| **Data fixes** | run_all_repairs (Binance payload → order columns) | **Repairs:** Rebuild positions from orders; fix/flag positions or fills when drift detected. |

PMS loop: read orders → derive positions → enrich with mark price → write positions.

---

## 5. Redis Key Layout (PMS)

**Reads:** PMS reads **Postgres** (orders; balances when OMS syncs). Does not read OMS Redis positions. **Cash** = **balances** table.

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

## 7. Reference Data and Cash

- **Symbols table:** OMS-managed (e.g. from broker exchangeInfo); PMS may read for display/validation. Optional for core.
- **Cash vs positions:** **Positions** = open exposure per symbol (from orders); grain (account_id, book, symbol). **Cash** = **balances** table per asset (broker-backed; OMS syncs when `sync_balances=True`). Do not put cash in the positions table.
- **Capital:** asset_value (positions × mark) + cash (from balances). No separate book_cash or build-from-order cash.

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

Current balance per (account_id, asset). **Cash source for PMS** when OMS runs with `sync_balances=True`. Columns: account_id (FK to accounts.id), asset, available, locked, updated_at.

#### balance_changes (OMS-owned; optional)

Historical deposit/withdrawal/transfer only (from broker balanceUpdate). Audit/history; not used by PMS for current cash. See **docs/oms/BALANCE_CHANGES_HISTORY.md**.

#### symbols (OMS-owned; optional)

Symbol → base_asset, quote_asset, etc. OMS populates from broker exchangeInfo; PMS may read for display/validation.

### 8.2 Write schema (tables PMS writes)

- **positions** — Full column list and indexes in **§6.1**. Grain (account_id, book, symbol); UPSERT by unique (account_id, book, symbol). **Core.** Cash = **balances** table (OMS); no book_cash.
- **Redis** `pnl:{account_id}`, `margin:{account_id}` — **(Optional / not yet implemented)** Intended for Admin/UI.
- **pnl_snapshots** — **(Optional)** account_id, book (optional), realized_pnl, unrealized_pnl, mark_price_used, timestamp. Not implemented.
- **margin_snapshots** — **(Optional)** account_id, total_margin, available_balance, timestamp. Not implemented.

---

## 9. Main Loop and Processing (PMS)

**Entrypoint:** e.g. `pms/main.py` → `run_pms_loop()`.

1. **Bootstrap:** Postgres connection (or callable), mark price provider, config (interval).
2. **Loop (each iteration or timer):**
   - **Read:** **orders** from Postgres (and **balances** when OMS syncs; for cash).
   - **Derive:** Positions from orders (filter partial/fully filled).
   - **Calculate:** Realized PnL (from order rows), unrealized PnL (mark × open_qty when open_qty ≠ 0), margin.
   - **Write (core):** Update **positions** table. **Optional:** Redis (`pnl:{account_id}`, `margin:{account_id}`), pnl_snapshots.
3. **Optional:** Repairs / rebuild-from-orders if drift.

Blocking vs polling: PMS is **timer-driven** (no Redis stream consumer in minimal design). For event-driven behavior later, could subscribe to Redis keyspace or a small “tick” stream on fill.

---

## 10. Configuration (Environment)

| Variable | Purpose |
|----------|--------|
| REDIS_URL | Redis connection; PMS writes PnL/margin keys only. |
| DATABASE_URL | Postgres; read orders (and balances when OMS syncs); write positions. |
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

**Recommendation:** You do **not** need to build a dedicated account data management service or session layer beforehand. Ensure the **accounts** table exists (OMS schema 12.2.1) and at least one row (e.g. seed default account); PMS can then run and scope by account_id. No dedicated account service required.

---

## 15. References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS data flow, sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §3 Postgres schema, §8 PMS, §12.3 PMS task checklist.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 PMS, §12 task checklist.
- **OMS:** Blocking read on risk_approved when block_ms > 0; PMS uses timer-based loop.
