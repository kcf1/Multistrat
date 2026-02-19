# PMS Architecture: Data Flow, Interfaces & Design

Single reference for the **Portfolio Management System (PMS)**: data flow, Redis/Postgres interfaces, and alignment with OMS. PMS is the **source of truth** for PnL and margin; it reads from OMS **Postgres only** (orders, accounts, balances). **PMS does not read OMS Redis positions** (that path is dummy). Positions used by PMS are **derived from the orders table** (and optionally fills). Follows the same patterns as **OMS** (periodic sync, event-driven updates, data fixes). See **docs/PHASE2_DETAILED_PLAN.md** §8 for the PMS role.

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
│ Postgres      │     (optional: fills table from oms_fills consumer)
│ orders        │
│ accounts      │
│ balances      │
│ balance_changes│
└───────┬───────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                        PMS                                   │
│  - Read: orders, balances, optional fills (PG only)           │
│  - Positions: derived from orders table (not from OMS Redis) │
│  - Compute: realized PnL (fills), unrealized (mark), margin   │
│  - Write: pnl_snapshots / margin_snapshots, Redis cache       │
└─────────────────────────────────────────────────────────────┘
```

- **Upstream:** OMS syncs **orders** and **accounts/balances** to Postgres. OMS may keep positions in Redis internally; **PMS does not read OMS Redis positions** (that path is dummy). OMS produces **oms_fills**; a downstream consumer (e.g. PMS) may write a **fills** table.
- **PMS:** Reads **Postgres only** (orders, balances, optional fills). **Positions** used by PMS are **derived from the orders table** (and optionally fills), not from OMS Redis. PMS aggregates PnL and margin and writes snapshots and cache. **PMS is the source of truth for PnL and margin.**

---

## 2. PMS Role (Phase 2)

From **docs/PHASE2_DETAILED_PLAN.md** §8:

- **Periodically** (e.g. every few seconds) read **balances** from Postgres and **derive positions** from the **orders** table (and optionally fills). **PMS does not read OMS Redis positions** (that is dummy).
- Compute **realized PnL** (from fills), **unrealized PnL** (from current mark price if available), and **margin** (for futures).
- Write results to Postgres (`pnl_snapshots` or similar) and/or to Redis (e.g. `pnl:{account_id}`, `margin:{account_id}`) for Admin/UI.

**Using the orders table for positions:**

- **Source for positions:** PMS **derives** positions from the **orders** table (e.g. net executed quantity per account/symbol/side from terminal orders). PMS does **not** read positions from OMS Redis (dummy).
- **Reconciliation / audit:** Order-derived positions are the canonical view; optional reconciliation can compare with other sources (e.g. a future Postgres positions table) or balance_changes.
- **Consistency:** Orders table is the audit trail (OMS sync); positions derived from orders are the source of truth for PMS. **PMS is the source of truth for PnL and margin.**

---

## 3. Data Flows (OMS-Aligned)

Same conceptual patterns as OMS:

| Pattern | OMS | PMS |
|--------|-----|-----|
| **Event-driven** | Fill callback → sync_one_order, produce oms_fills | PMS can react to Redis cache updates or run on interval (no direct stream consumer in minimal design). |
| **Periodic sync** | sync_terminal_orders every N seconds | Periodic read of balances from Postgres; **derive positions from orders** (PMS does not read OMS Redis positions). |
| **Triggered sync** | On terminal status → sync_one_order | On-demand or timer: recompute positions from orders. |
| **Data fixes** | run_all_repairs (Binance payload → order columns) | **Repairs:** Rebuild positions from orders; fix/flag positions or fills when drift detected. |

See **docs/pms/PMS_DATA_FLOWS.md** for detailed flow descriptions.

---

## 4. Redis Key Layout (PMS)

**Reads:** PMS reads **Postgres only** (orders, balances, optional fills). **PMS does not read OMS Redis positions or balances** (that path is dummy). Positions are derived from the orders table.

**Writes** (PMS):

| Key pattern | Type | Purpose |
|-------------|------|--------|
| `pnl:{account_id}` | Hash / JSON | Realized PnL, unrealized PnL, timestamp. |
| `margin:{account_id}` | Hash / JSON | Total margin, available balance (PMS-owned). |

Document exact value format in PMS code for PnL/margin keys.

---

## 5. Postgres Schema (Reads & Writes)

**PMS reads** (no schema ownership; tables from OMS):

- **accounts** — id, name, broker, config (for account list and config).
- **orders** — To **derive positions** (aggregate executed_qty by account_id, symbol, side). Primary source for position view; PMS does not read OMS Redis positions.
- **fills** — For realized PnL and audit (if PMS or another consumer writes a fills table from oms_fills).
- **balances** — Current balances per account/asset (synced by OMS).

**PMS writes:**

- **pnl_snapshots** (optional) — account_id, realized_pnl, unrealized_pnl, mark_price_used, timestamp.
- **margin_snapshots** (optional) — account_id, total_margin, available_balance, timestamp.

---

## 6. Main Loop and Processing (PMS)

**Entrypoint:** e.g. `pms/main.py` → `run_pms_loop()`.

1. **Bootstrap:** Redis client, Postgres connection (or callable), config (interval, which accounts).
2. **Loop (each iteration or timer):**
   - **Read:** Balances from Postgres; **derive positions** from **orders** table (PMS does not read OMS Redis positions).
   - **Calculate:** Realized PnL (from fills or cached), unrealized PnL (mark price if available), margin.
   - **Optional periodic rebuild:** Recompute positions from **orders**; optionally compare to other sources or run **repairs** if drift (flag or correct).
   - **Write:** Update Redis cache (`pnl:{account_id}`, `margin:{account_id}`); optionally write Postgres `pnl_snapshots` / `margin_snapshots`.
3. **Periodic repairs:** Same loop or separate interval: rebuild-from-orders, diff, fix/flag (see **docs/pms/PMS_DATA_FLOWS.md**).

Blocking vs polling: PMS is **timer-driven** (no Redis stream consumer in minimal design). For event-driven behavior later, could subscribe to Redis keyspace or a small “tick” stream on fill.

---

## 7. Configuration (Environment)

| Variable | Purpose |
|----------|--------|
| REDIS_URL | Redis connection; PMS writes PnL/margin keys only. |
| DATABASE_URL | Postgres; read orders, balances, optional fills; write snapshots. |
| PMS_TICK_INTERVAL_SECONDS | How often to run read → derive → calculate → write (e.g. 5). |
| PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS | How often to rebuild positions from orders and run repairs (e.g. 60 or 300). |
| PMS_MARK_PRICE_SOURCE | `binance` (Phase 2: wrap Binance REST/WS) or `redis` / `market_data` (Phase 4: read from Redis/DB fed by Market Data service). Chooses which **mark price provider** implementation to use. |

---

## 8. Mark Price (Unrealized PnL) — Interface and Implementations

**Approach:** Define a **mark price provider interface** first; PMS depends only on this contract. Implementations are swappable via config.

- **Interface (contract):** e.g. `get_mark_prices(symbols: list[str]) -> dict[str, Decimal]` (or similar). Used by PMS when computing unrealized PnL. Unit tests can use a fake implementation.
- **Phase 2:** Implement the interface by **wrapping Binance** (REST or WebSocket) — e.g. Binance mark/last price per symbol. PMS gets real unrealized PnL without a separate Market Data service.
- **Phase 4:** Add a second implementation that **reads from Redis or DB** (fed by the Market Data service). Wire it via the same interface; switch with `PMS_MARK_PRICE_SOURCE` (e.g. `binance` vs `redis` or `market_data`). No change to PMS calculation logic.

**Config:** `PMS_MARK_PRICE_SOURCE` selects the implementation (`binance` now; `redis`/`market_data` in Phase 4). If unset or unsupported, unrealized PnL can be zero.

**File map:** e.g. `pms/mark_price.py` — protocol/interface + Binance implementation; Phase 4 adds Redis/DB implementation in same module or `pms/mark_price_redis.py`.

**Pydantic:** Use Pydantic for (1) **mark price provider** — parse Binance mark/last-price API response with a Pydantic model; optional return model for `get_mark_prices` (e.g. `MarkPricesResult`) for consistent typing. (2) **Phase 4** Redis/DB payloads from Market Data can be validated with Pydantic before use.

---

## 9. Pydantic (recommended usage)

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

## 10. File Map (Concise)

| Area | Files (suggested) |
|------|-------------------|
| Entry & loop | `pms/main.py` |
| Read positions/balances | `pms/read_store.py` or `storage/` |
| Rebuild from orders | `pms/rebuild_from_orders.py` |
| PnL / margin calculation | `pms/calculations.py` |
| Repairs (position drift) | `pms/repair.py` |
| Redis cache read/write | `pms/cache.py` (PMS writes PnL/margin keys only; does not read OMS Redis) |
| Mark price provider | `pms/mark_price.py` (interface + Binance impl); Phase 4: Redis/DB impl |
| Snapshots write | `pms/snapshots.py` |
| Logging | `pms/log.py` |

---

## 11. Account Data and Session Dependency

Whether **account data management** (or session) must be built **before** PMS:

- **Accounts table:** Already part of Phase 2 OMS schema (§3.1). PMS only needs **account_id** to scope reads/writes (e.g. which accounts to compute PnL for). No separate “account service” is required for Phase 2; a **default account** or env-based account list is enough.
- **Session:** No session is required to run PMS. It is a background loop that runs per process; “session” in the sense of user login is an Admin/Phase 3 concern. If “session” means “which broker/account is active,” that is already encoded in **account_id** and the **accounts** table; PMS can iterate over all accounts or a configured subset.

**Recommendation:** You do **not** need to build a dedicated account data management service or session layer beforehand. Ensure the **accounts** table exists (OMS schema 12.2.1) and at least one row (e.g. seed default account); PMS can then run and scope by account_id. See **docs/pms/PMS_ACCOUNT_AND_SESSION.md** for details.

---

## 12. References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS data flow, sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §3 Postgres schema, §8 PMS, §12.3 PMS task checklist.
- **docs/pms/PMS_DATA_MODEL.md** — Symbol vs asset, building positions, reconciliation.
- **docs/oms/OMS_BLOCKING_EXPLAINED.md** — Event-driven vs polling (PMS uses timer-based polling unless extended).
