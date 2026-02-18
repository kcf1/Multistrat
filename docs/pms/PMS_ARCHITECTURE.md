# PMS Architecture: Data Flow, Interfaces & Design

Single reference for the **Portfolio Management System (PMS)**: data flow, Redis/Postgres interfaces, and alignment with OMS. PMS is the **source of truth** for PnL and margin; it reads from OMS (orders, accounts/balances, positions in Redis). Follows the same patterns as **OMS** (periodic sync, event-driven updates, data fixes). See **docs/PHASE2_DETAILED_PLAN.md** §8 for the PMS role.

---

## 1. High-Level Data Flow

```
┌──────────────┐
│     OMS      │  • Order sync → Postgres orders
└──────┬───────┘  • Account sync → Postgres accounts, balances; Redis positions
       │          • Produces oms_fills (Redis stream)
       │
       ├──────────────────────────────────────────────────────────────┐
       │                                                              │
       ▼                                                              ▼
┌───────────────┐                                              ┌───────────────┐
│ Postgres      │                                              │ Redis         │
│ orders        │                                              │ account:*     │
│ accounts      │                                              │ :balances     │
│ balances      │                                              │ :positions    │
│ balance_changes│                                             └───────┬───────┘
└───────┬───────┘                                                      │
        │     (optional: fills table from oms_fills consumer)          │
        │                                                              │
        └──────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼
           ┌─────────────────────────────────────────────────────────────┐
           │                        PMS                                  │
           │  - Read: orders, balances (PG); positions (Redis from OMS)   │
           │  - Optional: fills table; rebuild positions from orders     │
           │  - Compute: realized PnL (fills), unrealized (mark), margin │
           │  - Write: pnl_snapshots / margin_snapshots, Redis cache      │
           └─────────────────────────────────────────────────────────────┘
```

- **Upstream:** OMS syncs **orders** and **accounts/balances** to Postgres; **positions** live in Redis only (OMS account store). OMS produces **oms_fills**; a downstream consumer (e.g. PMS) may write a **fills** table.
- **PMS:** Reads Postgres (orders, balances, optional fills) and Redis (positions, balances); aggregates PnL and margin; optionally recomputes positions from **orders** for reconciliation; writes snapshots and cache. **PMS is the source of truth for PnL and margin.**

---

## 2. PMS Role (Phase 2)

From **docs/PHASE2_DETAILED_PLAN.md** §8:

- **Periodically** (e.g. every few seconds) read **positions** and **balances** from Redis (OMS account store) or from Postgres (balances).
- Compute **realized PnL** (from fills), **unrealized PnL** (from current mark price if available), and **margin** (for futures).
- Write results to Postgres (`pnl_snapshots` or similar) and/or to Redis (e.g. `pnl:{account_id}`, `margin:{account_id}`) for Admin/UI.

**Using the orders table for positions:**

- **Primary source** for live view: **positions** in Redis (OMS account store). In a future phase, a Postgres **positions** table may be added; PMS would read from it or Redis when available.
- **Reconciliation / audit:** PMS can **recompute** positions from the **orders** table (e.g. net executed quantity per account/symbol/side from terminal orders). Use this for:
  - Periodic **rebuild** to detect drift vs OMS-maintained positions in Redis.
  - **Data fixes:** Correct positions (or flag for manual repair) when drift is found.
- **Consistency:** Orders table is the audit trail (OMS sync); positions derived from orders are the canonical reference for reconciliation. **PMS is the source of truth for PnL and margin.**

---

## 3. Data Flows (OMS-Aligned)

Same conceptual patterns as OMS:

| Pattern | OMS | PMS |
|--------|-----|-----|
| **Event-driven** | Fill callback → sync_one_order, produce oms_fills | PMS can react to Redis cache updates or run on interval (no direct stream consumer in minimal design). |
| **Periodic sync** | sync_terminal_orders every N seconds | Periodic read of positions/balances from Redis and Postgres; optional periodic **rebuild** of position view from orders. |
| **Triggered sync** | On terminal status → sync_one_order | On-demand or timer: recompute from orders and compare to Redis positions. |
| **Data fixes** | run_all_repairs (Binance payload → order columns) | **Repairs:** Rebuild positions from orders; fix/flag positions or fills when drift detected. |

See **docs/pms/PMS_DATA_FLOWS.md** for detailed flow descriptions.

---

## 4. Redis Key Layout (PMS)

**Reads** (written by OMS; schema matches OMS account store):

| Key pattern | Type | Purpose |
|-------------|------|--------|
| `account:{broker}:{account_id}:positions` | Hash | Current positions (symbol, side, quantity, entry_price_avg). |
| `account:{broker}:{account_id}:balances` | Hash | Available and locked balance per asset. |
| (Optional) `margin:{account_id}` | Hash / JSON | Margin snapshot (futures). |

**Writes** (PMS):

| Key pattern | Type | Purpose |
|-------------|------|--------|
| `pnl:{account_id}` | Hash / JSON | Realized PnL, unrealized PnL, timestamp. |
| `margin:{account_id}` | Hash / JSON | Total margin, available balance (PMS-owned). |

Document exact value format in PMS code and align with OMS account store (Phase 2 task 12.2.x).

---

## 5. Postgres Schema (Reads & Writes)

**PMS reads** (no schema ownership; tables from OMS):

- **accounts** — id, name, broker, config (for account list and config).
- **orders** — For reconciliation: aggregate executed_qty by account_id, symbol, side to derive net position; compare to Redis positions.
- **fills** — For realized PnL and audit (if PMS or another consumer writes a fills table from oms_fills).
- **balances** — Current balances per account/asset (synced by OMS). **Positions** in Phase 2 are Redis-only (OMS account store).

**PMS writes:**

- **pnl_snapshots** (optional) — account_id, realized_pnl, unrealized_pnl, mark_price_used, timestamp.
- **margin_snapshots** (optional) — account_id, total_margin, available_balance, timestamp.

---

## 6. Main Loop and Processing (PMS)

**Entrypoint:** e.g. `pms/main.py` → `run_pms_loop()`.

1. **Bootstrap:** Redis client, Postgres connection (or callable), config (interval, which accounts).
2. **Loop (each iteration or timer):**
   - **Read:** Positions and balances from Redis (OMS account store) and Postgres (balances).
   - **Calculate:** Realized PnL (from fills or cached), unrealized PnL (mark price if available), margin.
   - **Optional periodic rebuild:** Recompute positions from **orders** (terminal orders, sum executed_qty by account/symbol/side); compare to Redis positions; run **repairs** if drift (update or flag).
   - **Write:** Update Redis cache (`pnl:{account_id}`, `margin:{account_id}`); optionally write Postgres `pnl_snapshots` / `margin_snapshots`.
3. **Periodic repairs:** Same loop or separate interval: rebuild-from-orders, diff, fix/flag (see **docs/pms/PMS_DATA_FLOWS.md**).

Blocking vs polling: PMS is **timer-driven** (no Redis stream consumer in minimal design). For event-driven behavior later, could subscribe to Redis keyspace or a small “tick” stream on fill.

---

## 7. Configuration (Environment)

| Variable | Purpose |
|----------|--------|
| REDIS_URL | Redis connection (read cache from Booking; write PnL/margin). |
| DATABASE_URL | Postgres; read orders, fills, positions, balances; write snapshots. |
| PMS_TICK_INTERVAL_SECONDS | How often to run read → calculate → write (e.g. 5). |
| PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS | How often to rebuild positions from orders and run repairs (e.g. 60 or 300). |
| PMS_MARK_PRICE_SOURCE | Optional: `binance` REST/WS or `market_data` (Phase 4); else unrealized PnL zero. |

---

## 8. File Map (Concise)

| Area | Files (suggested) |
|------|-------------------|
| Entry & loop | `pms/main.py` |
| Read positions/balances | `pms/read_store.py` or `storage/` |
| Rebuild from orders | `pms/rebuild_from_orders.py` |
| PnL / margin calculation | `pms/calculations.py` |
| Repairs (position drift) | `pms/repair.py` |
| Redis cache read/write | `pms/cache.py` (align with OMS account store key layout) |
| Snapshots write | `pms/snapshots.py` |
| Logging | `pms/log.py` |

---

## 9. Account Data and Session Dependency

Whether **account data management** (or session) must be built **before** PMS:

- **Accounts table:** Already part of Phase 2 OMS schema (§3.1). PMS only needs **account_id** to scope reads/writes (e.g. which accounts to compute PnL for). No separate “account service” is required for Phase 2; a **default account** or env-based account list is enough.
- **Session:** No session is required to run PMS. It is a background loop that runs per process; “session” in the sense of user login is an Admin/Phase 3 concern. If “session” means “which broker/account is active,” that is already encoded in **account_id** and the **accounts** table; PMS can iterate over all accounts or a configured subset.

**Recommendation:** You do **not** need to build a dedicated account data management service or session layer beforehand. Ensure the **accounts** table exists (OMS schema 12.2.1) and at least one row (e.g. seed default account); PMS can then run and scope by account_id. See **docs/pms/PMS_ACCOUNT_AND_SESSION.md** for details.

---

## 10. References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS data flow, sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §3 Postgres schema, §8 PMS, §12.3 PMS task checklist.
- **docs/pms/PMS_DATA_MODEL.md** — Symbol vs asset, building positions, reconciliation.
- **docs/oms/OMS_BLOCKING_EXPLAINED.md** — Event-driven vs polling (PMS uses timer-based polling unless extended).
