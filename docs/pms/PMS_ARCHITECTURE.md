# PMS Architecture: Data Flow, Interfaces & Design

Single reference for the Portfolio Management System (PMS) and Position Keeper: data flow, Redis/Postgres interfaces, and alignment with OMS/Booking. Follows the same patterns as **OMS** (periodic sync, event-driven updates, data fixes). See **docs/PHASE2_DETAILED_PLAN.md** §8 for the Position Keeper role.

---

## 1. High-Level Data Flow

```
┌──────────────┐     oms_fills       ┌──────────────┐
│     OMS      │ ─────────────────►  │   Booking    │
└──────────────┘   Redis Stream      └──────┬───────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
                    ▼                      ▼                      ▼
           ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
           │ Postgres      │      │ Redis cache   │      │ Postgres      │
           │ fills         │      │ positions:*   │      │ positions     │
           │ positions     │      │ balance:*     │      │ balances      │
           │ balances      │      │ margin:*      │      │ (accounts)     │
           └───────┬───────┘      └───────┬───────┘      └───────┬───────┘
                   │                      │                      │
                   │     ┌────────────────┴────────────────┐     │
                   │     │                                 │     │
                   ▼     ▼                                 ▼     ▼
           ┌─────────────────────────────────────────────────────────────┐
           │                  Position Keeper (PMS)                      │
           │  - Read: orders, fills, positions, balances (PG + Redis)     │
           │  - Compute: realized PnL (fills), unrealized (mark), margin │
           │  - Positions: primary from positions table; rebuild from     │
           │    orders table for reconciliation / audit                 │
           │  - Write: pnl_snapshots / margin_snapshots, Redis cache     │
           └─────────────────────────────────────────────────────────────┘
```

- **Upstream:** Booking consumes `oms_fills`, writes **fills**, **positions**, **balances** (Postgres + Redis cache). OMS syncs **orders** to Postgres.
- **Position Keeper:** Reads Postgres (orders, fills, positions, balances) and/or Redis cache; aggregates PnL and margin; optionally recomputes positions from **orders** for consistency; writes snapshots and cache.

---

## 2. Position Keeper Role (Phase 2)

From **docs/PHASE2_DETAILED_PLAN.md** §8:

- **Periodically** (e.g. every few seconds) read **positions** and **balances** from Postgres or from Redis cache.
- Compute **realized PnL** (from fills), **unrealized PnL** (from current mark price if available), and **margin** (for futures).
- Write results to Postgres (`margin_snapshots` or dedicated `pnl_snapshots` table) and/or to Redis (e.g. `pnl:{account_id}`, `margin:{account_id}`) for Admin/UI.

**Using the orders table for positions:**

- **Primary source** for live view: **positions** table (maintained by Booking from fills). Fast reads from Redis cache when available.
- **Reconciliation / audit:** Position Keeper can **recompute** positions from the **orders** table (e.g. net executed quantity per account/symbol/side from terminal orders). Use this for:
  - Periodic **rebuild** to detect drift vs Booking-maintained positions.
  - **Data fixes:** Correct positions table when drift is found (or flag for manual repair).
- **Consistency:** Orders table is the audit trail (OMS sync); positions derived from orders are the canonical reference for reconciliation.

---

## 3. Data Flows (OMS-Aligned)

Same conceptual patterns as OMS:

| Pattern | OMS | PMS / Position Keeper |
|--------|-----|------------------------|
| **Event-driven** | Fill callback → sync_one_order, produce oms_fills | Booking writes on fill → Position Keeper can react to Redis cache updates or run on interval (no direct stream consumer in minimal design). |
| **Periodic sync** | sync_terminal_orders every N seconds | Periodic read of positions/balances; optional periodic **rebuild** of position view from orders. |
| **Triggered sync** | On terminal status → sync_one_order | On-demand or timer: recompute from orders and compare to positions table. |
| **Data fixes** | run_all_repairs (Binance payload → order columns) | **Repairs:** Rebuild positions from orders; fix/flag positions or fills when drift detected. |

See **docs/pms/PMS_DATA_FLOWS.md** for detailed flow descriptions.

---

## 4. Redis Key Layout (PMS / Position Keeper)

**Reads** (written by Booking; schema must match Booking):

| Key pattern | Type | Purpose |
|-------------|------|--------|
| `positions:{account_id}` | Hash / JSON | Current positions (symbol, side, quantity, entry_price_avg). |
| `balance:{account_id}:{asset}` | String / Hash | Available and locked balance per asset. |
| `margin:{account_id}` | Hash / JSON | Margin snapshot (optional; futures). |

**Writes** (Position Keeper):

| Key pattern | Type | Purpose |
|-------------|------|--------|
| `pnl:{account_id}` | Hash / JSON | Realized PnL, unrealized PnL, timestamp. |
| `margin:{account_id}` | Hash / JSON | Total margin, available balance (if not fully owned by Booking). |

Document exact value format in **position_keeper** (and align with Booking cache schema from Phase 2 task 12.2.3).

---

## 5. Postgres Schema (Reads & Writes)

**Position Keeper reads** (no schema ownership; tables from Booking/OMS):

- **accounts** — id, name, broker, env, config (for account list and config).
- **orders** — For reconciliation: aggregate executed_qty by account_id, symbol, side to derive net position; compare to positions table.
- **fills** — For realized PnL and audit.
- **positions** — Primary source for current positions (per account, symbol, side).
- **balances** — Current balances per account/asset.

**Position Keeper writes:**

- **margin_snapshots** (optional) — account_id, total_margin, available_balance, timestamp.
- **pnl_snapshots** (optional dedicated table) — account_id, realized_pnl, unrealized_pnl, mark_price_used, timestamp.

---

## 6. Main Loop and Processing (Position Keeper)

**Entrypoint:** e.g. `position_keeper/main.py` → `run_position_keeper_loop()`.

1. **Bootstrap:** Redis client, Postgres connection (or callable), config (interval, which accounts).
2. **Loop (each iteration or timer):**
   - **Read:** Positions and balances from Postgres or Redis cache (by account).
   - **Calculate:** Realized PnL (from fills or cached), unrealized PnL (mark price if available), margin.
   - **Optional periodic rebuild:** Recompute positions from **orders** (terminal orders, sum executed_qty by account/symbol/side); compare to positions table; run **repairs** if drift (update positions or flag).
   - **Write:** Update Redis cache (`pnl:{account_id}`, `margin:{account_id}`); optionally write Postgres `pnl_snapshots` / `margin_snapshots`.
3. **Periodic repairs:** Same loop or separate interval: rebuild-from-orders, diff, fix/flag (see **docs/pms/PMS_DATA_FLOWS.md**).

Blocking vs polling: Position Keeper is **timer-driven** (no Redis stream consumer in minimal design). For event-driven behavior later, could subscribe to Redis keyspace or a small “tick” stream produced by Booking on fill.

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
| Entry & loop | `position_keeper/main.py` |
| Read positions/balances | `position_keeper/read_store.py` or `storage/` |
| Rebuild from orders | `position_keeper/rebuild_from_orders.py` |
| PnL / margin calculation | `position_keeper/calculations.py` |
| Repairs (position drift) | `position_keeper/repair.py` |
| Redis cache read/write | `position_keeper/cache.py` or reuse Booking key layout in read_store |
| Snapshots write | `position_keeper/snapshots.py` |
| Logging | `position_keeper/log.py` |

---

## 9. Account Data and Session Dependency

Whether **account data management** (or session) must be built **before** Position Keeper:

- **Accounts table:** Already part of Phase 2 Booking schema (§3.1). Position Keeper only needs **account_id** to scope reads/writes (e.g. which accounts to compute PnL for). No separate “account service” is required for Phase 2; a **default account** or env-based account list is enough.
- **Session:** No session is required to run Position Keeper. It is a background loop that runs per process; “session” in the sense of user login is an Admin/Phase 3 concern. If “session” means “which broker/account is active,” that is already encoded in **account_id** and **accounts** table; Position Keeper can iterate over all accounts or a configured subset.

**Recommendation:** You do **not** need to build a dedicated account data management service or session layer beforehand. Ensure the **accounts** table exists (Booking schema 12.2.1) and at least one row (e.g. seed default account); Position Keeper can then run and scope by account_id. See **docs/pms/PMS_ACCOUNT_AND_SESSION.md** for details.

---

## 10. References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS data flow, sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §3 Postgres schema, §7 Booking, §8 Position Keeper, §12.3 Position Keeper task checklist.
- **docs/oms/OMS_BLOCKING_EXPLAINED.md** — Event-driven vs polling (Position Keeper uses timer-based polling unless extended).
