# PMS Data Flows: Periodic Rebuild, Event-Driven, Data Fixes

This document details the data flows for the Portfolio Management System (Position Keeper), aligned with OMS patterns: **periodic sync**, **event-driven** behavior where applicable, and **data fixes** (repairs). It also defines how the **orders** table is used to compute or reconcile positions.

---

## 1. OMS Pattern Recap

| Pattern | OMS Implementation | Purpose |
|--------|--------------------|--------|
| **Event-driven** | XREADGROUP BLOCK on `risk_approved`; fill callback on terminal → sync_one_order, produce oms_fills | Low latency; react as soon as data is available. |
| **Periodic sync** | `sync_terminal_orders(redis, store, pg_connect)` every `sync_interval_seconds` | Ensure Postgres `orders` is up to date; catch any missed trigger syncs. |
| **Triggered sync** | On terminal status (fill callback or reject path) → `sync_one_order` | Immediate write to Postgres when order completes. |
| **Data fixes** | `run_all_repairs(pg_connect)` after sync: fix order columns from `payload` (e.g. price, time_in_force, binance_cumulative_quote_qty) | Correct flawed or missing fields from authoritative source (broker payload). |

PMS applies the same ideas: **primary view** from one store (positions table), **periodic full refresh** from the canonical source (orders table), **triggered or timer-based** updates, and **repairs** when derived state drifts.

---

## 2. Position Keeper: Primary Data Source vs Orders

- **Primary source for “current positions”:** The **positions** table (and Redis cache) maintained by **Booking** from `oms_fills` (fill events). Position Keeper **reads** this for low-latency PnL/margin.
- **Canonical / audit source:** The **orders** table (OMS sync). Net position can be **recomputed** from orders as: for each (account_id, symbol, side), sum `executed_qty` over terminal orders (e.g. status in `filled`, `partially_filled` with final state, or only `filled` depending on how you treat partials).

So:

- **Live view** → positions table (and Redis cache).
- **Reconciliation and repairs** → recompute from orders, compare to positions, fix or flag drift.

---

## 3. Periodic Rebuild (From Orders)

**What:** Periodically recompute per-account, per-symbol (and per-side for futures) **net position** from the **orders** table, then compare to the **positions** table.

**Why:** Detects drift (e.g. missed fill, duplicate apply, or Booking bug). Keeps positions consistent with the audit trail (orders).

**How:**

1. **Query orders:** e.g. `SELECT account_id, symbol, side, SUM(executed_qty) AS net_qty FROM orders WHERE status IN ('filled', ...) GROUP BY account_id, symbol, side`.
2. **Map to “expected” positions:** Convert to the same shape as the positions table (account_id, symbol, side, quantity, optionally entry_price from fills or orders).
3. **Read current positions** from Postgres (or Redis).
4. **Compare:** For each (account_id, symbol, side), compare expected quantity vs current position quantity.
5. **Repair or flag:** If drift:
   - **Repair:** Update positions table (and optionally Redis cache) to expected values; or
   - **Flag:** Write to a reconciliation table or log for manual review.

**When:** Every `PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS` (e.g. 60–300s), inside the same loop as PnL/margin tick or in a separate periodic task.

**Alignment with OMS:** Analogous to OMS **periodic** `sync_terminal_orders`: a full pass to bring persistent state in line with the canonical source.

---

## 4. Event-Driven Behavior

**Current (minimal) design:** Position Keeper does **not** consume a Redis stream. It runs on a **timer** (e.g. every 5s): read positions/balances → calculate PnL/margin → write snapshots/cache. So “event-driven” here means “Booking and OMS are event-driven”; Position Keeper follows with a short delay.

**Optional extensions:**

- **Redis pub/sub or stream:** Booking (or a thin service) publishes a message on each fill (e.g. `pms_tick` or `position_updated`). Position Keeper subscribes and runs one read→calculate→write cycle on each message (in addition to or instead of timer). Same pattern as OMS waking on `risk_approved`.
- **Redis keyspace notifications:** If Booking updates `positions:{account_id}`, Position Keeper could subscribe to key events and refresh only that account. More complex; timer is usually enough for Phase 2.

So: **no session or event-driven Position Keeper is required** for Phase 2; timer-based flow is sufficient and matches “periodic” in the OMS sense.

---

## 5. Data Fixes (Repairs)

**What:** Correct **positions** (and optionally balances) when **rebuild from orders** finds drift.

**Why:** Same as OMS repairs: the derived store (positions) can be wrong; the canonical store (orders) is used to fix it.

**How:**

1. Run **periodic rebuild** (see §3) to get expected positions from orders.
2. For each (account_id, symbol, side) where current ≠ expected:
   - **Option A (auto-repair):** UPDATE positions table (and Redis cache) to expected quantity/entry_price; optionally log to an audit table.
   - **Option B (flag only):** Insert into `position_drift` or log; do not auto-update. Admin or batch job fixes later.
3. Optionally: run repairs only when drift is above a threshold (e.g. quantity difference > 0) or after a cooldown to avoid flapping.

**When:** Same interval as rebuild (e.g. after each rebuild-from-orders run), or on a separate, slower schedule.

**Isolation:** One repair per (account, symbol, side) or one repair function per “kind” (e.g. `repair_positions_from_orders`), similar to OMS `repair_binance_*_from_payload`.

---

## 6. Flow Summary Table

| Flow | Trigger | Action | Purpose |
|------|--------|--------|--------|
| **Timer (PnL/margin)** | Every `PMS_TICK_INTERVAL_SECONDS` | Read positions/balances (PG or Redis) → compute PnL/margin → write Redis + optional PG snapshots | Serve current PnL/margin to Admin/UI. |
| **Periodic rebuild** | Every `PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS` | Recompute positions from orders; compare to positions table | Keep positions consistent with orders. |
| **Data fixes (repairs)** | After rebuild (or separate interval) | Where drift: update positions (or flag) from orders-derived expected state | Correct drift; audit trail stays in orders. |
| **Event-driven (optional)** | On fill or position update (Redis msg) | One tick: read → calculate → write for affected account(s) | Lower latency; optional for Phase 2. |

---

## 7. Orders Table Usage (Concrete)

**Query pattern for “expected” net position from orders:**

- Filter: `status IN ('filled')` (and optionally handle `partially_filled` if you store final state in orders).
- Group by: `account_id`, `symbol`, `side`.
- Aggregate: `SUM(executed_qty)` for quantity; for **entry_price_avg** you can derive from **fills** (avg price per fill) or from orders (e.g. order-level price when filled).

**Example (conceptual):**

```sql
-- Expected net position per account/symbol/side from orders
SELECT account_id, symbol, side,
       SUM(COALESCE(executed_qty, 0)) AS expected_qty
FROM orders
WHERE status = 'filled'
GROUP BY account_id, symbol, side;
```

Then join or compare with `positions` (account_id, symbol, side, quantity). Difference → repair or flag.

---

## 8. References

- **docs/oms/OMS_ARCHITECTURE.md** — Sync (trigger + periodic), repairs, main loop.
- **docs/pms/PMS_ARCHITECTURE.md** — Position Keeper role, Redis/Postgres, account/session.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 Position Keeper, §12.3 task checklist.
