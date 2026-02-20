# PMS Data Flows: Periodic Rebuild, Event-Driven, Data Fixes

This document details the data flows for the **Portfolio Management System (PMS)**, aligned with OMS patterns: **periodic sync**, **event-driven** behavior where applicable, and **data fixes** (repairs). It also defines how the **orders** table is used to compute or reconcile positions.

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

## 2. PMS: Primary Data Source for Positions

- **Source for “current positions”:** PMS **derives** positions from the **orders** table only (no fills table). Filter by status **partially_filled**, **filled**; at grain (account_id, **book**, symbol) compute **signed net**: open_qty = sum(BUY executed_qty) − sum(SELL executed_qty); **position_side** = 'long' | 'short' | 'flat'; **entry_avg** = cost basis of open quantity only (FIFO). **PMS does not read OMS Redis positions** (that path is dummy). Book in grain supports capital-by-book and grouping.
- **Balances:** Read from Postgres (OMS account sync). PMS does not read OMS Redis for positions or balances.

So:

- **Position view** → derived from orders table only (filter partial/fully filled; no fills table).
- **Balance view** → Postgres balances.
- **Reconciliation and repairs** → optional: compare order-derived positions to other sources (e.g. balance_changes) or flag drift.

---

## 3. Periodic Rebuild (From Orders)

**What:** Periodically recompute per-account, per-symbol **signed net position** from the **orders** table (open_qty = sum(BUY) − sum(SELL); position_side = long/short/flat). This is the canonical position view for PMS (no comparison to OMS Redis — PMS does not read OMS Redis positions).

**Why:** Detects drift (e.g. missed fill, duplicate apply, or Booking bug). Keeps positions consistent with the audit trail (orders).

**How:**

1. **Query orders:** Filter by status **partially_filled**, **filled**: compute signed net per (account_id, book, symbol): open_qty = sum(BUY executed_qty) − sum(SELL executed_qty); derive position_side ('long' | 'short' | 'flat').
2. **Map to “expected” positions:** Convert to the same shape as the granular store (account_id, book, symbol, open_qty, position_side, entry_avg). **entry_avg** = cost basis of the **open** (net) quantity only (FIFO from orders; no fills table).
3. **Use order-derived positions** as the current view (PMS does not read from OMS Redis).
4. **Compare (optional):** If another source of positions exists (e.g. future Postgres positions table), compare expected quantity vs that source.
5. **Repair or flag:** If drift: **flag** (write to a reconciliation table or log for manual review) or update the other source if PMS owns it.

**When:** Every `PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS` (e.g. 60–300s), inside the same loop as PnL/margin tick or in a separate periodic task.

**Alignment with OMS:** Analogous to OMS **periodic** `sync_terminal_orders`: a full pass to bring persistent state in line with the canonical source.

---

## 4. Event-Driven Behavior

**Current (minimal) design:** PMS does **not** consume a Redis stream and **does not read OMS Redis positions** (dummy). It runs on a **timer** (e.g. every 5s): read orders/balances from Postgres → derive positions from orders → calculate PnL/margin → write snapshots/cache.

**Optional extensions:**

- **Redis pub/sub or stream:** A thin service (or OMS) publishes a message on each fill (e.g. `pms_tick`). PMS subscribes and runs one read→calculate→write cycle on each message (in addition to or instead of timer).

So: **no session or event-driven PMS consumer is required** for Phase 2; timer-based flow is sufficient and matches “periodic” in the OMS sense.

---

## 5. Data Fixes (Repairs)

**What:** When PMS has another store of positions (e.g. a future Postgres positions table it owns), correct it from **order-derived** expected positions when drift is found. (PMS does not read or update OMS Redis positions — dummy.)

**Why:** Orders table is the canonical source; any derived position store can be repaired from it.

**How:**

1. Run **periodic rebuild** (see §3) to get expected positions from orders.
2. If another position store exists and current ≠ expected:
   - **Option A (auto-repair):** UPDATE that store to expected values; optionally log to an audit table.
   - **Option B (flag only):** Insert into `position_drift` or log for manual review.
3. Optionally: run repairs only when drift is above a threshold (e.g. quantity difference > 0) or after a cooldown to avoid flapping.

**When:** Same interval as rebuild (e.g. after each rebuild-from-orders run), or on a separate, slower schedule.

**Isolation:** One repair per (account, book, symbol) or one repair function per “kind” (e.g. `repair_positions_from_orders`), similar to OMS `repair_binance_*_from_payload`.

---

## 6. Flow Summary Table

| Flow | Trigger | Action | Purpose |
|------|--------|--------|--------|
| **Timer (PnL/margin)** | Every `PMS_TICK_INTERVAL_SECONDS` | Read orders/balances from PG → derive positions from orders → compute PnL/margin → write Redis + optional PG snapshots. (PMS does not read OMS Redis.) | Serve current PnL/margin to Admin/UI. |
| **Periodic rebuild** | Every `PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS` | Recompute positions from orders (canonical view for PMS) | Keep position view consistent with orders. |
| **Data fixes (repairs)** | After rebuild (or separate interval) | Where drift in another store: update or flag from orders-derived expected state | Correct drift; orders are audit trail. |
| **Event-driven (optional)** | On fill (Redis msg) | One tick: read → derive → calculate → write | Lower latency; optional for Phase 2. |

---

## 7. Orders Table Usage (Concrete)

**Query pattern for “expected” net position from orders:**

- **Filter:** `status IN ('partially_filled', 'filled')` — orders only; no fills table.
- **Signed net** per (account_id, book, symbol): open_qty = sum(BUY executed_qty) − sum(SELL executed_qty); **position_side** = 'long' | 'short' | 'flat' from sign of open_qty.
- **entry_avg** = cost basis of the **open** (net) quantity only (FIFO; from orders; do not average over all orders). **Unrealized PnL** when open_qty ≠ 0 (signed open_qty).

**Example (conceptual):**

```sql
-- Expected signed net position per account/book/symbol from orders (no fills table)
SELECT account_id, book, symbol,
       SUM(CASE WHEN side = 'BUY' THEN COALESCE(executed_qty, 0) ELSE -COALESCE(executed_qty, 0) END) AS open_qty
FROM orders
WHERE status IN ('partially_filled', 'filled')
GROUP BY account_id, book, symbol;
-- Then derive position_side: 'long' when open_qty > 0, 'short' when open_qty < 0, 'flat' when open_qty = 0.
```

Order-derived positions are the canonical view for PMS. Optionally compare with another source (e.g. balance_changes) for reconciliation; difference → flag or repair that source.

---

## 8. References

- **docs/oms/OMS_ARCHITECTURE.md** — Sync (trigger + periodic), repairs, main loop.
- **docs/pms/PMS_ARCHITECTURE.md** — Position Keeper role, Redis/Postgres, account/session.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 Position Keeper, §12.3 task checklist.
