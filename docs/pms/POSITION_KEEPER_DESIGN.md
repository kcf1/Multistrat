# PMS: Orders-Based Positions and Rebuild Design

This document details how **PMS** uses the **orders** table to compute and reconcile positions, and how it fits with OMS. See **docs/PHASE2_DETAILED_PLAN.md** §8 and **docs/pms/PMS_ARCHITECTURE.md**.

---

## 1. Role Summary

- **Read:** Positions and balances (Redis from OMS account store; Postgres balances); optionally **orders** and **fills** for reconciliation and PnL.
- **Compute:** Realized PnL (from fills), unrealized PnL (from mark price), margin (futures).
- **Positions:** Primary view from **Redis** (OMS account store); **recompute from orders** for reconciliation and repairs.
- **Write:** PnL/margin snapshots to Postgres and/or Redis for Admin/UI.

PMS does **not** write to `orders` or `fills`; those are owned by OMS. It may **update** Redis positions (OMS account store) when running **repairs** (drift correction from orders).

---

## 2. Using the Orders Table for Positions

### 2.1 Why Orders?

- **orders** is the audit trail: every executed order is synced by OMS with `executed_qty`, `price`, `status`, etc.
- **positions** is maintained by Booking from `oms_fills`. If a fill is missed, applied twice, or Booking has a bug, positions can drift from what orders imply.
- **Recomputing** net position from orders gives a **canonical** view: for each (account_id, symbol, side), sum of `executed_qty` over filled orders.

### 2.2 Derived Position from Orders

**Formula (conceptual):**

- For **spot:** One position per (account_id, symbol): net quantity = sum of (executed_qty for BUY) − sum of (executed_qty for SELL). Often stored as long-only quantity with sign or as two “sides.”
- For **futures:** One row per (account_id, symbol, side) with quantity; net = long_qty − short_qty per symbol.

**SQL (expected net from orders):**

```sql
SELECT account_id, symbol, side,
       SUM(COALESCE(executed_qty, 0)) AS expected_qty
FROM orders
WHERE status = 'filled'
GROUP BY account_id, symbol, side;
```

**Entry price:** Positions table usually stores `entry_price_avg`. From orders alone you have per-order `price` (executed). Optionally:

- Use **fills** table: average of (price * quantity) / sum(quantity) per (account_id, symbol, side) for realized cost basis.
- Or derive from orders: weighted average of order `price` by `executed_qty` for filled orders.

PMS can implement a function **expected_positions_from_orders(pg_connect)** that returns a list of (account_id, symbol, side, expected_qty, optional expected_entry_avg).

### 2.3 Reconcile vs Redis Positions

- **Read** current positions from Redis (OMS account store).
- **Compare** (account_id, symbol, side): expected_qty vs current quantity (and optionally entry_price).
- **Drift:** expected ≠ current → run **repair** (update Redis positions) or **flag** (write to drift table / log).

This is the same idea as OMS **repairs**: fix derived state (positions) from canonical state (orders).

---

## 3. Data Flow Summary

| Step | Source | Action |
|------|--------|--------|
| Live view | Redis positions (OMS account store) | Read by account; use for PnL/margin calculation on each tick. |
| Realized PnL | fills table (or cached) | Sum (price * qty) or use fill-level PnL if stored. |
| Unrealized PnL | positions + mark price | (mark - entry_avg) * quantity (simplified; futures may use margin/PnL API). |
| Margin | positions + balances / broker | From Redis or REST if futures. |
| Rebuild | orders table | Periodic: expected_positions_from_orders → compare to Redis → repair or flag. |

---

## 4. Repairs (Position Drift)

**Input:** Expected positions from orders (and optionally fills for entry_avg).

**Logic:**

1. For each (account_id, symbol, side) in expected:
   - If no entry in Redis positions: add (or treat as 0 and add if expected_qty ≠ 0).
   - If entry exists and quantity ≠ expected_qty: UPDATE quantity (and optionally entry_price) in Redis; log.
2. For each entry in Redis positions with no expected (expected_qty = 0): remove or set quantity to 0 (policy choice).
3. Optionally write to `position_drift_log` (account_id, symbol, side, old_qty, new_qty, source = 'orders_rebuild') for audit.

**When:** After each periodic rebuild-from-orders run (see **docs/pms/PMS_DATA_FLOWS.md**).

---

## 5. Mark Price (Unrealized PnL)

Phase 2 plan (§8.2): Mark price can come from Binance (REST or websocket) or from Market Data (Phase 4). For Phase 2, optional: fetch in PMS or leave unrealized PnL zero until Phase 4.

- **Option A:** PMS calls Binance REST (or existing client) for mark price per symbol when computing unrealized PnL.
- **Option B:** Unrealized PnL = 0 until Market Data service exists; only realized PnL and margin are shown.

---

## 6. Task Checklist Mapping (Phase 2 §12.3)

| Task | Description | Orders usage |
|------|-------------|--------------|
| 12.3.1 | PnL/margin calculation | Realized from fills; unrealized from mark; margin from positions/balances. |
| 12.3.2 | Postgres reads | positions, balances, fills; **add** orders for rebuild. |
| 12.3.3 | Postgres read queries | Standard queries + **expected_positions_from_orders**. |
| 12.3.4–12.3.5 | Redis cache read | positions:{account_id}, balance:{account_id}:{asset} (Booking schema). |
| 12.3.6–12.3.7 | PnL/margin snapshot writes | Redis + optional pnl_snapshots / margin_snapshots. |
| 12.3.8 | Loop | Timer: read → calculate → write; **optional** periodic rebuild + repairs. |

**New/optional tasks (PMS plan):**

- **Rebuild from orders:** Implement expected_positions_from_orders; run periodically.
- **Repairs:** Implement position drift detection and update (or flag); run after rebuild.

---

## 7. References

- **docs/PHASE2_DETAILED_PLAN.md** — §8 Position Keeper, §3.1 orders/positions schema.
- **docs/pms/PMS_ARCHITECTURE.md** — Data flow, Redis/Postgres, account/session.
- **docs/pms/PMS_DATA_FLOWS.md** — Periodic rebuild, event-driven, data fixes.
