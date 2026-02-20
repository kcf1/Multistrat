# PMS: Orders-Based Positions and Rebuild Design

This document details how **PMS** uses the **orders** table to compute and reconcile positions, and how it fits with OMS. See **docs/PHASE2_DETAILED_PLAN.md** §8 and **docs/pms/PMS_ARCHITECTURE.md**.

---

## 1. Role Summary

- **Read:** **Postgres only** — orders, balances, optional fills. **PMS does not read OMS Redis positions** (that is dummy).
- **Compute:** Realized PnL (from fills), unrealized PnL (from mark price), margin (futures).
- **Positions:** **Derived from the orders table** (and optionally fills). No read from OMS Redis.
- **Write:** PnL/margin snapshots to Postgres and/or Redis for Admin/UI.

PMS does **not** write to `orders` or `fills`; those are owned by OMS. PMS does **not** read or update OMS Redis positions.

---

## 2. Using the Orders Table for Positions

### 2.1 Why Orders?

- **orders** is the audit trail: every executed order is synced by OMS with `executed_qty`, `price`, `status`, etc.
- **positions** is maintained by Booking from `oms_fills`. If a fill is missed, applied twice, or Booking has a bug, positions can drift from what orders imply.
- **Recomputing** net position from orders gives a **canonical** view: for each (account_id, **book**, symbol, side), sum of `executed_qty` over filled orders. Including **book** supports capital-by-book and grouping by book/symbol. See **docs/pms/PMS_ARCHITECTURE.md** §6–7 (granular store, reference data / capital by book).

### 2.2 Derived Position from Orders

**Formula (conceptual):**

- For **spot:** One position per (account_id, symbol): net quantity = sum of (executed_qty for BUY) − sum of (executed_qty for SELL). Often stored as long-only quantity with sign or as two “sides.”
- For **futures:** One row per (account_id, symbol, side) with quantity; net = long_qty − short_qty per symbol.

**SQL (expected net from orders):**

```sql
SELECT account_id, book, symbol, side,
       SUM(COALESCE(executed_qty, 0)) AS expected_qty
FROM orders
WHERE status = 'filled'
GROUP BY account_id, book, symbol, side;
```

**Entry price:** Positions table usually stores `entry_price_avg`. From orders alone you have per-order `price` (executed). Optionally:

- Use **fills** table: average of (price * quantity) / sum(quantity) per (account_id, symbol, side) for realized cost basis.
- Or derive from orders: weighted average of order `price` by `executed_qty` for filled orders.

PMS can implement a function **expected_positions_from_orders(pg_connect)** that returns a list of (account_id, book, symbol, side, expected_qty, optional expected_entry_avg). Grain (account_id, book, symbol, side) supports capital-by-book and on-request grouping.

### 2.3 Reconcile (optional)

- **Current view:** Order-derived positions are the canonical view for PMS (PMS does not read OMS Redis positions).
- **Optional:** If PMS or another system maintains a separate position store, compare expected_qty (from orders) to that store and **flag** or repair drift. PMS does not update OMS Redis.

---

## 3. Data Flow Summary

| Step | Source | Action |
|------|--------|--------|
| Position view | **Orders table** (derived) | Derive by account/symbol/side; use for PnL/margin. PMS does not read OMS Redis positions. |
| Realized PnL | fills table (or cached) | Sum (price * qty) or use fill-level PnL if stored. |
| Unrealized PnL | order-derived positions + mark price | (mark - entry_avg) * quantity (simplified; futures may use margin/PnL API). |
| Margin | order-derived positions + balances / broker | Balances from Postgres; positions from orders. |
| Rebuild | orders table | Periodic: expected_positions_from_orders; optional compare/flag if another store exists. |

---

## 4. Repairs (Position Drift)

**Input:** Expected positions from orders (and optionally fills for entry_avg).

**Logic:** Order-derived positions are the canonical view. If PMS (or another system) maintains a separate position store (e.g. future Postgres positions table), repairs can update that store from expected values or **flag** drift in a log. **PMS does not read or update OMS Redis positions** (dummy).

**When:** After each periodic rebuild-from-orders run, if a separate position store exists (see **docs/pms/PMS_DATA_FLOWS.md**).

---

## 5. Mark Price (Unrealized PnL) — Interface First, Then Implementations

**Recommended approach:**

1. **Define a mark price provider interface** (e.g. `get_mark_prices(symbols) -> dict[symbol, price]`). PMS uses only this contract when computing unrealized PnL; no direct Binance or Redis calls in calculation code.
2. **Phase 2:** Implement the interface by **wrapping Binance** (REST or WebSocket). PMS gets real mark prices and unrealized PnL without a separate Market Data service.
3. **Phase 4:** Add a second implementation that **reads from Redis or DB** (fed by the Market Data service). Same interface; switch via config (`PMS_MARK_PRICE_SOURCE`: `binance` vs `redis` or `market_data`). No change to PMS calculation logic.

Config-driven: `PMS_MARK_PRICE_SOURCE` selects which implementation to inject. If unset or unsupported, unrealized PnL can be zero.

---

## 6. Task Checklist Mapping (Phase 2 §12.3)

| Task | Description | Orders usage |
|------|-------------|--------------|
| 12.3.1 | PnL/margin calculation | Realized from fills; unrealized from mark; margin from positions/balances. |
| 12.3.2 | Postgres reads | positions, balances, fills; **add** orders for rebuild. |
| 12.3.3 | Postgres read queries | Standard queries + **expected_positions_from_orders**. |
| 12.3.4–12.3.5 | Redis | PMS does not read OMS Redis positions/balances (dummy). PMS writes PnL/margin keys only. |
| 12.3.6–12.3.7 | PnL/margin snapshot writes | Redis + optional pnl_snapshots / margin_snapshots. |
| 12.3.8 | Loop | Timer: read → calculate → write; **optional** periodic rebuild + repairs. |

**New/optional tasks (PMS plan):**

- **Rebuild from orders:** Implement expected_positions_from_orders; run periodically.
- **Repairs:** Implement position drift detection and update (or flag); run after rebuild.

---

## 7. References

- **docs/PHASE2_DETAILED_PLAN.md** — §8 PMS, §3.1 orders/schema.
- **docs/pms/PMS_ARCHITECTURE.md** — Data flow, Redis/Postgres, account/session.
- **docs/pms/PMS_DATA_FLOWS.md** — Periodic rebuild, event-driven, data fixes.
