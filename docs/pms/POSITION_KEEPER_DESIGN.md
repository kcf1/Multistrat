# PMS: Orders-Based Positions and Rebuild Design

This document details how **PMS** uses the **orders** table to compute and reconcile positions, and how it fits with OMS. See **docs/PHASE2_DETAILED_PLAN.md** §8 and **docs/pms/PMS_ARCHITECTURE.md**.

---

## 1. Role Summary

- **Read:** **Postgres only** — orders, balances. **No fills table;** orders are the primary data source. **PMS does not read OMS Redis positions** (that is dummy).
- **Compute:** Realized PnL (from executed order rows), unrealized PnL (mark price × signed open_qty when open_qty ≠ 0; flattened positions have no unrealized), margin (futures).
- **Positions:** **Derived from the orders table only** (filter by status: **partially_filled**, **filled**). No read from OMS Redis.
- **Write:** PnL/margin snapshots to Postgres and/or Redis for Admin/UI.

PMS does **not** write to `orders`. PMS does **not** use or write a fills table. PMS does **not** read or update OMS Redis positions.

---

## 2. Using the Orders Table for Positions

### 2.1 Why Orders?

- **orders** is the audit trail: every executed order is synced by OMS with `executed_qty`, `price`, `status`, etc.
- **positions** is maintained by Booking from `oms_fills`. If a fill is missed, applied twice, or Booking has a bug, positions can drift from what orders imply.
- **Recomputing** net position from orders gives a **canonical** view: for each (account_id, **book**, symbol), **signed net** = sum(executed_qty for BUY) − sum(executed_qty for SELL). Including **book** supports capital-by-book and grouping by book/symbol. See **docs/pms/PMS_ARCHITECTURE.md** §6–7 (granular store, reference data / capital by book).

### 2.2 Derived Position from Orders (signed approach)

**Formula:** One position per (account_id, book, symbol) with **signed** quantity. Closing orders offset opens; flattened position has open_qty = 0.

- **open_qty** (signed): positive = net long, negative = net short, zero = flat.
- **position_side**: 'long' | 'short' | 'flat' — derived from sign of open_qty (shows net long/short for display and queries).

**SQL (expected net from orders):** Filter by **partially_filled** and **filled**; net BUY − SELL per (account_id, book, symbol):

```sql
SELECT account_id, book, symbol,
       SUM(CASE WHEN side = 'BUY' THEN COALESCE(executed_qty, 0) ELSE -COALESCE(executed_qty, 0) END) AS open_qty
FROM orders
WHERE status IN ('partially_filled', 'filled')
GROUP BY account_id, book, symbol;
```

Then derive **position_side**: 'long' when open_qty > 0, 'short' when open_qty < 0, 'flat' when open_qty = 0.

**Entry average (entry_avg):** Cost basis of the **open** (net) quantity only — use FIFO over orders so closed chunks are excluded. For unrealized PnL: (mark_price − entry_avg) × open_qty (signed); **only when open_qty ≠ 0**. Flattened positions have no unrealized (PnL already realized).

PMS can implement **expected_positions_from_orders(pg_connect)** that returns (account_id, book, symbol, open_qty, position_side, entry_avg). Grain (account_id, book, symbol) with signed open_qty and position_side supports capital-by-book and on-request grouping.

### 2.3 Reconcile (optional)

- **Current view:** Order-derived positions are the canonical view for PMS (PMS does not read OMS Redis positions).
- **Optional:** If PMS or another system maintains a separate position store, compare expected_qty (from orders) to that store and **flag** or repair drift. PMS does not update OMS Redis.

---

## 3. Data Flow Summary

| Step | Source | Action |
|------|--------|--------|
| Position view | **Orders table** (derived) | Filter status partially_filled/filled; **signed net** per (account, book, symbol); position_side = long/short/flat. No fills table. PMS does not read OMS Redis positions. |
| Realized PnL | **Orders** (executed rows) | From order-level executed_qty and price (the part that has been closed/flattened). |
| Unrealized PnL | order-derived **open** positions + mark price | (mark - entry_avg) × **open_qty** (signed) when open_qty ≠ 0; flattened positions: no unrealized (PnL already realized). |
| Margin | order-derived positions + balances / broker | Balances from Postgres; positions from orders. |
| Rebuild | orders table | Periodic: expected_positions_from_orders (filter partial/fully filled); optional compare/flag if another store exists. |

---

## 4. Repairs (Position Drift)

**Input:** Expected positions from orders only (filter partial/fully filled; entry_avg = cost basis of open qty).

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
| 12.3.1 | PnL/margin calculation | Realized from orders (executed rows); unrealized from mark × signed open qty when open_qty ≠ 0; margin from positions/balances. |
| 12.3.2 | Postgres reads | orders (primary), balances, accounts; no fills table. |
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
