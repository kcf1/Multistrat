# PMS: Orders-Based Positions and Rebuild Design

This document details how **PMS** uses the **orders** and **balance_changes** tables to compute positions at **asset** grain, and how it fits with OMS. See **docs/PHASE2_DETAILED_PLAN.md** §8 and **docs/pms/PMS_ARCHITECTURE.md**.

---

## 1. Role Summary

- **Read:** **Postgres only** — orders, balance_changes, symbols, assets. **No fills table;** orders are the primary data source. **PMS does not read OMS Redis positions** (that is dummy).
- **Compute:** Realized PnL (from executed order rows), unrealized PnL (mark price × signed open_qty when open_qty ≠ 0; flattened positions have no unrealized), margin (futures).
- **Positions:** **Derived from orders (base + quote legs) and balance_changes** at grain **(account_id, book, asset)**. Filter orders by status: **partially_filled**, **filled**. Each order contributes two legs (base asset, quote asset); balance_changes add deposit/withdrawal/transfer deltas.
- **Write:** Positions to Postgres; PnL/margin snapshots to Postgres and/or Redis for Admin/UI.

PMS does **not** write to `orders`. PMS does **not** use or write a fills table. PMS does **not** read or update OMS Redis positions.

---

## 2. Using the Orders Table for Positions

### 2.1 Why Orders?

- **orders** is the audit trail: every executed order is synced by OMS with `executed_qty`, `price`, `status`, etc.
- **positions** is maintained by PMS from orders + balance_changes. Recomputing from orders (and balance_changes) gives a **canonical** view.
- **Grain:** (account_id, **book**, **asset**). Each **asset** (e.g. BTC, USDT) has one position per (account_id, book). Orders are converted to **base** and **quote** legs via the **symbols** table (symbol → base_asset, quote_asset). See **docs/pms/PMS_ARCHITECTURE.md** §6–7 (granular store, reference data / capital by book).

### 2.2 Position build formula

**Final position per (account_id, book, asset):**

```
open_qty(account_id, book, asset) = order_derived_open_qty + SUM(balance_changes.delta)
```

- **order_derived_open_qty:** For each order (status partially_filled or filled), resolve symbol → (base_asset, quote_asset) from **symbols**. **Base leg:** +executed_qty (BUY) or −executed_qty (SELL) on base_asset. **Quote leg:** −(executed_qty × price) or use binance_cumulative_quote_qty (BUY = spend quote), +(same) for SELL. Group lots by (account_id, book, asset); run FIFO per asset to get open_qty and entry_avg.
- **balance_changes:** Filter by change_type IN (deposit, withdrawal, adjustment, transfer). Sum delta per (account_id, book, asset). Add this net to order_derived_open_qty to get final open_qty.
- **position_side:** 'long' when open_qty > 0, 'short' when open_qty < 0, 'flat' when open_qty = 0.
- **entry_avg:** Cost basis of the **open** quantity from order-derived FIFO only; balance_changes do not change entry_avg.

### 2.3 Derived Position from Orders (legs + balance_changes)

- **open_qty** (signed): positive = net long, negative = net short, zero = flat.
- **position_side**: 'long' | 'short' | 'flat' — derived from sign of open_qty (shows net long/short for display and queries).

**Entry average (entry_avg):** Cost basis of the **open** quantity from order-derived FIFO only (balance_changes do not affect entry_avg). For unrealized PnL: (mark_price − entry_avg) × open_qty (signed); **only when open_qty ≠ 0**. Flattened positions have no unrealized (PnL already realized).

PMS implements **derive_positions_from_orders_and_balance_changes(pg_connect)** that returns (account_id, book, asset, open_qty, position_side, entry_avg). Grain (account_id, book, asset) with signed open_qty and position_side supports capital-by-book and on-request grouping.

### 2.4 Reconcile (optional)

- **Current view:** Order-derived positions (plus balance_changes) at **asset** grain are the canonical view for PMS (PMS does not read OMS Redis positions).
- **Optional:** If PMS or another system maintains a separate position store, compare expected_qty (from orders) to that store and **flag** or repair drift. PMS does not update OMS Redis.

---

## 3. Data Flow Summary

| Step | Source | Action |
|------|--------|--------|
| Position view | **Orders + balance_changes** (derived) | Filter orders status partially_filled/filled; convert to **base/quote legs** per (account, book, **asset**); add balance_changes net delta; position_side = long/short/flat. Grain (account_id, book, asset). |
| Realized PnL | **Orders** (executed rows) | From order-level executed_qty and price (the part that has been closed/flattened). |
| Unrealized PnL | order-derived **open** positions + mark price | (mark - entry_avg) × **open_qty** (signed) when open_qty ≠ 0; flattened positions: no unrealized (PnL already realized). |
| Margin | order-derived positions + balances / broker | Balances from Postgres; positions from orders + balance_changes at asset grain. |
| Rebuild | orders + balance_changes | Periodic: derive_positions_from_orders_and_balance_changes; optional compare/flag if another store exists. |

---

## 4. Repairs (Position Drift)

**Input:** Expected positions from orders (base/quote legs) + balance_changes; filter orders partial/fully filled; entry_avg = cost basis of open qty from FIFO.

**Logic:** Order-derived positions are the canonical view. If PMS (or another system) maintains a separate position store (e.g. future Postgres positions table), repairs can update that store from expected values or **flag** drift in a log. **PMS does not read or update OMS Redis positions** (dummy).

**When:** After each periodic rebuild-from-orders run, if a separate position store exists (see **docs/pms/PMS_DATA_FLOWS.md**).

---

## 5. Mark Price and USD Valuation

**USD valuation via `assets` table:** PMS values positions in USD using the **assets** table. For each asset (e.g. USDT, USDC), if **usd_price** is set (e.g. 1 for stables), that value is used for mark and notional; unrealized_pnl = 0 for stables. For other assets, **usd_symbol** can point to a trading pair (e.g. BTCUSDT) for future mark-price fetch; see **docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md** §5. Stables-first: only assets with **usd_price** in the table get notional/unrealized in USD in the loop.

**Mark price provider (interface):**

1. **Define a mark price provider interface** (e.g. `get_mark_prices(symbols) -> dict[symbol, price]`). PMS uses this contract when computing unrealized PnL for assets that have **usd_symbol** (symbol-based fetch); for assets with **usd_price** in the assets table, PMS uses that value directly. No direct Binance or Redis calls in calculation code for stables.
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
