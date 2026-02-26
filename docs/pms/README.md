# Portfolio Management System (PMS) Documentation

Plans and architecture for the **Portfolio Management System (PMS)**. PMS is the **source of truth** for PnL, margin, and **cash**. It reads from **OMS Postgres only** (orders, accounts, **balance_changes**, **symbols**). **Positions** are derived from the orders table; **cash** is **built** from orders + balance_changes (double-entry; symbol→base/quote; **balance sync to `balances` is disabled**). PMS does **not** read OMS Redis positions (dummy). See **docs/PHASE2_DETAILED_PLAN.md**.

## Contents

| Document | Purpose |
|----------|--------|
| **PMS_ARCHITECTURE.md** | Data flow, **granular store** (one table, grain account/book/symbol; signed open_qty, position_side), **views on request**, **reference data** (symbols, allocations), **capital by book**, mark price interface, Pydantic, account/session. **§6.1** = positions table (write schema); **§8** = Postgres read/write schema (orders, accounts, balances columns; positions pointer). |
| **PMS_DATA_FLOWS.md** | Periodic rebuild from orders, event-driven options, and data fixes (position drift repairs). |
| **PMS_DATA_MODEL.md** | **Build-from-order cash** (orders + balance_changes; double-entry; symbol mapping). Symbol vs asset, **symbols** (OMS-managed, PMS reads), building positions and cash, **book** column, default cash book, reconciliation. |
| **POSITION_KEEPER_DESIGN.md** | How PMS uses the **orders** table to compute and reconcile positions; repairs; task checklist mapping. |
| **PMS_ACCOUNT_AND_SESSION.md** | Whether account data management or session must be built before PMS: **no** — accounts table + seed is enough. |
| **CONFLICTS_AND_CONSOLIDATION.md** | Walkthrough of conflicts resolved when consolidating position_keeper into PMS and making PMS the source of truth. |

## Core vs optional

**Core** (implement first): Derive **positions** from **orders** only (filter partial/fully filled; no fills table); **build cash** from **orders** (trade legs) + **balance_changes** (deposit/withdrawal only; **book** column; default cash book) using **symbols** for base/quote. Balance sync disabled; PMS does not read `balances`. Compute PnL (realized from order rows; unrealized = mark × open qty when open_qty ≠ 0) → **positions** table + **book_cash** table + optional Redis → **timer loop**. Reads: `orders`, `balance_changes`, `symbols`, `accounts`.

**Optional:** pnl_snapshots, margin_snapshots, Redis pnl/margin keys, repairs/rebuild, multiple mark price sources, SQL VIEWs. See **PMS_ARCHITECTURE.md** §2. **No fills table:** orders are the primary data source.

## Key points

- **PMS** reads **Postgres only** (orders, balance_changes, symbols; no fills table). Derives **positions** from orders; **builds cash** from orders + balance_changes (double-entry; **book** column; default cash book for broker-fed; book change records for strategy books). Writes **positions** (grain account_id, book, symbol) and **book_cash** (grain account_id, book, asset). **Grouping** on request; optional SQL VIEWs.
- **Cash:** Built in PMS from orders (trade legs via symbol→base/quote) + balance_changes (deposit/withdrawal only). **Symbols** table (base_asset, quote_asset) is **managed by OMS** (or reference sync); **PMS reads** it. Balance sync to `balances` is **disabled**.
- **Capital by book:** capital(book) = asset_value(book) + cash(book); cash(book) from **book_cash** (no separate allocations interface for core).
- **Mark price:** Interface first; Phase 2 Binance; Phase 4 Redis/DB. Config: `PMS_MARK_PRICE_SOURCE`.
- **Account/session:** No dedicated account service or session required; ensure **accounts** table exists and at least one account is seeded.

## References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 PMS, §12.3 task checklist, §3 schema.
