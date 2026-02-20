# Portfolio Management System (PMS) Documentation

Plans and architecture for the **Portfolio Management System (PMS)**. PMS is the **source of truth** for PnL and margin. It reads from **OMS Postgres only** (orders, accounts, balances); **positions are derived from the orders table**. PMS does **not** read OMS Redis positions (that path is dummy). See **docs/PHASE2_DETAILED_PLAN.md**.

## Contents

| Document | Purpose |
|----------|--------|
| **PMS_ARCHITECTURE.md** | Data flow, **granular store** (one table, grain account/book/symbol; signed open_qty, position_side), **views on request**, **reference data** (symbols, allocations), **capital by book**, mark price interface, Pydantic, account/session. |
| **PMS_DATA_FLOWS.md** | Periodic rebuild from orders, event-driven options, and data fixes (position drift repairs). |
| **PMS_DATA_MODEL.md** | Symbol vs asset, **symbols table**, building positions (with **book**), cash vs positions, **capital by book**, allocations (interface, managed outside), reconciliation. |
| **POSITION_KEEPER_DESIGN.md** | How PMS uses the **orders** table to compute and reconcile positions; repairs; task checklist mapping. |
| **PMS_ACCOUNT_AND_SESSION.md** | Whether account data management or session must be built before PMS: **no** — accounts table + seed is enough. |
| **CONFLICTS_AND_CONSOLIDATION.md** | Walkthrough of conflicts resolved when consolidating position_keeper into PMS and making PMS the source of truth. |

## Core vs optional

**Core** (implement first): Derive positions from **orders only** (filter partial/fully filled; no fills table) → compute PnL (realized from order rows; unrealized = mark × open qty only when open_qty > 0) → **one granular table** (`positions`) + **Redis** (`pnl:{account_id}`, `margin:{account_id}`) → **timer loop**. Reads: `orders`, `balances`, `accounts`.

**Optional:** Symbols table, book_allocations, pnl_snapshots, margin_snapshots, book_cash, repairs/rebuild, multiple mark price sources, SQL VIEWs. See **PMS_ARCHITECTURE.md** §2. **No fills table:** orders are the primary data source.

## Key points

- **PMS** reads **Postgres only** (orders, balances; no fills table — orders are primary). Derives positions (filter partial/fully filled; entry_avg = cost basis of open qty only) and **writes** granular store (grain account_id, book, symbol; open_qty signed, position_side) + Redis. **Grouping** (by symbol, by book/symbol, by broker/symbol) on request; optional SQL VIEWs on PMS-written tables.
- **Granular store vs orders:** Orders = ledger (one row per order); PMS writes a **position/snapshot table** (one row per account/book/symbol (signed open_qty, position_side)). **Cash** stays in OMS balances; **symbols** and **allocations** are reference data (PMS reads; allocations managed outside, provided via interface).
- **Capital by book:** capital(book) = asset_value(book) + cash(book); allocations read from interface (table, API, or file).
- **Mark price:** Interface first; Phase 2 Binance; Phase 4 Redis/DB. Config: `PMS_MARK_PRICE_SOURCE`.
- **Account/session:** No dedicated account service or session required; ensure **accounts** table exists and at least one account is seeded.

## References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 PMS, §12.3 task checklist, §3 schema.
