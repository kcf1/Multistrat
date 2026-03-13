# Portfolio Management System (PMS) Documentation

PMS is the **source of truth** for PnL and margin. It reads **orders** (and **balances** when OMS syncs) from Postgres, **derives positions** from orders, and writes the **positions** table. **Cash** = broker-backed **balances** table (OMS syncs when `sync_balances=True`). No separate book_cash or build-from-order cash.

## Documents

| Document | Purpose |
|----------|--------|
| **PMS_ARCHITECTURE.md** | Data flow, granular store (positions table), reads (orders, balances), mark price, config. |
| **docs/PHASE2_DETAILED_PLAN.md** | Phase 2 plan; §8 PMS role, §12.3 task checklist. |

## Core

- **Positions:** Derived from **orders** only (filter partially_filled, filled); grain (account_id, book, symbol); write **positions** table.
- **Cash:** From **balances** table when OMS syncs (`sync_balances=True`); PMS `query_balances`.
- **PnL:** Realized from order rows; unrealized = mark × open_qty when open_qty ≠ 0.
- **Loop:** `pms/loop.py` — read orders → derive positions → enrich with mark price → write positions.

No fills table; no OMS Redis positions read. Account/session: **accounts** table + seed is enough; no dedicated account service required.
