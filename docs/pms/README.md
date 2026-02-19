# Portfolio Management System (PMS) Documentation

Plans and architecture for the **Portfolio Management System (PMS)**. PMS is the **source of truth** for PnL and margin. It reads from **OMS Postgres only** (orders, accounts, balances); **positions are derived from the orders table**. PMS does **not** read OMS Redis positions (that path is dummy). See **docs/PHASE2_DETAILED_PLAN.md**.

## Contents

| Document | Purpose |
|----------|--------|
| **PMS_ARCHITECTURE.md** | Data flow, Redis/Postgres layout, PMS role, main loop, config, **mark price interface** (Binance now, Redis/DB in Phase 4), **Pydantic usage** (config, mark price, internal shapes, snapshots), and whether account/session is required. |
| **PMS_DATA_FLOWS.md** | Periodic rebuild from orders, event-driven options, and data fixes (position drift repairs). |
| **PMS_DATA_MODEL.md** | Symbol vs asset, building positions from orders, reconciling views (order-derived; balance_changes vs balances). PMS does not read OMS Redis positions. |
| **POSITION_KEEPER_DESIGN.md** | How PMS uses the **orders** table to compute and reconcile positions; repairs; task checklist mapping. |
| **PMS_ACCOUNT_AND_SESSION.md** | Whether account data management or session must be built before PMS: **no** — accounts table + seed is enough. |
| **CONFLICTS_AND_CONSOLIDATION.md** | Walkthrough of conflicts resolved when consolidating position_keeper into PMS and making PMS the source of truth. |

## Key points

- **PMS** reads **Postgres only** (orders, balances, optional fills); **derives positions from the orders table** (does not read OMS Redis positions — dummy); computes PnL and margin.
- **Mark price:** Define an **interface** first; Phase 2 wraps **Binance** for unrealized PnL; Phase 4 wires an implementation to **Redis/DB** (Market Data). Config: `PMS_MARK_PRICE_SOURCE`.
- **PMS is the source of truth** for PnL and margin. Same patterns as OMS: **periodic** rebuild/sync, **triggered** updates, **repairs** when derived state (positions) drifts from canonical state (orders).
- **Account/session:** No dedicated account service or session is required beforehand; ensure **accounts** table exists and at least one account is seeded.

## References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 PMS, §12.3 task checklist, §3 schema.
