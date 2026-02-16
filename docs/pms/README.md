# Portfolio Management System (PMS) Documentation

Plans and architecture for the **Portfolio Management System** and **Position Keeper**, aligned with OMS data flows (periodic sync, event-driven updates, data fixes) and with **docs/PHASE2_DETAILED_PLAN.md**.

## Contents

| Document | Purpose |
|----------|--------|
| **PMS_ARCHITECTURE.md** | Data flow, Redis/Postgres layout, Position Keeper role, main loop, config, and whether account/session is required. |
| **PMS_DATA_FLOWS.md** | Periodic rebuild from orders, event-driven options, and data fixes (position drift repairs). |
| **POSITION_KEEPER_DESIGN.md** | How Position Keeper uses the **orders** table to compute and reconcile positions; repairs; task checklist mapping. |
| **PMS_ACCOUNT_AND_SESSION.md** | Whether account data management or session must be built before PMS: **no** — accounts table + seed is enough. |

## Key points

- **Position Keeper** reads positions/balances (from Booking); computes PnL and margin; can **recompute positions from the orders table** for reconciliation and repairs.
- Same patterns as OMS: **periodic** rebuild/sync, **triggered** updates, **repairs** when derived state (positions) drifts from canonical state (orders).
- **Account/session:** No dedicated account service or session is required beforehand; ensure **accounts** table exists and at least one account is seeded.

## References

- **docs/oms/OMS_ARCHITECTURE.md** — OMS sync, repairs, Redis/Postgres.
- **docs/PHASE2_DETAILED_PLAN.md** — §8 Position Keeper, §12.3 task checklist, §3 schema.
