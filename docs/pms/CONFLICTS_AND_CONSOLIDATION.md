# PMS Consolidation: Conflicts Resolved and Source of Truth

This document walks through the conflicts that were resolved when consolidating **position_keeper** into **PMS** and making **PMS the source of truth** for PnL and margin. It also summarizes what was removed and what was kept.

---

## 1. Naming and Service Boundary

| Before | After | Reason |
|--------|--------|--------|
| **Position Keeper** (separate service name and folder `position_keeper/`) | **PMS** (Portfolio Management System); folder `pms/` | Single service name; PMS is the portfolio/position layer. |
| Docs under `docs/position_keeper/` | All under `docs/pms/` | One place for PMS design and data model. |

**Conflict:** Phase 2 plan and OMS docs referred to a “Position Keeper” service and `position_keeper/` in repo layout and Docker.  
**Resolution:** Everywhere we now say **PMS** and use **pms** for the service name and folder. Docker service is `pms`; suggested code lives under `pms/`.

---

## 2. Data Flow: Booking vs OMS

| Before (in PMS_ARCHITECTURE) | After | Reason |
|------------------------------|--------|--------|
| **Booking** consumes `oms_fills`, writes fills/positions/balances to Postgres + Redis | **OMS** syncs orders and accounts/balances to Postgres; **positions** stay in Redis only. No separate “Booking” service. | Phase 2 explicitly discards Booking; account management is integrated into OMS. |
| Position Keeper reads from “positions table” and “Booking” cache | PMS reads **positions from Redis** (OMS account store) and **balances from Postgres** (OMS account sync). | Aligns with Phase 2: no Postgres `positions` table; OMS owns Redis account store. |

**Conflict:** PMS_ARCHITECTURE described a “Booking” service writing a Postgres positions table. PHASE2_DETAILED_PLAN says “no separate Booking service” and “no positions table (positions in Redis only)”.  
**Resolution:** All PMS docs now describe OMS as the writer of orders, accounts, balances (Postgres) and positions (Redis). PMS reads from OMS (Postgres + Redis) and is the **source of truth for PnL and margin** only.

---

## 3. Positions: Postgres vs Redis

| Before | After | Reason |
|--------|--------|--------|
| Some text implied a Postgres **positions** table filled by Booking | **Positions** are **Redis-only** in Phase 2 (OMS account store: `account:{broker}:{account_id}:positions`). | PHASE2_DETAILED_PLAN §3.1 and 12.2.7: positions table dropped for PMS; OMS does not sync positions to Postgres. |
| Reconcile “positions table” vs order-derived | Reconcile **Redis positions** (OMS) vs order-derived positions. | Same reconciliation idea; store is Redis, not Postgres. |

**Conflict:** PMS design doc referred to a “positions table” as primary source; Phase 2 has no such table.  
**Resolution:** PMS docs now state that the primary source for current positions is **Redis** (OMS account store). Rebuild-from-orders compares to Redis positions; repairs update Redis (or flag). A future Postgres positions table can be added later if needed.

---

## 4. References to position_keeper

| Location | Change |
|----------|--------|
| **docs/position_keeper/README.md** | **Removed.** Content merged into **docs/pms/PMS_DATA_MODEL.md** (symbol vs asset, building positions, reconciling two views). |
| **docs/PHASE2_DETAILED_PLAN.md** | All “Position Keeper” → “PMS”; `position_keeper/` → `pms/`; `docs/position_keeper/README.md` → `docs/pms/PMS_DATA_MODEL.md` (or PMS_ARCHITECTURE where appropriate). Task §12.3 is “PMS”; Docker and repo layout use `pms`. |
| **docs/IMPLEMENTATION_PLAN.md** | `position_keeper/` → `pms/` in suggested repo layout. |
| **docs/oms/OMS_ARCHITECTURE.md** | “Booking (downstream)” → “PMS / downstream consumers”; oms_fills consumer “Booking” → “PMS / downstream”. Outbound clarified: positions in Redis only. |
| **docs/pms/** | All internal references to “Position Keeper” as the service replaced with “PMS”; file paths `position_keeper/` → `pms/`. **POSITION_KEEPER_DESIGN.md** kept as filename but title and body now describe “PMS” and Redis-based positions. |

---

## 5. New and Updated PMS Docs

| Document | Purpose |
|----------|--------|
| **PMS_DATA_MODEL.md** | **New.** Symbol vs asset, building positions from orders, reconciling order-derived vs Redis positions and balance_changes vs balances. Replaces the former `docs/position_keeper/README.md`. |
| **PMS_ARCHITECTURE.md** | Updated: PMS as source of truth; data flow from OMS (no Booking); positions from Redis; file map under `pms/`. |
| **PMS_DATA_FLOWS.md** | Updated: primary source Redis; no Booking; “Position Keeper” → “PMS”. |
| **PMS_ACCOUNT_AND_SESSION.md** | Updated: “Position Keeper” → “PMS”; “Booking schema” → “OMS schema”. |
| **POSITION_KEEPER_DESIGN.md** | Updated: describes PMS and orders-based rebuild; positions in Redis; repairs update Redis. |
| **README.md** | Updated: PMS as source of truth; contents table includes PMS_DATA_MODEL; references point to PMS. |
| **CONFLICTS_AND_CONSOLIDATION.md** | This walkthrough. |

---

## 6. Summary

- **position_keeper** is **removed** as a name and doc location; **PMS** is the single service and source of truth for PnL/margin.
- **Booking** is **removed** from the documented data flow; OMS syncs orders and accounts/balances to Postgres and keeps positions in Redis.
- **Positions** are **Redis-only** in Phase 2; PMS reads and reconciles against Redis; repairs update Redis (or flag).
- All plan and architecture docs now reference **docs/pms/** and the **pms** service; no remaining references to `docs/position_keeper/` or the `position_keeper` service.
