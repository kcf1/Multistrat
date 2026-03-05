# PMS Consolidation: Conflicts Resolved and Source of Truth

This document walks through the conflicts that were resolved when consolidating **position_keeper** into **PMS** and making **PMS the source of truth** for PnL and margin. It also summarizes what was removed and what was kept. **§7** documents conflict resolutions for the **build-from-order cash** model (cash built in PMS from orders + balance_changes; balance sync disabled).

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
| Position Keeper reads from “positions table” and “Booking” cache | PMS reads **Postgres only** (orders, balances); **derives positions from orders**. PMS does **not** read OMS Redis positions (dummy). | Aligns with Phase 2; OMS Redis positions are not consumed by PMS. |

**Conflict:** PMS_ARCHITECTURE described a “Booking” service writing a Postgres positions table. PHASE2_DETAILED_PLAN says “no separate Booking service” and “no positions table (positions in Redis only)”.  
**Resolution:** OMS writes orders and accounts/balances to Postgres; OMS may keep positions in Redis. **PMS does not read OMS Redis positions** (dummy). PMS reads Postgres only and **derives positions from the orders table**; PMS is the **source of truth for PnL and margin**.

---

## 3. Positions: Postgres vs Redis

| Before | After | Reason |
|--------|--------|--------|
| Some text implied a Postgres **positions** table filled by Booking | **Positions** are **Redis-only** in Phase 2 (OMS account store: `account:{broker}:{account_id}:positions`). | PHASE2_DETAILED_PLAN §3.1 and 12.2.7: positions table dropped for PMS; OMS does not sync positions to Postgres. |
| Reconcile “positions table” vs order-derived | Order-derived positions are PMS source; **PMS does not read OMS Redis**. Optional reconcile vs other stores. | PMS uses orders table only for positions. |

**Conflict:** PMS design doc referred to a “positions table” as primary source; Phase 2 has no such table.  
**Resolution:** PMS docs now state that **PMS does not read OMS Redis positions** (dummy). Positions for PMS are **derived from the orders table**. Rebuild-from-orders is the canonical position view; optional reconciliation with other stores. A future Postgres positions table can be added later if needed.

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
| **PMS_DATA_MODEL.md** | **New.** Symbol vs asset, building positions from orders, reconciling order-derived view and balance_changes vs balances. PMS does not read OMS Redis positions. Replaces the former `docs/position_keeper/README.md`. |
| **PMS_ARCHITECTURE.md** | Updated: PMS as source of truth; data flow from OMS Postgres only; positions derived from orders (PMS does not read OMS Redis — dummy); file map under `pms/`. |
| **PMS_DATA_FLOWS.md** | Updated: primary source Redis; no Booking; “Position Keeper” → “PMS”. |
| **PMS_ACCOUNT_AND_SESSION.md** | Updated: “Position Keeper” → “PMS”; “Booking schema” → “OMS schema”. |
| **POSITION_KEEPER_DESIGN.md** | Updated: describes PMS and orders-based rebuild; positions derived from orders; PMS does not read OMS Redis. |
| **README.md** | Updated: PMS as source of truth; contents table includes PMS_DATA_MODEL; references point to PMS. |
| **CONFLICTS_AND_CONSOLIDATION.md** | This walkthrough. |

---

## 6. Summary

- **position_keeper** is **removed** as a name and doc location; **PMS** is the single service and source of truth for PnL/margin.
- **Booking** is **removed** from the documented data flow; OMS syncs orders and accounts/balances to Postgres and keeps positions in Redis.
- **Positions** for PMS are **derived from the orders table**; PMS does **not** read OMS Redis positions (dummy). Optional reconciliation with other stores.
- All plan and architecture docs now reference **docs/pms/** and the **pms** service; no remaining references to `docs/position_keeper/` or the `position_keeper` service.

---

## 7. Build-from-Order Cash: Conflict Resolutions

When adopting **build-from-order cash** (PMS builds cash from orders + balance_changes + symbol mapping; double-entry; OMS balance sync disabled), the following conflict resolutions apply.

### 7.1 Reconciliation (recs)

- **Recs are updated for the new model.** Reconcile (1) order-derived positions vs PMS position store; (2) PMS-built cash vs balance_changes + order impact (sum of balance_changes per account/book/asset + order-derived trade legs); (3) symbol mapping in sync with broker. Document which recs run where and how often. See **docs/pms/PMS_DATA_MODEL.md** (Reconciliation for the new model).

### 7.2 Double-counting

- **Not an issue.** OMS **balance sync to Postgres `balances` is disabled**. **balance_changes** is populated **only from balanceUpdate** events. Per Binance API (**docs/BINANCE_API_RULES.md** §1.1), **balanceUpdate** is triggered by deposits, withdrawals, and transfers — **not** by trades (trades trigger `outboundAccountPosition`). Observed in tests: balance_changes is only used for deposit/withdrawal operations. So trade-related cash is applied **only** from the orders table; no double-counting.

### 7.3 Initial balance

- When no **balance_changes** records have been received from the broker (e.g. account pre-dates recording or testnet with no deposits), **initial balance** is established by **manual booking** or **reconciliation (recs)** entries: e.g. insert balance_changes with change_type `adjustment` or `initial` and the chosen book. If no balance_change records exist from the broker, initial balance is set via recs/manual entry.

### 7.4 Account vs book

- **Broker-fed data** has no book. **balance_changes** has a **book** column. Broker-fed records (from balanceUpdate) use the constant **default cash book** `"default"`. To move cash to strategy books, **book change records** are used: manual (or admin) entries that debit the default book and credit the target book for the same asset. So: (1) broker deposit → balance_change with book = default; (2) operator books a transfer from default to book A → balance_change (or equivalent) debiting default, crediting book A. See **docs/pms/PMS_DATA_MODEL.md** (Account vs book).
