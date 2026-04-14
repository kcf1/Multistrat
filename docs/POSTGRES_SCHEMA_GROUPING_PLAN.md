# Postgres schema grouping plan

This document updates the earlier draft: live table inventory (§1a), cursor grouping (§1b), what auto-updates when moving schemas (§2a), ownership for `symbols` / `assets` (§3), optional split for cursors (§4), target mapping (§5), implementation steps (§6), **codebase touchpoints from a repo scan (§7)**, and tests (§8).

**Chosen SQL style (see §7.8):** each service sets **`search_path`** on connect so **same-schema** SQL stays **unqualified**; any query that touches **another** module’s schema uses **fully qualified** `schema.table` names.

---

## 1a. Current tables (checked against live DB)

**Environment:** `DATABASE_URL` from project `.env`, database reachable from this workspace.

**Query used:** `pg_tables` for all schemas other than `pg_catalog` / `information_schema`.

**Tables present (all currently in `public`):**

| Schema | Table |
|--------|--------|
| public | accounts |
| public | alembic_version |
| public | assets |
| public | balance_changes |
| public | balances |
| public | basis_cursor |
| public | basis_rate |
| public | ingestion_cursor |
| public | ohlcv |
| public | open_interest |
| public | open_interest_cursor |
| public | orders |
| public | positions |
| public | scheduler_runs |
| public | symbols |
| public | taker_buy_sell_volume |
| public | taker_buy_sell_volume_cursor |
| public | top_trader_long_short |
| public | top_trader_long_short_cursor |

Re-run after migrations (from repo root, venv with deps):

```python
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

load_dotenv(Path(".") / ".env")
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute(
    """
    SELECT schemaname || '.' || tablename
    FROM pg_tables
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY 1
    """
)
print("\n".join(r[0] for r in cur.fetchall()))
```

Or use `psql` if installed: `\dt *.*`.

---

## 1b. Cursor tables belong with the data they advance

Treat **`ingestion_cursor`** the same as the other **ingest / sync cursors**: it is operational metadata for OHLCV (and related) ingestion, not a separate domain.

**Cursor tables (keep grouped conceptually with market data):**

- `ingestion_cursor` — OHLCV / kline ingestion progress  
- `basis_cursor`, `open_interest_cursor`, `taker_buy_sell_volume_cursor`, `top_trader_long_short_cursor` — progress for their respective series  

**Series / fact tables:**

- `ohlcv`, `basis_rate`, `open_interest`, `taker_buy_sell_volume`, `top_trader_long_short`

All of the above are candidates for the same **PostgreSQL schema** (see §4).

---

## 2a. Do references auto-update when you move tables to a new schema?

**Inside PostgreSQL**

- **`ALTER TABLE … SET SCHEMA new_schema`** updates the catalog. The table’s **OID** stays the same; **foreign keys** between tables in the same database generally **remain valid** after the move (including FKs across schemas).
- **Views, functions, and procedures** that embed **qualified** names may need updates if they pointed at `public.table`.
- **Unqualified** references in SQL run in the session depend on **`search_path`**: after a move, `SELECT * FROM orders` resolves to whatever `search_path` says first—so it may **silently hit the wrong object** or fail if nothing matches.

**Application code and migrations**

- **Nothing auto-updates.** Raw SQL strings (`FROM orders`, `INSERT INTO symbols`, etc.), ORM table definitions, and **past Alembic revision files** still say what they said unless you change them.
- **Going forward:** follow **§7.8** — per-service `search_path` for **home** tables, **fully qualified** names for **cross-schema** access.

**Summary:** DB-internal FK wiring survives `SET SCHEMA`; **code, ad-hoc SQL, and tooling** follow the chosen policy in **§7.8**.

---

## 3. `symbols` / `assets`: which module, which schema?

| Table | Managed by | Rationale | Proposed PG schema |
|-------|------------|-----------|---------------------|
| **symbols** | **OMS** | Populated by `oms/symbol_sync.py` (e.g. Binance exchangeInfo UPSERT); startup sync in `oms/main.py`. | **`oms`** |
| **assets** | **PMS** | Valuation config and prices for position USD enrichment; PMS asset init / price feed write paths. | **`pms`** |

**PMS still reads `symbols`** for order→legs (base/quote). Under **§7.8**, PMS uses **`oms.symbols`** (fully qualified) while `search_path` keeps **`pms.*`** tables unqualified.

Do **not** put both in a generic `ref` schema unless you prefer shared catalog over **write ownership**; this plan follows **owner module = schema** for these two tables.

---

## 4. Separate schema for cursors vs “real” market data?

**Default recommendation: one schema, e.g. `market_data`**

- Holds **both** fact tables (`ohlcv`, `basis_rate`, …) **and** cursor tables.
- Simpler grants, one `search_path` entry for the market-data service, fewer cross-schema joins (cursors are often read/written alongside the same series).

**Optional second schema (e.g. `market_data_ingest` or `market_data_meta`)** only if you need:

- Different **roles** (e.g. ingest job may write cursors but not truncate facts), or  
- Different **backup / retention** policy, or  
- Strong **mental separation** in admin tools.

For this codebase, **start with a single `market_data` schema** unless you already have one of those requirements.

---

## 5. Proposed mapping (target state)

| PostgreSQL schema | Tables |
|-------------------|--------|
| **oms** | `accounts`, `balances`, `balance_changes`, `orders`, `symbols` |
| **pms** | `assets`, `positions` |
| **market_data** | `ohlcv`, `ingestion_cursor`, `basis_rate`, `basis_cursor`, `open_interest`, `open_interest_cursor`, `taker_buy_sell_volume`, `taker_buy_sell_volume_cursor`, `top_trader_long_short`, `top_trader_long_short_cursor` |
| **scheduler** | `scheduler_runs` |
| **public** | `alembic_version` (optional convention: leave here so Alembic defaults keep working without extra config) |

Adjust if you adopt a second schema for cursors (§4).

---

## 6. Implementation checklist (schemas + dependent code)

1. **Alembic revision(s):** `CREATE SCHEMA` for `oms`, `pms`, `market_data`, `scheduler`; `GRANT USAGE` / table privileges for app roles.  
2. **Move tables:** `ALTER TABLE public.<name> SET SCHEMA <schema>;` in an order that respects FKs (e.g. move `accounts` before `balances` if FKs to `accounts.id` remain).  
3. **Connection behavior:** Implement **§7.8** — `search_path` on connect per service; **fully qualify** only cross-schema SQL. See **§7** for files that need cross-schema edits.  
4. **Code touchpoints:** Follow the file checklist in **§7** (OMS, PMS, market_data, scheduler, scripts, tests).  
5. **Docs:** Update [ARCHITECTURE.md](ARCHITECTURE.md) §3 to show `schema.table`.  
6. **Tests:** See **§8**.

**Note:** `market_data/schemas.py` is **Pydantic** API/dataset shapes, not Postgres DDL. Change it only if API or naming should reflect new DB layout.

---

## 6.1 Phased rollout plan: migrate module by module

Goal: move one domain at a time so you can stop after each step, verify behavior, and avoid a single high-blast-radius cutover.

### Phase 0. Foundations (before moving any tables)

Do this once first (four required steps; optional smoke checks deferred — see below):

1. Add a small shared DB helper or connection convention for `search_path` so each service can be switched independently.
2. Create the target schemas in Postgres: `oms`, `pms`, `market_data`, `scheduler`.
3. Grant `USAGE` on those schemas to the app role(s).
4. Implement **§7.8** in a shared connection helper: `SET search_path` per service for **home** schema(s); document which statements must use **`schema.table`** for cross-module reads/writes.

**Skipped for now (optional later):** per-service smoke query / healthcheck after connect — useful after each phase cutover; add when you want faster automated verification. Until then, rely on existing tests and manual checks in §6.1 phases 1–4.

Do **not** move all tables yet. Land the connection helper + **cross-schema** SQL edits first (especially PMS → OMS, PMS → `market_data`, scheduler → OMS/PMS, scripts), then run phased `ALTER … SET SCHEMA` per §6.1.

#### Phase 0 status in this repo (implemented)

| Step | What was done |
|------|----------------|
| 1–4 | Package **`pgconn/`**: `configure_for_oms`, `configure_for_pms`, `configure_for_market_data`, `configure_for_scheduler` run `SET search_path TO <home>, public` on real `psycopg2` connections only (skips mocks). Schema name constants: `SCHEMA_OMS`, `SCHEMA_PMS`, `SCHEMA_MARKET_DATA`, `SCHEMA_SCHEDULER` (for future qualified cross-schema SQL). Optional `pgconn.connect(dsn, PostgresService.…)` wrapper exists. |
| 2–3 | Alembic revision **`a7b8c9d0e1f2`** (`create_app_schemas_postgres.py`): `CREATE SCHEMA IF NOT EXISTS` for `oms`, `pms`, `market_data`, `scheduler`; `GRANT USAGE` and `GRANT CREATE` to `CURRENT_USER`. |
| Wiring | Every production `psycopg2.connect` path in **OMS**, **PMS**, **market_data** jobs, **scheduler** jobs, **scripts** listed in §7, integration tests in §7, and **`strategies/research/mom_research.py`** (SQLAlchemy `connect_args` for `market_data,public`) calls the matching `configure_for_*` immediately after connect. |
| Not done yet | **Cross-schema** SQL still uses unqualified names where tables live in `public` (safe until phased `ALTER … SET SCHEMA`). Add `oms.*` / `pms.*` / `market_data.*` per §7.8 in later phases. |

### Phase 1. OMS first

**Why first:** OMS owns the core write path for `orders`, `accounts`, `balances`, `balance_changes`, and `symbols`. PMS and scheduler mostly consume what OMS writes, so OMS should establish the first non-`public` schema.

**Tables to move in this phase:**

- `orders`
- `accounts`
- `balances`
- `balance_changes`
- `symbols`

**Code to update before moving tables:**

- [oms/sync.py](../oms/sync.py) -> `orders`
- [oms/account_sync.py](../oms/account_sync.py) -> `accounts`, `balances`, `balance_changes`
- [oms/symbol_sync.py](../oms/symbol_sync.py) -> `symbols`
- [oms/repair.py](../oms/repair.py) -> `orders`
- OMS tests listed in §7.2

**Migration procedure:**

1. OMS connections: `SET search_path TO oms, public` (or equivalent) after connect. Keep **all OMS-owned** SQL **unqualified** (`orders`, `accounts`, …) — they resolve to `oms.*` once tables live there.
2. Deploy that code while tables are still in `public` only if the code is backward-compatible with the current layout (often: set `search_path` to `public` only until cutover, then switch to `oms, public` in the same release as the migration).
3. Run an Alembic revision that moves OMS tables from `public` to `oms`.
4. Run OMS-only smoke tests:
   - order sync writes to `oms.orders`
   - account sync writes to `oms.accounts` / `oms.balances`
   - balance change callback writes to `oms.balance_changes`
   - symbol sync writes to `oms.symbols`
5. Pause here and observe before touching PMS.

**Stop / rollback point:** if OMS breaks, revert OMS code and move only OMS tables back to `public`; PMS and market data have not been touched yet.

### Phase 2. PMS second

**Why second:** PMS depends on OMS data, so it should be updated only after OMS tables have settled in their new schema.

**Tables to move in this phase:**

- `assets`
- `positions`

**Cross-schema reads PMS must handle:**

- `oms.orders`
- `oms.balance_changes`
- `oms.symbols`
- optionally `oms.balances`
- `market_data.ohlcv` later for `ohlcv_db` price provider

**Code to update before moving tables:**

- [pms/granular_store.py](../pms/granular_store.py) -> `positions`
- [pms/reads.py](../pms/reads.py) -> `orders`, `balance_changes`, `symbols`, `assets`, `balances`
- [pms/asset_init.py](../pms/asset_init.py) -> `assets`, `symbols`
- [pms/asset_price_feed.py](../pms/asset_price_feed.py) -> `assets`
- [pms/asset_price_providers/ohlcv_db.py](../pms/asset_price_providers/ohlcv_db.py) -> `assets`, later `market_data.ohlcv`
- PMS tests and helper scripts listed in §7.3

**Migration procedure:**

1. PMS connections: `SET search_path TO pms, public` (home tables unqualified). Update **cross-schema** reads/writes to **fully qualified** names: `oms.orders`, `oms.balance_changes`, `oms.symbols`, optionally `oms.balances`; keep `assets` / `positions` as bare names in SQL (they resolve via `pms` first).
2. After market data moves (phase 3), qualify `market_data.ohlcv` in [pms/asset_price_providers/ohlcv_db.py](../pms/asset_price_providers/ohlcv_db.py) (and any similar readers).
3. Run an Alembic revision that moves `assets` and `positions` from `public` to `pms`.
4. Run PMS smoke tests:
   - derive positions from `oms.orders` + `oms.balance_changes`
   - write to `pms.positions`
   - asset init / sync writes to `pms.assets`
   - asset price feed updates `pms.assets`
5. Re-run any scripts that combine OMS + PMS tables (see §7.6).

**Stop / rollback point:** revert PMS code and move only `pms.assets` / `pms.positions` back if needed; OMS remains migrated.

### Phase 3. Market data third

**Why third:** market data is mostly self-contained, but PMS has one read path into `ohlcv`, so move it after PMS has already been taught to read cross-schema tables.

**Tables to move in this phase:**

- `ohlcv`
- `ingestion_cursor`
- `basis_rate`
- `basis_cursor`
- `open_interest`
- `open_interest_cursor`
- `taker_buy_sell_volume`
- `taker_buy_sell_volume_cursor`
- `top_trader_long_short`
- `top_trader_long_short_cursor`

**Code to update before moving tables:**

- [market_data/storage.py](../market_data/storage.py)
- repair / gap jobs listed in §7.4
- [market_data/tests/test_storage.py](../market_data/tests/test_storage.py)
- [pms/asset_price_providers/ohlcv_db.py](../pms/asset_price_providers/ohlcv_db.py) because it reads `ohlcv`
- [strategies/research/mom_research.py](../strategies/research/mom_research.py)

**Migration procedure:**

1. Market-data connections: `SET search_path TO market_data, public`; keep storage / job SQL **unqualified** for tables that live in `market_data`.
2. Update PMS `ohlcv_db` and research scripts to read **`market_data.ohlcv`** (cross-schema from PMS’s session).
3. Run an Alembic revision that moves the market-data fact and cursor tables to `market_data`.
4. Run market-data smoke tests:
   - OHLCV insert / upsert
   - cursor read / write for ingestion
   - one repair-gap query per family
   - PMS `ohlcv_db` provider still returns prices

**Stop / rollback point:** move only the market-data tables back; OMS and PMS stay migrated.

### Phase 4. Scheduler last

**Why last:** it is low-risk and mostly reads from OMS / PMS, so it should adapt after those schemas are already final.

**Tables to move in this phase:**

- `scheduler_runs`

**Code to update before moving tables:**

- [scheduler/run_history.py](../scheduler/run_history.py)
- [scheduler/jobs/reports/position_snapshot_hourly.py](../scheduler/jobs/reports/position_snapshot_hourly.py)
- [scheduler/jobs/reconciliation/order_recon.py](../scheduler/jobs/reconciliation/order_recon.py)
- [scheduler/jobs/reconciliation/position_recon.py](../scheduler/jobs/reconciliation/position_recon.py)

**Migration procedure:**

1. Scheduler connections: `SET search_path TO scheduler, public` so **`scheduler_runs`** can stay unqualified once moved.
2. Scheduler jobs that read OMS/PMS data: use **fully qualified** `oms.orders`, `pms.positions` (scheduler has no ownership of those tables).
3. Run an Alembic revision that moves `scheduler_runs` to `scheduler`.
4. Run scheduler smoke tests:
   - insert/update one `scheduler_runs` row
   - run one position snapshot
   - run one order reconciliation read

**Stop / rollback point:** scheduler can be reverted independently with minimal impact on OMS / PMS / market data.

### Phase 5. Cleanup after all phases

1. Update docs to show **home** tables by service and **`schema.table`** for every documented cross-schema query.
2. Remove any temporary `search_path` shims used only for cutover.
3. **`scripts/`** (multi-module in one process): prefer **`schema.table` everywhere** in raw SQL, or set an explicit `search_path` at the start of each script and document it; full qualification is usually clearer for one-off tools.
4. Re-run the full integration flow: OMS -> PMS -> market_data -> scheduler.
5. Keep `public.alembic_version` unless you have a strong reason to move it.

### Recommended revision structure

Use multiple small Alembic revisions instead of one giant move:

1. `create_app_schemas`
2. `move_oms_tables_to_oms`
3. `move_pms_tables_to_pms`
4. `move_market_data_tables_to_market_data`
5. `move_scheduler_tables_to_scheduler`

This matches the phased rollout and gives you a clean stop point after each domain.

---

## 7. Codebase scan: who touches which tables, and what must change

Scan method: ripgrep for `INSERT INTO` / `FROM` / `UPDATE` / `DELETE FROM` against application tables across `*.py` (2026-04 snapshot). **Admin** had no raw SQL matches. Below, “**cross-schema**” means SQL issued from a service that touches tables **outside** that service’s **home** schema — those references must be **`schema.table`** per **§7.8** (not left to `search_path` ordering).

### 7.1 Interaction summary (by target schema)

| Target schema | Tables | Primary writers (modules) | Primary readers (modules) |
|---------------|--------|---------------------------|---------------------------|
| **oms** | `accounts`, `balances`, `balance_changes`, `orders`, `symbols` | `oms/account_sync.py`, `oms/sync.py`, `oms/symbol_sync.py`, `oms/repair.py` | `pms/reads.py`, `scheduler/jobs/reconciliation/order_recon.py`, many `scripts/`, `oms/tests/` |
| **pms** | `assets`, `positions` | `pms/granular_store.py`, `pms/asset_init.py`, `pms/asset_price_feed.py` | `pms/reads.py`, `pms/asset_price_feed.py`, `pms/asset_price_providers/ohlcv_db.py`, `scheduler/jobs/reports/position_snapshot_hourly.py`, `scheduler/jobs/reconciliation/position_recon.py`, `scripts/` |
| **market_data** | `ohlcv`, cursors, `basis_rate`, `open_interest`, `taker_buy_sell_volume`, `top_trader_long_short` | `market_data/storage.py` (central) + jobs that call it | `market_data/tests/`, `pms/asset_price_providers/ohlcv_db.py`, `strategies/research/mom_research.py` |
| **scheduler** | `scheduler_runs` | `scheduler/run_history.py` | (reports / ops only) |

**Cross-schema SQL to qualify explicitly (§7.8):**

- **PMS:** qualify `oms.orders`, `oms.balance_changes`, `oms.symbols`, `oms.balances` (if used); keep `assets`, `positions` unqualified with `search_path` … `pms` first. **`pms/asset_init.sync_assets_from_symbols`:** `FROM oms.symbols`, writes to bare `assets` → `pms.assets`. **`pms/asset_price_providers/ohlcv_db.py`:** `pms.assets` (or unqualified `assets` with `pms` first) and **`market_data.ohlcv`** after phase 3.
- **Scheduler:** qualify **`oms.orders`**, **`pms.positions`** in reconciliation / snapshot jobs; `scheduler_runs` unqualified with `search_path` … `scheduler` first.
- **Scripts / research:** any SQL that spans modules should use **full names** (or one documented `search_path` at script start); see §7.6.

### 7.2 OMS — files with SQL (home `oms`: unqualified + `search_path`)

| File | Tables referenced |
|------|---------------------|
| [oms/sync.py](../oms/sync.py) | `orders` |
| [oms/account_sync.py](../oms/account_sync.py) | `accounts`, `balances`, `balance_changes` |
| [oms/symbol_sync.py](../oms/symbol_sync.py) | `symbols` |
| [oms/repair.py](../oms/repair.py) | `orders` |

**OMS tests:** ensure test connections run the same `SET search_path` as production OMS; substring assertions on `INSERT INTO orders` etc. usually **unchanged** (still unqualified). [oms/tests/test_account_schema.py](../oms/tests/test_account_schema.py) may need `schemaname` + `tablename` checks or `search_path` on the test connection.

### 7.3 PMS — files with SQL (home `pms`: unqualified; qualify `oms.*`, `market_data.*`)

| File | Tables referenced |
|------|---------------------|
| [pms/granular_store.py](../pms/granular_store.py) | `positions` |
| [pms/reads.py](../pms/reads.py) | `symbols`, `orders`, `balance_changes`, `assets`, `balances` |
| [pms/asset_init.py](../pms/asset_init.py) | `assets`, `symbols` (read for sync) |
| [pms/asset_price_feed.py](../pms/asset_price_feed.py) | `assets` |
| [pms/asset_price_providers/ohlcv_db.py](../pms/asset_price_providers/ohlcv_db.py) | `assets`, `ohlcv` |

**PMS tests:** after qualifying cross-schema SQL, update any assertions that expected unqualified `FROM orders` etc. [pms/tests/test_asset_init.py](../pms/tests/test_asset_init.py), [pms/tests/test_asset_price_feed.py](../pms/tests/test_asset_price_feed.py), [pms/tests/test_reads.py](../pms/tests/test_reads.py), [pms/tests/test_granular_store.py](../pms/tests/test_granular_store.py).

**Scripts that delegate to PMS** (no direct SQL in file; still affected via `asset_init`): [scripts/sync_assets_from_symbols.py](../scripts/sync_assets_from_symbols.py), [scripts/upsert_asset.py](../scripts/upsert_asset.py) — ensure underlying connections use §7.8.

### 7.4 Market data — files with SQL (home `market_data`: unqualified + `search_path`)

| File | Tables referenced |
|------|---------------------|
| [market_data/storage.py](../market_data/storage.py) | `ohlcv`, `ingestion_cursor`, `basis_rate`, `basis_cursor`, `open_interest`, `open_interest_cursor`, `taker_buy_sell_volume`, `taker_buy_sell_volume_cursor`, `top_trader_long_short`, `top_trader_long_short_cursor` |

**Jobs** that run raw SQL for gaps / samples (not exhaustive if they only call `storage`): [market_data/jobs/repair_gap.py](../market_data/jobs/repair_gap.py) (`ohlcv`), [market_data/jobs/repair_gap_basis_rate.py](../market_data/jobs/repair_gap_basis_rate.py), [market_data/jobs/repair_gap_open_interest.py](../market_data/jobs/repair_gap_open_interest.py), [market_data/jobs/repair_gap_taker_buy_sell_volume.py](../market_data/jobs/repair_gap_taker_buy_sell_volume.py), [market_data/jobs/repair_gap_top_trader_long_short.py](../market_data/jobs/repair_gap_top_trader_long_short.py).

**Tests:** [market_data/tests/test_storage.py](../market_data/tests/test_storage.py) — set `search_path` on test DB connections like production, or qualify if tests use raw SQL.

### 7.5 Scheduler (home `scheduler`: unqualified `scheduler_runs`; qualify `oms.*`, `pms.*`)

| File | Tables referenced |
|------|---------------------|
| [scheduler/run_history.py](../scheduler/run_history.py) | `scheduler_runs` |
| [scheduler/jobs/reports/position_snapshot_hourly.py](../scheduler/jobs/reports/position_snapshot_hourly.py) | `positions` |
| [scheduler/jobs/reconciliation/order_recon.py](../scheduler/jobs/reconciliation/order_recon.py) | `orders` |
| [scheduler/jobs/reconciliation/position_recon.py](../scheduler/jobs/reconciliation/position_recon.py) | `positions` |

### 7.6 Scripts and research

| File | Tables referenced |
|------|---------------------|
| [scripts/e2e_with_risk.py](../scripts/e2e_with_risk.py) | `orders` |
| [scripts/e2e_12_4_4.py](../scripts/e2e_12_4_4.py) | `orders`, `symbols`, `positions` |
| [scripts/inject_test_order.py](../scripts/inject_test_order.py) | `orders` |
| [scripts/inject_orders_and_check_pms.py](../scripts/inject_orders_and_check_pms.py) | `orders`, `positions` |
| [scripts/inject_balance_change_and_check.py](../scripts/inject_balance_change_and_check.py) | `balance_changes` |
| [scripts/testnet_two_orders_and_wipe.py](../scripts/testnet_two_orders_and_wipe.py) | `balances` |
| [scripts/full_pipeline_test.py](../scripts/full_pipeline_test.py) | `accounts`, `balances` |
| [scripts/bulk_order_test.py](../scripts/bulk_order_test.py) | `accounts`, `balances` |
| [strategies/research/mom_research.py](../strategies/research/mom_research.py) | `ohlcv` |

### 7.7 Alembic

- **Existing revision files** under [alembic/versions/](../alembic/versions/) still target unqualified / `public` names; they are **not** rewritten automatically when you move objects (see §2a).  
- **New revision** that performs `CREATE SCHEMA`, `ALTER … SET SCHEMA`, and grants is required.  
- **Greenfield** installs that replay full history may need either (a) `SET search_path` at the start of online migration, or (b) later revisions that assume new layouts—team policy choice.  
- Data migrations that embed SQL (e.g. [alembic/versions/p6q7r8s9t0u1_seed_stables_assets.py](../alembic/versions/p6q7r8s9t0u1_seed_stables_assets.py) touching `assets`) must use the **post-move** qualified name or the correct `search_path` when that revision runs.

### 7.8 Chosen approach: `search_path` at home, full names for cross-schema

This is the **project policy** for application SQL (Alembic is separate; see §7.7).

**Rule 1 — Home schema via `search_path`**

After each `psycopg2.connect` (or in one shared helper used by the service), run:

```sql
SET search_path TO <home_schema>, public;
```

Recommended defaults:

| Service / process | `SET search_path TO` | Tables that stay **unqualified** in SQL |
|-------------------|----------------------|----------------------------------------|
| **OMS** | `oms, public` | `orders`, `accounts`, `balances`, `balance_changes`, `symbols` |
| **PMS** | `pms, public` | `assets`, `positions` |
| **Market data** | `market_data, public` | `ohlcv`, all cursor tables, `basis_rate`, `open_interest`, `taker_buy_sell_volume`, `top_trader_long_short`, … |
| **Scheduler** | `scheduler, public` | `scheduler_runs` |

Unqualified names then resolve to the **home** schema first. **`public`** stays last for extensions / `alembic_version` if needed.

**Rule 2 — Cross-schema calls use full `schema.table` names**

Any SQL that touches a table **not** owned by the process’s home schema must use an explicit qualifier. Do **not** rely on listing extra schemas in `search_path` to make foreign tables appear “unqualified” — that is fragile and easy to mis-order.

Examples:

- PMS reading broker state: `FROM oms.orders`, `FROM oms.symbols`, `JOIN oms.balance_changes …`, optional `oms.balances`.
- PMS reading bars: `FROM market_data.ohlcv` (after market-data migration).
- Scheduler reconciliation: `FROM oms.orders`, `FROM pms.positions`.

**Rule 3 — Optional schema constants**

Centralize string literals in a tiny module (e.g. `SCHEMA_OMS = "oms"`) and build `"{}.orders".format(SCHEMA_OMS)` only where needed for cross-schema SQL; **home** SQL stays plain table names.

**Rule 4 — Scripts and one-off tools**

Prefer **fully qualified** names for all application tables, **or** one `SET search_path` at the top of the script documented in the script’s docstring. Full qualification is clearer when a single script touches OMS + PMS + market data.

**Summary:** **`search_path`** minimizes edits for **same-module** SQL; **`schema.table`** is required for **cross-module** SQL.

---

## 8. Tests (for this plan)

| What | How |
|------|-----|
| Inventory | Re-run §1a query after any migration. |
| FK integrity | `pg_constraint` where `contype = 'f'` before/after move. |
| Unit / integration | All tests under `oms/tests/`, `pms/tests/`, `market_data/tests/` that hit Postgres; substring assertions listed in §7. |
| Apps | OMS sync, PMS loop, market_data ingest, scheduler `run_history` + one reconciliation job smoke run. |

---

*Last updated: §7.8 chosen policy (`search_path` at home, qualify cross-schema); codebase scan in §7.*
