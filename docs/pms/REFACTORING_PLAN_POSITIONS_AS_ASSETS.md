# Refactoring Plan: Positions as Asset Holdings (Cash-in-Asset)

**Status:** Draft  
**Goal:** Refactor PMS position building so that (1) every coin is a position (asset-level grain), (2) each order contributes base and quote legs to the same book, (3) deposit/withdrawal from `balance_changes` are included, and (4) position value is evaluated in USD via an assets table.

**References:** Analysis in prior discussion; current design in [POSITION_KEEPER_DESIGN.md](POSITION_KEEPER_DESIGN.md), [PMS_DATA_MODEL.md](PMS_DATA_MODEL.md).

---

## 1. Summary of Changes

| Current | Target |
|--------|--------|
| Position grain `(account_id, book, symbol)` | Position grain `(account_id, book, asset)` |
| One position per trading pair (e.g. BTCUSDT) | One position per asset (e.g. BTC, USDT, ETH) |
| Positions derived from **orders only** (signed qty per symbol) | Positions = **orders** (base + quote legs) + **balance_changes** (deposit/withdrawal) |
| Mark/notional per symbol | Mark/notional per asset in **USD** (via assets table) |
| `symbols` table unused in PMS | PMS joins orders ??symbols for base/quote; new `assets` table for USD valuation |

---

## 2. Phases Overview

| Phase | Scope | Outcome |
|-------|--------|---------|
| **Phase A** | Schema: positions `symbol` ??`asset`; new `assets` table | DB and migrations ready |
| **Phase B** | Reads: symbol map, order??legs, balance_changes aggregation | Position derivation from orders + balance_changes at asset grain |
| **Phase C** | Mark price / USD: assets table usage, enrich positions in USD | notional and unrealized_pnl in USD |
| **Phase D** | Loop, store, tests, docs, E2E | Full integration and verification |

---

## 3. Phase A ??Schema

### 3.1 Positions table: `symbol` ??`asset`

**File:** New Alembic revision under `alembic/versions/`.

**Changes:**

- Rename column `symbol` ??`asset` (TEXT, same semantics: e.g. `BTC`, `USDT`).
- Rename unique constraint/index `uq_positions_account_book_symbol` ??`uq_positions_account_book_asset` on `(account_id, book, asset)`.
- Rename index `ix_positions_symbol` ??`ix_positions_asset` on `(asset)`.

**Migration strategy:** Single revision: `ALTER TABLE positions RENAME COLUMN symbol TO asset`; then rename indexes. No data transform in this step (existing rows are pair-level; Phase B backfill or rebuild handles legacy data ??see ?5.1).

**Checklist:**

- [x] Create revision `positions_symbol_to_asset` (or similar name).
- [x] Implement `upgrade()` and `downgrade()`.
- [x] Run `alembic upgrade head` locally and verify table shape.
- [x] Update any raw SQL in codebase that references `positions.symbol` (e.g. granular_store) ? done in Phase D; all code uses `asset` (and broker).

### 3.2 New table: `assets`

**Purpose:** Per-asset metadata for USD valuation and display. Cross-referenced from `symbols` (base_asset, quote_asset) but not a strict FK.

**File:** New Alembic revision.

**Columns (minimal):**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `asset` | TEXT | NOT NULL | PK; e.g. BTC, USDT, ETH |
| `usd_symbol` | TEXT | YES | Symbol for mark price in USD (e.g. BTCUSDT for BTC). NULL = use `usd_price`. |
| `usd_price` | NUMERIC(36,18) | YES | Fixed USD value (e.g. 1 for USDT/USDC). When set, used instead of fetching `usd_symbol`. |
| `updated_at` | TIMESTAMPTZ | YES | Last sync or manual update |

**Indexes:** PK on `asset`. Optional: index on `usd_symbol` for lookups.

**Population:** Seed from `symbols`: for each distinct `base_asset` and `quote_asset`, insert/update `assets`; set `usd_symbol` for base assets to a chosen *USDT (or *USDC) pair from symbols; set `usd_price = 1` for USDT, USDC, etc. Can be done in a data migration or a small script/sync job. **Done:** migration `p6q7r8s9t0u1_seed_stables_assets` seeds USDT, USDC, BUSD, DAI, TUSD, USDP with `usd_price=1`.

**OMS sync from broker:** Do **not** add a dedicated OMS sync from the broker for the assets table by default. The list of assets is derived from the existing `symbols` table (which OMS already syncs from the broker, e.g. Binance exchangeInfo). If a broker later exposes a dedicated asset/coin API and you want OMS to own ??list of assets??or asset-level metadata (e.g. deposit/withdraw status), add an optional OMS sync that UPSERTs into `assets`; keep `usd_symbol` / `usd_price` derived from `symbols` + config so valuation stays in one place.

**Checklist:**

- [x] Create revision `add_assets_table`.
- [x] Implement upgrade/downgrade.
- [x] Document population strategy (sync from symbols + known stables).
- [x] Seed migration for known stables (usd_price=1).

---

## 4. Phase B ??Position Derivation (Reads)

### 4.1 Symbol map

**File:** `pms/reads.py` (or new `pms/symbols.py` if preferred).

**New function:** `query_symbol_map(pg_connect) -> Dict[str, Tuple[str, str]]`  
Returns `symbol ??(base_asset, quote_asset)` from table `symbols`. Used by order??legs conversion.

**Alternative:** Extend `query_orders_for_positions` to JOIN `symbols` and return each order row with `base_asset` and `quote_asset` (so derivation does not need a separate symbol map call). Choose one approach and stick to it.

**Checklist:**

- [x] Implement `query_symbol_map` (or extended orders query with base/quote).
- [x] Unit test with mocked DB returning symbol rows.

### 4.2 Orders ??base and quote legs

**File:** `pms/reads.py`.

**Logic:**

- For each order row (partially_filled / filled, with executed_qty > 0):
  - Resolve `base_asset`, `quote_asset` from symbol map (skip or log if symbol missing).
  - **Base leg:** signed qty in base units = `+executed_qty` (BUY) or `-executed_qty` (SELL). Post to `(account_id, book, base_asset)`.
  - **Quote leg:** signed qty in quote units = `executed_qty * price` (or use `binance_cumulative_quote_qty` when present). BUY ??negative (spend quote), SELL ??positive (receive quote). Post to `(account_id, book, quote_asset)`.
- Group all lots by `(account_id, book, asset)`; run existing FIFO logic **per asset** to get `open_qty` and `entry_avg` per (account_id, book, asset). Entry_avg for quote asset is in quote units (e.g. USDT); for base asset in base units.

**Data flow:** Either (a) keep `query_orders_for_positions` and in a new `derive_asset_positions_from_orders(order_rows, symbol_map)` build the per-asset lots and then FIFO, or (b) merge orders query + symbol join in one place. Expose a single entry point used by the loop, e.g. `derive_positions_from_orders_and_balance_changes(pg_connect)` that returns list of derived positions at grain (account_id, book, asset).

**Checklist:**

- [x] Implement order ??base/quote legs using symbol map.
- [x] Reuse or adapt `_fifo_open_qty_and_entry_avg` per (account_id, book, asset).
- [x] Add unit tests: one BUY BTCUSDT ??BTC +qty, USDT ??qty*price); one SELL; mixed orders; unknown symbol skipped or fallback.

### 4.3 Balance_changes aggregation

**File:** `pms/reads.py`.

**New function:** `query_balance_changes_net_by_account_book_asset(pg_connect, account_ids: Optional[List[str]] = None) -> Dict[Tuple[str, str, str], Decimal]`  
- `balance_changes.account_id` is broker account id (TEXT, same as `orders.account_id`); **no join to accounts** needed.  
- Filter by `change_type` IN (`deposit`, `withdrawal`, `adjustment`, `transfer`) ??exclude or include `snapshot` per product decision.  
- Aggregate: `SUM(delta)` per `(account_id, book, asset)`.  
- Return a dict keyed by `(account_id, book, asset)` with net delta (positive = more cash/position).

**Checklist:**

- [x] Implement query (no join to accounts).
- [x] Add unit test with mocked rows: one deposit, one withdrawal; assert net per (account_id, book, asset).

### 4.4 Combine orders + balance_changes

**File:** `pms/reads.py`.

**Function:** `derive_positions_from_orders_and_balance_changes(pg_connect) -> List[DerivedPosition]`  
- Call order-derived asset positions (open_qty per (account_id, book, asset)).  
- Call `query_balance_changes_net_by_account_book_asset`.  
- For each (account_id, book, asset): final `open_qty` = order_derived_open_qty + balance_changes_net_delta.  
- Derive `position_side` from sign of final open_qty; keep `entry_avg` from order-derived FIFO (balance_changes do not affect entry_avg).  
- Return list of `DerivedPosition` with `asset` (not `symbol`).

**Checklist:**

- [x] Implement combined derivation.
- [x] Unit test: orders only; balance_changes only; orders + deposit for same asset.

---

## 5. Phase C ??Mark Price and USD Valuation

### 5.1 Asset ??USD price

**Stables-first (implemented):** Use `assets` table only for USD: if `usd_price` is set (e.g. 1 for USDT, USDC), use it for mark and notional; unrealized_pnl = 0 for stables. No live prices stored in the table; asset table is config (fixed `usd_price` or `usd_symbol` for future fetch). Seed migration populates known stables with `usd_price = 1`.

**Option A ??Use existing mark provider by symbol (for non-stables, follow-up):**  
- Helper: `query_assets_usd_config(pg_connect) -> Dict[str, dict]` (asset ??`usd_symbol` or `usd_price`).  
- For each position asset: if `usd_price` set, use it (stables); else request `get_mark_prices([usd_symbol])` from existing provider.  
- Mark provider remains symbol-based; PMS resolves asset ??symbol (or 1) before calling.

**Option B ??New interface:**  
- Add `get_usd_prices(assets: List[str]) -> Dict[str, Decimal]` to mark price module (or a small USD valuation module). Implementation reads `assets` table and uses existing Binance (or other) symbol price fetch for `usd_symbol`; returns 1 for stables with `usd_price=1`.

**Recommendation:** Stables first via `usd_price` from `assets` only; no live prices in table unless a sync job is added later. Option A for non-stables when needed.

**E.3 vs this follow-up:** The follow-up below (call mark provider at runtime for assets with only `usd_symbol`) is **superseded by E.3**: when E.3 (job or API to refresh `assets.usd_price` / backfill) is implemented, the loop already reads from `assets`; no separate runtime fetch in the loop is needed.

**Checklist:**

- [x] Implement `query_assets_usd_config` from `assets` table.
- [x] Seed migration: known stables (USDT, USDC, BUSD, DAI, TUSD, USDP) with `usd_price=1`.
- [x] In loop enrichment: for each position (asset), if asset has `usd_price` in config set `mark_price`, `notional`; stables unrealized_pnl = 0.
- [x] (Follow-up) For assets with only `usd_symbol`: **covered by E.3** ? when E.3 keeps `assets.usd_price` updated, loop uses table only; no 5.1 runtime fetch needed.

### 5.2 Loop integration

**File:** `pms/loop.py`.

- Use `derive_positions_from_orders_and_balance_changes` (replaces `derive_positions_from_orders`).
- Enrich with asset-based USD from assets table only (stables-first: `enrich_positions_with_usd_from_assets` uses `query_assets_usd_config`; only assets with `usd_price` get mark/notional).
- Pass `asset` (not symbol) into USD lookup; mark provider reserved for future `usd_symbol` fetch for non-stables.

**Checklist:**

- [x] Wire new derivation and USD enrichment in `run_one_tick`.
- [x] No references to `position.symbol` in loop for derivation/enrichment; positions use `asset`.

---

## 6. Phase D ??Store, Models, Tests, Docs

### 6.1 Pydantic and granular store

**File:** `pms/schemas_pydantic.py`  
- In `DerivedPosition`, use **asset** only (symbol field removed). Grain is (account_id, book, asset).

**File:** `pms/granular_store.py`  
- All INSERT/UPDATE and conflict target: use `asset` column and grain `(account_id, book, asset)`.

**Checklist:**

- [x] Rename `DerivedPosition.symbol` ??`asset` (symbol removed; asset only).
- [x] Update `write_pms_positions` to use `asset` and correct unique constraint name if applicable.

### 6.2 Unit tests

**File:** `pms/tests/test_reads.py`  
- Update all derivation tests to use **asset** and base/quote legs: e.g. BUY 1 BTCUSDT at 50k ??two positions (BTC +1, USDT ??0k).  
- Add tests for balance_changes only; orders + balance_changes combined; unknown symbol handling.

**File:** `pms/tests/test_granular_store.py`  
- Use `DerivedPosition(..., asset="BTC", ...)`; assert DB params use `asset`.

**File:** `pms/tests/test_loop.py`  
- Mock derivation to return asset-based positions; mock USD price lookup; assert notional/USD logic.

**Checklist:**

- [x] Update test_reads tests for asset grain and legs.
- [x] Update test_granular_store for `asset`.
- [x] Update test_loop for USD enrichment.

### 6.3 E2E and scripts

**Files:** `scripts/e2e_12_4_4.py`, `scripts/inject_orders_and_check_pms.py`, and any other script that asserts on positions.

- Change expectations: positions keyed by **asset** (e.g. BTC, USDT), not symbol (BTCUSDT).  
- Expect more rows when one order creates two legs (base + quote).  
- Optionally add a balance_changes step (deposit/withdrawal) and assert on resulting position.

**Checklist:**

- [x] Update E2E expected positions (asset list and quantities).
- [x] Update inject_orders_and_check_pms assertions.
- [x] Run E2E and fix any remaining breakage (requires live services).

### 6.4 Documentation

**File:** `docs/pms/POSITION_KEEPER_DESIGN.md`  
- Update grain to (account_id, book, **asset**).  
- Document: orders ??base/quote legs; balance_changes included; formula for position = order_derived + sum(balance_changes.delta).  
- Document USD valuation via `assets` table.

**File:** `docs/pms/PMS_DATA_MODEL.md`  
- Revise ??Cash vs positions??to ??all coins are positions??  
- Document build = orders (base + quote) + balance_changes; document `assets` table and USD valuation.  
- Remove or soften "do not put cash into positions".

**Checklist:**

- [x] Update POSITION_KEEPER_DESIGN.
- [x] Update PMS_DATA_MODEL.
- [x] Add a short "Position build formula" section in one of the docs or in this plan.

**Position build formula (reference):** `open_qty(account_id, book, asset) = order_derived_open_qty + SUM(balance_changes.delta)`. Full derivation (orders ? base/quote legs, FIFO, balance_changes) is in **docs/pms/POSITION_KEEPER_DESIGN.md** ?2.2.

---

## 7. Risks and Edge Cases (planned one by one)

### 7.1 Order has symbol not in `symbols`

**Status:** **Fixed** (skip + log warning in `derive_asset_positions_from_orders`; `query_orders_for_positions` includes `internal_id` for log).

**Decision: take first approach.**

- **Mitigation:** Skip that order's legs for position build and **log a warning**. Do not add fallback (e.g. *USDT suffix) for now; keep logic simple and explicit.
- **Implementation:** In derivation, when symbol is missing from symbol map, skip base/quote legs for that order and log (e.g. "symbol X not in symbols, skipping legs for order Y").

---

### 7.2 `balance_changes.account_id` is broker account id (TEXT)

**Status:** **Fixed** (no code change needed; already using TEXT and same grain as orders).

**What this means:** `account_id` in both `orders` and `balance_changes` is the **broker's account identifier** (e.g. `"default"` or a string id from the broker), not an internal database FK. It was changed from BIGINT (FK to `accounts.id`) to TEXT so that:
- PMS can aggregate by `(account_id, book, asset)` without joining to `accounts`.
- The same identifier is used in orders (from OMS) and in balance_changes (from balanceUpdate events), so position build can add order-derived open_qty and balance_changes net delta for the same (account_id, book, asset).

**Mitigation:** No change needed; aggregate directly by (account_id, book, asset). When we add **broker** to the grain (see 7.5), aggregation will be by (broker, account_id, book, asset).

---

### 7.3 Quote leg precision

**Status:** **No need to fix** (already using executed_qty; optional broker-specific field when present is fine).

**Decision: use generic `executed_qty`; quote = `executed_qty * price`.**

- **Rationale:** `executed_qty` is a generic field across brokers. Using it (and `executed_qty * price` for quote amount) keeps PMS broker-agnostic.
- **Mitigation:** For quote leg, use **executed_qty * price** (or order's executed price) as the standard. Broker-specific fields (e.g. Binance `binance_cumulative_quote_qty`) may be used **when present** as an optional refinement for precision, but the default and documented formula is executed_qty and executed_qty * price.

---

### 7.4 Asset in balance_changes (or orders) but not in `assets`

**Status:** **No need to fix for now** (defer; planned feature to update asset prices later).

**Decision: notional null or 0; document; plan feature to update asset prices.**

- **Mitigation:** For an asset with no row in `assets` (or no `usd_price` / `usd_symbol`), set notional and unrealized_pnl to null or 0; document behavior in PMS and in this plan.
- **Planned feature:** Build a **feature to update asset prices** later: job or API that updates the `assets` table (e.g. refresh `usd_price` or backfill from a feed). This allows notional/USD to be filled in for assets that are added or updated after initial seed.

---

### 7.5 Multiple brokers / same account_id

**Status:** **Done** (E.2: broker in positions + balance_changes, OMS listener, PMS grain).

**Decision: two approaches; add broker to position store and to balance_changes.**

- **Two approaches:**
  1. **Without broker in schema:** Rely on account_id being unique per broker in practice (e.g. different brokers use different account_id namespaces). Fragile if two brokers ever share the same account_id string.
  2. **With broker in schema (chosen):** Add **broker** to the position store and to **balance_changes** so grain is **(broker, account_id, book, asset)**. Positions table and balance_changes both get a `broker` column; derivation and aggregation use (broker, account_id, book, asset).

- **Required work:**
  - **Positions table:** Add `broker` column; update unique constraint to (account_id, book, asset, broker) or (broker, account_id, book, asset). Update PMS derivation and granular_store to pass through broker; update conflict target.
  - **balance_changes table:** Add `broker` column (TEXT). Schema migration + **OMS listener** (and any writer) that inserts into balance_changes must set `broker` (e.g. from the connection or config). PMS aggregation by (account_id, book, asset) becomes (broker, account_id, book, asset).
  - **Orders:** Orders table already has broker (or is identifiable by source); ensure derivation has broker when building positions so that written positions have broker set.

**Checklist (future):**

- [x] Migration: add `broker` to `positions`; update unique constraint and indexes.
- [x] Migration: add `broker` to `balance_changes`.
- [x] OMS: adjust balance_changes writer/listener to set `broker` when writing balance_changes.
- [x] PMS: derivation and granular_store use broker in grain; query balance_changes and orders by broker where needed.

---

## 8. Task Checklist (Consolidated)

- [x] **A.1** Migration: positions `symbol` ??`asset`, rename indexes.
- [x] **A.2** Migration: add `assets` table; seed stables migration added.
- [x] **B.1** Implement symbol map (or orders query with base/quote).
- [x] **B.2** Implement order ??base/quote legs and per-asset FIFO.
- [x] **B.3** Implement balance_changes aggregation by (account_id, book, asset).
- [x] **B.4** Implement combined derivation (orders + balance_changes).
- [x] **C.1** Asset ??USD price: stables-first (query_assets_usd_config, seed stables, enrich from assets table).
- [x] **C.2** Loop: wire new derivation and USD enrichment (stables from assets only).
- [x] **D.1** DerivedPosition and granular_store use `asset` (symbol removed from model).
- [x] **D.2** Unit tests updated (reads, granular_store, loop).
- [x] **D.3** E2E and scripts updated (asset-keyed positions; inject script expects BTC + USDT rows).
- [x] **D.4** Docs updated (POSITION_KEEPER_DESIGN, PMS_DATA_MODEL).
- [x] **E.1** (Need to fix) Unknown symbol: skip order legs + log warning in derivation (7.1).
- [x] **E.2** (Need to fix) Add `broker` to `positions` and `balance_changes`; update OMS listener; PMS grain (broker, account_id, book, asset) (7.5).
- [ ] **E.3** (Follow-up) Feature: update asset prices (job or API to refresh `assets.usd_price` / backfill). When done, loop uses `assets` for all USD valuation; no separate ?5.1 runtime mark-provider fetch needed.

**Still to do:** Only **E.3** (asset price update job/API). All other checklist items are done. **Phase 2:** E.3 is task **12.3.17** in **docs/PHASE2_DETAILED_PLAN.md** §12.3.

---

## 9. File-Level Summary

| File | Action |
|------|--------|
| `alembic/versions/xxx_positions_symbol_to_asset.py` | New: rename column and indexes. |
| `alembic/versions/xxx_add_assets_table.py` | New: create assets table. |
| `alembic/versions/xxx_seed_stables_assets.py` | New: seed USDT, USDC, etc. with usd_price=1. |
| `pms/reads.py` | Add symbol map, order??legs, balance_changes aggregation, combined derivation; change grain to asset. |
| `pms/schemas_pydantic.py` | Rename `DerivedPosition.symbol` ??`asset`. |
| `pms/granular_store.py` | Use `asset` column and (account_id, book, asset) conflict. |
| `pms/loop.py` | Use new derivation and asset-based USD enrichment. |
| `pms/mark_price.py` (or new module) | Optional: `get_usd_prices(assets)` using assets table. |
| `pms/tests/test_reads.py` | Asset grain, base/quote legs, balance_changes. |
| `pms/tests/test_granular_store.py` | Use `asset` in DerivedPosition and asserts. |
| `pms/tests/test_loop.py` | Asset positions and USD enrichment mocks. |
| `scripts/e2e_12_4_4.py`, `scripts/inject_orders_and_check_pms.py` | Expect asset-keyed positions. |
| `docs/pms/POSITION_KEEPER_DESIGN.md` | Update grain and formula. |
| `docs/pms/PMS_DATA_MODEL.md` | Cash = positions; build formula; assets table. |
| *(Done E.2)* `alembic/versions/xxx_add_broker_*.py` | Add `broker` to positions and balance_changes. |
| *(Done E.2)* OMS balance_changes listener/writer | Set `broker` when writing balance_changes. |
| *(Follow-up E.3)* Asset price update | Job or API to update `assets.usd_price` (or backfill from feed). Supersedes ?5.1 runtime fetch.

This refactoring plan can be used as the single source of truth for the positions-as-assets work; update the checklists and **Status** as tasks are completed.
