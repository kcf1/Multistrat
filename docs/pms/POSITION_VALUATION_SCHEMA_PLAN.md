# Position valuation schema restructure

**Goal:** Single numeraire (USD) for position valuation. Remove mixed-quote fields; store one price-per-unit in USD; derive notional and (optionally) unrealized PnL in USD.

**References:** Current schema in `positions` table and `DerivedPosition` (docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md). Enrichment in `pms/loop.py`, `pms/granular_store.py`.

---

## 1. Rationale (your points)

| Current | Issue | Change |
|--------|--------|--------|
| **entry_avg** | Cost basis per unit, but in **mixed quote** (BTC in USDT, USDT in 1). Not comparable across assets. | **Remove.** Optionally replace with **cost_basis_usd** (see below). |
| **unrealized_pnl** | In mixed units (USDT for BTC leg, etc.). Not additive across positions. | **Remove** from stored schema. Derive in USD when needed (see below). |
| **mark_price** | Per-unit price in quote (e.g. BTC in USDT). Redundant once we have a single numeraire. | **Replace** with **usd_price** (price per unit in USD; aligns with assets.usd_price). |
| **notional** | open_qty × price. Redundant if we store price. | **Remove** from storage. **Derive:** `usd_notional = open_qty * usd_price`. |

---

## 2. Target schema: `positions` table

**Keep (grain + core):**

- `broker`, `account_id`, `book`, `asset` — grain
- `open_qty` — signed quantity
- `position_side` — 'long' | 'short' | 'flat'
- `updated_at` — last write

**Add:**

- **`usd_price`** — NUMERIC(36,18) NULL. Price **per unit** of `asset` in USD. Source: `assets.usd_price` (stables) or E.3 / mark provider in USD. Same name as assets table for consistency.

**Remove:**

- `entry_avg`
- `mark_price`
- `notional`
- `unrealized_pnl`

**Optional (deferred — ignore for now):**

- **`cost_basis_usd`** — NUMERIC(36,18) NULL. *(Deferred.)* Average USD cost per unit of the open position (FIFO in USD). Would enable **unrealized_pnl_usd** = `open_qty * (usd_price - cost_basis_usd)`. Not in scope for initial implementation; see §6.
- **unrealized_pnl_usd** — *(Deferred.)* Derive only when cost_basis_usd is implemented. Ignore for now.

**Derived (never stored):**

- **usd_notional** = `open_qty * usd_price`. Stored as a **generated column** in the table (PostgreSQL: `GENERATED ALWAYS AS (open_qty * usd_price) STORED`); also computed on DerivedPosition.

---

## 3. Add / remove summary

| Action | Column | Notes |
|--------|--------|--------|
| **Remove** | entry_avg | Mixed quote; replaced by cost_basis_usd if needed |
| **Remove** | mark_price | Replaced by usd_price |
| **Remove** | notional | Derive as usd_notional = open_qty * usd_price |
| **Remove** | unrealized_pnl | Derive in USD from usd_price and cost_basis_usd when needed |
| **Add** | usd_price | Per-unit price in USD (required for valuation; same name as assets.usd_price) |
| **Add (optional)** | cost_basis_usd | *(Deferred.)* Per-unit open cost in USD for unrealized_pnl_usd — ignore for now. |

Nothing else removed; no other redundancies identified. Grain and open_qty/position_side stay.

---

## 4. Implementation plan

### 4.1 Migration (Alembic)

- One revision after current head:
  - Add `usd_price` NUMERIC(36,18) NULL (renamed from usd_value in a follow-up revision for consistency with assets.usd_price).
  - Drop `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
  - *(Deferred)* Do **not** add `cost_basis_usd` in this phase.
- No data backfill required; existing rows lose dropped columns.

### 4.2 Pydantic / derivation

- **DerivedPosition:** Remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`. Add `usd_price: Optional[float]` and computed `usd_notional`. Do **not** add `cost_basis_usd` (deferred).
- **Derivation (reads.py):** Keep FIFO for `open_qty` and (if we keep it) for cost in **USD** when we have a way to get USD price per fill (e.g. from `assets` or E.3). Until then, derivation can leave `cost_basis_usd` None and only set `usd_price` in the enrichment step.
- **Enrichment (loop.py):** Replace “mark price + notional + unrealized” with:
  - Set **usd_price** from `assets.usd_price` or (E.3) from refreshed asset prices.
  - Optionally set **cost_basis_usd** if we implement FIFO cost in USD (from fills + USD prices at fill time, or from E.3).
  - **usd_notional** is computed on the model (open_qty * usd_price); do not set notional or unrealized_pnl.

### 4.3 Granular store

- **write_pms_positions:** INSERT/UPDATE grain columns + `open_qty`, `position_side`, `usd_price` only. Remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.

### 4.4 Mark price / E.3

- **Mark price provider:** Today it returns prices in quote (e.g. BTCUSDT). For `usd_price` we need **USD per unit**. Options:
  - **Stables:** Already have `assets.usd_price` (e.g. 1).
  - **Non-stables:** E.3 (or similar) writes `assets.usd_price` from a feed; enrichment reads `usd_price` from `assets`.
  - Alternatively, a “USD valuation” step that converts symbol mark to USD (e.g. via USDT pair) and writes `usd_price` only. Then `mark_price` is no longer stored.
- No need to store `mark_price` once `usd_price` exists.

### 4.5 API / display

- Expose **usd_notional** = `open_qty * usd_price` (computed on DerivedPosition). *(Deferred)* unrealized_pnl_usd when cost_basis_usd is added later.

### 4.6 Tests and scripts

- **Unit tests:** Use `usd_price` and `usd_notional`; remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
- **E2E / inject scripts:** Select and assert `usd_price`; drop checks on removed columns.

### 4.7 Docs

- Refactoring plan and Phase 2: state that position valuation is USD-only; `usd_price` per unit (same name as assets); `usd_notional` = open_qty * usd_price; unrealized PnL in USD deferred.

---

## 5. Checklist

**Implemented in this phase (no cost_basis_usd):**

- [x] Migration: add `usd_price` (rename from usd_value); drop `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
- [x] DerivedPosition: add `usd_price`; add computed `usd_notional`; remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
- [x] Derivation (reads): output positions with `usd_price=None`; no entry_avg/mark/notional/unrealized on model.
- [x] Enrichment (loop): set `usd_price` from assets; remove mark/notional/unrealized logic.
- [x] Granular store: write `usd_price` only; remove dropped columns from INSERT/UPDATE.
- [x] Tests and scripts: update to new schema.

**Deferred (ignore for now):**

- [ ] cost_basis_usd column and snap-at-fill / lookup logic.
- [ ] unrealized_pnl_usd (derive when cost_basis_usd exists).
- [ ] API/display: expose usd_notional (already on model; can use when building APIs).

---

## 6. Optional: cost_basis_usd

- **If we add it:** Derivation or a separate job must compute average cost per unit in **USD** (e.g. from order fills + USD price at fill time). E.3 or an analytics step could backfill it. Then unrealized_pnl_usd is well-defined.
- **When to snap:** We need a **USD price at fill time** for each fill. Two options:
  - **Snap at fill time:** When an order is filled, record the USD price for that asset at that moment (e.g. add `usd_price_at_fill` to the **orders** row when we set executed_qty / terminal status, or in a fills table). Derivation then uses it for FIFO cost in USD. So yes: **snap whenever orders are made (filled)**.
  - **Lookup at derivation time:** Maintain a time-series of USD prices (e.g. E.3 or price_series table by asset + time). At derivation, look up USD price at each fill’s timestamp and compute FIFO cost_basis_usd. No extra write on fill, but requires price history.
- **If we omit it:** We do not store cost basis. Unrealized PnL in USD would require an offline or separate process that has fill-level USD data. usd_notional remains `open_qty * usd_price`.

Recommendation: add the column as nullable; populate when we have a clear source. Snap-at-fill is simpler if we can add one column or write path when orders are filled.
