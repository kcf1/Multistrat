# Position valuation schema restructure

**Goal:** Single numeraire (USD) for position valuation. Remove mixed-quote fields; store one price-per-unit in USD; derive notional and (optionally) unrealized PnL in USD.

**References:** Current schema in `positions` table and `DerivedPosition` (docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md). Enrichment in `pms/loop.py`, `pms/granular_store.py`.

---

## 1. Rationale (your points)

| Current | Issue | Change |
|--------|--------|--------|
| **entry_avg** | Cost basis per unit, but in **mixed quote** (BTC in USDT, USDT in 1). Not comparable across assets. | **Remove.** Optionally replace with **cost_basis_usd** (see below). |
| **unrealized_pnl** | In mixed units (USDT for BTC leg, etc.). Not additive across positions. | **Remove** from stored schema. Derive in USD when needed (see below). |
| **mark_price** | Per-unit price in quote (e.g. BTC in USDT). Redundant once we have a single numeraire. | **Replace** with **usd_value** (price per unit in USD). |
| **notional** | open_qty × price. Redundant if we store price. | **Remove** from storage. **Derive:** `notional_usd = open_qty * usd_value`. |

---

## 2. Target schema: `positions` table

**Keep (grain + core):**

- `broker`, `account_id`, `book`, `asset` — grain
- `open_qty` — signed quantity
- `position_side` — 'long' | 'short' | 'flat'
- `updated_at` — last write

**Add:**

- **`usd_value`** — NUMERIC(36,18) NULL. Price **per unit** of `asset` in USD. Source: `assets.usd_price` (stables) or E.3 / mark provider in USD. One numeraire for all positions.

**Remove:**

- `entry_avg`
- `mark_price`
- `notional`
- `unrealized_pnl`

**Optional (deferred — ignore for now):**

- **`cost_basis_usd`** — NUMERIC(36,18) NULL. *(Deferred.)* Average USD cost per unit of the open position (FIFO in USD). Would enable **unrealized_pnl_usd** = `open_qty * (usd_value - cost_basis_usd)`. Not in scope for initial implementation; see §6.
- **unrealized_pnl_usd** — *(Deferred.)* Derive only when cost_basis_usd is implemented. Ignore for now.

**Derived (never stored):**

- **notional_usd** = `open_qty * usd_value` (compute in API/display when needed).

---

## 3. Add / remove summary

| Action | Column | Notes |
|--------|--------|--------|
| **Remove** | entry_avg | Mixed quote; replaced by cost_basis_usd if needed |
| **Remove** | mark_price | Replaced by usd_value |
| **Remove** | notional | Derive as open_qty * usd_value |
| **Remove** | unrealized_pnl | Derive in USD from usd_value and cost_basis_usd when needed |
| **Add** | usd_value | Per-unit price in USD (required for valuation) |
| **Add (optional)** | cost_basis_usd | *(Deferred.)* Per-unit open cost in USD for unrealized_pnl_usd — ignore for now. |

Nothing else removed; no other redundancies identified. Grain and open_qty/position_side stay.

---

## 4. Implementation plan

### 4.1 Migration (Alembic)

- One revision after current head:
  - Add `usd_value` NUMERIC(36,18) NULL.
  - Drop `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
  - *(Deferred)* Do **not** add `cost_basis_usd` in this phase.
- No data backfill required; existing rows lose dropped columns.

### 4.2 Pydantic / derivation

- **DerivedPosition:** Remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`. Add `usd_value: Optional[float]`. Do **not** add `cost_basis_usd` (deferred).
- **Derivation (reads.py):** Keep FIFO for `open_qty` and (if we keep it) for cost in **USD** when we have a way to get USD price per fill (e.g. from `assets` or E.3). Until then, derivation can leave `cost_basis_usd` None and only set `usd_value` in the enrichment step.
- **Enrichment (loop.py):** Replace “mark price + notional + unrealized” with:
  - Set **usd_value** from `assets.usd_price` or (E.3) from refreshed asset prices.
  - Optionally set **cost_basis_usd** if we implement FIFO cost in USD (from fills + USD prices at fill time, or from E.3).
  - Do **not** set notional or unrealized_pnl on the model; they are derived when needed.

### 4.3 Granular store

- **write_pms_positions:** INSERT/UPDATE grain columns + `open_qty`, `position_side`, `usd_value` only. Remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.

### 4.4 Mark price / E.3

- **Mark price provider:** Today it returns prices in quote (e.g. BTCUSDT). For `usd_value` we need **USD per unit**. Options:
  - **Stables:** Already have `assets.usd_price` (e.g. 1).
  - **Non-stables:** E.3 (or similar) writes `assets.usd_price` from a feed; enrichment reads `usd_value` from `assets`.
  - Alternatively, a “USD valuation” step that converts symbol mark to USD (e.g. via USDT pair) and writes `usd_value` only. Then `mark_price` is no longer stored.
- No need to store `mark_price` once `usd_value` exists.

### 4.5 API / display

- Expose **notional_usd** = `open_qty * usd_value` (computed). *(Deferred)* unrealized_pnl_usd when cost_basis_usd is added later.

### 4.6 Tests and scripts

- **Unit tests:** Use `usd_value`; remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`. Derive notional_usd where needed.
- **E2E / inject scripts:** Select and assert `usd_value`; drop checks on removed columns.

### 4.7 Docs

- Refactoring plan and Phase 2: state that position valuation is USD-only; `usd_value` per unit; notional and unrealized PnL in USD are derived.

---

## 5. Checklist

**Implemented in this phase (no cost_basis_usd):**

- [x] Migration: add `usd_value`; drop `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
- [x] DerivedPosition: add `usd_value`; remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
- [x] Derivation (reads): output positions with `usd_value=None`; no entry_avg/mark/notional/unrealized on model.
- [x] Enrichment (loop): set `usd_value` from assets; remove mark/notional/unrealized logic.
- [x] Granular store: write `usd_value` only; remove dropped columns from INSERT/UPDATE.
- [x] Tests and scripts: update to new schema.

**Deferred (ignore for now):**

- [ ] cost_basis_usd column and snap-at-fill / lookup logic.
- [ ] unrealized_pnl_usd (derive when cost_basis_usd exists).
- [ ] API/display: derive notional_usd (can add when building APIs).

---

## 6. Optional: cost_basis_usd

- **If we add it:** Derivation or a separate job must compute average cost per unit in **USD** (e.g. from order fills + USD price at fill time). E.3 or an analytics step could backfill it. Then unrealized_pnl_usd is well-defined.
- **When to snap:** We need a **USD price at fill time** for each fill. Two options:
  - **Snap at fill time:** When an order is filled, record the USD price for that asset at that moment (e.g. add `usd_price_at_fill` to the **orders** row when we set executed_qty / terminal status, or in a fills table). Derivation then uses it for FIFO cost in USD. So yes: **snap whenever orders are made (filled)**.
  - **Lookup at derivation time:** Maintain a time-series of USD prices (e.g. E.3 or price_series table by asset + time). At derivation, look up USD price at each fill’s timestamp and compute FIFO cost_basis_usd. No extra write on fill, but requires price history.
- **If we omit it:** We do not store cost basis. Unrealized PnL in USD would require an offline or separate process that has fill-level USD data. Notional_usd remains `open_qty * usd_value`.

Recommendation: add the column as nullable; populate when we have a clear source. Snap-at-fill is simpler if we can add one column or write path when orders are filled.
