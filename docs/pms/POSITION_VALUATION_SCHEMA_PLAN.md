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

**Optional (recommended for reporting):**

- **`cost_basis_usd`** — NUMERIC(36,18) NULL. Average **USD cost per unit** of the open position (FIFO in USD). Enables:  
  **unrealized_pnl_usd** = `open_qty * (usd_value - cost_basis_usd)` (derive in API or VIEW, not stored).  
  If omitted, we do not store cost basis; unrealized PnL in USD would require a separate service that converts historical fills to USD (e.g. E.3 or analytics).

**When to snap cost_basis_usd:** Yes — we need a **USD price at fill time** for each fill that affects the position. Either: **(A)** when an order is filled, capture and store the USD price for that asset at that time (e.g. `usd_price_at_fill` on the order row or in a fills table), then derivation does FIFO in USD using that; or **(B)** maintain a time-series of USD prices (e.g. E.3 or market data) and at derivation time look up the USD price at each fill’s timestamp. We **recompute** cost_basis_usd every tick when we re-derive positions from orders; the “snap” is either at write-time (per fill) or at read-time (lookup by timestamp). Without one of these, cost_basis_usd cannot be computed correctly.

**Derived (never stored):**

- **notional_usd** = `open_qty * usd_value`
- **unrealized_pnl_usd** = `open_qty * (usd_value - cost_basis_usd)` when `cost_basis_usd` is present

---

## 3. Add / remove summary

| Action | Column | Notes |
|--------|--------|--------|
| **Remove** | entry_avg | Mixed quote; replaced by cost_basis_usd if needed |
| **Remove** | mark_price | Replaced by usd_value |
| **Remove** | notional | Derive as open_qty * usd_value |
| **Remove** | unrealized_pnl | Derive in USD from usd_value and cost_basis_usd when needed |
| **Add** | usd_value | Per-unit price in USD (required for valuation) |
| **Add (optional)** | cost_basis_usd | Per-unit open cost in USD for unrealized_pnl_usd |

Nothing else removed; no other redundancies identified. Grain and open_qty/position_side stay.

---

## 4. Implementation plan

### 4.1 Migration (Alembic)

- One revision after current head:
  - Add `usd_value` NUMERIC(36,18) NULL.
  - Optionally add `cost_basis_usd` NUMERIC(36,18) NULL.
  - Drop `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
- No data backfill required for new columns (NULL ok); existing rows lose dropped columns.

### 4.2 Pydantic / derivation

- **DerivedPosition:** Remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`. Add `usd_value: Optional[float]`. Optionally add `cost_basis_usd: Optional[float]`.
- **Derivation (reads.py):** Keep FIFO for `open_qty` and (if we keep it) for cost in **USD** when we have a way to get USD price per fill (e.g. from `assets` or E.3). Until then, derivation can leave `cost_basis_usd` None and only set `usd_value` in the enrichment step.
- **Enrichment (loop.py):** Replace “mark price + notional + unrealized” with:
  - Set **usd_value** from `assets.usd_price` or (E.3) from refreshed asset prices.
  - Optionally set **cost_basis_usd** if we implement FIFO cost in USD (from fills + USD prices at fill time, or from E.3).
  - Do **not** set notional or unrealized_pnl on the model; they are derived when needed.

### 4.3 Granular store

- **write_pms_positions:** INSERT/UPDATE only grain columns + `open_qty`, `position_side`, `usd_value`, and optionally `cost_basis_usd`. Remove any reference to `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.

### 4.4 Mark price / E.3

- **Mark price provider:** Today it returns prices in quote (e.g. BTCUSDT). For `usd_value` we need **USD per unit**. Options:
  - **Stables:** Already have `assets.usd_price` (e.g. 1).
  - **Non-stables:** E.3 (or similar) writes `assets.usd_price` from a feed; enrichment reads `usd_value` from `assets`.
  - Alternatively, a “USD valuation” step that converts symbol mark to USD (e.g. via USDT pair) and writes `usd_value` only. Then `mark_price` is no longer stored.
- No need to store `mark_price` once `usd_value` exists.

### 4.5 API / display

- Expose **notional_usd** = `open_qty * usd_value` (computed).
- Expose **unrealized_pnl_usd** = `open_qty * (usd_value - cost_basis_usd)` when `cost_basis_usd` is not null; otherwise null or 0 by policy.

### 4.6 Tests and scripts

- **Unit tests:** Update all construction and assertions to use `usd_value` (and optional `cost_basis_usd`); remove `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`. Add tests for derived notional_usd and unrealized_pnl_usd where relevant.
- **E2E / inject scripts:** Select and assert `usd_value` (and optional `cost_basis_usd`); drop checks on removed columns. Use derived notional where needed.

### 4.7 Docs

- Refactoring plan and Phase 2: state that position valuation is USD-only; `usd_value` per unit; notional and unrealized PnL in USD are derived.

---

## 5. Checklist

- [ ] Migration: add `usd_value` (and optionally `cost_basis_usd`); drop `entry_avg`, `mark_price`, `notional`, `unrealized_pnl`.
- [ ] DerivedPosition: new shape (usd_value; optional cost_basis_usd); remove old fields.
- [ ] Derivation (reads): output usd_value/cost_basis_usd instead of entry_avg/mark/notional/unrealized (cost_basis_usd only if FIFO in USD is implemented).
- [ ] Enrichment (loop): set usd_value from assets (and E.3); optionally set cost_basis_usd; remove mark/notional/unrealized logic.
- [ ] Granular store: write only new columns; remove dropped columns from INSERT/UPDATE.
- [ ] Mark price / E.3: ensure USD valuation path feeds `usd_value` (no stored mark_price).
- [ ] **If using cost_basis_usd:** Either snap USD price at fill time (e.g. `usd_price_at_fill` on orders or fills) or provide price time-series for lookup at derivation time.
- [ ] API/display: derive notional_usd and unrealized_pnl_usd.
- [ ] Tests and scripts: update to new schema and derived fields.
- [ ] Docs: update refactoring plan and Phase 2.

---

## 6. Optional: cost_basis_usd

- **If we add it:** Derivation or a separate job must compute average cost per unit in **USD** (e.g. from order fills + USD price at fill time). E.3 or an analytics step could backfill it. Then unrealized_pnl_usd is well-defined.
- **When to snap:** We need a **USD price at fill time** for each fill. Two options:
  - **Snap at fill time:** When an order is filled, record the USD price for that asset at that moment (e.g. add `usd_price_at_fill` to the **orders** row when we set executed_qty / terminal status, or in a fills table). Derivation then uses it for FIFO cost in USD. So yes: **snap whenever orders are made (filled)**.
  - **Lookup at derivation time:** Maintain a time-series of USD prices (e.g. E.3 or price_series table by asset + time). At derivation, look up USD price at each fill’s timestamp and compute FIFO cost_basis_usd. No extra write on fill, but requires price history.
- **If we omit it:** We do not store cost basis. Unrealized PnL in USD would require an offline or separate process that has fill-level USD data. Notional_usd remains `open_qty * usd_value`.

Recommendation: add the column as nullable; populate when we have a clear source. Snap-at-fill is simpler if we can add one column or write path when orders are filled.
