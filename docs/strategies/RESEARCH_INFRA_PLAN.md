# Research Infrastructure Plan (Factor Research First)

Purpose: define the first research-core utilities needed before implementing strategy runtime details.

Scope target: factor long/short research workflow from raw data to bin portfolio analysis and backtest statistics.

---

## 1) Goals

- Build reusable research utilities for signal research and validation.
- Standardize data contracts so results are reproducible and comparable.
- Keep this layer isolated from execution/OMS details.

---

## 2) Core utilities needed

1. Transform raw data to signal-ready datasets.
2. Plot signal distributions and produce summary stats.
3. Handle outliers (winsorize, robust clipping).
4. Rank cross-section and form equal-weighted bin portfolios at `t`.
5. Compute forward returns for each bin portfolio.
6. Plot bin average return charts (bar/box/cumulative).
7. Compute core backtest statistics.

---

## 3) Proposed module layout

Package path: `strategies/research_core/`

- `transform.py`
  - raw -> aligned signal frame
  - input validation, time/symbol alignment, missing-data policy
- `cleaning.py`
  - winsorization by timestamp group
  - robust z-score clipping and outlier diagnostics
- `portfolio.py`
  - cross-sectional ranking
  - quantile/bin assignment
  - equal-weight weights per bin
- `forward_returns.py`
  - horizon return computation (`1/5/10...`)
  - point-in-time safe merge with bin assignments
- `analytics.py`
  - signal summary (coverage, dispersion, skew/kurtosis, stability)
  - bin-level performance summaries
  - rank-IC / hit-rate style diagnostics (optional in v1)
- `plots.py`
  - signal distribution plots
  - bin bar/box/cumulative return plots
- `backtest.py`
  - lightweight vectorized bin/LS backtest wrapper
  - aggregate stats
- `constants.py` (optional)
  - canonical column names and default parameters

---

## 4) Canonical research data contract

Base columns expected:
- `ts` (timestamp)
- `symbol`
- `close`

Core derived columns:
- `signal_raw`
- `signal_clean`
- `rank`
- `bin`
- `weight`
- `fwd_ret_{h}`
- `bin_ret`
- `ls_ret` (optional long-short aggregate)

Rules:
- Use explicit timezone handling.
- Enforce point-in-time integrity (no lookahead).
- Keep deterministic sorting by (`ts`, `symbol`) at key steps.

---

## 5) Implementation tasks (small and concrete)

- [x] Create `strategies/research_core/` package scaffold.
- [x] Implement `transform.py` entrypoint (`build_signal_frame`).
- [x] Implement `cleaning.py` (winsorize/robust clipping).
- [x] Implement `portfolio.py` (rank/bin/EW weights).
- [x] Implement `forward_returns.py` (multi-horizon returns).
- [x] Implement `analytics.py` summary statistics.
- [x] Implement `plots.py` for distribution + bin charts.
- [x] Implement `backtest.py` wrapper + stats report.
- [x] Add a runnable sample in `strategies/research/` (script or notebook).
- [x] Document usage examples and defaults.

Quick usage:

- Example script: `python strategies/research/run_research_core_example.py`
- Typical flow:
  1. `build_signal_frame(...)`
  2. `winsorize_by_ts(...)`
  3. `add_cross_sectional_ranks(...)`
  4. `assign_bins(...)`
  5. `add_equal_weight_by_bin(...)`
  6. `add_forward_returns(...)`
  7. `signal_summary(...)`, `average_return_by_bin(...)`, `long_short_stats(...)`

---

## 6) Testing plan

### 6.1 Unit tests

- `cleaning.py`
  - winsorization boundary correctness
  - NaN handling and empty-group behavior
- `portfolio.py`
  - bin assignment correctness with ties/small universes
  - equal-weight sums and sign conventions
- `forward_returns.py`
  - horizon return correctness
  - off-by-one and alignment checks

### 6.2 Integration tests

- End-to-end synthetic pipeline:
  - raw frame -> cleaned signal -> bins -> forward returns -> stats table
- Ensure deterministic outputs on fixed fixtures.

### 6.3 Regression tests

- Fixed input fixture with snapshot metrics:
  - average bin returns
  - long-short return series summary
  - turnover proxy (if implemented)

### 6.4 Plot smoke tests

- Plotting functions return figure objects and run without errors on fixtures.

---

## 7) Delivery order (recommended)

1. `portfolio.py` + `forward_returns.py`
2. `cleaning.py`
3. `analytics.py`
4. `plots.py`
5. `backtest.py`
6. sample runner + documentation polish

Reason: this order unlocks usable factor evaluation quickly while keeping complexity incremental.

---

## 8) Out of scope (first pass)

- Live execution integration.
- Full transaction-cost model and slippage microstructure engine.
- Distributed feature store orchestration.
- Multi-model training pipelines.

These can be layered after core research utilities are stable.

