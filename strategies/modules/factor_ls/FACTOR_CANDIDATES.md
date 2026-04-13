# Factor Candidates (persistent-first)

Use this checklist to decide what to include in v1. Most entries below are intended as persistent features.

## Momentum
- [ ] `mom_1d` return
- [ ] `mom_5d` return
- [ ] `mom_20d` return
- [ ] residual momentum (beta/market-neutral)

## Mean Reversion
- [ ] z-score vs rolling mean (`z_20`)
- [ ] short-term reversal (`ret_1d` vs trailing trend)

## Volatility / Risk
- [ ] rolling realized volatility (`rv_20`)
- [ ] downside volatility (`down_vol_20`)
- [ ] ATR normalized by price

## Liquidity / Microstructure
- [ ] quote volume rolling average
- [ ] turnover proxy
- [ ] spread proxy (if available)

## Carry / Derivatives (if available)
- [ ] funding-rate carry
- [ ] basis (perp vs spot)
- [ ] open-interest trend / z-score

## Quality / Stability
- [ ] signal stability score
- [ ] data quality flags / missingness

## Cross-sectional transforms
- [ ] winsorize by date
- [ ] rank by date
- [ ] z-score by date
- [ ] sector/bucket neutralization (if grouping available)

## Composite signal
- [ ] weighted linear blend
- [ ] regime-conditioned blend
- [ ] confidence-scaled blend

## Execution-time ephemeral checks
- [ ] tradability filter (halt, stale)
- [ ] min-liquidity guard
- [ ] turnover gate
- [ ] position/risk-budget scaling

