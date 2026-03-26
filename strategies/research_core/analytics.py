"""Summary statistics and performance aggregation helpers."""

from __future__ import annotations

import math

import pandas as pd

from strategies.research_core.constants import BIN_COL, SIGNAL_CLEAN_COL, SIGNAL_RAW_COL


def signal_summary(df: pd.DataFrame, *, signal_col: str = SIGNAL_CLEAN_COL) -> dict[str, float]:
    """Return compact descriptive statistics for a signal column."""
    series = pd.to_numeric(df[signal_col], errors="coerce").dropna()
    if series.empty:
        return {"count": 0.0}
    return {
        "count": float(series.count()),
        "mean": float(series.mean()),
        "std": float(series.std(ddof=1)),
        "min": float(series.min()),
        "p01": float(series.quantile(0.01)),
        "p50": float(series.quantile(0.50)),
        "p99": float(series.quantile(0.99)),
        "max": float(series.max()),
        "skew": float(series.skew()),
        "kurt": float(series.kurt()),
    }


def average_return_by_bin(df: pd.DataFrame, *, horizon: int = 1) -> pd.DataFrame:
    """Average forward return by bin."""
    col = f"fwd_ret_{horizon}"
    grouped = df.groupby(BIN_COL, dropna=True)[col].mean().reset_index(name="avg_ret")
    return grouped.sort_values(BIN_COL).reset_index(drop=True)


def basic_backtest_stats(returns: pd.Series, *, periods_per_year: int = 252) -> dict[str, float]:
    """Compute compact return-series statistics."""
    r = pd.to_numeric(returns, errors="coerce").dropna()
    if r.empty:
        return {"count": 0.0}
    cum = (1 + r).cumprod()
    running_max = cum.cummax()
    drawdown = cum / running_max - 1
    ann_return = float((1 + r.mean()) ** periods_per_year - 1)
    ann_vol = float(r.std(ddof=1) * math.sqrt(periods_per_year))
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0.0
    return {
        "count": float(r.count()),
        "total_return": float(cum.iloc[-1] - 1),
        "ann_return": ann_return,
        "ann_vol": ann_vol,
        "sharpe": float(sharpe),
        "max_drawdown": float(drawdown.min()),
    }

