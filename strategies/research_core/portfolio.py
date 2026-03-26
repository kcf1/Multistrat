"""Cross-sectional ranking and bin-portfolio construction."""

from __future__ import annotations

import pandas as pd

from strategies.research_core.constants import BIN_COL, RANK_COL, SYMBOL_COL, TS_COL, WEIGHT_COL


def add_cross_sectional_ranks(
    df: pd.DataFrame,
    *,
    signal_col: str,
    rank_col: str = RANK_COL,
    ascending: bool = True,
) -> pd.DataFrame:
    """Add per-timestamp percentile rank for the signal."""
    out = df.copy()
    out[rank_col] = out.groupby(TS_COL)[signal_col].rank(pct=True, method="average", ascending=ascending)
    return out


def assign_bins(
    df: pd.DataFrame,
    *,
    rank_col: str = RANK_COL,
    n_bins: int = 5,
    bin_col: str = BIN_COL,
) -> pd.DataFrame:
    """Assign bins using rank percentiles into [1..n_bins]."""
    if n_bins < 2:
        raise ValueError("n_bins must be >= 2")
    out = df.copy()
    bins = (out[rank_col] * n_bins).apply(int).clip(lower=1, upper=n_bins)
    out[bin_col] = bins
    return out


def add_equal_weight_by_bin(
    df: pd.DataFrame,
    *,
    bin_col: str = BIN_COL,
    weight_col: str = WEIGHT_COL,
) -> pd.DataFrame:
    """Within each (ts, bin), assign equal weights that sum to 1."""
    out = df.copy()
    counts = out.groupby([TS_COL, bin_col])[SYMBOL_COL].transform("count")
    out[weight_col] = 1.0 / counts
    return out

