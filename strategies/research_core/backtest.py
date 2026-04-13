"""Simple vectorized bin/LS backtest helpers."""

from __future__ import annotations

import pandas as pd

from strategies.research_core.analytics import basic_backtest_stats
from strategies.research_core.constants import BIN_COL, DEFAULT_BINS, LS_RET_COL, TS_COL


def build_long_short_returns(
    df: pd.DataFrame,
    *,
    horizon: int = 1,
    top_bin: int | None = None,
    bottom_bin: int = 1,
) -> pd.DataFrame:
    """Build long-short return series from top and bottom bins."""
    col = f"fwd_ret_{horizon}"
    n_bins = int(df[BIN_COL].max())
    long_bin = top_bin if top_bin is not None else n_bins
    by_ts_bin = df.groupby([TS_COL, BIN_COL], dropna=True)[col].mean().reset_index()
    wide = by_ts_bin.pivot(index=TS_COL, columns=BIN_COL, values=col).sort_index()
    out = wide.copy()
    out[LS_RET_COL] = out[long_bin] - out[bottom_bin]
    return out.reset_index()


def long_short_stats(
    df: pd.DataFrame,
    *,
    horizon: int = 1,
) -> dict[str, float]:
    ls = build_long_short_returns(df, horizon=horizon)[LS_RET_COL]
    return basic_backtest_stats(ls)

