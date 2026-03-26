"""Outlier handling and signal cleaning helpers."""

from __future__ import annotations

import pandas as pd

from strategies.research_core.constants import SIGNAL_CLEAN_COL, SIGNAL_RAW_COL, TS_COL


def winsorize_by_ts(
    df: pd.DataFrame,
    *,
    source_col: str = SIGNAL_RAW_COL,
    output_col: str = SIGNAL_CLEAN_COL,
    lower_q: float = 0.01,
    upper_q: float = 0.99,
) -> pd.DataFrame:
    """Winsorize source signal cross-sectionally at each timestamp."""
    if not 0 <= lower_q < upper_q <= 1:
        raise ValueError("Require 0 <= lower_q < upper_q <= 1")
    if TS_COL not in df.columns or source_col not in df.columns:
        raise ValueError(f"DataFrame must include `{TS_COL}` and `{source_col}`")

    out = df.copy()

    def _winsorize(group: pd.Series) -> pd.Series:
        low = group.quantile(lower_q)
        high = group.quantile(upper_q)
        return group.clip(lower=low, upper=high)

    out[output_col] = out.groupby(TS_COL, group_keys=False)[source_col].transform(_winsorize)
    return out


def robust_clip(
    df: pd.DataFrame,
    *,
    source_col: str = SIGNAL_RAW_COL,
    output_col: str = SIGNAL_CLEAN_COL,
    group_col: str = TS_COL,
    zmax: float = 5.0,
) -> pd.DataFrame:
    """Cross-sectional robust clipping using median absolute deviation by group."""
    if zmax <= 0:
        raise ValueError("zmax must be > 0")
    if group_col not in df.columns or source_col not in df.columns:
        raise ValueError(f"DataFrame must include `{group_col}` and `{source_col}`")

    out = df.copy()

    def _clip_group(series: pd.Series) -> pd.Series:
        median = series.median()
        mad = (series - median).abs().median()
        if pd.isna(mad) or mad == 0:
            return series.copy()
        robust_z = 0.6745 * (series - median) / mad
        clipped_z = robust_z.clip(-zmax, zmax)
        return median + (clipped_z * mad / 0.6745)

    out[output_col] = out.groupby(group_col, group_keys=False)[source_col].transform(_clip_group)
    return out

