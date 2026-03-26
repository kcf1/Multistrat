"""Forward return utilities with point-in-time-safe alignment."""

from __future__ import annotations

import pandas as pd

from strategies.research_core.constants import CLOSE_COL, SYMBOL_COL


def add_forward_returns(
    df: pd.DataFrame,
    *,
    horizons: tuple[int, ...] = (1, 5, 10),
    price_col: str = CLOSE_COL,
) -> pd.DataFrame:
    """Add forward returns per symbol for each horizon."""
    out = df.copy()
    grouped = out.groupby(SYMBOL_COL)[price_col]
    for h in horizons:
        if h <= 0:
            raise ValueError("Horizon must be positive")
        fwd = grouped.shift(-h) / out[price_col] - 1.0
        out[f"fwd_ret_{h}"] = fwd
    return out

