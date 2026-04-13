"""Transform raw data into canonical research dataframe."""

from __future__ import annotations

import pandas as pd

from strategies.research_core.constants import CLOSE_COL, SYMBOL_COL, TS_COL


def build_signal_frame(
    df: pd.DataFrame,
    *,
    signal_col: str,
    copy: bool = True,
) -> pd.DataFrame:
    """Validate and normalize a raw dataframe into canonical sorted form."""
    required = {TS_COL, SYMBOL_COL, CLOSE_COL, signal_col}
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df.copy() if copy else df
    out[TS_COL] = pd.to_datetime(out[TS_COL], utc=True, errors="raise")
    out[SYMBOL_COL] = out[SYMBOL_COL].astype(str)
    out[CLOSE_COL] = pd.to_numeric(out[CLOSE_COL], errors="coerce")
    out[signal_col] = pd.to_numeric(out[signal_col], errors="coerce")
    out = out.sort_values([TS_COL, SYMBOL_COL], kind="mergesort").reset_index(drop=True)
    return out

