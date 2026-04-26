"""Cross-sectional percentile ranks (``xsecs_daily``)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .validators import FACTORS_SCORE_COLUMNS, XSECS_RANK_COLUMNS


def compute_cross_section_ranks(factors: pd.DataFrame) -> pd.DataFrame:
    """``groupby(bar_ts).rank(pct=True)`` on each ``*_score`` (Phase 1 default)."""
    if factors.empty:
        return pd.DataFrame(columns=["bar_ts", "symbol"] + list(XSECS_RANK_COLUMNS) + ["n_symbols_xs"])

    df = factors.copy()
    df["bar_ts"] = pd.to_datetime(df["bar_ts"], utc=True)

    rank_map = {
        "mom_rank": "mom_score",
        "trend_rank": "trend_score",
        "breakout_rank": "breakout_score",
        "vwaprev_rank": "vwaprev_score",
        "resrev_rank": "resrev_score",
        "skew_rank": "skew_score",
        "vol_rank": "vol_score",
        "betasq_rank": "betasq_score",
        "maxret_rank": "maxret_score",
        "takerratio_rank": "takerratio_score",
        "vlm_rank": "vlm_score",
        "quotevlm_rank": "quotevlm_score",
        "retvlmcor_rank": "retvlmcor_score",
    }

    out = df[["bar_ts", "symbol"]].copy()
    g = df.groupby("bar_ts", sort=True)
    out["n_symbols_xs"] = g["symbol"].transform("count")

    for rcol, scol in rank_map.items():
        if scol not in df.columns:
            out[rcol] = np.nan
            continue
        out[rcol] = g[scol].rank(pct=True)

    return out.sort_values(["bar_ts", "symbol"]).reset_index(drop=True)
