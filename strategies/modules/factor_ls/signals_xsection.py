"""Cross-sectional percentile ranks (``xsecs_daily``)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .validators import FACTORS_SCORE_COLUMNS, XSECS_RANK_COLUMNS

# Global ranks on volume scores; other families ranked within quotevlm tertile bins.
_WITHIN_BIN_SPECS: tuple[tuple[str, str], ...] = tuple(
    (f"{fam}_rank", f"{fam}_score")
    for fam in (
        "mom",
        "trend",
        "breakout",
        "vwaprev",
        "resrev",
        "skew",
        "vol",
        "betasq",
        "maxret",
        "takerratio",
        "retvlmcor",
    )
)


def _assign_volume_tertile(sub: pd.DataFrame) -> pd.Series:
    """Equal-count tertiles from ``quotevlm_rank``; fallback bin 0 when n < 3 or qcut fails."""
    idx = sub.index
    r = sub["quotevlm_rank"]
    out = pd.Series(np.nan, index=idx, dtype=float)
    valid = r.notna()
    if not valid.any():
        return out
    rv = r[valid]
    if len(rv) < 3:
        out.loc[valid] = 0.0
        return out
    rk = rv.rank(method="first")
    try:
        bins = pd.qcut(rk, q=3, labels=False)
        out.loc[rv.index] = bins.astype(float)
    except ValueError:
        out.loc[valid] = 0.0
    return out


def compute_cross_section_ranks(factors: pd.DataFrame) -> pd.DataFrame:
    """
    Per ``bar_ts``:

    - ``quotevlm_rank``: global ``rank(pct=True)`` of ``quotevlm_score``.
    - Tertile bins from ``quotevlm_rank`` (equal-count ``qcut`` on tie-broken ranks).
    - ``vlm_rank``: global rank on ``vlm_score`` (not within-bin).
    - Other ``*_rank``: ``rank(pct=True)`` within ``(bar_ts, tertile_bin)``.
    """
    if factors.empty:
        return pd.DataFrame(columns=["bar_ts", "symbol"] + list(XSECS_RANK_COLUMNS) + ["n_symbols_xs"])

    df = factors.copy()
    df["bar_ts"] = pd.to_datetime(df["bar_ts"], utc=True)

    out = df[["bar_ts", "symbol"]].copy()
    g = df.groupby("bar_ts", sort=True)
    out["n_symbols_xs"] = g["symbol"].transform("count")

    if "quotevlm_score" in df.columns:
        out["quotevlm_rank"] = g["quotevlm_score"].rank(pct=True)
    else:
        out["quotevlm_rank"] = np.nan

    if "vlm_score" in df.columns:
        out["vlm_rank"] = g["vlm_score"].rank(pct=True)
    else:
        out["vlm_rank"] = np.nan

    work = df[["bar_ts", "symbol"]].copy()
    work["quotevlm_rank"] = out["quotevlm_rank"].to_numpy()

    vol_bin = pd.Series(np.nan, index=work.index, dtype=float)
    for _, sub in work.groupby("bar_ts", sort=True):
        vol_bin.loc[sub.index] = _assign_volume_tertile(sub).to_numpy()
    work["_vol_bin"] = vol_bin

    for scol in FACTORS_SCORE_COLUMNS:
        if scol in df.columns:
            work[scol] = df[scol].to_numpy()

    for rcol, scol in _WITHIN_BIN_SPECS:
        if scol not in df.columns:
            out[rcol] = np.nan
            continue
        ranked = pd.Series(np.nan, index=work.index, dtype=float)
        m = work["_vol_bin"].notna()
        if m.any():
            sub = work.loc[m, ["bar_ts", "_vol_bin", scol]].copy()
            sub["_r"] = sub.groupby(["bar_ts", "_vol_bin"], sort=True)[scol].rank(pct=True)
            ranked.loc[sub.index] = sub["_r"]
        out[rcol] = ranked

    ordered = out[["bar_ts", "symbol"] + list(XSECS_RANK_COLUMNS) + ["n_symbols_xs"]]
    return ordered.sort_values(["bar_ts", "symbol"]).reset_index(drop=True)
