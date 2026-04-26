"""Scalar ``*_score`` columns for ``factors_daily`` (notebook ``combine_features`` + ``get_*_score``)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .validators import FACTORS_SCORE_COLUMNS, PRECOMBINED_COLUMNS


def combine_features(X: list[pd.Series], rescale: bool = True) -> pd.Series:
    """Match ``double_sort.ipynb`` ``combine_features``."""
    if rescale:
        deflator = [x.abs().expanding(20).mean() for x in X]
        rescaled = [x / d for x, d in zip(X, deflator)]
    else:
        deflator = [pd.Series(1.0, index=x.index) for x in X]
        rescaled = list(X)
    signal = pd.Series(0.0, index=X[0].index)
    scaler = pd.Series(0.0, index=X[0].index)
    for x, d in zip(rescaled, deflator):
        signal = signal + x
        scaler = scaler + d
    return (signal * scaler) / len(X)


def _scores_for_symbol_block(pre: pd.DataFrame) -> pd.DataFrame:
    """One contiguous block for a single ``symbol`` (sorted by ``bar_ts``)."""
    out = pre[["bar_ts", "symbol"]].copy()
    # Pull pre columns as series
    def tri(abbr: str, ns: tuple[int, ...]) -> list[pd.Series]:
        return [pre[f"{abbr}_{n}"].astype(float) for n in ns]

    out["mom_score"] = combine_features(tri("mom", (10, 20, 40)), rescale=True)
    out["trend_score"] = combine_features(tri("trend", (5, 10, 20)), rescale=True)
    out["breakout_score"] = combine_features(tri("breakout", (10, 20, 40)), rescale=True)
    out["vwaprev_score"] = combine_features(tri("vwaprev", (5, 10, 20)), rescale=True)
    out["resrev_score"] = combine_features(tri("resrev", (5, 10, 20)), rescale=True)
    out["skew_score"] = combine_features(tri("skew", (10, 20, 40)), rescale=True)
    out["vol_score"] = combine_features(tri("vol", (10, 20, 40)), rescale=True)
    out["betasq_score"] = combine_features(tri("betasq", (10, 20, 40)), rescale=True)
    out["maxret_score"] = combine_features(tri("maxret", (10, 20, 40)), rescale=True)
    out["takerratio_score"] = combine_features(tri("takerratio", (10, 20, 40)), rescale=True)
    out["vlm_score"] = combine_features(tri("vlm", (10, 20, 40)), rescale=True)
    out["quotevlm_score"] = combine_features(tri("quotevlm", (10, 20, 40)), rescale=True)
    out["retvlmcor_score"] = combine_features(tri("retvlmcor", (10, 20, 40)), rescale=True)

    mat = out[list(FACTORS_SCORE_COLUMNS)].to_numpy(dtype=float)
    out["n_nonfinite"] = np.sum(~np.isfinite(mat), axis=1).astype(int)
    return out


def compute_factor_scores(pre: pd.DataFrame) -> pd.DataFrame:
    """Consume precombined columns only (no DB read of ``signals_daily`` required)."""
    if pre.empty:
        return pd.DataFrame(columns=["bar_ts", "symbol"] + list(FACTORS_SCORE_COLUMNS) + ["n_nonfinite"])
    missing = [c for c in PRECOMBINED_COLUMNS if c not in pre.columns]
    if missing:
        raise ValueError(f"precombined frame missing columns: {missing[:5]}")

    parts = []
    for _, g in pre.groupby("symbol", sort=False):
        parts.append(_scores_for_symbol_block(g.sort_values("bar_ts")))
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["symbol", "bar_ts"]).reset_index(drop=True)
