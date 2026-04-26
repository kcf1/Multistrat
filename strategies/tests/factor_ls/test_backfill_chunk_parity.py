"""Backfill-related invariants (determinism; xsection tertile path smoke)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.modules.factor_ls import config
from strategies.modules.factor_ls.features_l1 import compute_l1_features
from strategies.modules.factor_ls.signals_xsection import compute_cross_section_ranks


def _tiny_daily():
    syms = list(config.MARKET_BASKET) + [f"A{i:03d}USDT" for i in range(96)]
    rows = []
    for d in range(120):
        ts = pd.Timestamp("2022-01-01", tz="UTC") + pd.Timedelta(days=d)
        for s in syms:
            rows.append(
                {
                    "bar_ts": ts,
                    "symbol": s,
                    "close": 50.0 + 0.02 * d + 0.001 * (hash(s) % 500),
                    "volume": 800.0,
                    "quote_volume": (50.0 + 0.02 * d) * 800.0,
                    "taker_buy_base_volume": 400.0,
                }
            )
    return pd.DataFrame(rows)


def test_compute_l1_features_idempotent_on_same_daily():
    daily = _tiny_daily()
    a = compute_l1_features(daily)
    b = compute_l1_features(daily)
    pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True))


def test_norm_close_equals_cumsum_norm_return_per_symbol():
    daily = _tiny_daily()
    l1 = compute_l1_features(daily)
    for _, g in l1.groupby("symbol"):
        g = g.sort_values("bar_ts")
        exp = g["norm_return"].cumsum()
        np.testing.assert_allclose(
            exp.to_numpy(),
            g["norm_close"].to_numpy(),
            rtol=1e-9,
            atol=1e-9,
            equal_nan=True,
        )


def test_xsection_quotevlm_tertiles_vlm_global():
    """One date: quotevlm_rank global tertiles; mom_rank within bin; vlm_rank global."""
    ts = pd.Timestamp("2023-06-01", tz="UTC")
    n = 9
    rows = []
    for i in range(n):
        rows.append(
            {
                "bar_ts": ts,
                "symbol": f"X{i}USDT",
                "mom_score": float(i),
                "trend_score": float(i),
                "breakout_score": float(i),
                "vwaprev_score": float(i),
                "resrev_score": float(i),
                "skew_score": float(i),
                "vol_score": float(i),
                "betasq_score": float(i),
                "maxret_score": float(i),
                "takerratio_score": float(i),
                "vlm_score": float(100 + i),
                "quotevlm_score": float(i),
                "retvlmcor_score": float(i),
            }
        )
    fac = pd.DataFrame(rows)
    x = compute_cross_section_ranks(fac)
    g = x[x["bar_ts"] == ts].merge(fac[["symbol", "mom_score"]], on="symbol").sort_values("quotevlm_rank")
    assert len(g) == n
    assert g["vlm_rank"].min() >= 0 and g["vlm_rank"].max() <= 1
    assert (g["quotevlm_rank"].diff().dropna() >= 0).all()
    bins = pd.qcut(g["quotevlm_rank"].rank(method="first"), q=3, labels=False)
    for _, h in g.groupby(bins, sort=False):
        assert h["mom_rank"].min() >= 0 and h["mom_rank"].max() <= 1
        if len(h) == 3:
            mr = h.sort_values("mom_score")["mom_rank"].to_numpy()
            assert len(np.unique(np.round(mr, 6))) == 3
            assert mr[0] < mr[1] < mr[2] and mr[0] >= 0 and mr[2] <= 1
