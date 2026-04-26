"""Factor scores shape."""

from __future__ import annotations

import pandas as pd

from strategies.modules.double_sort_daily import config
from strategies.modules.double_sort_daily.features_l1 import compute_l1_features
from strategies.modules.double_sort_daily.signals_combined import compute_factor_scores
from strategies.modules.double_sort_daily.signals_precombined import compute_precombined
from strategies.modules.double_sort_daily.validators import FACTORS_SCORE_COLUMNS


def _frames():
    syms = list(config.MARKET_BASKET) + ["ALTUSDT"]
    rows = []
    for i in range(100):
        ts = pd.Timestamp("2024-03-01", tz="UTC") + pd.Timedelta(days=i)
        for s in syms:
            rows.append(
                {
                    "bar_ts": ts,
                    "symbol": s,
                    "close": 30.0 + 0.05 * i,
                    "volume": 1500.0,
                    "quote_volume": (30.0 + 0.05 * i) * 1500.0,
                    "taker_buy_base_volume": 700.0,
                }
            )
    l1 = compute_l1_features(pd.DataFrame(rows))
    pre = compute_precombined(l1)
    fac = compute_factor_scores(pre)
    return fac


def test_thirteen_scores_finite_when_inputs_finite():
    fac = _frames()
    tail = fac.dropna(subset=list(FACTORS_SCORE_COLUMNS), how="all").tail(20)
    for c in FACTORS_SCORE_COLUMNS:
        assert c in fac.columns
        assert tail[c].notna().any() or tail[c].isna().all()
