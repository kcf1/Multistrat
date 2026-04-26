"""Precombined bundle shape and column contract."""

from __future__ import annotations

import pandas as pd

from strategies.modules.factor_ls import config
from strategies.modules.factor_ls.features_l1 import compute_l1_features
from strategies.modules.factor_ls.signals_precombined import compute_precombined
from strategies.modules.factor_ls.validators import PRECOMBINED_COLUMNS


def _minimal_l1():
    syms = list(config.MARKET_BASKET) + ["ALTUSDT"]
    rows = []
    for i in range(120):
        ts = pd.Timestamp("2024-06-01", tz="UTC") + pd.Timedelta(days=i)
        for s in syms:
            rows.append(
                {
                    "bar_ts": ts,
                    "symbol": s,
                    "close": 50.0 + 0.1 * i,
                    "volume": 2000.0,
                    "quote_volume": (50.0 + 0.1 * i) * 2000.0,
                    "taker_buy_base_volume": 1000.0,
                }
            )
    daily = pd.DataFrame(rows)
    return compute_l1_features(daily)


def test_precombined_has_39_family_columns():
    l1 = _minimal_l1()
    pre = compute_precombined(l1)
    for c in PRECOMBINED_COLUMNS:
        assert c in pre.columns
    assert "n_nonfinite" in pre.columns
