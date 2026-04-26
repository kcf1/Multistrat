"""Cross-section rank invariants."""

from __future__ import annotations

import pandas as pd

from strategies.modules.factor_ls import config
from strategies.modules.factor_ls.features_l1 import compute_l1_features
from strategies.modules.factor_ls.signals_combined import compute_factor_scores
from strategies.modules.factor_ls.signals_precombined import compute_precombined
from strategies.modules.factor_ls.signals_xsection import compute_cross_section_ranks
from strategies.modules.factor_ls.validators import XSECS_RANK_COLUMNS


def _frames():
    syms = list(config.MARKET_BASKET) + ["ALTUSDT", "OTHERUSDT"]
    rows = []
    for i in range(80):
        ts = pd.Timestamp("2024-02-01", tz="UTC") + pd.Timedelta(days=i)
        for s in syms:
            rows.append(
                {
                    "bar_ts": ts,
                    "symbol": s,
                    "close": 20.0 + 0.02 * i + 0.001 * hash(s),
                    "volume": 800.0,
                    "quote_volume": (20.0 + 0.02 * i) * 800.0,
                    "taker_buy_base_volume": 400.0,
                }
            )
    l1 = compute_l1_features(pd.DataFrame(rows))
    pre = compute_precombined(l1)
    fac = compute_factor_scores(pre)
    x = compute_cross_section_ranks(fac)
    return x


def test_ranks_in_unit_interval_when_scores_valid():
    x = _frames()
    tail = x.tail(50)
    for c in XSECS_RANK_COLUMNS:
        s = tail[c].dropna()
        if len(s):
            assert (s >= 0).all() and (s <= 1).all(), c
