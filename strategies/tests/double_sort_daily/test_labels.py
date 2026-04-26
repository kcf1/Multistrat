"""Label forward-shift correctness."""

from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.modules.double_sort_daily.features_l1 import compute_l1_features
from strategies.modules.double_sort_daily.labels import compute_labels


def test_logret_fwd_1_matches_manual_shift():
    rows = []
    for i in range(10):
        ts = pd.Timestamp("2025-01-01", tz="UTC") + pd.Timedelta(days=i)
        rows.append(
            {
                "bar_ts": ts,
                "symbol": "TSTUSDT",
                "close": float(100 + i),
                "volume": 1000.0,
                "quote_volume": float(100 + i) * 1000.0,
                "taker_buy_base_volume": 500.0,
            }
        )
    l1 = compute_l1_features(pd.DataFrame(rows))
    lab = compute_labels(l1)
    g = l1[l1["symbol"] == "TSTUSDT"].sort_values("bar_ts")
    log_close = np.log(g["close"].to_numpy(dtype=float))
    manual = pd.Series(log_close).diff(1).shift(-1).to_numpy()
    got = lab[lab["symbol"] == "TSTUSDT"].sort_values("bar_ts")["logret_fwd_1"].to_numpy()
    np.testing.assert_allclose(manual[:-1], got[:-1], rtol=1e-9, atol=1e-9)
    assert pd.isna(got[-1])
