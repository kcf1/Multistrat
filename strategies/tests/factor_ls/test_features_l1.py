"""L1 feature tests."""

from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.modules.factor_ls import config
from strategies.modules.factor_ls.features_l1 import compute_l1_features


def _synth_daily(symbols: list[str], n: int = 300) -> pd.DataFrame:
    rows = []
    for i in range(n):
        ts = pd.Timestamp("2024-01-01", tz="UTC") + pd.Timedelta(days=i)
        for s in symbols:
            close = 100.0 + 0.01 * i + hash(s) % 7
            vol = 1000.0 + i
            qv = close * vol
            rows.append(
                {
                    "bar_ts": ts,
                    "symbol": s,
                    "close": close,
                    "volume": vol,
                    "quote_volume": qv,
                    "taker_buy_base_volume": vol * 0.55,
                }
            )
    return pd.DataFrame(rows)


def test_vwap_250_matches_ratio_of_rolling_sums():
    syms = list(config.MARKET_BASKET) + ["ALTUSDT"]
    daily = _synth_daily(syms, n=280)
    l1 = compute_l1_features(daily)
    sub = l1[l1["symbol"] == "ALTUSDT"].sort_values("bar_ts").reset_index(drop=True)
    qv_roll = sub["quote_volume"].rolling(250, min_periods=250).sum()
    v_roll = sub["volume"].rolling(250, min_periods=250).sum()
    expected = qv_roll / v_roll
    m = sub["vwap_250"].notna()
    np.testing.assert_allclose(sub.loc[m, "vwap_250"].to_numpy(), expected.loc[m].to_numpy(), rtol=1e-9, atol=1e-9)


def test_determinism():
    daily = _synth_daily(list(config.MARKET_BASKET) + ["ZZZUSDT"], n=120)
    a = compute_l1_features(daily)
    b = compute_l1_features(daily.sample(frac=1, random_state=42))
    a = a.sort_values(["symbol", "bar_ts"]).reset_index(drop=True)
    b = b.sort_values(["symbol", "bar_ts"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b, check_dtype=False)
