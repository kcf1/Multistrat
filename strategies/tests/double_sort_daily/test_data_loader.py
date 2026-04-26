"""Tests for ``data_loader`` daily aggregation."""

from __future__ import annotations

import pandas as pd

from strategies.modules.double_sort_daily.data_loader import aggregate_to_daily_bars


def test_aggregate_daily_last_close_and_sums():
    intraday = pd.DataFrame(
        {
            "symbol": ["AAAUSDT"] * 3,
            "open_time": pd.to_datetime(
                [
                    "2026-01-01 10:00:00+00:00",
                    "2026-01-01 14:00:00+00:00",
                    "2026-01-01 18:00:00+00:00",
                ],
                utc=True,
            ),
            "close": [100.0, 110.0, 105.0],
            "volume": [1.0, 2.0, 3.0],
            "quote_volume": [10.0, 22.0, 33.0],
            "taker_buy_base_volume": [0.5, 1.0, 1.5],
        }
    )
    out = aggregate_to_daily_bars(intraday)
    assert len(out) == 1
    row = out.iloc[0]
    assert row["close"] == 105.0
    assert row["volume"] == 6.0
    assert row["quote_volume"] == 65.0
    assert row["taker_buy_base_volume"] == 3.0
    assert pd.Timestamp(row["bar_ts"]).normalize() == pd.Timestamp("2026-01-01", tz="UTC")
