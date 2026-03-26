from __future__ import annotations

import pandas as pd

from strategies.research_core.cleaning import robust_clip, winsorize_by_ts


def test_winsorize_by_ts_clips_extremes() -> None:
    df = pd.DataFrame(
        {
            "ts": ["2026-01-01"] * 5,
            "symbol": ["A", "B", "C", "D", "E"],
            "signal_raw": [1.0, 2.0, 3.0, 4.0, 100.0],
        }
    )
    out = winsorize_by_ts(df, lower_q=0.0, upper_q=0.8)
    assert out["signal_clean"].max() < 100.0


def test_robust_clip_preserves_nans() -> None:
    series = pd.Series([1.0, 2.0, float("nan"), 3.0, 100.0])
    out = robust_clip(series, zmax=2.0)
    assert out.isna().sum() == 1
    assert out.dropna().max() < 100.0

