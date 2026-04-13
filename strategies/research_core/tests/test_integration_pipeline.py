from __future__ import annotations

import pandas as pd

from strategies.research_core.analytics import average_return_by_bin, signal_summary
from strategies.research_core.backtest import long_short_stats
from strategies.research_core.cleaning import winsorize_by_ts
from strategies.research_core.forward_returns import add_forward_returns
from strategies.research_core.portfolio import add_cross_sectional_ranks, add_equal_weight_by_bin, assign_bins
from strategies.research_core.transform import build_signal_frame


def test_end_to_end_pipeline_synthetic() -> None:
    df = pd.DataFrame(
        {
            "ts": [
                "2026-01-01",
                "2026-01-01",
                "2026-01-02",
                "2026-01-02",
                "2026-01-03",
                "2026-01-03",
            ],
            "symbol": ["A", "B", "A", "B", "A", "B"],
            "close": [100.0, 100.0, 110.0, 90.0, 120.0, 80.0],
            "signal_raw": [0.1, 0.9, 0.2, 0.8, 0.3, 0.7],
        }
    )
    frame = build_signal_frame(df, signal_col="signal_raw")
    frame = winsorize_by_ts(frame)
    frame = add_cross_sectional_ranks(frame, signal_col="signal_clean")
    frame = assign_bins(frame, n_bins=2)
    frame = add_equal_weight_by_bin(frame)
    frame = add_forward_returns(frame, horizons=(1,))

    stats = signal_summary(frame)
    by_bin = average_return_by_bin(frame, horizon=1)
    ls_stats = long_short_stats(frame, horizon=1)

    assert stats["count"] > 0
    assert len(by_bin) == 2
    assert "sharpe" in ls_stats

