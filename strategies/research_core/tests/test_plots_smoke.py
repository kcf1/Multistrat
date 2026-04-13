from __future__ import annotations

import pytest

matplotlib = pytest.importorskip("matplotlib")
import pandas as pd

from strategies.research_core.plots import (
    plot_bin_average_bar,
    plot_bin_cumulative,
    plot_bin_return_box,
    plot_signal_distribution,
)


def _df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts": pd.to_datetime(
                ["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02"], utc=True
            ),
            "symbol": ["A", "B", "A", "B"],
            "bin": [1, 2, 1, 2],
            "signal_clean": [0.1, 0.9, 0.2, 0.8],
            "fwd_ret_1": [0.01, -0.02, 0.03, 0.01],
        }
    )


def test_plot_functions_return_figure() -> None:
    df = _df()
    figs = [
        plot_signal_distribution(df, signal_col="signal_clean"),
        plot_bin_average_bar(df, horizon=1),
        plot_bin_return_box(df, horizon=1),
        plot_bin_cumulative(df, horizon=1),
    ]
    for fig in figs:
        assert hasattr(fig, "axes")

