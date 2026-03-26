"""Runnable example for strategies.research_core utilities."""

from __future__ import annotations

import pathlib
import sys

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategies.research_core.analytics import average_return_by_bin, signal_summary
from strategies.research_core.backtest import long_short_stats
from strategies.research_core.cleaning import winsorize_by_ts
from strategies.research_core.forward_returns import add_forward_returns
from strategies.research_core.plots import (
    plot_bin_average_bar,
    plot_bin_cumulative,
    plot_bin_return_box,
    plot_signal_distribution,
)
from strategies.research_core.portfolio import add_cross_sectional_ranks, add_equal_weight_by_bin, assign_bins
from strategies.research_core.transform import build_signal_frame


def main() -> None:
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

    print("signal_summary:", signal_summary(frame))
    print("bin_returns:")
    print(average_return_by_bin(frame, horizon=1))
    print("ls_stats:", long_short_stats(frame, horizon=1))

    out_dir = ROOT / "research" / "plots_out"
    out_dir.mkdir(parents=True, exist_ok=True)

    figs = {
        "signal_distribution.png": plot_signal_distribution(frame, signal_col="signal_clean"),
        "bin_avg_bar_h1.png": plot_bin_average_bar(frame, horizon=1),
        "bin_box_h1.png": plot_bin_return_box(frame, horizon=1),
        "bin_cumulative_h1.png": plot_bin_cumulative(frame, horizon=1),
    }
    for filename, fig in figs.items():
        fig.savefig(out_dir / filename, dpi=120, bbox_inches="tight")
        fig.clf()

    print(f"saved_plots_dir: {out_dir}")


if __name__ == "__main__":
    main()

