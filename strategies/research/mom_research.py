"""Runnable example for strategies.research_core utilities."""

from __future__ import annotations

import os
import pathlib
import sys

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sqlalchemy import create_engine

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from strategies.research_core.analytics import average_return_by_bin, signal_summary
from strategies.research_core.backtest import long_short_stats
from strategies.research_core.cleaning import winsorize_by_ts, robust_clip
from strategies.research_core.constants import BIN_COL
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
    load_dotenv(ROOT / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL is not set. Add it to .env or environment variables.")

    engine = create_engine(
        database_url,
        connect_args={"options": "-csearch_path=market_data,public"},
    )
    df = pd.read_sql_query(
        """
        SELECT 
            open_time as ts,
            symbol,
            close as close
        FROM ohlcv
        WHERE 
            interval = '1h' AND
            open_time <= '2024-01-01'
        """, 
        con=engine
    )
    df = df.sort_values(by=["ts","symbol"])
    n = 24
    df["close_log"] = np.log(df["close"])
    df["signal_raw"] = df.groupby("symbol")["close_log"].diff(n)
    df = df.dropna(subset=["signal_raw"])
    

    frame = build_signal_frame(df, signal_col="signal_raw")
    #frame = winsorize_by_ts(frame)
    frame = robust_clip(frame)
    frame = add_cross_sectional_ranks(frame, signal_col="signal_clean")
    frame = assign_bins(frame, n_bins=10)
    sub = frame[frame["ts"] == "2024-01-01"]
    plt.scatter(sub['signal_clean'], sub[BIN_COL])
    plt.show()
    frame = add_equal_weight_by_bin(frame)
    h = 24
    frame = add_forward_returns(frame, horizons=(h,))

    print("signal_summary:", signal_summary(frame))
    print("bin_returns:")
    print(average_return_by_bin(frame, horizon=h))
    print("ls_stats:", long_short_stats(frame, horizon=h))

    out_dir = ROOT / "strategies" / "research" / "plots_out" / "factor_ls"
    out_dir.mkdir(parents=True, exist_ok=True)

    figs = {
        f"signal_distribution_ts_winsorized_{n}.png": plot_signal_distribution(frame, signal_col="signal_clean"),
        "bin_avg_bar_h1.png": plot_bin_average_bar(frame, horizon=h),
        "bin_box_h1.png": plot_bin_return_box(frame, horizon=h),
        "bin_cumulative_h1.png": plot_bin_cumulative(frame, horizon=h),
    }
    
    plt.show()
    #for filename, fig in figs.items():
    #    fig.savefig(out_dir / filename, dpi=120, bbox_inches="tight")
    #    fig.clf()

    #print(f"saved_plots_dir: {out_dir}")


if __name__ == "__main__":
    main()

