"""Plot helpers for signal and bin performance analysis."""

from __future__ import annotations

import pandas as pd

from strategies.research_core.constants import BIN_COL, TS_COL


def _plt():
    import matplotlib.pyplot as plt

    return plt


def plot_signal_distribution(df: pd.DataFrame, *, signal_col: str):
    plt = _plt()
    fig, ax = plt.subplots()
    series = pd.to_numeric(df[signal_col], errors="coerce").dropna()
    series.hist(ax=ax, bins=40)
    ax.set_title(f"Distribution: {signal_col}")
    ax.set_xlabel(signal_col)
    ax.set_ylabel("count")

    if not series.empty:
        moments_text = "\n".join(
            [
                f"mean: {series.mean():.4f}",
                f"std: {series.std(ddof=1):.4f}",
                f"skew: {series.skew():.4f}",
                f"kurt: {series.kurt():.4f}",
            ]
        )
        ax.text(
            0.98,
            0.98,
            moments_text,
            transform=ax.transAxes,
            ha="right",
            va="top",
            fontsize=9,
            bbox={"boxstyle": "round", "alpha": 0.15},
        )
    return fig


def plot_bin_average_bar(df: pd.DataFrame, *, horizon: int = 1):
    plt = _plt()
    col = f"fwd_ret_{horizon}"
    table = df.groupby(BIN_COL)[col].mean().sort_index()
    fig, ax = plt.subplots()
    table.plot(kind="bar", ax=ax)
    ax.set_title(f"Average Bin Returns (h={horizon})")
    ax.set_xlabel("bin")
    ax.set_ylabel("avg return")
    return fig


def plot_bin_return_box(df: pd.DataFrame, *, horizon: int = 1):
    plt = _plt()
    col = f"fwd_ret_{horizon}"
    fig, ax = plt.subplots()
    df.boxplot(column=col, by=BIN_COL, ax=ax)
    ax.set_title(f"Bin Return Boxplot (h={horizon})")
    ax.set_xlabel("bin")
    ax.set_ylabel("forward return")
    fig.suptitle("")
    return fig


def plot_bin_cumulative(
    df: pd.DataFrame,
    *,
    horizon: int = 1,
    ts_col: str = TS_COL,
):
    plt = _plt()
    col = f"fwd_ret_{horizon}"
    table = (
        df.groupby([ts_col, BIN_COL], dropna=True)[col]
        .mean()
        .reset_index()
        .pivot(index=ts_col, columns=BIN_COL, values=col)
        .sort_index()
    )
    cum = (1 + table.fillna(0)).cumprod() - 1
    fig, ax = plt.subplots()
    cum.plot(ax=ax)
    ax.set_title(f"Cumulative Bin Returns (h={horizon})")
    ax.set_xlabel("timestamp")
    ax.set_ylabel("cumulative return")
    return fig

