"""
Walk-forward XGBoost + quintile PnL (same logic as ``double_sort.ipynb``).

**Default data source:** backfilled ``strategies_daily.xsecs_daily`` joined to
``labels_daily``, using ``STRATEGIES_PIPELINE_DATABASE_URL`` or ``DATABASE_URL``
(see ``strategies.modules.factor_ls.data_loader.database_url``).

Features: all cross-sectional rank columns from ``xsecs_daily``
(``validators.XSECS_RANK_COLUMNS``).

Supervised columns (**all from ``labels_daily``** for the chosen horizon): keys
``bar_ts``, ``symbol``, ``label_asof_ts``, ``bar_ts_venue``, plus wide
``logret_fwd_*``, ``normret_fwd_*``, ``simpret_fwd_*``, ``vol_weight_*``,
``vol_weighted_return_*``. The model still uses notebook names
``ts``, ``vol_weight``, ``vol_weighted_return``, ``fwd_return`` (``fwd_return``
is ``logret_fwd`` from the label row).

Usage (from repo root)::

    python strategies/research/walk_forward_double_sort.py --pnl-out pnl.parquet

    python strategies/research/walk_forward_double_sort.py --panel-in panel.parquet --pnl-out pnl.parquet

Compare to a notebook-exported baseline::

    python strategies/research/walk_forward_double_sort.py --baseline-pnl baseline.parquet

2×2 PnL figure (same as ``double_sort.ipynb``)::

    python strategies/research/walk_forward_double_sort.py --pnl-in wf_pnl.parquet --plot-out strategies/research/plots_out/wf_grid.png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tqdm import tqdm

_REPO_ROOT = Path(__file__).resolve().parents[2]
_research_dir = Path(__file__).resolve().parent
for _p in (_REPO_ROOT, _research_dir):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from strategies.modules.factor_ls import config as factor_config  # noqa: E402
from strategies.modules.factor_ls.data_loader import database_url  # noqa: E402
from strategies.modules.factor_ls.validators import XSECS_RANK_COLUMNS  # noqa: E402


def _notebook_y_aliases(label_horizon: int) -> dict[str, str]:
    """Map ``labels_daily`` wide columns to notebook-style names (training / PnL)."""
    h = label_horizon
    return {
        "bar_ts": "ts",
        f"vol_weight_{h}": "vol_weight",
        f"vol_weighted_return_{h}": "vol_weighted_return",
        f"logret_fwd_{h}": "fwd_return",
        f"normret_fwd_{h}": "normret_fwd",
        f"simpret_fwd_{h}": "simpret_fwd",
    }


def load_backfilled_panel(
    *,
    label_horizon: int = 1,
    bar_ts_ge: object | None = None,
    bar_ts_le: object | None = None,
) -> pd.DataFrame:
    """
    **Keys and all y (label) columns** from ``labels_daily``; ranks from
    ``xsecs_daily`` joined on ``(bar_ts, symbol)``.

    Uses env-backed URL from :func:`database_url`. ``label_horizon`` must be in
    ``factor_config.DEFAULT_LABEL_HORIZONS``.
    """
    if label_horizon not in factor_config.DEFAULT_LABEL_HORIZONS:
        raise ValueError(
            f"label_horizon={label_horizon} not in DEFAULT_LABEL_HORIZONS "
            f"{factor_config.DEFAULT_LABEL_HORIZONS}"
        )

    schema = factor_config.SCHEMA_STRATEGIES_DAILY
    rank_list = ", ".join(f'x."{c}"' for c in XSECS_RANK_COLUMNS)
    h = label_horizon

    where_clauses: list[str] = []
    params: dict[str, object] = {}
    if bar_ts_ge is not None:
        where_clauses.append("l.bar_ts >= :bar_ts_ge")
        params["bar_ts_ge"] = pd.Timestamp(bar_ts_ge)
    if bar_ts_le is not None:
        where_clauses.append("l.bar_ts <= :bar_ts_le")
        params["bar_ts_le"] = pd.Timestamp(bar_ts_le)
    where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

    # l.* = supervised / metadata from labels_daily only; x.* = cross-section ranks.
    sql = f"""
    SELECT
        l.bar_ts,
        l.symbol,
        l.label_asof_ts,
        l.bar_ts_venue,
        l.logret_fwd_{h},
        l.normret_fwd_{h},
        l.simpret_fwd_{h},
        l.vol_weight_{h},
        l.vol_weighted_return_{h},
        {rank_list}
    FROM "{schema}"."labels_daily" l
    INNER JOIN "{schema}"."xsecs_daily" x
      ON l.bar_ts = x.bar_ts AND l.symbol = x.symbol
    WHERE 1=1
    {where_sql}
    ORDER BY l.bar_ts, l.symbol
    """
    engine = create_engine(database_url())
    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        return df

    df["bar_ts"] = pd.to_datetime(df["bar_ts"], utc=True)
    if "label_asof_ts" in df.columns:
        df["label_asof_ts"] = pd.to_datetime(df["label_asof_ts"], utc=True)
    df = df.rename(columns=_notebook_y_aliases(label_horizon))
    return df


# Notebook-style y columns after :func:`load_backfilled_panel` rename (or parquet with same names).
Y_COLS = ["ts", "symbol", "vol_weight", "vol_weighted_return", "fwd_return"]


def build_ts_df(
    df: pd.DataFrame,
    *,
    n_samples: int = 540,
    n_steps: int = 5,
    offset: int = 50,
    lags: int = 0,
) -> pd.DataFrame:
    ts = pd.Series(df["ts"].unique()).sort_values()
    ts_df = pd.DataFrame(
        {
            "train_start": ts,
            "train_end": ts.shift(-n_samples),
            "test_start": ts.shift(-n_samples - 1 - lags),
            "test_end": ts.shift(-n_samples - 1 - lags - n_steps),
        }
    ).dropna()
    ts_df = ts_df.iloc[offset::n_steps].reset_index(drop=True)
    return ts_df


def run_walk_forward(
    df: pd.DataFrame,
    ts_df: pd.DataFrame,
    *,
    x_cols: list[str],
    device: str = "cpu",
    random_state: int | None = None,
) -> pd.DataFrame:
    try:
        from xgboost import XGBRegressor
    except ImportError as e:  # pragma: no cover
        raise SystemExit(
            "Install xgboost: pip install xgboost"
        ) from e

    missing = [c for c in x_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing feature columns: {missing}")

    pnl_wf: list[pd.DataFrame] = []
    for _, r in tqdm(ts_df.iterrows(), total=len(ts_df), desc="walk-forward"):
        train_df = df[(df["ts"] >= r["train_start"]) & (df["ts"] <= r["train_end"])]
        test_df = df[(df["ts"] >= r["test_start"]) & (df["ts"] <= r["test_end"])]

        if len(train_df) == 0 or len(test_df) == 0:
            continue

        train_x, train_y = train_df[x_cols], train_df[Y_COLS]
        test_x, test_y = test_df[x_cols], test_df[Y_COLS]

        model = XGBRegressor(
            n_estimators=800,
            objective="reg:squarederror",
            tree_method="hist",
            learning_rate=0.05,
            reg_alpha=3,
            max_bin=256,
            max_depth=3,
            n_jobs=-1,
            device=device,
            random_state=random_state,
        )

        model.fit(train_x, train_y[["vol_weighted_return"]])
        test_y = test_y.copy()
        test_y["pred"] = model.predict(test_x)
        test_y["pred_rank"] = test_y.groupby(["ts"])["pred"].rank(pct=True)
        test_y["pred_bin"] = (test_y["pred_rank"] * 5).astype(int).clip(0, 4)

        sort_df = test_y.groupby(["pred_bin", "ts"])["vol_weighted_return"].mean()
        pnl_ts = sort_df.unstack().T
        pnl_wf.append(pnl_ts)

    if pnl_wf:
        return pd.concat(pnl_wf)
    return pd.DataFrame()


def _assert_pnl_close(a: pd.DataFrame, b: pd.DataFrame, *, rtol: float, atol: float) -> None:
    """Align on index/columns and compare floats (order may differ slightly after concat)."""
    a2 = a.sort_index().sort_index(axis=1)
    b2 = b.sort_index().sort_index(axis=1)
    common_idx = a2.index.intersection(b2.index)
    common_cols = a2.columns.intersection(b2.columns)
    if len(common_idx) == 0 or len(common_cols) == 0:
        raise SystemExit("Baseline has no overlapping index/columns with run output.")
    a2 = a2.loc[common_idx, common_cols]
    b2 = b2.loc[common_idx, common_cols]
    if not np.allclose(a2.to_numpy(dtype=float), b2.to_numpy(dtype=float), rtol=rtol, atol=atol):
        diff = (a2 - b2).abs()
        worst = diff.stack().idxmax()
        raise SystemExit(
            f"PnL differs from baseline (max abs diff {diff.max().max():.6g} at {worst}). "
            f"Try smaller rtol or inspect rows."
        )


def _x_cols_from_frame(df: pd.DataFrame) -> list[str]:
    """Use all xsec rank columns present in the frame (DB contract order)."""
    return [c for c in XSECS_RANK_COLUMNS if c in df.columns]


def _load_pnl(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, index_col=0, parse_dates=True)
    else:
        df = pd.read_parquet(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
    else:
        df = df.copy()
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
    return df.sort_index()


def _drawdown_duration_stats(drawdown: pd.Series) -> tuple[float, float]:
    """Return (max_drawdown_duration, avg_drawdown_duration) in index steps (notebook logic)."""
    drawdown_bool = drawdown > 0
    drawdown_change = drawdown_bool.astype(int).diff().fillna(0)
    start_idxs = drawdown_change[drawdown_change == 1].index.tolist()
    end_idxs = drawdown_change[drawdown_change == -1].index.tolist()
    if drawdown_bool.iloc[0]:
        start_idxs = [drawdown.index[0]] + start_idxs
    if drawdown_bool.iloc[-1]:
        end_idxs = end_idxs + [drawdown.index[-1]]

    durations: list[int] = []
    for start, end in zip(start_idxs, end_idxs):
        start_pos = drawdown.index.get_indexer_for([start])[0]
        end_pos = drawdown.index.get_indexer_for([end])[0]
        durations.append(end_pos - start_pos + 1)

    if not durations:
        return 0.0, 0.0
    return float(max(durations)), float(np.mean(durations))


def _ls_stats(pnl_ls: pd.Series) -> dict[str, float]:
    mean_ann = float(pnl_ls.mean() * 250)
    vol_ann = float(pnl_ls.std() * (250**0.5))
    sr_ann = mean_ann / vol_ann if vol_ann != 0 else float("nan")
    cum_pnl = pnl_ls.cumsum()
    roll_max = cum_pnl.cummax()
    drawdown = roll_max - cum_pnl
    avg_drawdown = float(drawdown.mean())
    max_drawdown = float(drawdown.max())
    calmar = mean_ann / max_drawdown if max_drawdown != 0 else float("nan")
    max_dd_dur, avg_dd_dur = _drawdown_duration_stats(drawdown)
    return {
        "mean_ann": mean_ann,
        "vol_ann": vol_ann,
        "sr_ann": sr_ann,
        "avg_drawdown": avg_drawdown,
        "max_drawdown": max_drawdown,
        "calmar": calmar,
        "max_drawdown_duration": max_dd_dur,
        "avg_drawdown_duration": avg_dd_dur,
    }


def plot_double_sort_grid(
    pnl_ts: pd.DataFrame,
    *,
    out_path: Path,
    plot_since: str = "2026-01-01",
    bar_from: str = "2024",
    dpi: int = 120,
) -> None:
    """
    Same 2×2 figure as ``double_sort.ipynb``: cumulative PnL by bin, long-short 4−0 stats,
    subsample since ``plot_since``, bin annualized means from ``bar_from``.
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    p = pnl_ts.copy()
    if not isinstance(p.index, pd.DatetimeIndex):
        p.index = pd.to_datetime(p.index, utc=True)
    elif p.index.tz is None:
        p.index = p.index.tz_localize("UTC")
    else:
        p.index = p.index.tz_convert("UTC")
    p = p.sort_index()

    for col in (0, 4):
        if col not in p.columns:
            raise ValueError(
                f"pnl_ts must include pred_bin columns 0..4; missing {col!r}. "
                f"Columns: {list(p.columns)[:20]}..."
            )

    p.loc[:, "4-0"] = p[4] - p[0]
    p.loc[:, "4-1"] = p[4] - p[1]

    pnl_ls = p["4-0"]
    s = _ls_stats(pnl_ls)

    pnl_ts_since = p.loc[plot_since:]
    if pnl_ts_since.empty:
        pnl_ts_since = p
    pnl_ls_since = pnl_ts_since["4-0"]
    s_s = _ls_stats(pnl_ls_since)

    fig, axes = plt.subplots(2, 2, figsize=(18, 12))

    ax00 = axes[0, 0]
    p.cumsum().plot(ax=ax00, title="Cumulative PNL (All Data)")
    ax00.annotate(
        f"Ann. Mean: {s['mean_ann']:.2%}\nAnn. Vol: {s['vol_ann']:.2%}\nAnn. SR: {s['sr_ann']:.2f}\n"
        f"Max DD: {s['max_drawdown']:.2%}\nAvg DD: {s['avg_drawdown']:.2%}\nCalmar: {s['calmar']:.2f}\n"
        f"Max DD Dur: {s['max_drawdown_duration']:.0f}\nAvg DD Dur: {s['avg_drawdown_duration']:.1f}",
        xy=(0.99, 0.02),
        xycoords="axes fraction",
        fontsize=10,
        ha="right",
        va="bottom",
        bbox=dict(boxstyle="round", fc="w", ec="0.6", alpha=0.9),
    )

    annualized_bin_mean = p.loc[bar_from:].mean() * 250
    ax01 = axes[0, 1]
    bins_to_plot = (
        [b for b in range(10)]
        if all(b in annualized_bin_mean.index for b in range(10))
        else list(annualized_bin_mean.index)
    )
    bar_container = ax01.bar(
        [str(b) for b in bins_to_plot],
        [float(annualized_bin_mean[b]) for b in bins_to_plot],
    )
    ax01.set_title(f"Annualized Mean Return by Bin ({bar_from}~, All Data)")
    ax01.set_xlabel("Predicted Score Bin")
    ax01.set_ylabel("Ann. Mean Return")
    ax01.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    for rect, b in zip(bar_container, bins_to_plot):
        height = rect.get_height()
        offset = -0.05 if height >= 0 else 0.05
        y_pos = height + offset
        ax01.text(
            rect.get_x() + rect.get_width() / 2,
            y_pos,
            f"{float(annualized_bin_mean[b]):+.0%}",
            ha="center",
            va="center",
            fontsize=15,
            rotation=0,
            color="white",
        )

    ax10 = axes[1, 0]
    pnl_ts_since.cumsum().plot(ax=ax10, title=f"Cumulative PNL (Since {plot_since})")
    ax10.annotate(
        f"Ann. Mean: {s_s['mean_ann']:.2%}\nAnn. Vol: {s_s['vol_ann']:.2%}\nAnn. SR: {s_s['sr_ann']:.2f}\n"
        f"Max DD: {s_s['max_drawdown']:.2%}\nAvg DD: {s_s['avg_drawdown']:.2%}\nCalmar: {s_s['calmar']:.2f}\n"
        f"Max DD Dur: {s_s['max_drawdown_duration']:.0f}\nAvg DD Dur: {s_s['avg_drawdown_duration']:.1f}",
        xy=(0.99, 0.02),
        xycoords="axes fraction",
        fontsize=10,
        ha="right",
        va="bottom",
        bbox=dict(boxstyle="round", fc="w", ec="0.6", alpha=0.9),
    )

    annualized_bin_mean_since = pnl_ts_since.loc[bar_from:].mean() * 250
    ax11 = axes[1, 1]
    bins_to_plot_since = (
        [b for b in range(10)]
        if all(b in annualized_bin_mean_since.index for b in range(10))
        else list(annualized_bin_mean_since.index)
    )
    bar_container_since = ax11.bar(
        [str(b) for b in bins_to_plot_since],
        [float(annualized_bin_mean_since[b]) for b in bins_to_plot_since],
    )
    ax11.set_title(f"Annualized Mean Return by Bin ({bar_from}~, Since {plot_since})")
    ax11.set_xlabel("Predicted Score Bin")
    ax11.set_ylabel("Ann. Mean Return")
    ax11.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    for rect, b in zip(bar_container_since, bins_to_plot_since):
        height = rect.get_height()
        offset = -0.05 if height >= 0 else 0.05
        y_pos = height + offset
        ax11.text(
            rect.get_x() + rect.get_width() / 2,
            y_pos,
            f"{float(annualized_bin_mean_since[b]):+.0%}",
            ha="center",
            va="center",
            fontsize=15,
            rotation=0,
            color="white",
        )

    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    load_dotenv(_REPO_ROOT / ".env")

    p = argparse.ArgumentParser(description="Walk-forward double-sort (xsecs_daily + labels_daily).")
    src = p.add_mutually_exclusive_group()
    src.add_argument(
        "--panel-in",
        type=Path,
        help="Load panel parquet/csv instead of DB (must include rank + y columns)",
    )
    src.add_argument(
        "--from-create-data",
        action="store_true",
        help="Build panel from research/create_data.py (legacy OHLCV path, not backfill tables)",
    )
    p.add_argument(
        "--pnl-in",
        type=Path,
        help="Load existing PnL parquet/csv; skip panel + walk-forward (use with --plot-out)",
    )
    p.add_argument("--panel-out", type=Path, help="Save panel parquet after load")
    p.add_argument("--pnl-out", type=Path, help="Save PnL time series (parquet or .csv from suffix)")
    p.add_argument("--baseline-pnl", type=Path, help="Compare output to this parquet/csv (aligned subset)")
    p.add_argument("--rtol", type=float, default=1e-5)
    p.add_argument("--atol", type=float, default=1e-8)
    p.add_argument("--n-samples", type=int, default=540)
    p.add_argument("--n-steps", type=int, default=5)
    p.add_argument("--offset", type=int, default=50)
    p.add_argument("--lags", type=int, default=0)
    p.add_argument("--device", default="cpu", help="xgboost device: cpu or cuda")
    p.add_argument("--random-state", type=int, default=None)
    p.add_argument(
        "--label-horizon",
        type=int,
        default=1,
        help="Wide label suffix on labels_daily (e.g. 1 -> vol_weighted_return_1); default 1",
    )
    p.add_argument("--bar-ts-ge", type=str, default=None, help="Optional ISO date lower bound on bar_ts")
    p.add_argument("--bar-ts-le", type=str, default=None, help="Optional ISO date upper bound on bar_ts")
    p.add_argument(
        "--plot-out",
        type=Path,
        help="Write 2×2 summary figure (png/pdf/svg) matching double_sort.ipynb",
    )
    p.add_argument(
        "--plot-since",
        type=str,
        default="2026-01-01",
        help="Lower date for bottom-row cumulative PnL slice (ISO)",
    )
    p.add_argument(
        "--plot-bar-from",
        type=str,
        default="2024",
        help="Start label for bin bar charts (annualized mean from this date)",
    )
    p.add_argument("--plot-dpi", type=int, default=120, help="Figure DPI for --plot-out")
    args = p.parse_args()

    if args.pnl_in is not None and (args.panel_in is not None or args.from_create_data):
        raise SystemExit("--pnl-in cannot be combined with --panel-in or --from-create-data.")

    bar_ge = pd.Timestamp(args.bar_ts_ge) if args.bar_ts_ge else None
    bar_le = pd.Timestamp(args.bar_ts_le) if args.bar_ts_le else None

    if args.pnl_in is not None:
        pnl_ts = _load_pnl(args.pnl_in)
    else:
        if args.panel_in is not None:
            path = args.panel_in
            if path.suffix.lower() == ".csv":
                peek = pd.read_csv(path, nrows=2)
                date_cols = ["ts"] if "ts" in peek.columns else (["bar_ts"] if "bar_ts" in peek.columns else [])
                df = pd.read_csv(path, parse_dates=date_cols or False)
            else:
                df = pd.read_parquet(path)
            if "bar_ts" in df.columns and "ts" not in df.columns:
                df = df.rename(columns={"bar_ts": "ts"})
            h = args.label_horizon
            if f"vol_weighted_return_{h}" in df.columns and "vol_weighted_return" not in df.columns:
                ren = {
                    f"vol_weight_{h}": "vol_weight",
                    f"vol_weighted_return_{h}": "vol_weighted_return",
                    f"logret_fwd_{h}": "fwd_return",
                }
                if f"normret_fwd_{h}" in df.columns:
                    ren[f"normret_fwd_{h}"] = "normret_fwd"
                if f"simpret_fwd_{h}" in df.columns:
                    ren[f"simpret_fwd_{h}"] = "simpret_fwd"
                df = df.rename(columns=ren)
        elif args.from_create_data:
            from create_data import build_ranked_panel_df  # noqa: WPS433

            df = build_ranked_panel_df()
        else:
            df = load_backfilled_panel(
                label_horizon=args.label_horizon,
                bar_ts_ge=bar_ge,
                bar_ts_le=bar_le,
            )

        if df.empty:
            raise SystemExit("Panel is empty (check DB URL, backfill, or date filters).")

        x_cols = _x_cols_from_frame(df)
        if len(x_cols) != len(XSECS_RANK_COLUMNS):
            missing = [c for c in XSECS_RANK_COLUMNS if c not in df.columns]
            raise SystemExit(
                f"Expected all {len(XSECS_RANK_COLUMNS)} xsec rank columns; missing {missing}. "
                "Reload from DB or use a full xsecs export."
            )

        for c in Y_COLS:
            if c not in df.columns:
                raise SystemExit(f"Panel missing required column {c!r} (y / ids).")

        df = df.dropna(subset=x_cols + ["vol_weight", "vol_weighted_return", "fwd_return"])

        if args.panel_out is not None:
            args.panel_out.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(args.panel_out)

        ts_df = build_ts_df(
            df,
            n_samples=args.n_samples,
            n_steps=args.n_steps,
            offset=args.offset,
            lags=args.lags,
        )
        print(f"Panel rows={len(df):,} ts unique={df['ts'].nunique()} wf windows={len(ts_df)} x_cols={len(x_cols)}")

        pnl_ts = run_walk_forward(
            df,
            ts_df,
            x_cols=x_cols,
            device=args.device,
            random_state=args.random_state,
        )
        print(f"PnL shape={pnl_ts.shape} index range {pnl_ts.index.min()} .. {pnl_ts.index.max()}")

    if pnl_ts.empty:
        raise SystemExit("PnL is empty (nothing to save or plot).")

    if args.pnl_out is not None:
        args.pnl_out.parent.mkdir(parents=True, exist_ok=True)
        if args.pnl_out.suffix.lower() == ".csv":
            pnl_ts.to_csv(args.pnl_out)
        else:
            pnl_ts.to_parquet(args.pnl_out)

    if args.baseline_pnl is not None:
        base = (
            pd.read_parquet(args.baseline_pnl)
            if args.baseline_pnl.suffix.lower() == ".parquet"
            else pd.read_csv(args.baseline_pnl, index_col=0, parse_dates=True)
        )
        _assert_pnl_close(pnl_ts, base, rtol=args.rtol, atol=args.atol)
        print("Baseline comparison: OK (within rtol/atol on overlapping index/columns).")

    if args.plot_out is not None:
        plot_double_sort_grid(
            pnl_ts,
            out_path=args.plot_out,
            plot_since=args.plot_since,
            bar_from=args.plot_bar_from,
            dpi=args.plot_dpi,
        )
        print(f"Wrote plot {args.plot_out}")


if __name__ == "__main__":
    main()
