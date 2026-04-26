#!/usr/bin/env python3
"""Wide-window pandas-style backfill for ``factor_ls`` (single or few ``run_pipeline`` calls).

Computes intraday load from ``first_bar_ts - WARMUP_CALENDAR_DAYS`` through
``last_bar_ts + 1 day`` (exclusive on ``open_time``), then persists only
``[first_bar_ts, last_bar_ts]`` daily keys. ``norm_close`` is correct in-process
via ``cumsum`` as long as this window includes continuous per-symbol history.

For **chunked** backfills (multiple disjoint ``run_pipeline`` calls without overlapping
daily history per symbol), ``norm_close`` would reset unless you overlap loads or seed
from ``l1feats_daily``—prefer one wide run when memory allows.
"""

from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd

from strategies.modules.factor_ls import config
from strategies.modules.factor_ls.pipeline import run_pipeline


def main() -> None:
    p = argparse.ArgumentParser(description="Backfill strategies_daily via one wide factor_ls run.")
    p.add_argument(
        "--first-bar-ts",
        type=str,
        required=True,
        help="First UTC midnight bar_ts to persist (ISO date, e.g. 2020-01-01).",
    )
    p.add_argument(
        "--last-bar-ts",
        type=str,
        required=True,
        help="Last UTC midnight bar_ts to persist (inclusive).",
    )
    p.add_argument("--symbols", type=str, default=None, help="Optional comma-separated symbol subset.")
    p.add_argument("--interval", type=str, default=None, help="OHLCV interval (default from config).")
    p.add_argument("--no-persist", action="store_true", help="Compute only.")
    p.add_argument("--pipeline-version", type=str, default=None, help="Tag rows in DB.")
    args = p.parse_args()

    first = pd.Timestamp(args.first_bar_ts, tz="UTC").normalize()
    last = pd.Timestamp(args.last_bar_ts, tz="UTC").normalize()
    if last < first:
        raise SystemExit("last-bar-ts must be >= first-bar-ts")

    load_ge = first - timedelta(days=config.WARMUP_CALENDAR_DAYS)
    load_lt = last + timedelta(days=1)

    symbols = tuple(s.strip().upper() for s in args.symbols.split(",")) if args.symbols else None

    res = run_pipeline(
        symbols=symbols,
        open_time_ge=load_ge,
        open_time_lt=load_lt,
        bar_ts_ge=first,
        bar_ts_le=last,
        persist=not args.no_persist,
        pipeline_version=args.pipeline_version,
        interval=args.interval,
    )
    print("L1 rows:", len(res.l1))
    print("Pre rows:", len(res.pre))
    print("Factors rows:", len(res.factors))
    print("Xsecs rows:", len(res.xsecs))
    print("Labels rows:", len(res.labels))
    if not res.l1.empty:
        print("bar_ts range:", res.l1["bar_ts"].min(), "…", res.l1["bar_ts"].max())


if __name__ == "__main__":
    main()
