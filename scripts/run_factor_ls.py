#!/usr/bin/env python3
"""CLI entrypoint for the Phase 1 daily pipeline (see ``--help``)."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from strategies.modules.factor_ls import run_pipeline


def main() -> None:
    p = argparse.ArgumentParser(description="Run factor_ls Phase 1 daily pipeline.")
    p.add_argument(
        "--run-at-utc",
        type=str,
        default=None,
        help="ISO timestamp for production-style window (default: now UTC). Ignored if --open-time-ge set.",
    )
    p.add_argument("--open-time-ge", type=str, default=None, help="Intraday load lower bound (ISO, UTC).")
    p.add_argument("--open-time-lt", type=str, default=None, help="Intraday load upper bound exclusive (ISO, UTC).")
    p.add_argument("--bar-ts-ge", type=str, default=None, help="Output bar_ts lower bound inclusive (ISO date or ts).")
    p.add_argument("--bar-ts-le", type=str, default=None, help="Output bar_ts upper bound inclusive.")
    p.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Optional comma-separated subset; omit to load every symbol present in OHLCV for the window.",
    )
    p.add_argument("--interval", type=str, default=None, help="OHLCV interval (default from config).")
    p.add_argument("--no-persist", action="store_true", help="Compute only; do not write to Postgres.")
    p.add_argument("--pipeline-version", type=str, default=None, help="Override pipeline_version on rows.")
    args = p.parse_args()

    symbols = tuple(s.strip().upper() for s in args.symbols.split(",")) if args.symbols else None

    kwa: dict = {
        "symbols": symbols,
        "persist": not args.no_persist,
        "pipeline_version": args.pipeline_version,
        "interval": args.interval,
    }
    if args.open_time_ge and args.open_time_lt:
        kwa["open_time_ge"] = args.open_time_ge
        kwa["open_time_lt"] = args.open_time_lt
        kwa["bar_ts_ge"] = args.bar_ts_ge
        kwa["bar_ts_le"] = args.bar_ts_le
    else:
        kwa["run_at_utc"] = args.run_at_utc or datetime.now(timezone.utc)
        kwa["bar_ts_ge"] = args.bar_ts_ge
        kwa["bar_ts_le"] = args.bar_ts_le

    res = run_pipeline(**kwa)
    print("L1 rows:", len(res.l1))
    print("Pre rows:", len(res.pre))
    print("Factors rows:", len(res.factors))
    print("Xsecs rows:", len(res.xsecs))
    print("Labels rows:", len(res.labels))
    if not res.l1.empty:
        print("bar_ts range:", res.l1["bar_ts"].min(), "…", res.l1["bar_ts"].max())


if __name__ == "__main__":
    main()
