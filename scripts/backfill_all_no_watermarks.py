#!/usr/bin/env python3
"""
Run *all* market_data backfill scripts with no cursor/high-water usage.

This is a convenience wrapper around:
  - scripts/backfill_ohlcv.py
  - scripts/backfill_basis_rate.py
  - scripts/backfill_open_interest.py
  - scripts/backfill_taker_buy_sell_volume.py
  - scripts/backfill_top_trader_long_short.py

Each underlying script supports:
  --no-watermark         => ignore ingestion_cursor (start from now - *_INITIAL_BACKFILL_DAYS)
  --skip-existing       => only with --no-watermark: skip contiguous existing bars and
                             refetch gaps when possible.

Usage:
  python scripts/backfill_all_no_watermarks.py
  python scripts/backfill_all_no_watermarks.py --skip-existing
  python scripts/backfill_all_no_watermarks.py --only ohlcv,basis_rate
  python scripts/backfill_all_no_watermarks.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"


DATASET_ORDER: tuple[str, ...] = (
    "ohlcv",
    "basis_rate",
    "open_interest",
    "taker_buy_sell_volume",
    "top_trader_long_short",
)

DATASET_TO_SCRIPT: dict[str, str] = {
    "ohlcv": "backfill_ohlcv.py",
    "basis_rate": "backfill_basis_rate.py",
    "open_interest": "backfill_open_interest.py",
    "taker_buy_sell_volume": "backfill_taker_buy_sell_volume.py",
    "top_trader_long_short": "backfill_top_trader_long_short.py",
}


@dataclass(frozen=True)
class BackfillInvocation:
    dataset: str
    cmd: list[str]


def _parse_only_arg(only: str | None) -> set[str] | None:
    if only is None:
        return None
    only = only.strip()
    if not only:
        return None
    parts = [p.strip() for p in only.split(",") if p.strip()]
    return set(parts) if parts else None


def build_backfill_plan(*, only: str | None, skip_existing: bool) -> list[BackfillInvocation]:
    only_set = _parse_only_arg(only)
    if only_set is not None:
        unknown = sorted(only_set - set(DATASET_TO_SCRIPT.keys()))
        if unknown:
            raise ValueError(f"Unknown dataset key(s) in --only: {unknown}. Valid: {sorted(DATASET_TO_SCRIPT.keys())}")

    base_args = ["--no-watermark"]
    if skip_existing:
        base_args.append("--skip-existing")

    invocations: list[BackfillInvocation] = []
    for dataset in DATASET_ORDER:
        if only_set is not None and dataset not in only_set:
            continue
        script = DATASET_TO_SCRIPT[dataset]
        script_path = str(SCRIPTS_DIR / script)
        cmd = [sys.executable, script_path, *base_args]
        invocations.append(BackfillInvocation(dataset=dataset, cmd=cmd))
    return invocations


def run_invocations(invocations: Sequence[BackfillInvocation], *, dry_run: bool) -> None:
    for inv in invocations:
        printable = " ".join(inv.cmd)
        if dry_run:
            print(f"[dry-run] {inv.dataset}: {printable}", flush=True)
            continue
        print(f"Running {inv.dataset}: {printable}", flush=True)
        subprocess.run(inv.cmd, check=True)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backfill all market_data datasets without cursor watermarks.")
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Only with --no-watermark: attempt to skip contiguous existing bars when the scripts support it.",
    )
    parser.add_argument(
        "--only",
        default=None,
        metavar="DATASETS",
        help="Comma-separated dataset keys. Available: " + ", ".join(sorted(DATASET_TO_SCRIPT.keys())),
    )
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing.")
    parser.add_argument(
        "--symbols",
        default=None,
        metavar="SYMBOLS",
        help="Optional comma-separated symbols to backfill (passed through to underlying scripts).",
    )
    parser.add_argument(
        "--pairs",
        default=None,
        metavar="PAIRS",
        help="Optional comma-separated pairs to backfill for basis (passed through).",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    # Ensure each backfill script finds the repo-root `.env` (they `chdir` themselves as well).
    os.chdir(str(REPO_ROOT))

    invocations = build_backfill_plan(only=args.only, skip_existing=args.skip_existing)
    if args.symbols:
        for i, inv in enumerate(invocations):
            invocations[i] = BackfillInvocation(inv.dataset, inv.cmd + ["--symbols", args.symbols])
    if args.pairs:
        for i, inv in enumerate(invocations):
            invocations[i] = BackfillInvocation(inv.dataset, inv.cmd + ["--pairs", args.pairs])
    if not invocations:
        print("No datasets selected; nothing to run.", flush=True)
        return 0

    run_invocations(invocations, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

