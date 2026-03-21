#!/usr/bin/env python3
"""
One-shot OHLCV backfill / catch-up for Postgres ``ohlcv`` (Phase 4).

Uses ``MARKET_DATA_DATABASE_URL`` / ``DATABASE_URL``, ``MARKET_DATA_BINANCE_BASE_URL`` (optional),
and micro constants in ``market_data.config`` (symbols, intervals, ``OHLCV_INITIAL_BACKFILL_DAYS``).

Run from repo root (so ``.env`` is found):

    python scripts/backfill_ohlcv.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root on sys.path when run as ``python scripts/backfill_ohlcv.py``
_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from loguru import logger

from market_data.config import OHLCV_INITIAL_BACKFILL_DAYS, load_settings
from market_data.jobs import run_ingest_ohlcv


def main() -> int:
    import os

    os.chdir(_root)
    settings = load_settings()
    logger.info(
        "OHLCV ingest: symbols={} intervals={} backfill_days={}",
        settings.symbols,
        settings.intervals,
        OHLCV_INITIAL_BACKFILL_DAYS,
    )
    results = run_ingest_ohlcv(settings)
    total_bars = 0
    for r in results:
        total_bars += r.bars_upserted
        logger.info(
            "{} {} — bars_upserted={} chunks={}",
            r.symbol,
            r.interval,
            r.bars_upserted,
            r.chunks,
        )
    logger.info("Done. Total bars upserted (all series): {}", total_bars)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
