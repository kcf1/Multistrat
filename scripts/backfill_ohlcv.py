#!/usr/bin/env python3
"""
One-shot OHLCV ingest for Postgres ``ohlcv`` (Phase 4).

Manual CLI: shows a **tqdm** bar per ``(symbol, interval)`` for every run (initial
backfill or incremental catch-up). Automated jobs should call
``ingest_ohlcv_series`` / ``run_ingest_ohlcv`` without ``chunk_progress`` so logs stay clean.

Uses ``MARKET_DATA_DATABASE_URL`` / ``DATABASE_URL``, optional ``MARKET_DATA_BINANCE_BASE_URL``,
and micro constants in ``market_data.config``.

Run from repo root (so ``.env`` is found):

    python scripts/backfill_ohlcv.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Repo root on sys.path when run as ``python scripts/backfill_ohlcv.py``
_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import psycopg2
from loguru import logger
from tqdm import tqdm

from market_data.config import OHLCV_INITIAL_BACKFILL_DAYS, load_settings
from market_data.jobs.ingest_ohlcv import ingest_ohlcv_series
from market_data.providers.binance_spot import build_binance_spot_provider


def main() -> int:
    os.chdir(_root)
    settings = load_settings()
    logger.info(
        "OHLCV ingest: symbols={} intervals={} backfill_days={}",
        settings.symbols,
        settings.intervals,
        OHLCV_INITIAL_BACKFILL_DAYS,
    )
    prov = build_binance_spot_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    results = []
    try:
        for sym in settings.symbols:
            for iv in settings.intervals:
                desc = f"{sym} {iv}"
                with tqdm(
                    desc=desc,
                    unit="bar",
                    dynamic_ncols=True,
                    miniters=1,
                ) as pbar:
                    r = ingest_ohlcv_series(
                        conn,
                        prov,
                        sym,
                        iv,
                        chunk_progress=lambda batch: pbar.update(len(batch)),
                    )
                results.append(r)
                logger.info(
                    "{} {} — bars_upserted={} chunks={}",
                    r.symbol,
                    r.interval,
                    r.bars_upserted,
                    r.chunks,
                )
    finally:
        conn.close()

    total_bars = sum(r.bars_upserted for r in results)
    logger.info("Done. Total bars upserted (all series): {}", total_bars)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
