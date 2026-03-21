#!/usr/bin/env python3
"""
One-shot OHLCV ingest for Postgres ``ohlcv`` (Phase 4).

Manual CLI: **tqdm** per ``(symbol, interval)`` (estimated total + per-bar updates).

**Watermark (default on):** start after ``max(ingestion_cursor, max(open_time))``, or from
``backfill_days`` horizon when empty—same as scheduled ingest.

**``--no-watermark``:** ignore ``ingestion_cursor``; start from ``now - OHLCV_INITIAL_BACKFILL_DAYS``
and upsert (idempotent). Use to re-pull history without trusting the cursor table.

**``--skip-existing``** (requires ``--no-watermark``): for each ``(symbol, interval)``, run
``detect_ohlcv_time_gaps`` on ``[now - backfill_days, now]`` and refetch **only** those
missing spans, then a tail catch-up from ``max(open_time)``. Does **not** re-download
contiguous stretches you already have. The ``ingestion_cursor`` table is still ignored for
where to look; cursor is updated after each chunk as usual.

Uses ``MARKET_DATA_DATABASE_URL`` / ``DATABASE_URL``, optional ``MARKET_DATA_BINANCE_BASE_URL``,
and micro constants in ``market_data.config``.

Run from repo root (so ``.env`` is found):

    python scripts/backfill_ohlcv.py
    python scripts/backfill_ohlcv.py --no-watermark
    python scripts/backfill_ohlcv.py --no-watermark --skip-existing
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Repo root on sys.path when run as ``python scripts/backfill_ohlcv.py``
_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pandas as pd
import psycopg2
from loguru import logger
from tqdm import tqdm

from market_data.config import OHLCV_INITIAL_BACKFILL_DAYS, load_settings
from market_data.intervals import interval_to_millis
from market_data.jobs.common import expected_ohlcv_slots, utc_now_ms
from market_data.jobs.ingest_ohlcv import ingest_ohlcv_series, resolve_ingest_start_ms
from market_data.providers.binance_spot import build_binance_spot_provider
from market_data.storage import ohlcv_window_stats


def _expected_bar_total(
    conn,
    symbol: str,
    interval: str,
    *,
    end_ms: int,
    backfill_days: int,
    use_watermark: bool,
    skip_existing_when_no_watermark: bool,
) -> int:
    """Rough max bars for this run (for tqdm total); actual may differ slightly."""
    start_ms = resolve_ingest_start_ms(
        conn,
        symbol,
        interval,
        now_ms=end_ms,
        backfill_days=backfill_days,
        use_watermark=use_watermark,
        skip_existing_when_no_watermark=skip_existing_when_no_watermark,
    )
    if start_ms >= end_ms:
        return 0
    iv_ms = interval_to_millis(interval)
    return max(1, (end_ms - start_ms) // iv_ms + 2)


def _log_completeness_summary(
    conn,
    *,
    symbols: tuple[str, ...],
    intervals: tuple[str, ...],
    end_ms: int,
    backfill_days: int,
    results: list,
) -> None:
    """Print policy-window stats as a pandas DataFrame (approximate; not gap-aware)."""
    horizon_ms = end_ms - backfill_days * 86_400_000
    start_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    by_key = {(r.symbol, r.interval): r for r in results}
    rows: list[dict[str, object]] = []
    for sym in symbols:
        for iv in intervals:
            r = by_key[(sym, iv)]
            iv_ms = interval_to_millis(iv)
            exp_pol = expected_ohlcv_slots(horizon_ms, end_ms, iv_ms)
            stored, oldest, _ = ohlcv_window_stats(
                conn, sym, iv, open_time_ge=start_dt, open_time_le=end_dt
            )
            pct_pol = (100.0 * stored / exp_pol) if exp_pol else 0.0
            if stored and oldest is not None:
                oldest_ms = int(oldest.timestamp() * 1000)
                exp_span = expected_ohlcv_slots(oldest_ms, end_ms, iv_ms)
                pct_span = (100.0 * stored / exp_span) if exp_span else 0.0
                oldest_s = oldest.isoformat()
            else:
                exp_span = 0
                pct_span = 0.0
                oldest_s = ""
            rows.append(
                {
                    "symbol": sym,
                    "interval": iv,
                    "stored": stored,
                    "exp_pol": exp_pol,
                    "pct_pol": round(pct_pol, 2),
                    "exp_span": exp_span,
                    "pct_span": round(pct_span, 2),
                    "upserted_run": r.bars_upserted,
                    "fetch_give_ups": len(r.fetch_give_ups),
                    "oldest_open_utc": oldest_s,
                    "give_up_detail": "; ".join(r.fetch_give_ups),
                }
            )
    df = pd.DataFrame(rows)
    print("\n=== OHLCV completeness (policy window, approximate) ===", flush=True)
    print(f"window_utc: {start_dt.isoformat()} .. {end_dt.isoformat()}\n", flush=True)
    print(
        "exp_pol = slots in full policy window | pct_pol = stored/exp_pol "
        "(low if listed late or delisted before end)\n"
        "exp_span / pct_span = from oldest stored bar to end (low if large interior gaps)\n",
        flush=True,
    )
    with pd.option_context(
        "display.max_columns", None,
        "display.width", None,
        "display.max_colwidth", 120,
        "display.float_format", lambda x: f"{x:.2f}",
    ):
        print(df.to_string(index=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="OHLCV backfill / catch-up into Postgres.")
    parser.add_argument(
        "--no-watermark",
        action="store_true",
        help=(
            "Ignore ingestion_cursor; start from now minus backfill_days (idempotent upsert). "
            "Default uses watermarks like the scheduler."
        ),
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help=(
            "With --no-watermark only: start after max(ohlcv.open_time) when inside the window "
            "so existing bars are not re-downloaded."
        ),
    )
    args = parser.parse_args()
    if args.skip_existing and not args.no_watermark:
        parser.error("--skip-existing requires --no-watermark")
    use_watermark = not args.no_watermark
    skip_existing = args.skip_existing

    os.chdir(_root)
    settings = load_settings()
    logger.info(
        "OHLCV ingest: symbols={} intervals={} backfill_days={} use_watermark={} skip_existing_when_no_watermark={}",
        settings.symbols,
        settings.intervals,
        OHLCV_INITIAL_BACKFILL_DAYS,
        use_watermark,
        skip_existing,
    )
    prov = build_binance_spot_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    results = []
    try:
        for sym in settings.symbols:
            for iv in settings.intervals:
                end_ms = utc_now_ms()
                total_est = _expected_bar_total(
                    conn,
                    sym,
                    iv,
                    end_ms=end_ms,
                    backfill_days=OHLCV_INITIAL_BACKFILL_DAYS,
                    use_watermark=use_watermark,
                    skip_existing_when_no_watermark=skip_existing,
                )
                desc = f"{sym} {iv}"

                if total_est <= 0:
                    r = ingest_ohlcv_series(
                        conn,
                        prov,
                        sym,
                        iv,
                        now_ms=end_ms,
                        use_watermark=use_watermark,
                        skip_existing_when_no_watermark=skip_existing,
                    )
                else:
                    with tqdm(
                        desc=desc,
                        total=total_est,
                        unit="bar",
                        dynamic_ncols=True,
                        mininterval=0.05,
                        smoothing=0.05,
                        file=sys.stderr,
                    ) as pbar:

                        def on_chunk(batch) -> None:
                            for _ in batch:
                                pbar.update(1)

                        r = ingest_ohlcv_series(
                            conn,
                            prov,
                            sym,
                            iv,
                            now_ms=end_ms,
                            chunk_progress=on_chunk,
                            use_watermark=use_watermark,
                            skip_existing_when_no_watermark=skip_existing,
                        )
                results.append(r)
                logger.info(
                    "{} {} — bars_upserted={} chunks={} fetch_give_ups={}",
                    r.symbol,
                    r.interval,
                    r.bars_upserted,
                    r.chunks,
                    len(r.fetch_give_ups),
                )
        total_bars = sum(r.bars_upserted for r in results)
        logger.info("Done. Total bars upserted (all series): {}", total_bars)
        _log_completeness_summary(
            conn,
            symbols=tuple(settings.symbols),
            intervals=tuple(settings.intervals),
            end_ms=utc_now_ms(),
            backfill_days=OHLCV_INITIAL_BACKFILL_DAYS,
            results=results,
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
