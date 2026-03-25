#!/usr/bin/env python3
"""
One-shot basis ingest for Postgres ``basis_rate``.

Usage:
    python scripts/backfill_basis_rate.py
    python scripts/backfill_basis_rate.py --no-watermark
    python scripts/backfill_basis_rate.py --no-watermark --skip-existing
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from loguru import logger
from tqdm import tqdm

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from market_data.config import (  # noqa: E402
    BASIS_CONTRACT_TYPES,
    BASIS_INITIAL_BACKFILL_DAYS,
    BASIS_PAIRS,
    BASIS_PERIODS,
    load_settings,
)
from market_data.intervals import interval_to_millis  # noqa: E402
from market_data.jobs.common import expected_ohlcv_slots, utc_now_ms  # noqa: E402
from market_data.jobs.ingest_basis_rate import (  # noqa: E402
    ingest_basis_series,
    resolve_basis_ingest_start_ms,
)
from market_data.providers.binance_perps import build_binance_perps_provider  # noqa: E402
from market_data.storage import basis_window_stats  # noqa: E402


def _expected_total(
    conn,
    pair: str,
    contract_type: str,
    period: str,
    *,
    end_ms: int,
    backfill_days: int,
    use_watermark: bool,
) -> int:
    start_ms = resolve_basis_ingest_start_ms(
        conn,
        pair,
        contract_type,
        period,
        now_ms=end_ms,
        backfill_days=backfill_days,
        use_watermark=use_watermark,
    )
    if start_ms >= end_ms:
        return 0
    pd_ms = interval_to_millis(period)
    return max(1, (end_ms - start_ms) // pd_ms + 2)


def _log_summary(conn, *, end_ms: int, backfill_days: int, results: list) -> None:
    horizon_ms = end_ms - backfill_days * 86_400_000
    start_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    by_key = {(r.pair, r.contract_type, r.period): r for r in results}

    rows: list[dict[str, object]] = []
    for pair in BASIS_PAIRS:
        for ct in BASIS_CONTRACT_TYPES:
            for period in BASIS_PERIODS:
                r = by_key[(pair, ct, period)]
                pd_ms = interval_to_millis(period)
                exp_pol = expected_ohlcv_slots(horizon_ms, end_ms, pd_ms)
                stored, oldest, newest = basis_window_stats(
                    conn,
                    pair,
                    ct,
                    period,
                    sample_time_ge=start_dt,
                    sample_time_le=end_dt,
                )
                pct_pol = (100.0 * stored / exp_pol) if exp_pol else 0.0
                rows.append(
                    {
                        "pair": pair,
                        "contract_type": ct,
                        "period": period,
                        "stored": stored,
                        "exp_pol": exp_pol,
                        "pct_pol": round(pct_pol, 2),
                        "upserted_run": r.rows_upserted,
                        "fetch_give_ups": len(r.fetch_give_ups),
                        "oldest_sample_utc": oldest.isoformat() if oldest is not None else "",
                        "newest_sample_utc": newest.isoformat() if newest is not None else "",
                        "give_up_detail": "; ".join(r.fetch_give_ups),
                    }
                )
    df = pd.DataFrame(rows)
    print("\n=== Basis completeness (policy window, approximate) ===", flush=True)
    print(f"window_utc: {start_dt.isoformat()} .. {end_dt.isoformat()}\n", flush=True)
    with pd.option_context(
        "display.max_columns",
        None,
        "display.width",
        None,
        "display.max_colwidth",
        120,
        "display.float_format",
        lambda x: f"{x:.2f}",
    ):
        print(df.to_string(index=False), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Basis backfill / catch-up into Postgres.")
    parser.add_argument("--no-watermark", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()
    if args.skip_existing and not args.no_watermark:
        parser.error("--skip-existing requires --no-watermark")
    use_watermark = not args.no_watermark
    skip_existing = args.skip_existing

    os.chdir(_root)
    settings = load_settings()
    logger.info(
        "Basis ingest: pairs={} contract_types={} periods={} backfill_days={} use_watermark={} skip_existing_when_no_watermark={}",
        BASIS_PAIRS,
        BASIS_CONTRACT_TYPES,
        BASIS_PERIODS,
        BASIS_INITIAL_BACKFILL_DAYS,
        use_watermark,
        skip_existing,
    )
    prov = build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    results = []
    try:
        for pair in BASIS_PAIRS:
            for ct in BASIS_CONTRACT_TYPES:
                for period in BASIS_PERIODS:
                    end_ms = utc_now_ms()
                    total_est = _expected_total(
                        conn,
                        pair,
                        ct,
                        period,
                        end_ms=end_ms,
                        backfill_days=BASIS_INITIAL_BACKFILL_DAYS,
                        use_watermark=use_watermark,
                    )
                    desc = f"{pair} {ct} {period}"
                    if total_est <= 0:
                        r = ingest_basis_series(
                            conn,
                            prov,
                            pair,
                            ct,
                            period,
                            now_ms=end_ms,
                            use_watermark=use_watermark,
                            skip_existing_when_no_watermark=skip_existing,
                        )
                    else:
                        with tqdm(
                            desc=desc,
                            total=total_est,
                            unit="row",
                            dynamic_ncols=True,
                            mininterval=0.05,
                            smoothing=0.05,
                            file=sys.stderr,
                        ) as pbar:

                            def on_chunk(batch) -> None:
                                for _ in batch:
                                    pbar.update(1)

                            r = ingest_basis_series(
                                conn,
                                prov,
                                pair,
                                ct,
                                period,
                                now_ms=end_ms,
                                chunk_progress=on_chunk,
                                use_watermark=use_watermark,
                                skip_existing_when_no_watermark=skip_existing,
                            )
                    results.append(r)
                    logger.info(
                        "{} {} {} — rows_upserted={} chunks={} fetch_give_ups={}",
                        r.pair,
                        r.contract_type,
                        r.period,
                        r.rows_upserted,
                        r.chunks,
                        len(r.fetch_give_ups),
                    )
        logger.info("Done. Total rows upserted (all series): {}", sum(r.rows_upserted for r in results))
        _log_summary(
            conn,
            end_ms=utc_now_ms(),
            backfill_days=BASIS_INITIAL_BACKFILL_DAYS,
            results=results,
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
