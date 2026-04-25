"""Scheduler job: backfill all datasets once for newly eligible symbols."""

from __future__ import annotations

from datetime import date, timezone, datetime

from loguru import logger
from pgconn import configure_for_market_data

from market_data.config import MarketDataSettings
from market_data.storage import mark_universe_backfill_done, universe_backfill_is_done
from market_data.symbol_resolution import resolve_binance_usdt_spot_symbols_for_bases


def _query_new_base_assets(conn, *, as_of_date: date) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT base_asset
            FROM universe_assets
            WHERE first_seen_date = %s
              AND was_ever_top100 = true
            ORDER BY base_asset ASC
            """,
            (as_of_date,),
        )
        return [str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]]


def run_universe_backfill_new_symbols(
    settings: MarketDataSettings,
    *,
    quote_asset: str = "USDT",
    broker: str = "binance",
    skip_existing: bool = True,
) -> int:
    """
    For base assets first-seen today, resolve to tradable symbols and run all dataset backfills
    once per symbol (idempotency tracked in market_data.universe_symbol_backfills).
    """
    import os
    import subprocess
    import sys
    from pathlib import Path

    import psycopg2

    today = datetime.now(timezone.utc).date()
    conn = psycopg2.connect(settings.database_url)
    try:
        configure_for_market_data(conn)
        bases = _query_new_base_assets(conn, as_of_date=today)
        if not bases:
            return 0
        mapping = resolve_binance_usdt_spot_symbols_for_bases(
            conn, base_assets=bases, quote_asset=quote_asset, broker=broker
        )
    finally:
        conn.close()

    # Run scripts as subprocesses to reuse existing backfill behavior.
    repo_root = Path(__file__).resolve().parents[2]
    script = repo_root / "scripts" / "backfill_all_no_watermarks.py"
    if not script.exists():
        raise FileNotFoundError(str(script))

    os.chdir(str(repo_root))
    ran = 0
    for base in bases:
        sym = mapping.get(base)
        if not sym:
            logger.info("universe_backfill_new_symbols: skip base={} (no tradable symbol)", base)
            continue

        conn2 = psycopg2.connect(settings.database_url)
        try:
            configure_for_market_data(conn2)
            if universe_backfill_is_done(conn2, symbol=sym):
                continue
        finally:
            conn2.close()

        cmd = [sys.executable, str(script), "--only", "ohlcv,basis_rate,open_interest,taker_buy_sell_volume,top_trader_long_short", "--symbols", sym, "--no-watermark"]
        if skip_existing:
            cmd.append("--skip-existing")
        logger.info("universe_backfill_new_symbols: running base={} symbol={} cmd={}", base, sym, " ".join(cmd))
        subprocess.run(cmd, check=True)

        conn3 = psycopg2.connect(settings.database_url)
        try:
            configure_for_market_data(conn3)
            mark_universe_backfill_done(conn3, symbol=sym)
            conn3.commit()
        finally:
            conn3.close()
        ran += 1

    return ran

