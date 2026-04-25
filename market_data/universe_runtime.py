"""Runtime universe resolver for market-data jobs.

Goal: jobs consume base-asset universe from DB, then resolve tradable symbols from oms.symbols.
Fallback: if DB universe is empty/unavailable, use existing static config symbols.
"""

from __future__ import annotations

from collections.abc import Sequence

import psycopg2
from loguru import logger

from pgconn import configure_for_market_data

from market_data.config import MarketDataSettings
from market_data.storage import query_universe_base_assets
from market_data.symbol_resolution import resolve_binance_usdt_spot_symbols_for_bases


def resolve_runtime_symbols(
    settings: MarketDataSettings,
    *,
    mode: str = "ever_seen",
    quote_asset: str = "USDT",
    broker: str = "binance",
    static_fallback: Sequence[str] | None = None,
) -> tuple[str, ...]:
    """
    Return tuple of tradable symbols for market-data ingestion.

    - Reads base assets from `market_data.universe_assets`.
    - Resolves tradable symbols via `oms.symbols` for (broker, quote_asset).
    - Falls back to `static_fallback` if resolver yields nothing or errors.
    """
    fallback = tuple((s or "").strip().upper() for s in (static_fallback or ()) if (s or "").strip())
    try:
        conn = psycopg2.connect(settings.database_url)
        try:
            configure_for_market_data(conn)
            bases = query_universe_base_assets(conn, mode=mode)
            if not bases:
                return fallback
            mapping = resolve_binance_usdt_spot_symbols_for_bases(
                conn,
                base_assets=bases,
                quote_asset=quote_asset,
                broker=broker,
            )
            syms = sorted({v.strip().upper() for v in mapping.values() if (v or "").strip()})
            return tuple(syms) if syms else fallback
        finally:
            conn.close()
    except Exception as e:
        logger.warning("resolve_runtime_symbols failed (using fallback): {}", e)
        return fallback

