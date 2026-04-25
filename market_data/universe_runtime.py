"""Runtime universe resolver for market-data jobs.

Goal: jobs consume base-asset universe from DB, then resolve tradable symbols from oms.symbols.
"""

from __future__ import annotations

import psycopg2

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
) -> tuple[str, ...]:
    """
    Return tuple of tradable symbols for market-data ingestion.

    - Reads base assets from `market_data.universe_assets`.
    - Resolves tradable symbols via `oms.symbols` for (broker, quote_asset).
    - No static fallback: if universe is empty/unavailable, caller should treat as a hard failure.
    """
    conn = psycopg2.connect(settings.database_url)
    try:
        configure_for_market_data(conn)
        bases = query_universe_base_assets(conn, mode=mode)
        if not bases:
            raise RuntimeError("universe_assets is empty for mode={!r}".format(mode))
        mapping = resolve_binance_usdt_spot_symbols_for_bases(
            conn,
            base_assets=bases,
            quote_asset=quote_asset,
            broker=broker,
        )
        syms = sorted({v.strip().upper() for v in mapping.values() if (v or "").strip()})
        if not syms:
            raise RuntimeError("no tradable symbols resolved from oms.symbols for universe")
        return tuple(syms)
    finally:
        conn.close()

