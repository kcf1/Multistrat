"""One-time universe seed from market_data.universe constants."""

from __future__ import annotations

from loguru import logger
from pgconn import configure_for_market_data

from market_data.config import MarketDataSettings
from market_data.storage import upsert_universe_seed_base_assets
from market_data.universe import DATA_COLLECTION_BASE_ASSETS


def run_universe_seed_from_static(
    settings: MarketDataSettings,
    *,
    source: str = "static_seed",
) -> int:
    """Seed `universe_assets` from `market_data.universe.DATA_COLLECTION_BASE_ASSETS`."""
    import psycopg2

    conn = psycopg2.connect(settings.database_url)
    try:
        configure_for_market_data(conn)
        n = upsert_universe_seed_base_assets(
            conn,
            base_assets=list(DATA_COLLECTION_BASE_ASSETS),
            source=source,
        )
        conn.commit()
        logger.info("universe_seed_static: upserted {} base asset(s) source={}", n, source)
        return n
    finally:
        conn.close()

