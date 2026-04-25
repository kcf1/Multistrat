"""Universe refresh job: CMC top-N -> market_data.universe_assets."""

from __future__ import annotations

from datetime import date

from loguru import logger
from pgconn import configure_for_market_data

from market_data.cmc import CmcClient
from market_data.config import MarketDataSettings
from market_data.storage import upsert_universe_topn_members


def run_universe_refresh_top100(
    settings: MarketDataSettings,
    *,
    as_of_date: date | None = None,
    n: int = 100,
    source: str = "cmc_top100",
) -> None:
    """
    Fetch CMC top-N and upsert into universe table.

    Idempotent-ish: re-running the same day overwrites rank/current flag for present members,
    and marks dropped assets as not-current.
    """
    api_key = (settings.market_data_cmc_api_key or "").strip()
    if not api_key:
        raise ValueError("MARKET_DATA_CMC_API_KEY is required for universe refresh")

    client = CmcClient(api_key=api_key, base_url=settings.cmc_base_url)
    result = client.fetch_top_n_by_market_cap(n=n)
    run_date = as_of_date or result.as_of_date

    import psycopg2

    conn = psycopg2.connect(settings.database_url)
    try:
        configure_for_market_data(conn)
        res = upsert_universe_topn_members(
            conn,
            as_of_date=run_date,
            source=source,
            members=result.members,
        )
        conn.commit()
        logger.info(
            "universe_refresh_top100: as_of_date={} requested={} valid={} newly_added={} dropped={}",
            res.as_of_date,
            res.requested,
            res.valid_members,
            res.newly_added_base_assets,
            res.dropped_base_assets,
        )
    finally:
        conn.close()

