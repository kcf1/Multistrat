"""
Asset price feed: query assets to update, call provider, write usd_price + price_source to DB.

One feed step: get asset list → get_prices(assets) → UPDATE assets.usd_price, price_source.
See docs/pms/ASSET_PRICE_FEED_PLAN.md §3.3.
"""

from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Union

from pms.asset_price_providers.interface import AssetPriceProvider


def query_assets_for_price_source(
    pg_connect: Union[str, Callable[[], Any]],
) -> List[str]:
    """
    Return list of asset codes that the feed should try to update.

    Uses assets that have usd_symbol set (fetchable from a symbol, e.g. Binance).
    Caller may further filter by config (e.g. per-source asset list).
    """
    if callable(pg_connect):
        conn = pg_connect()
    else:
        import psycopg2
        conn = psycopg2.connect(pg_connect)

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT asset FROM assets WHERE usd_symbol IS NOT NULL AND TRIM(usd_symbol) != ''
            """
        )
        return [str(row[0]).strip() for row in cur.fetchall() if row and row[0]]
    finally:
        conn.close()


def update_asset_prices(
    pg_connect: Union[str, Callable[[], Any]],
    source: str,
    prices: Dict[str, float],
) -> int:
    """
    UPDATE assets set usd_price and price_source for each (asset, price).

    Only updates rows that exist; does not insert. Returns number of rows updated.
    """
    if not prices:
        return 0

    if callable(pg_connect):
        conn = pg_connect()
    else:
        import psycopg2
        conn = psycopg2.connect(pg_connect)

    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)
        source_val = (source or "").strip() or None
        count = 0
        for asset, price in prices.items():
            asset_val = (asset or "").strip()
            if not asset_val:
                continue
            try:
                price_f = float(price)
                if price_f < 0:
                    continue
            except (TypeError, ValueError):
                continue
            cur.execute(
                """
                UPDATE assets
                SET usd_price = %s, updated_at = %s, price_source = %s
                WHERE asset = %s
                """,
                (price_f, now, source_val, asset_val),
            )
            count += cur.rowcount
        conn.commit()
        return count
    finally:
        conn.close()


def run_asset_price_feed_step(
    pg_connect: Union[str, Callable[[], Any]],
    provider: AssetPriceProvider,
    source: str,
    assets: List[str],
) -> int:
    """
    Run one feed step: get prices from provider, write to assets table.

    Returns number of assets updated. Skips prices that are None.
    """
    if not assets:
        return 0
    prices = provider.get_prices(assets)
    to_write = {k: v for k, v in prices.items() if v is not None}
    return update_asset_prices(pg_connect, source, to_write)
