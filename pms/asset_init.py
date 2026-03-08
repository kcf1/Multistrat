"""
Assets table init: upsert fixed usd_price for major quoting (stable) assets.

Similar to OMS symbol sync at startup: ensures assets table has known stables
with usd_price=1 so PMS can value positions in USD without a price feed.
Invoke from PMS startup or run scripts/init_assets.py once after migration.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Sequence, Union

from pms.log import logger

# Major quoting/stable assets: fixed usd_price = 1 for stables-first valuation
STABLE_ASSETS_WITH_USD_PRICE_ONE = (
    "USDT",
    "USDC",
    "BUSD",
    "DAI",
    "TUSD",
    "USDP",
    "FDUSD",
    "PYUSD",
)


def _pg_conn(pg_connect: Union[str, Callable[[], Any]]):
    """Return (conn, we_opened). Caller must close conn if we_opened."""
    if callable(pg_connect):
        return pg_connect(), False
    import psycopg2
    return psycopg2.connect(pg_connect), True


def init_assets_stables(
    pg_connect: Union[str, Callable[[], Any]],
    assets: Sequence[str] = STABLE_ASSETS_WITH_USD_PRICE_ONE,
    usd_price: Decimal = Decimal("1"),
) -> int:
    """
    UPSERT assets table with fixed usd_price for the given stable assets.
    ON CONFLICT (asset) DO UPDATE so re-running sets usd_price for stables.
    Returns number of rows upserted.
    """
    if not assets:
        return 0
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)
        count = 0
        for asset in assets:
            asset = (asset or "").strip()
            if not asset:
                continue
            cur.execute(
                """
                INSERT INTO assets (asset, usd_price, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (asset) DO UPDATE SET
                    usd_price = EXCLUDED.usd_price,
                    updated_at = EXCLUDED.updated_at
                """,
                (asset, usd_price, now),
            )
            count += 1
        conn.commit()
        if count:
            logger.info("init_assets_stables: upserted %s asset(s) with usd_price=%s", count, usd_price)
        return count
    finally:
        if we_opened:
            conn.close()
