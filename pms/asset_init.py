"""
Assets table init: upsert fixed usd_price for major quoting (stable) assets.

Similar to OMS symbol sync at startup: ensures assets table has known stables
with usd_price=1 so PMS can value positions in USD without a price feed.
Invoke from PMS startup or run scripts/init_assets.py once after migration.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Optional, Sequence, Union

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


# Price source for fixed stables (1:1 USD); feed uses e.g. 'binance'
PRICE_SOURCE_FIXED = "fixed"


def truncate_assets(pg_connect: Union[str, Callable[[], Any]]) -> None:
    """Delete all rows from the assets table. Use for reset before reseed."""
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM assets")
        conn.commit()
    finally:
        if we_opened:
            conn.close()


def upsert_asset(
    pg_connect: Union[str, Callable[[], Any]],
    asset: str,
    usd_symbol: Optional[str] = None,
    usd_price: Optional[Union[Decimal, float, int, str]] = None,
    price_source: Optional[str] = None,
) -> bool:
    """
    Insert or update a single asset row. For use in scripts; no sync from symbols.

    Args:
        pg_connect: DB connection URL or callable.
        asset: Asset code (e.g. BTC, USDT).
        usd_symbol: Optional trading symbol for price fetch (e.g. BTCUSDT).
        usd_price: Optional fixed USD price (e.g. 1 for stables).
        price_source: Optional source label (e.g. 'fixed' for stables, 'binance' from feed).

    Returns:
        True if a row was inserted or updated.
    """
    asset = (asset or "").strip()
    if not asset:
        return False
    usd_sym = (usd_symbol or "").strip() or None
    usd_pr = None
    if usd_price is not None:
        try:
            usd_pr = Decimal(str(usd_price))
        except (TypeError, ValueError):
            pass
    src = (price_source or "").strip() or None
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)
        cur.execute(
            """
            INSERT INTO assets (asset, usd_symbol, usd_price, updated_at, price_source)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (asset) DO UPDATE SET
                usd_symbol = COALESCE(EXCLUDED.usd_symbol, assets.usd_symbol),
                usd_price = COALESCE(EXCLUDED.usd_price, assets.usd_price),
                updated_at = EXCLUDED.updated_at,
                price_source = COALESCE(EXCLUDED.price_source, assets.price_source)
            """,
            (asset, usd_sym, usd_pr, now, src),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        if we_opened:
            conn.close()


def init_assets_stables(
    pg_connect: Union[str, Callable[[], Any]],
    assets: Sequence[str] = STABLE_ASSETS_WITH_USD_PRICE_ONE,
    usd_price: Decimal = Decimal("1"),
    price_source: str = PRICE_SOURCE_FIXED,
) -> int:
    """
    UPSERT assets table with fixed usd_price for the given stable assets.
    Uses price_source='fixed' (or given) so stables are distinguished from feed-filled prices.
    ON CONFLICT (asset) DO UPDATE so re-running sets usd_price for stables.
    Returns number of rows upserted.
    """
    if not assets:
        return 0
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)
        src = (price_source or PRICE_SOURCE_FIXED).strip()
        count = 0
        for asset in assets:
            asset = (asset or "").strip()
            if not asset:
                continue
            cur.execute(
                """
                INSERT INTO assets (asset, usd_price, updated_at, price_source)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (asset) DO UPDATE SET
                    usd_price = EXCLUDED.usd_price,
                    updated_at = EXCLUDED.updated_at,
                    price_source = EXCLUDED.price_source
                """,
                (asset, usd_price, now, src),
            )
            count += 1
        conn.commit()
        if count:
            logger.info("init_assets_stables: upserted {} asset(s) with usd_price={}", count, usd_price)
        return count
    finally:
        if we_opened:
            conn.close()


def sync_assets_from_symbols(
    pg_connect: Union[str, Callable[[], Any]],
    quote_asset: str = "USDT",
) -> int:
    """
    UPSERT into assets from symbols table: one row per base_asset where quote_asset matches.

    Sets asset = base_asset, usd_symbol = base_asset + quote_asset (e.g. BTCUSDT).
    Does not overwrite usd_price or price_source; the price feed fills those later.
    Returns number of rows upserted.
    """
    quote = (quote_asset or "USDT").strip().upper()
    if not quote:
        return 0
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT base_asset
            FROM symbols
            WHERE TRIM(UPPER(quote_asset)) = %s AND TRIM(base_asset) != ''
            """,
            (quote,),
        )
        bases = [str(row[0]).strip() for row in cur.fetchall() if row and row[0]]
        if not bases:
            return 0
        now = datetime.now(timezone.utc)
        count = 0
        for base in bases:
            if not base:
                continue
            usd_symbol = f"{base}{quote}"
            cur.execute(
                """
                INSERT INTO assets (asset, usd_symbol, updated_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (asset) DO UPDATE SET
                    usd_symbol = EXCLUDED.usd_symbol,
                    updated_at = EXCLUDED.updated_at
                """,
                (base, usd_symbol, now),
            )
            count += 1
        conn.commit()
        if count:
            logger.info("sync_assets_from_symbols: upserted {} asset(s) with usd_symbol (quote={})", count, quote)
        return count
    finally:
        if we_opened:
            conn.close()
