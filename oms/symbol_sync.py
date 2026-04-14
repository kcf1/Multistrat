"""
Symbol sync from broker (task 12.2.15).

Fetches Binance GET /api/v3/exchangeInfo, parses symbols, and UPSERTs into
Postgres symbols table (base_asset, quote_asset, tick_size, lot_size, min_qty, etc.).
Invoked from OMS job or one-off/periodic script. Public endpoint; no signing.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union

import requests

from pgconn import configure_for_oms
from oms.log import logger


def _pg_conn(pg_connect: Union[str, Callable[[], Any]]):
    """Return (conn, we_opened). Caller must close conn if we_opened."""
    if callable(pg_connect):
        conn = pg_connect()
        configure_for_oms(conn)
        return conn, False
    import psycopg2
    conn = psycopg2.connect(pg_connect)
    configure_for_oms(conn)
    return conn, True


def _find_filter(filters: List[Dict], filter_type: str) -> Optional[Dict]:
    """Return first filter with filterType == filter_type."""
    if not filters:
        return None
    for f in filters:
        if f.get("filterType") == filter_type:
            return f
    return None


def fetch_binance_exchange_info(
    base_url: str,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetch Binance spot exchangeInfo (GET /api/v3/exchangeInfo). Public endpoint.

    Returns list of dicts with keys: symbol, base_asset, quote_asset, tick_size,
    lot_size, min_qty, product_type (always "spot").
    """
    url = f"{base_url.rstrip('/')}/api/v3/exchangeInfo"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    symbols_raw = data.get("symbols") or []
    out: List[Dict[str, Any]] = []
    for s in symbols_raw:
        symbol = (s.get("symbol") or "").strip()
        if not symbol:
            continue
        base_asset = (s.get("baseAsset") or "").strip()
        quote_asset = (s.get("quoteAsset") or "").strip()
        filters = s.get("filters") or []
        tick_size: Optional[Decimal] = None
        lot_size: Optional[Decimal] = None
        min_qty: Optional[Decimal] = None
        pf = _find_filter(filters, "PRICE_FILTER")
        if pf and pf.get("tickSize") is not None:
            try:
                tick_size = Decimal(str(pf["tickSize"]))
            except (TypeError, ValueError):
                pass
        ls = _find_filter(filters, "LOT_SIZE")
        if ls:
            if ls.get("stepSize") is not None:
                try:
                    lot_size = Decimal(str(ls["stepSize"]))
                except (TypeError, ValueError):
                    pass
            if ls.get("minQty") is not None:
                try:
                    min_qty = Decimal(str(ls["minQty"]))
                except (TypeError, ValueError):
                    pass
        out.append({
            "symbol": symbol,
            "base_asset": base_asset or symbol,
            "quote_asset": quote_asset or "",
            "tick_size": tick_size,
            "lot_size": lot_size,
            "min_qty": min_qty,
            "product_type": "spot",
        })
    return out


def sync_symbols_to_postgres(
    pg_connect: Union[str, Callable[[], Any]],
    symbols: List[Dict[str, Any]],
    broker: str = "binance",
) -> int:
    """
    UPSERT symbol rows into Postgres symbols table.
    Each dict must have symbol, base_asset, quote_asset; optional tick_size, lot_size, min_qty, product_type.
    Returns number of rows upserted.
    """
    if not symbols:
        return 0
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)
        count = 0
        for row in symbols:
            symbol = (row.get("symbol") or "").strip()[:255]
            base_asset = (row.get("base_asset") or "").strip()[:255]
            quote_asset = (row.get("quote_asset") or "").strip()[:255]
            if not symbol or not base_asset or not quote_asset:
                continue
            tick_size = row.get("tick_size")
            lot_size = row.get("lot_size")
            min_qty = row.get("min_qty")
            product_type = (row.get("product_type") or "")[:64] or None
            if isinstance(tick_size, (int, float, str)):
                try:
                    tick_size = Decimal(str(tick_size))
                except (TypeError, ValueError):
                    tick_size = None
            elif tick_size is not None and not isinstance(tick_size, Decimal):
                tick_size = None
            if isinstance(lot_size, (int, float, str)):
                try:
                    lot_size = Decimal(str(lot_size))
                except (TypeError, ValueError):
                    lot_size = None
            elif lot_size is not None and not isinstance(lot_size, Decimal):
                lot_size = None
            if isinstance(min_qty, (int, float, str)):
                try:
                    min_qty = Decimal(str(min_qty))
                except (TypeError, ValueError):
                    min_qty = None
            elif min_qty is not None and not isinstance(min_qty, Decimal):
                min_qty = None
            cur.execute(
                """
                INSERT INTO symbols (
                    symbol, base_asset, quote_asset, tick_size, lot_size, min_qty,
                    product_type, broker, updated_at
                ) VALUES (
                    %(symbol)s, %(base_asset)s, %(quote_asset)s, %(tick_size)s, %(lot_size)s, %(min_qty)s,
                    %(product_type)s, %(broker)s, %(updated_at)s
                )
                ON CONFLICT (symbol) DO UPDATE SET
                    base_asset = EXCLUDED.base_asset,
                    quote_asset = EXCLUDED.quote_asset,
                    tick_size = EXCLUDED.tick_size,
                    lot_size = EXCLUDED.lot_size,
                    min_qty = EXCLUDED.min_qty,
                    product_type = EXCLUDED.product_type,
                    broker = EXCLUDED.broker,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "symbol": symbol,
                    "base_asset": base_asset,
                    "quote_asset": quote_asset,
                    "tick_size": tick_size,
                    "lot_size": lot_size,
                    "min_qty": min_qty,
                    "product_type": product_type,
                    "broker": broker[:64] if broker else None,
                    "updated_at": now,
                },
            )
            count += 1
        conn.commit()
        if count:
            logger.info("sync_symbols_to_postgres: upserted {} symbol(s)", count)
        return count
    finally:
        if we_opened:
            conn.close()


def sync_symbols_from_binance(
    base_url: str,
    pg_connect: Union[str, Callable[[], Any]],
    broker: str = "binance",
    timeout: int = 30,
) -> int:
    """
    Fetch Binance exchangeInfo and UPSERT into Postgres symbols table.
    Returns number of symbols upserted.
    """
    symbols = fetch_binance_exchange_info(base_url, timeout=timeout)
    return sync_symbols_to_postgres(pg_connect, symbols, broker=broker)
