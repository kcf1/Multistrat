"""
OMS → Postgres order sync (task 12.1.10).

Sync orders from Redis to Postgres `orders` table (UPSERT by internal_id).
On trigger (terminal status) or every 60s; expire Redis key after sync.

DB columns and injection: see docs/OMS_ORDERS_DB_FIELDS.md for which fields
are set from risk_approved, place_order response (including payload), and fills.
"""

import json
from typing import Any, Callable, Dict, List, Optional, Union

from redis import Redis

from oms.log import logger

from oms.cleanup import set_order_key_ttl
from oms.redis_flow import TERMINAL_STATUSES
from oms.storage.redis_order_store import RedisOrderStore

# Defaults (override via env in caller if needed)
DEFAULT_SYNC_TTL_AFTER_SECONDS = 300  # 5 minutes after sync
DEFAULT_SYNC_INTERVAL_SECONDS = 60


def _order_to_row(order: Dict[str, Any], order_id: str) -> Dict[str, Any]:
    """Map Redis order dict to Postgres row dict (column names, types)."""
    row: Dict[str, Any] = {
        "internal_id": order_id,
        "broker": (order.get("broker") or "")[:255],
        "account_id": (order.get("account_id") or "")[:255],
        "broker_order_id": (order.get("broker_order_id") or "")[:255],
        "symbol": (order.get("symbol") or "")[:64],
        "side": (order.get("side") or "")[:32],
        "order_type": (order.get("order_type") or "")[:32],
        "quantity": order.get("quantity"),
        "price": order.get("price"),
        "limit_price": order.get("limit_price"),
        "time_in_force": (order.get("time_in_force") or "")[:16] or None,
        "status": (order.get("status") or "pending")[:64],
        "executed_qty": order.get("executed_qty"),
        "book": (order.get("book") or "")[:255] or None,
        "comment": (order.get("comment") or "")[:1024] or None,
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at"),
        "binance_cumulative_quote_qty": order.get("binance_cumulative_quote_qty"),
        "binance_transact_time": order.get("binance_transact_time"),
        "payload": order.get("payload"),
    }
    return row


def sync_one_order(
    redis: Redis,
    store: RedisOrderStore,
    pg_connect: Union[str, Callable[[], Any]],
    order_id: str,
    ttl_after_sync_seconds: Optional[int] = DEFAULT_SYNC_TTL_AFTER_SECONDS,
) -> bool:
    """
    Sync a single order from Redis to Postgres (UPSERT by internal_id), then set TTL on Redis key.

    pg_connect: DATABASE_URL string or a callable that returns a connection-like object
                with .cursor() and .commit() (e.g. psycopg2 connection).
    Returns True if order was synced, False if order not in Redis.
    """
    order = store.get_order(order_id)
    if not order:
        return False

    row = _order_to_row(order, order_id)
    logger.debug(
        "sync_one_order: order_id={} status={} symbol={}",
        order_id, order.get("status"), order.get("symbol"),
    )

    we_opened = False
    if callable(pg_connect):
        conn = pg_connect()
    else:
        import psycopg2
        conn = psycopg2.connect(pg_connect)
        we_opened = True

    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO orders (
                internal_id, broker, account_id, broker_order_id, symbol, side, order_type,
                quantity, price, limit_price, time_in_force, status, executed_qty, book, comment,
                created_at, updated_at, binance_cumulative_quote_qty, binance_transact_time, payload
            ) VALUES (
                %(internal_id)s, %(broker)s, %(account_id)s, %(broker_order_id)s, %(symbol)s,
                %(side)s, %(order_type)s, %(quantity)s, %(price)s, %(limit_price)s, %(time_in_force)s, %(status)s,
                %(executed_qty)s, %(book)s, %(comment)s, %(created_at)s, %(updated_at)s,
                %(binance_cumulative_quote_qty)s, %(binance_transact_time)s, %(payload)s
            )
            ON CONFLICT (internal_id) DO UPDATE SET
                broker = EXCLUDED.broker,
                account_id = EXCLUDED.account_id,
                broker_order_id = EXCLUDED.broker_order_id,
                symbol = EXCLUDED.symbol,
                side = EXCLUDED.side,
                order_type = EXCLUDED.order_type,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                limit_price = EXCLUDED.limit_price,
                time_in_force = EXCLUDED.time_in_force,
                status = EXCLUDED.status,
                executed_qty = EXCLUDED.executed_qty,
                book = EXCLUDED.book,
                comment = EXCLUDED.comment,
                updated_at = EXCLUDED.updated_at,
                binance_cumulative_quote_qty = EXCLUDED.binance_cumulative_quote_qty,
                binance_transact_time = EXCLUDED.binance_transact_time,
                payload = EXCLUDED.payload
                """,
                _pg_params(row),
            )
            conn.commit()
        except Exception as e:
            logger.warning("sync_one_order failed order_id={} error={!s}", order_id, e)
            raise
    finally:
        if we_opened:
            conn.close()

    if ttl_after_sync_seconds is not None and ttl_after_sync_seconds > 0:
        set_order_key_ttl(redis, order_id, ttl_after_sync_seconds)
    return True


def _pg_params(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert row to psycopg2 params; payload as JSONB."""
    try:
        from psycopg2.extras import Json
    except ImportError:
        Json = lambda x: json.dumps(x) if isinstance(x, dict) else x  # noqa: E731
    out = dict(row)
    if out.get("payload") is not None:
        out["payload"] = Json(out["payload"]) if isinstance(out["payload"], dict) else Json(json.loads(out["payload"]))
    return out


def get_terminal_order_ids(store: RedisOrderStore) -> List[str]:
    """Return order_ids that are in a terminal status (filled, rejected, cancelled, expired)."""
    seen: set = set()
    result: List[str] = []
    for status in TERMINAL_STATUSES:
        for oid in store.get_order_ids_in_status(status):
            if oid not in seen:
                seen.add(oid)
                result.append(oid)
    return result


def sync_terminal_orders(
    redis: Redis,
    store: RedisOrderStore,
    pg_connect: Union[str, Callable[[], Any]],
    ttl_after_sync_seconds: Optional[int] = DEFAULT_SYNC_TTL_AFTER_SECONDS,
) -> int:
    """
    Sync all terminal orders (filled, rejected, cancelled, expired) from Redis to Postgres,
    then set TTL on each synced key. Call every 60s (or on interval) for periodic sync.
    Returns number of orders synced.
    """
    order_ids = get_terminal_order_ids(store)
    count = 0
    for order_id in order_ids:
        if sync_one_order(redis, store, pg_connect, order_id, ttl_after_sync_seconds):
            count += 1
    if count:
        logger.info("sync_terminal_orders synced {} order(s)", count)
    return count
