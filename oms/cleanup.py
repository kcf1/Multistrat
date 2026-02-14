"""
OMS Redis stream and order-key cleanup (task 12.1.9b).

- XTRIM risk_approved and oms_fills to cap stream length (e.g. MAXLEN ~ 10000).
- Optional TTL on terminal order keys (orders:{order_id}) after status is filled/rejected/cancelled/expired.
"""

from typing import Optional

from redis import Redis

from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM

DEFAULT_STREAM_MAXLEN = 10000
ORDER_KEY_PREFIX = "orders:"


def trim_oms_streams(
    redis: Redis,
    maxlen: int = DEFAULT_STREAM_MAXLEN,
    risk_approved: bool = True,
    oms_fills: bool = True,
) -> int:
    """
    XTRIM risk_approved and/or oms_fills to at most maxlen entries (keeps newest).
    Streams that do not exist are skipped (no error).
    Returns the number of entries removed (sum across streams).
    """
    removed = 0
    for stream in (RISK_APPROVED_STREAM, OMS_FILLS_STREAM):
        if stream == RISK_APPROVED_STREAM and not risk_approved:
            continue
        if stream == OMS_FILLS_STREAM and not oms_fills:
            continue
        if not redis.exists(stream):
            continue
        try:
            n = redis.xtrim(stream, maxlen=maxlen)
            if n is not None:
                removed += int(n)
        except Exception:
            # Stream may not support xtrim in some mock setups; ignore
            pass
    return removed


def set_order_key_ttl(redis: Redis, order_id: str, ttl_seconds: int) -> bool:
    """
    Set TTL on the order hash key orders:{order_id}.
    Use when order reaches a terminal status (filled, rejected, cancelled, expired)
    so Redis can expire the key after sync or retention period.
    Returns True if the key existed and TTL was set, False if key does not exist.
    """
    key = ORDER_KEY_PREFIX + order_id
    if not redis.exists(key):
        return False
    redis.expire(key, ttl_seconds)
    return True
