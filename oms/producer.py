"""
OMS Redis producer (task 12.1.8).

XADD to oms_fills stream; format unified fill/reject events per oms_fills schema.
"""

from typing import Any, Dict

from redis import Redis

from oms.schemas import OMS_FILLS_FIELDS, OMS_FILLS_STREAM
from oms.streams import add_message


def format_oms_fill_event(
    event_type: str,
    order_id: str,
    broker_order_id: str,
    symbol: str,
    side: str,
    quantity: Any,
    price: Any = "",
    fee: Any = "",
    fee_asset: Any = "",
    executed_at: str = "",
    fill_id: str = "",
    reject_reason: str = "",
    book: str = "",
    comment: str = "",
    **extra: Any,
) -> Dict[str, Any]:
    """
    Build a dict suitable for oms_fills stream (all values stringifiable).

    Aligns with OMS_FILLS_FIELDS. Optional fields default to empty string.
    """
    payload: Dict[str, Any] = {
        "event_type": event_type,
        "order_id": order_id,
        "broker_order_id": broker_order_id,
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": price,
        "fee": fee,
        "fee_asset": fee_asset,
        "executed_at": executed_at,
        "fill_id": fill_id,
        "reject_reason": reject_reason,
        "book": book,
        "comment": comment,
    }
    payload.update(extra)
    return payload


def produce_oms_fill(redis: Redis, event: Dict[str, Any]) -> str:
    """
    Format event per oms_fills schema and XADD to oms_fills stream.

    Args:
        redis: Redis client.
        event: Dict with event_type, order_id, broker_order_id, symbol, side,
               quantity, optional price, fee, fee_asset, executed_at, fill_id,
               reject_reason, book, comment. Extra keys are included.

    Returns:
        Stream entry id from XADD.
    """
    # Ensure required fields present; use empty string for missing optional
    out: Dict[str, Any] = {}
    for key in OMS_FILLS_FIELDS:
        out[key] = event.get(key, "")
    for k, v in event.items():
        if k not in out:
            out[k] = v
    return add_message(redis, OMS_FILLS_STREAM, out)
