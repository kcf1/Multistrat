"""
Consume order intents from strategy_orders stream (12.5.1).

XREAD blocking read; parse with OrderIntent (same schema as OMS risk_approved).
"""

from typing import Any, Dict, List, Optional, Tuple

from pydantic import ValidationError
from redis import Redis

from risk.models import OrderIntent
from risk.schemas import STRATEGY_ORDERS_STREAM


class OrderIntentParseError(Exception):
    """Raised when a strategy_orders message cannot be parsed."""
    pass


def _fields_to_order_intent(fields: Dict[str, str]) -> OrderIntent:
    """Parse stream fields (str->str) into OrderIntent. Map 'price' -> limit_price."""
    if not isinstance(fields, dict):
        raise OrderIntentParseError("fields must be a dict")
    model_fields = dict(fields)
    if "price" in model_fields and "limit_price" not in model_fields:
        model_fields["limit_price"] = model_fields.pop("price")
    try:
        return OrderIntent(**model_fields)
    except ValidationError as e:
        msg = "; ".join(f"{err['loc'][0]}: {err['msg']}" for err in e.errors())
        raise OrderIntentParseError(msg) from e


def parse_strategy_order_message(fields: Dict[str, str]) -> OrderIntent:
    """
    Parse strategy_orders stream entry into OrderIntent.

    Args:
        fields: Raw stream entry (string keys and values).

    Returns:
        Validated OrderIntent.

    Raises:
        OrderIntentParseError: If required fields missing or invalid.
    """
    return _fields_to_order_intent(fields)


def read_messages(
    redis: Redis,
    stream: str,
    start_id: str = "0",
    count: int = 1,
    block_ms: Optional[int] = None,
) -> List[Tuple[str, Dict[str, str]]]:
    """XREAD from stream; return list of (entry_id, fields)."""
    kwargs: Dict[str, Any] = {"streams": {stream: start_id}, "count": count}
    if block_ms is not None and block_ms > 0:
        kwargs["block"] = block_ms
    reply = redis.xread(**kwargs)
    if not reply:
        return []
    result: List[Tuple[str, Dict[str, str]]] = []
    for _stream_name, entries in reply:
        for eid, flds in entries:
            eid_str = eid.decode() if isinstance(eid, bytes) else eid
            decoded: Dict[str, str] = {}
            for k, v in (flds or {}).items():
                key = k.decode() if isinstance(k, bytes) else k
                val = v.decode() if isinstance(v, bytes) else v
                decoded[key] = val
            result.append((eid_str, decoded))
    return result


def read_one_strategy_order(
    redis: Redis,
    start_id: str = "0",
    block_ms: Optional[int] = None,
) -> Optional[Tuple[str, OrderIntent]]:
    """
    Read at most one message from strategy_orders; parse to OrderIntent.

    Returns:
        (entry_id, order_intent) or None if no message.
    """
    raw = read_messages(
        redis, STRATEGY_ORDERS_STREAM,
        start_id=start_id, count=1, block_ms=block_ms,
    )
    if not raw:
        return None
    entry_id, fields = raw[0]
    try:
        intent = parse_strategy_order_message(fields)
        return (entry_id, intent)
    except OrderIntentParseError:
        raise
