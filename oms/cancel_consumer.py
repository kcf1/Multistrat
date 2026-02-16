"""
OMS Redis consumer for cancel_requested stream (task 12.1.9f).

Reads cancel requests from cancel_requested stream; parse per schema.
Supports consumer group (XREADGROUP + XACK) so each message is consumed once.
"""

from typing import Any, Dict, List, Optional, Tuple

from redis import Redis
from redis.exceptions import ResponseError

from oms.log import logger
from oms.schemas import CANCEL_REQUESTED_STREAM
from oms.streams import ack_message, ensure_consumer_group, read_messages, read_messages_group


class CancelRequestParseError(Exception):
    """Raised when a cancel_requested message cannot be parsed."""
    pass


def parse_cancel_request_message(fields: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse stream entry fields per cancel_requested schema.

    Requires: (order_id) OR (broker_order_id AND symbol); broker required.

    Returns:
        Dict with order_id (optional), broker_order_id (optional), symbol, broker.
    """
    if not isinstance(fields, dict):
        raise CancelRequestParseError("fields must be a dict")

    order_id = (fields.get("order_id") or "").strip()
    broker_order_id = (fields.get("broker_order_id") or "").strip()
    symbol = (fields.get("symbol") or "").strip()
    broker = (fields.get("broker") or "").strip()

    if not broker:
        raise CancelRequestParseError("missing broker")
    if order_id:
        return {"order_id": order_id, "broker_order_id": broker_order_id or None, "symbol": symbol or None, "broker": broker}
    if broker_order_id and symbol:
        return {"order_id": None, "broker_order_id": broker_order_id, "symbol": symbol, "broker": broker}
    raise CancelRequestParseError("need order_id or (broker_order_id and symbol)")


def read_one_cancel_request(
    redis: Redis,
    start_id: str = "0",
    block_ms: Optional[int] = None,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Read at most one cancel_requested message (XREAD). Parse per schema.

    Returns:
        (entry_id, cancel_request_dict) or None if no message or parse failed.
    """
    raw = read_messages(redis, CANCEL_REQUESTED_STREAM, start_id=start_id, count=1, block_ms=block_ms)
    if not raw:
        return None
    entry_id, fields = raw[0]
    try:
        req = parse_cancel_request_message(fields or {})
        return (entry_id, req)
    except CancelRequestParseError:
        return None


def ensure_cancel_requested_consumer_group(redis: Redis, group: str = "oms", start_id: str = "0") -> None:
    """Ensure consumer group exists on cancel_requested stream (idempotent)."""
    ensure_consumer_group(redis, CANCEL_REQUESTED_STREAM, group, start_id=start_id)


def read_one_cancel_request_cg(
    redis: Redis,
    group: str,
    consumer: str,
    block_ms: Optional[int] = None,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Read at most one cancel_requested message via consumer group (XREADGROUP ">").
    Ensures group exists first. On NOGROUP (e.g. stream missing at startup), re-ensure and retry once.
    Returns (entry_id, cancel_request_dict) or None.
    """
    def _read() -> list:
        ensure_cancel_requested_consumer_group(redis, group)
        return read_messages_group(
            redis,
            CANCEL_REQUESTED_STREAM,
            group,
            consumer,
            id=">",
            count=1,
            block_ms=block_ms,
        )
    try:
        raw = _read()
    except ResponseError as e:
        if "NOGROUP" not in str(e):
            raise
        logger.warning("cancel_requested NOGROUP, re-ensuring consumer group: {}", e)
        ensure_cancel_requested_consumer_group(redis, group)
        raw = read_messages_group(
            redis,
            CANCEL_REQUESTED_STREAM,
            group,
            consumer,
            id=">",
            count=1,
            block_ms=block_ms,
        )
    for entry_id, fields in raw:
        try:
            req = parse_cancel_request_message(fields or {})
            return (entry_id, req)
        except CancelRequestParseError as e:
            logger.warning("cancel_requested parse error entry_id={} error={!s}", entry_id, e)
            continue
    return None


def read_many_cancel_request_cg(
    redis: Redis,
    group: str,
    consumer: str,
    count: int = 10,
    block_ms: Optional[int] = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Read multiple cancel_requested messages via consumer group (XREADGROUP).
    
    Args:
        redis: Redis client.
        group: Consumer group name.
        consumer: Consumer name.
        count: Maximum number of messages to read (default: 10).
        block_ms: Block up to this many ms waiting for messages.
    
    Returns:
        List of (entry_id, cancel_request_dict). Empty list if no messages.
    """
    def _read() -> list:
        ensure_cancel_requested_consumer_group(redis, group)
        return read_messages_group(
            redis,
            CANCEL_REQUESTED_STREAM,
            group,
            consumer,
            id=">",
            count=count,
            block_ms=block_ms,
        )
    try:
        raw = _read()
    except ResponseError as e:
        if "NOGROUP" not in str(e):
            raise
        logger.warning("cancel_requested NOGROUP, re-ensuring consumer group: {}", e)
        ensure_cancel_requested_consumer_group(redis, group)
        raw = read_messages_group(
            redis,
            CANCEL_REQUESTED_STREAM,
            group,
            consumer,
            id=">",
            count=count,
            block_ms=block_ms,
        )
    result: List[Tuple[str, Dict[str, Any]]] = []
    for entry_id, fields in raw:
        try:
            req = parse_cancel_request_message(fields)
            result.append((entry_id, req))
        except CancelRequestParseError as e:
            logger.warning("cancel_requested parse error entry_id={} error={!s}", entry_id, e)
            continue
    return result


def ack_cancel_requested(redis: Redis, group: str, entry_id: str) -> int:
    """XACK cancel_requested message so it is not redelivered. Returns count acked."""
    return ack_message(redis, CANCEL_REQUESTED_STREAM, group, entry_id)
