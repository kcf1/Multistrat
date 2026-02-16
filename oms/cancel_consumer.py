"""
OMS Redis consumer for cancel_requested stream (task 12.1.9f).

Reads cancel requests from cancel_requested stream; parse per schema.
Supports consumer group (XREADGROUP + XACK) so each message is consumed once.
"""

from typing import Any, Dict, List, Optional, Tuple

from pydantic import ValidationError
from redis import Redis
from redis.exceptions import ResponseError

from oms.log import logger
from oms.schemas import CANCEL_REQUESTED_STREAM
from oms.schemas_pydantic import CancelRequest
from oms.streams import ack_message, ensure_consumer_group, read_messages, read_messages_group


class CancelRequestParseError(Exception):
    """Raised when a cancel_requested message cannot be parsed."""
    pass


def parse_cancel_request_message(fields: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse stream entry fields per cancel_requested schema.

    Uses Pydantic validation for type safety and validation.

    Requires: (order_id) OR (broker_order_id AND symbol); broker required.

    Returns:
        Dict with order_id (optional), broker_order_id (optional), symbol, broker.
    """
    if not isinstance(fields, dict):
        raise CancelRequestParseError("fields must be a dict")

    try:
        # Validate with Pydantic model
        cancel_model = CancelRequest(**fields)
        # Convert to dict compatible with existing code
        return cancel_model.model_dump_dict()
    except CancelRequestParseError:
        # Re-raise CancelRequestParseError from model validator (if it bubbles up)
        raise
    except ValidationError as e:
        # Convert Pydantic ValidationError to CancelRequestParseError with user-friendly messages
        error_messages = []
        for error in e.errors():
            field = error["loc"][0] if error["loc"] else "unknown"
            msg = error["msg"]
            
            # Check if this is the custom validation error from model_validator
            if "need order_id or (broker_order_id and symbol)" in msg:
                error_messages.append("need order_id or (broker_order_id and symbol)")
                continue
            
            # Map common Pydantic errors to existing error format
            if "Field required" in msg or "none is not an allowed value" in msg.lower():
                if field == "broker":
                    error_messages.append("missing broker")
                else:
                    error_messages.append(f"missing {field}")
            else:
                error_messages.append(f"{field}: {msg}")
        
        error_msg = "; ".join(error_messages) if error_messages else str(e)
        raise CancelRequestParseError(error_msg) from e


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
