"""
OMS Redis consumer (task 12.1.6).

XREAD from risk_approved stream; parse messages per risk_approved schema.
Supports blocking read and consumer group (XREADGROUP + XACK) for no double-processing.
"""

from typing import Any, Dict, List, Optional, Tuple

from pydantic import ValidationError
from redis import Redis

from oms.log import logger
from oms.schemas import RISK_APPROVED_STREAM
from oms.schemas_pydantic import RiskApprovedOrder
from oms.streams import ack_message, ensure_consumer_group, read_messages, read_messages_group


class RiskApprovedParseError(Exception):
    """Raised when a risk_approved message cannot be parsed (e.g. missing required field)."""
    pass


def parse_risk_approved_message(fields: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse stream entry fields per risk_approved schema into an order dict.

    Uses Pydantic validation for type safety and validation.

    Args:
        fields: Raw stream entry (string keys and values).

    Returns:
        Order dict with broker, symbol, side, quantity, order_type, optional limit_price,
        time_in_force, book, comment, order_id, account_id. quantity as float.

    Raises:
        RiskApprovedParseError: If required fields are missing or invalid (e.g. quantity).
    """
    if not isinstance(fields, dict):
        raise RiskApprovedParseError("fields must be a dict")

    try:
        # Map 'price' field to 'limit_price' for Pydantic model
        model_fields = fields.copy()
        if "price" in model_fields and "limit_price" not in model_fields:
            model_fields["limit_price"] = model_fields.pop("price")

        # Validate with Pydantic model
        order_model = RiskApprovedOrder(**model_fields)
        # Convert to dict compatible with existing code
        order = order_model.model_dump_dict()
        return order
    except ValidationError as e:
        # Convert Pydantic ValidationError to RiskApprovedParseError with user-friendly messages
        error_messages = []
        for error in e.errors():
            field = error["loc"][0] if error["loc"] else "unknown"
            msg = error["msg"]
            
            # Map common Pydantic errors to existing error format
            if "Field required" in msg or "none is not an allowed value" in msg.lower():
                if field == "broker":
                    error_messages.append("missing broker")
                elif field == "symbol":
                    error_messages.append("missing symbol")
                elif field == "side":
                    error_messages.append("missing side")
                elif field == "quantity":
                    error_messages.append("missing quantity")
                else:
                    error_messages.append(f"missing {field}")
            elif "Input should be a valid number" in msg or "invalid" in msg.lower():
                if field == "quantity":
                    error_messages.append(f"invalid quantity: {fields.get('quantity', '')}")
                else:
                    error_messages.append(f"invalid {field}: {msg}")
            elif "greater than 0" in msg.lower() or "positive" in msg.lower():
                if field == "quantity":
                    error_messages.append("quantity must be positive")
                else:
                    error_messages.append(f"{field} must be positive")
            else:
                error_messages.append(f"{field}: {msg}")
        
        error_msg = "; ".join(error_messages) if error_messages else str(e)
        raise RiskApprovedParseError(error_msg) from e


def read_risk_approved(
    redis: Redis,
    start_id: str = "0",
    count: int = 1,
    block_ms: Optional[int] = None,
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    XREAD from risk_approved stream and parse each message per schema.

    Args:
        redis: Redis client.
        start_id: "0" = from start, "$" = only new messages.
        count: Max entries to return per call.
        block_ms: If set, block up to this many ms waiting for messages.

    Returns:
        List of (entry_id, order_dict). order_dict is parsed and validated.
        Invalid messages are skipped (not raised); use parse_risk_approved_message
        separately if you need to handle parse errors per-message.
    """
    raw = read_messages(redis, RISK_APPROVED_STREAM, start_id=start_id, count=count, block_ms=block_ms)
    result: List[Tuple[str, Dict[str, Any]]] = []
    for entry_id, fields in raw:
        try:
            order = parse_risk_approved_message(fields)
            result.append((entry_id, order))
        except RiskApprovedParseError as e:
            logger.warning("risk_approved parse error entry_id={} error={!s}", entry_id, e)
            continue
    return result


def read_one_risk_approved(
    redis: Redis,
    start_id: str = "0",
    block_ms: Optional[int] = None,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Read at most one risk_approved message; parse per schema (XREAD).

    Returns:
        (entry_id, order_dict) or None if no message (or parse failed).
    """
    messages = read_risk_approved(redis, start_id=start_id, count=1, block_ms=block_ms)
    if not messages:
        return None
    return messages[0]


def ensure_risk_approved_consumer_group(redis: Redis, group: str = "oms", start_id: str = "0") -> None:
    """Ensure consumer group exists on risk_approved stream (idempotent)."""
    ensure_consumer_group(redis, RISK_APPROVED_STREAM, group, start_id=start_id)


def read_one_risk_approved_cg(
    redis: Redis,
    group: str,
    consumer: str,
    block_ms: Optional[int] = None,
    read_id: str = ">",
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Read at most one risk_approved message via consumer group (XREADGROUP).

    read_id: ">" = new messages only; "0" = pending messages for this consumer.
    Ensures group exists first. Returns (entry_id, order_dict) or None.
    """
    ensure_risk_approved_consumer_group(redis, group)
    raw = read_messages_group(
        redis,
        RISK_APPROVED_STREAM,
        group,
        consumer,
        id=read_id,
        count=1,
        block_ms=block_ms,
    )
    for entry_id, fields in raw:
        try:
            order = parse_risk_approved_message(fields)
            return (entry_id, order)
        except RiskApprovedParseError as e:
            logger.warning("risk_approved parse error entry_id={} error={!s}", entry_id, e)
            continue
    return None


def read_many_risk_approved_cg(
    redis: Redis,
    group: str,
    consumer: str,
    count: int = 10,
    block_ms: Optional[int] = None,
    read_id: str = ">",
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Read multiple risk_approved messages via consumer group (XREADGROUP).
    
    Args:
        redis: Redis client.
        group: Consumer group name.
        consumer: Consumer name.
        count: Maximum number of messages to read (default: 10).
        block_ms: Block up to this many ms waiting for messages.
        read_id: ">" = new messages only; "0" = pending messages for this consumer.
    
    Returns:
        List of (entry_id, order_dict). Empty list if no messages.
    """
    ensure_risk_approved_consumer_group(redis, group)
    raw = read_messages_group(
        redis,
        RISK_APPROVED_STREAM,
        group,
        consumer,
        id=read_id,
        count=count,
        block_ms=block_ms,
    )
    result: List[Tuple[str, Dict[str, Any]]] = []
    for entry_id, fields in raw:
        try:
            order = parse_risk_approved_message(fields)
            result.append((entry_id, order))
        except RiskApprovedParseError as e:
            logger.warning("risk_approved parse error entry_id={} error={!s}", entry_id, e)
            continue
    return result


def read_one_risk_approved_pending(
    redis: Redis,
    group: str,
    consumer: str,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Read at most one pending risk_approved message (XREADGROUP id="0", no block).

    Used to retry messages that were not acked (e.g. place_order raised). Returns (entry_id, order) or None.
    """
    return read_one_risk_approved_cg(redis, group, consumer, block_ms=0, read_id="0")


def ack_risk_approved(redis: Redis, group: str, entry_id: str) -> int:
    """XACK risk_approved message so it is not redelivered. Returns count acked."""
    return ack_message(redis, RISK_APPROVED_STREAM, group, entry_id)
