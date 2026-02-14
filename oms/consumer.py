"""
OMS Redis consumer (task 12.1.6).

XREAD from risk_approved stream; parse messages per risk_approved schema.
Supports blocking read and consumer group (XREADGROUP + XACK) for no double-processing.
"""

from typing import Any, Dict, List, Optional, Tuple

from redis import Redis

from oms.schemas import RISK_APPROVED_STREAM
from oms.streams import ack_message, ensure_consumer_group, read_messages, read_messages_group


class RiskApprovedParseError(Exception):
    """Raised when a risk_approved message cannot be parsed (e.g. missing required field)."""
    pass


def parse_risk_approved_message(fields: Dict[str, str]) -> Dict[str, Any]:
    """
    Parse stream entry fields per risk_approved schema into an order dict.

    Args:
        fields: Raw stream entry (string keys and values).

    Returns:
        Order dict with broker, symbol, side, quantity, order_type, optional price,
        time_in_force, book, comment, order_id, account_id. quantity as float.

    Raises:
        RiskApprovedParseError: If required fields are missing or invalid (e.g. quantity).
    """
    if not isinstance(fields, dict):
        raise RiskApprovedParseError("fields must be a dict")

    broker = (fields.get("broker") or "").strip()
    symbol = (fields.get("symbol") or "").strip()
    side = (fields.get("side") or "").strip()
    qty_str = fields.get("quantity")
    if not broker:
        raise RiskApprovedParseError("missing broker")
    if not symbol:
        raise RiskApprovedParseError("missing symbol")
    if not side:
        raise RiskApprovedParseError("missing side")
    if qty_str is None or (isinstance(qty_str, str) and not qty_str.strip()):
        raise RiskApprovedParseError("missing quantity")
    try:
        quantity = float(qty_str)
    except (TypeError, ValueError) as e:
        raise RiskApprovedParseError(f"invalid quantity: {qty_str}") from e
    if quantity <= 0:
        raise RiskApprovedParseError("quantity must be positive")

    order: Dict[str, Any] = {
        "broker": broker,
        "account_id": (fields.get("account_id") or "").strip(),
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "order_type": (fields.get("order_type") or "MARKET").strip(),
        "book": (fields.get("book") or "").strip(),
        "comment": (fields.get("comment") or "").strip(),
    }
    order_id = (fields.get("order_id") or "").strip()
    if order_id:
        order["order_id"] = order_id

    price_str = fields.get("price")
    if price_str is not None and str(price_str).strip():
        try:
            order["price"] = float(price_str)
        except (TypeError, ValueError):
            order["price"] = None
    else:
        order["price"] = None

    tif = (fields.get("time_in_force") or "").strip()
    if tif:
        order["time_in_force"] = tif

    return order


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
        except RiskApprovedParseError:
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
) -> Optional[Tuple[str, Dict[str, Any]]]:
    """
    Read at most one risk_approved message via consumer group (XREADGROUP ">").
    Ensures group exists first. Returns (entry_id, order_dict) or None.
    """
    ensure_risk_approved_consumer_group(redis, group)
    raw = read_messages_group(
        redis,
        RISK_APPROVED_STREAM,
        group,
        consumer,
        id=">",
        count=1,
        block_ms=block_ms,
    )
    for entry_id, fields in raw:
        try:
            order = parse_risk_approved_message(fields)
            return (entry_id, order)
        except RiskApprovedParseError:
            continue
    return None


def ack_risk_approved(redis: Redis, group: str, entry_id: str) -> int:
    """XACK risk_approved message so it is not redelivered. Returns count acked."""
    return ack_message(redis, RISK_APPROVED_STREAM, group, entry_id)
