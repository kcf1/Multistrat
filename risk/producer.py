"""
Publish approved orders to risk_approved stream (12.5.1).

Payload is OMS-compatible (same schema as OMS risk_approved input).
"""

from typing import Any, Dict

from redis import Redis

from risk.models import OrderIntent
from risk.schemas import RISK_APPROVED_STREAM


def _flatten(fields: Dict[str, Any]) -> Dict[str, str]:
    """Convert dict to string values for Redis XADD."""
    out: Dict[str, str] = {}
    for k, v in fields.items():
        if v is None:
            continue
        out[k] = str(v)
    return out


def publish_risk_approved(redis: Redis, order: OrderIntent) -> str:
    """
    Publish approved order to risk_approved stream (OMS-compatible schema).

    Args:
        redis: Redis client (decode_responses=True).
        order: Validated order intent (same fields as OMS expects).

    Returns:
        Stream entry id from XADD.
    """
    flat = _flatten(order.to_risk_approved_fields())
    if not flat:
        raise ValueError("Order has no fields to publish")
    return redis.xadd(RISK_APPROVED_STREAM, flat)  # type: ignore[return-value]
