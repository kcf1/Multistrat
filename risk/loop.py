"""
Risk service main loop (12.5.1–12.5.2).

Consume strategy_orders -> parse -> (optional) rule engine -> if ACCEPT publish to risk_approved.
Empty pipeline = pass-through.
"""

import logging
from typing import Optional

from redis import Redis

from risk.config import get_rule_pipeline
from risk.consumer import OrderIntentParseError, read_one_strategy_order
from risk.engine import RiskDecision, RiskStatus, RuleRegistry, evaluate
from risk.producer import publish_risk_approved

logger = logging.getLogger(__name__)

# Default block timeout for XREAD (ms)
DEFAULT_BLOCK_MS = 5000


def process_one(
    redis: Redis,
    registry: RuleRegistry,
    pipeline: Optional[list] = None,
    block_ms: Optional[int] = DEFAULT_BLOCK_MS,
    start_id: str = "$",
) -> Optional[str]:
    """
    Read one message from strategy_orders, evaluate, and if ACCEPT publish to risk_approved.

    Args:
        redis: Redis client.
        registry: Rule registry (may be empty).
        pipeline: Ordered list of rule_ids; None = use get_rule_pipeline().
        block_ms: XREAD block timeout (ms).
        start_id: XREAD start id ("$" = only new, "0" = from start; use "0" in tests).

    Returns:
        Entry id from strategy_orders if a message was read, else None.
    """
    out = read_one_strategy_order(redis, start_id=start_id, block_ms=block_ms)
    if out is None:
        return None

    entry_id, order_intent = out
    order_dict = order_intent.model_dump()

    rule_list = pipeline if pipeline is not None else get_rule_pipeline()
    decision = evaluate(order_dict, rule_list, registry, account=None, limits=None)

    if decision.status == RiskStatus.ACCEPT:
        publish_risk_approved(redis, order_intent)
        logger.info(
            "risk approved order_id=%s broker=%s symbol=%s side=%s quantity=%s",
            order_dict.get("order_id"),
            order_dict.get("broker"),
            order_dict.get("symbol"),
            order_dict.get("side"),
            order_dict.get("quantity"),
        )
    else:
        logger.warning(
            "risk rejected order_id=%s reason_codes=%s messages=%s",
            order_dict.get("order_id"),
            decision.reason_codes,
            decision.messages,
        )

    return entry_id


def run_loop(
    redis: Redis,
    registry: Optional[RuleRegistry] = None,
    pipeline: Optional[list] = None,
    block_ms: int = DEFAULT_BLOCK_MS,
) -> None:
    """
    Run risk loop until shutdown: read strategy_orders -> evaluate -> publish if ACCEPT.

    Empty pipeline (default) = pass-through.
    """
    reg = registry or RuleRegistry()
    while True:
        try:
            process_one(redis, reg, pipeline=pipeline, block_ms=block_ms)
        except OrderIntentParseError as e:
            logger.warning("strategy_orders parse error: %s", e)
            # Continue; next read will be "$" so we don't reprocess the bad message
        except Exception as e:
            logger.exception("risk loop error: %s", e)
            raise
