"""Integration test: strategy_orders -> risk -> risk_approved (12.5.1)."""

import pytest
from redis import Redis

from risk.schemas import RISK_APPROVED_STREAM, STRATEGY_ORDERS_STREAM
from risk.loop import process_one
from risk.engine import RuleRegistry


def _add_message(redis: Redis, stream: str, fields: dict) -> str:
    flat = {k: str(v) for k, v in fields.items() if v is not None}
    return redis.xadd(stream, flat)  # type: ignore[return-value]


def test_strategy_orders_to_risk_approved_unchanged_payload():
    """Message flows strategy_orders -> risk (process_one) -> risk_approved with OMS-valid payload."""
    import fakeredis
    redis = fakeredis.FakeRedis(decode_responses=True)

    payload = {
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": "0.001",
        "order_type": "MARKET",
        "book": "manual",
        "comment": "integration test",
    }
    _add_message(redis, STRATEGY_ORDERS_STREAM, payload)

    # Process with empty pipeline (pass-through); start_id="0" so we see the message we just added
    registry = RuleRegistry()
    entry_id = process_one(redis, registry, pipeline=[], block_ms=0, start_id="0")
    assert entry_id is not None

    # risk_approved should have one message with same/valid OMS fields
    entries = redis.xrange(RISK_APPROVED_STREAM)
    assert len(entries) == 1
    _eid, fields = entries[0]
    assert fields["broker"] == "binance"
    assert fields["symbol"] == "BTCUSDT"
    assert fields["side"] == "BUY"
    assert fields["quantity"] == "0.001"
    assert fields.get("order_type") == "MARKET"
    assert fields.get("book") == "manual"
    assert fields.get("comment") == "integration test"
