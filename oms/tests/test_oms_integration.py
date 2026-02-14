"""
Integration test for OMS (task 12.1.9): wire consumer → store → registry → place_order → producer.

Mock Redis (fakeredis) and mock adapter; verify full flow for one order.
"""

from typing import Any, Dict

import pytest

from oms.redis_flow import make_fill_callback, process_one
from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
from oms.streams import add_message, read_latest
from oms.storage.redis_order_store import RedisOrderStore


def _mock_adapter(place_result: Dict[str, Any]):
    """Return a mock adapter that place_order returns place_result."""

    class MockAdapter:
        def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
            return place_result

        def start_fill_listener(self, callback) -> None:
            pass
    return MockAdapter()


@pytest.fixture
def redis_client():
    from fakeredis import FakeRedis
    return FakeRedis(decode_responses=True)


@pytest.fixture
def store(redis_client):
    return RedisOrderStore(redis_client)


def test_oms_integration_consumer_store_registry_producer(redis_client, store):
    """Full flow: XADD risk_approved → process_one → order in store, reject on oms_fills."""
    from oms.registry import AdapterRegistry

    registry = AdapterRegistry()
    registry.register("binance", _mock_adapter({"rejected": True, "reject_reason": "mock reject"}))

    add_message(redis_client, RISK_APPROVED_STREAM, {
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": "0.001",
        "order_type": "MARKET",
        "book": "integration_test",
        "comment": "12.1.9",
    })

    result = process_one(redis_client, store, registry, start_id="0")
    assert result is not None
    assert result["order_id"]
    assert result["rejected"] is True
    assert "mock reject" in result["reject_reason"]

    order = store.get_order(result["order_id"])
    assert order is not None
    assert order["status"] == "rejected"
    assert order["symbol"] == "BTCUSDT"
    assert order["book"] == "integration_test"

    entries = read_latest(redis_client, OMS_FILLS_STREAM, count=5)
    assert len(entries) >= 1
    _eid, fields = entries[0]
    assert fields["event_type"] == "reject"
    assert fields["order_id"] == result["order_id"]
    assert fields["reject_reason"] == "mock reject"
    assert fields["book"] == "integration_test"


def test_oms_integration_sent_then_fill_callback_produces(redis_client, store):
    """process_one places order (sent); fill callback updates store and produces to oms_fills."""
    from oms.registry import AdapterRegistry

    registry = AdapterRegistry()
    registry.register("binance", _mock_adapter({
        "broker_order_id": "mock-123",
        "status": "NEW",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "executed_qty": 0,
    }))

    add_message(redis_client, RISK_APPROVED_STREAM, {
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": "0.001",
        "order_type": "MARKET",
    })

    result = process_one(redis_client, store, registry, start_id="0")
    assert result is not None
    assert result["rejected"] is False
    assert result["broker_order_id"] == "mock-123"

    order = store.get_order(result["order_id"])
    assert order["status"] == "sent"

    # Simulate fill callback (as adapter would call it)
    fill_cb = make_fill_callback(redis_client, store)
    fill_cb({
        "event_type": "fill",
        "order_id": result["order_id"],
        "broker_order_id": "mock-123",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.001,
        "price": 50000,
        "fee": 0,
        "fee_asset": "BNB",
        "executed_at": "2025-01-01T12:00:00Z",
        "fill_id": "trade-1",
    })

    order2 = store.get_order(result["order_id"])
    assert order2["status"] == "filled"
    assert order2["executed_qty"] == 0.001

    entries = read_latest(redis_client, OMS_FILLS_STREAM, count=5)
    assert len(entries) >= 1
    _eid, fields = entries[0]
    assert fields["event_type"] == "fill"
    assert fields["order_id"] == result["order_id"]
    assert fields["broker_order_id"] == "mock-123"


def test_oms_integration_unknown_broker_rejects(redis_client, store):
    """Unknown broker: no adapter → reject in store and on oms_fills."""
    from oms.registry import AdapterRegistry

    registry = AdapterRegistry()
    # no binance registered
    add_message(redis_client, RISK_APPROVED_STREAM, {
        "broker": "unknown_broker",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": "0.001",
    })

    result = process_one(redis_client, store, registry, start_id="0")
    assert result is not None
    assert result["rejected"] is True
    assert "adapter" in result["reject_reason"].lower()

    order = store.get_order(result["order_id"])
    assert order["status"] == "rejected"

    entries = read_latest(redis_client, OMS_FILLS_STREAM, count=1)
    assert len(entries) == 1
    assert entries[0][1]["event_type"] == "reject"
