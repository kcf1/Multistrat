"""
Integration test for OMS (task 12.1.9): wire consumer → store → registry → place_order → producer.

Mock Redis (fakeredis) and mock adapter; verify full flow for one order.
"""

from typing import Any, Dict, Optional

import pytest

from oms.redis_flow import make_fill_callback, process_one, process_one_cancel
from oms.schemas import CANCEL_REQUESTED_STREAM, OMS_FILLS_STREAM, RISK_APPROVED_STREAM
from oms.streams import add_message, read_latest
from oms.storage.redis_order_store import RedisOrderStore


def _mock_adapter(place_result: Dict[str, Any], cancel_result: Optional[Dict[str, Any]] = None):
    """Return a mock adapter that place_order returns place_result; optional cancel_order."""

    class MockAdapter:
        def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
            return place_result

        def start_fill_listener(self, callback) -> None:
            pass

        def cancel_order(self, broker_order_id: str, symbol: str) -> Dict[str, Any]:
            if cancel_result is None:
                return {"rejected": True, "reject_reason": "cancel not implemented"}
            return cancel_result
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

    # Simulate fill callback (as adapter would call it; Binance sends order_status + cumulative)
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
        "order_status": "FILLED",
        "executed_qty_cumulative": 0.001,
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


def test_oms_integration_partial_then_full_fill_status_and_cumulative(redis_client, store):
    """Fill callback: Binance order_status PARTIALLY_FILLED -> partially_filled + cumulative; FILLED -> filled."""
    from oms.redis_flow import make_fill_callback

    order_id = "ord-partial-full"
    store.stage_order(order_id, {
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 1.0,
        "order_type": "LIMIT",
        "book": "test",
    })
    store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": "b-123"})

    fill_cb = make_fill_callback(redis_client, store)

    # First fill: PARTIALLY_FILLED, cumulative 0.3
    fill_cb({
        "event_type": "fill",
        "order_id": order_id,
        "broker_order_id": "b-123",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.3,
        "price": 50000,
        "fee": 0,
        "fee_asset": "BNB",
        "executed_at": "2025-01-01T12:00:00Z",
        "fill_id": "t1",
        "order_status": "PARTIALLY_FILLED",
        "executed_qty_cumulative": 0.3,
    })
    order = store.get_order(order_id)
    assert order["status"] == "partially_filled"
    assert order.get("executed_qty") == 0.3

    # Second fill: FILLED, cumulative 1.0
    fill_cb({
        "event_type": "fill",
        "order_id": order_id,
        "broker_order_id": "b-123",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.7,
        "price": 50010,
        "fee": 0,
        "fee_asset": "BNB",
        "executed_at": "2025-01-01T12:00:01Z",
        "fill_id": "t2",
        "order_status": "FILLED",
        "executed_qty_cumulative": 1.0,
    })
    order2 = store.get_order(order_id)
    assert order2["status"] == "filled"
    assert order2.get("executed_qty") == 1.0


def test_oms_integration_fill_callback_cancelled_expired_updates_store(redis_client, store):
    """Fill callback: event_type cancelled/expired updates store and publishes to oms_fills (12.1.9c)."""
    from oms.redis_flow import make_fill_callback

    order_id = "ord-cancel-1"
    store.stage_order(order_id, {
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.001,
        "order_type": "LIMIT",
        "book": "test",
    })
    store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": "binance-999"})

    fill_cb = make_fill_callback(redis_client, store)

    fill_cb({
        "event_type": "cancelled",
        "order_id": order_id,
        "broker_order_id": "binance-999",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.001,
        "price": 50000,
        "reject_reason": "USER_CANCEL",
    })
    order = store.get_order(order_id)
    assert order["status"] == "cancelled"
    entries = read_latest(redis_client, OMS_FILLS_STREAM, count=1)
    assert len(entries) == 1
    assert entries[0][1]["event_type"] == "cancelled"
    assert entries[0][1]["reject_reason"] == "USER_CANCEL"

    order_id2 = "ord-expired-1"
    store.stage_order(order_id2, {
        "broker": "binance",
        "symbol": "ETHUSDT",
        "side": "SELL",
        "quantity": 0.01,
        "order_type": "LIMIT",
        "book": "test",
    })
    store.update_status(order_id2, "sent", "pending", extra_fields={"broker_order_id": "binance-1000"})
    fill_cb({
        "event_type": "expired",
        "order_id": order_id2,
        "broker_order_id": "binance-1000",
        "symbol": "ETHUSDT",
        "side": "SELL",
        "quantity": 0.01,
        "price": 3000,
        "reject_reason": "GTX",
    })
    order2 = store.get_order(order_id2)
    assert order2["status"] == "expired"
    entries2 = read_latest(redis_client, OMS_FILLS_STREAM, count=2)
    event_types = {e[1]["event_type"] for e in entries2}
    assert "expired" in event_types


def test_oms_integration_process_one_cancel_from_redis(redis_client, store):
    """Cancel command from Redis: XADD cancel_requested → process_one_cancel → store cancelled, oms_fills (12.1.9f)."""
    from oms.registry import AdapterRegistry

    order_id = "ord-to-cancel"
    broker_order_id = "binance-555"
    store.stage_order(order_id, {
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.001,
        "order_type": "LIMIT",
        "book": "test",
    })
    store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": broker_order_id})

    mock_cancel_result = {"status": "CANCELED", "broker_order_id": broker_order_id, "symbol": "BTCUSDT"}
    adapter = _mock_adapter(place_result={}, cancel_result=mock_cancel_result)
    registry = AdapterRegistry()
    registry.register("binance", adapter)

    add_message(redis_client, CANCEL_REQUESTED_STREAM, {"order_id": order_id, "broker": "binance"})

    result = process_one_cancel(redis_client, store, registry, start_id="0")
    assert result is not None
    assert result.get("cancelled") is True
    assert result.get("order_id") == order_id
    assert result.get("broker_order_id") == broker_order_id

    order = store.get_order(order_id)
    assert order["status"] == "cancelled"

    entries = read_latest(redis_client, OMS_FILLS_STREAM, count=1)
    assert len(entries) == 1
    assert entries[0][1]["event_type"] == "cancelled"
    assert entries[0][1]["order_id"] == order_id


def test_oms_integration_process_one_cancel_by_broker_order_id(redis_client, store):
    """Cancel by broker_order_id + symbol when order_id not in message."""
    from oms.registry import AdapterRegistry

    order_id = "ord-internal"
    broker_order_id = "binance-777"
    store.stage_order(order_id, {"broker": "binance", "symbol": "ETHUSDT", "side": "SELL", "quantity": 0.01, "order_type": "MARKET"})
    store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": broker_order_id})

    adapter = _mock_adapter(place_result={}, cancel_result={"status": "CANCELED", "broker_order_id": broker_order_id, "symbol": "ETHUSDT"})
    registry = AdapterRegistry()
    registry.register("binance", adapter)

    add_message(redis_client, CANCEL_REQUESTED_STREAM, {
        "broker_order_id": broker_order_id,
        "symbol": "ETHUSDT",
        "broker": "binance",
    })

    result = process_one_cancel(redis_client, store, registry, start_id="0")
    assert result is not None
    assert result.get("cancelled") is True
    assert result.get("order_id") == order_id
    order = store.get_order(order_id)
    assert order["status"] == "cancelled"


def test_oms_integration_process_one_cancel_consumer_group_no_double_process(redis_client, store):
    """Cancel with consumer group: each message delivered once; second read returns next message (12.1.9f)."""
    from oms.registry import AdapterRegistry

    for i, oid in enumerate(("ord-cancel-a", "ord-cancel-b")):
        store.stage_order(oid, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001, "order_type": "LIMIT"})
        store.update_status(oid, "sent", "pending", extra_fields={"broker_order_id": f"binance-{100+i}"})

    adapter = _mock_adapter(place_result={}, cancel_result={"status": "CANCELED"})
    registry = AdapterRegistry()
    registry.register("binance", adapter)

    add_message(redis_client, CANCEL_REQUESTED_STREAM, {"order_id": "ord-cancel-a", "broker": "binance"})
    add_message(redis_client, CANCEL_REQUESTED_STREAM, {"order_id": "ord-cancel-b", "broker": "binance"})

    r1 = process_one_cancel(redis_client, store, registry, consumer_group="oms", consumer_name="oms-1")
    assert r1 is not None and r1.get("order_id") == "ord-cancel-a" and r1.get("cancelled") is True
    r2 = process_one_cancel(redis_client, store, registry, consumer_group="oms", consumer_name="oms-1")
    assert r2 is not None and r2.get("order_id") == "ord-cancel-b" and r2.get("cancelled") is True
    r3 = process_one_cancel(redis_client, store, registry, consumer_group="oms", consumer_name="oms-1")
    assert r3 is None


def test_oms_integration_consumer_group_no_double_process(redis_client, store):
    """Consumer group: each message delivered once; second read returns next message, not first."""
    from oms.registry import AdapterRegistry

    registry = AdapterRegistry()
    registry.register("binance", _mock_adapter({"broker_order_id": "b-1", "status": "NEW"}))

    add_message(redis_client, RISK_APPROVED_STREAM, {
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": "0.001",
        "order_type": "MARKET",
        "order_id": "order-first",
    })
    add_message(redis_client, RISK_APPROVED_STREAM, {
        "broker": "binance",
        "symbol": "ETHUSDT",
        "side": "SELL",
        "quantity": "0.01",
        "order_type": "MARKET",
        "order_id": "order-second",
    })

    result1 = process_one(redis_client, store, registry)
    assert result1 is not None
    assert result1["order_id"] == "order-first"

    result2 = process_one(redis_client, store, registry)
    assert result2 is not None
    assert result2["order_id"] == "order-second", "Second read must return second message, not first (no double-process)"

    result3 = process_one(redis_client, store, registry)
    assert result3 is None, "No more messages after two"

    assert store.get_order("order-first") is not None
    assert store.get_order("order-second") is not None


def test_oms_integration_fill_callback_accumulates_when_no_cumulative(redis_client, store):
    """When event has no executed_qty_cumulative, callback accumulates incremental quantity."""
    from oms.redis_flow import make_fill_callback

    order_id = "ord-accum"
    store.stage_order(order_id, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 1.0})
    store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": "b-accum"})

    fill_cb = make_fill_callback(redis_client, store)

    fill_cb({
        "event_type": "fill",
        "order_id": order_id,
        "broker_order_id": "b-accum",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.2,
        "price": 50000,
        "order_status": "PARTIALLY_FILLED",
        "executed_at": "2025-01-01T12:00:00Z",
        "fill_id": "t1",
    })
    order = store.get_order(order_id)
    assert order["status"] == "partially_filled"
    assert order.get("executed_qty") == 0.2

    fill_cb({
        "event_type": "fill",
        "order_id": order_id,
        "broker_order_id": "b-accum",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.5,
        "price": 50010,
        "order_status": "FILLED",
        "executed_at": "2025-01-01T12:00:01Z",
        "fill_id": "t2",
    })
    order2 = store.get_order(order_id)
    assert order2["status"] == "filled"
    assert order2.get("executed_qty") == 0.7
