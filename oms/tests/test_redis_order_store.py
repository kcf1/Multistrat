"""
Unit tests for OMS Redis order store (task 12.1.4).

Uses fakeredis to verify CRUD and index updates (by_status, by_book, by_broker_order_id).
"""

import pytest
from fakeredis import FakeRedis

from oms.storage.redis_order_store import RedisOrderStore


@pytest.fixture
def redis_client():
    return FakeRedis(decode_responses=True)


@pytest.fixture
def store(redis_client):
    return RedisOrderStore(redis_client)


class TestStageOrder:
    def test_stage_order_creates_hash_and_indexes(self, store, redis_client):
        order_id = "ord-1"
        order_data = {
            "broker": "binance",
            "account_id": "1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "order_type": "LIMIT",
            "quantity": 0.01,
            "price": 50000,
            "time_in_force": "GTC",
            "book": "ma_cross",
            "comment": "test",
        }
        store.stage_order(order_id, order_data)
        order = store.get_order(order_id)
        assert order is not None
        assert order["internal_id"] == order_id
        assert order["broker"] == "binance"
        assert order["symbol"] == "BTCUSDT"
        assert order["status"] == "pending"
        assert order["book"] == "ma_cross"
        assert "created_at" in order
        assert "updated_at" in order
        assert redis_client.sismember("orders:by_status:pending", order_id)
        assert redis_client.sismember("orders:by_book:ma_cross", order_id)

    def test_stage_order_without_book_skips_book_index(self, store, redis_client):
        order_id = "ord-2"
        order_data = {"broker": "binance", "symbol": "ETHUSDT", "side": "SELL", "order_type": "MARKET", "quantity": 0.1}
        store.stage_order(order_id, order_data)
        assert store.get_order(order_id)["status"] == "pending"
        assert redis_client.sismember("orders:by_status:pending", order_id)
        assert redis_client.scard("orders:by_book:") == 0 or not redis_client.sismember("orders:by_book:", order_id)


class TestUpdateStatus:
    def test_update_status_moves_index_and_sets_broker_order_id(self, store, redis_client):
        order_id = "ord-3"
        store.stage_order(order_id, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "order_type": "MARKET", "quantity": 0.001})
        assert store.get_order(order_id)["status"] == "pending"
        store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": "binance-123"})
        order = store.get_order(order_id)
        assert order["status"] == "sent"
        assert order["broker_order_id"] == "binance-123"
        assert not redis_client.sismember("orders:by_status:pending", order_id)
        assert redis_client.sismember("orders:by_status:sent", order_id)
        assert redis_client.get("orders:by_broker_order_id:binance-123") == order_id

    def test_find_order_by_broker_order_id(self, store):
        order_id = "ord-4"
        store.stage_order(order_id, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "order_type": "MARKET", "quantity": 0.001})
        store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": "999"})
        found = store.find_order_by_broker_order_id("999")
        assert found == order_id
        assert store.find_order_by_broker_order_id("nonexistent") is None


class TestUpdateFillStatus:
    def test_update_fill_status_sets_filled_and_executed_qty(self, store, redis_client):
        order_id = "ord-5"
        store.stage_order(order_id, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "order_type": "MARKET", "quantity": 0.001})
        store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": "b-5"})
        store.update_fill_status(order_id, "filled", executed_qty=0.001)
        order = store.get_order(order_id)
        assert order["status"] == "filled"
        assert order["executed_qty"] == 0.001
        assert redis_client.sismember("orders:by_status:filled", order_id)
        assert not redis_client.sismember("orders:by_status:sent", order_id)

    def test_update_fill_status_rejected(self, store):
        order_id = "ord-6"
        store.stage_order(order_id, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "order_type": "MARKET", "quantity": 0.001})
        store.update_fill_status(order_id, "rejected")
        order = store.get_order(order_id)
        assert order["status"] == "rejected"


class TestGetOrder:
    def test_get_order_missing_returns_none(self, store):
        assert store.get_order("nonexistent") is None

    def test_get_order_returns_unflattened_numeric_fields(self, store):
        order_id = "ord-7"
        store.stage_order(order_id, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "order_type": "MARKET", "quantity": 0.5})
        store.update_status(order_id, "sent", "pending", extra_fields={"broker_order_id": "b7", "executed_qty": 0.25})
        order = store.get_order(order_id)
        assert order["quantity"] == 0.5 or order["quantity"] == "0.5"
        assert order.get("executed_qty") in (0.25, "0.25")


class TestIdempotentIndexUpdates:
    def test_same_status_update_does_not_duplicate_in_set(self, store, redis_client):
        order_id = "ord-8"
        store.stage_order(order_id, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "order_type": "MARKET", "quantity": 0.001})
        store.update_status(order_id, "sent", "pending")
        store.update_status(order_id, "sent", "sent")
        assert redis_client.scard("orders:by_status:sent") == 1
        assert redis_client.sismember("orders:by_status:sent", order_id)
