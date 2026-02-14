"""
Unit tests for OMS → Postgres order sync (task 12.1.10).

Tests sync_one_order (UPSERT, TTL after sync), get_terminal_order_ids, sync_terminal_orders.
Uses fakeredis for store and mock Postgres connection.
"""

import pytest
from fakeredis import FakeRedis

from oms.storage.redis_order_store import RedisOrderStore
from oms.sync import (
    DEFAULT_SYNC_TTL_AFTER_SECONDS,
    get_terminal_order_ids,
    sync_one_order,
    sync_terminal_orders,
)


@pytest.fixture
def redis_client():
    return FakeRedis(decode_responses=True)


@pytest.fixture
def store(redis_client):
    return RedisOrderStore(redis_client)


def _mock_pg_connect(execute_calls=None):
    """Return a callable that returns a mock connection; execute_calls list collects (sql, params)."""
    if execute_calls is None:
        execute_calls = []

    class MockCursor:
        def execute(self, sql, params=None):
            execute_calls.append((sql, params or {}))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    class MockConn:
        def cursor(self):
            return MockCursor()

        def commit(self):
            pass

    def connect():
        return MockConn()

    return connect, execute_calls


class TestGetTerminalOrderIds:
    def test_empty_store_returns_empty(self, store):
        assert get_terminal_order_ids(store) == []

    def test_returns_filled_and_rejected(self, store):
        store.stage_order("o1", {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001})
        store.update_fill_status("o1", "filled", executed_qty=0.001)
        store.stage_order("o2", {"broker": "binance", "symbol": "ETHUSDT", "side": "SELL", "quantity": 0.01})
        store.update_fill_status("o2", "rejected")
        ids = get_terminal_order_ids(store)
        assert set(ids) == {"o1", "o2"}

    def test_pending_not_included(self, store):
        store.stage_order("o-pending", {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001})
        assert get_terminal_order_ids(store) == []


class TestSyncOneOrder:
    def test_sync_one_order_writes_upsert(self, redis_client, store):
        store.stage_order(
            "ord-sync-1",
            {
                "broker": "binance",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.001,
                "order_type": "MARKET",
                "book": "test",
                "comment": "sync test",
            },
        )
        store.update_fill_status("ord-sync-1", "filled", executed_qty=0.001)
        connect, calls = _mock_pg_connect()
        result = sync_one_order(redis_client, store, connect, "ord-sync-1", ttl_after_sync_seconds=None)
        assert result is True
        assert len(calls) == 1
        sql, params = calls[0]
        assert "INSERT INTO orders" in sql
        assert "ON CONFLICT (internal_id) DO UPDATE" in sql
        assert params.get("internal_id") == "ord-sync-1"
        assert params.get("status") == "filled"
        assert params.get("symbol") == "BTCUSDT"

    def test_sync_one_order_includes_price_and_limit_price(self, redis_client, store):
        """Sync row includes price (executed) and limit_price (order limit) per 12.1.12."""
        store.stage_order(
            "ord-prices",
            {
                "broker": "binance",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.01,
                "order_type": "LIMIT",
                "limit_price": 50000.0,
                "book": "test",
            },
        )
        store.update_fill_status("ord-prices", "filled", executed_qty=0.01, price=50100.0)
        connect, calls = _mock_pg_connect()
        result = sync_one_order(redis_client, store, connect, "ord-prices", ttl_after_sync_seconds=None)
        assert result is True
        assert len(calls) == 1
        _, params = calls[0]
        assert params.get("limit_price") == 50000.0
        assert params.get("price") == 50100.0

    def test_sync_one_order_idempotent(self, redis_client, store):
        store.stage_order("ord-idem", {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001})
        store.update_fill_status("ord-idem", "rejected")
        connect, calls = _mock_pg_connect()
        sync_one_order(redis_client, store, connect, "ord-idem", ttl_after_sync_seconds=None)
        sync_one_order(redis_client, store, connect, "ord-idem", ttl_after_sync_seconds=None)
        assert len(calls) == 2
        for sql, params in calls:
            assert params.get("internal_id") == "ord-idem"

    def test_sync_one_order_missing_returns_false(self, redis_client, store):
        connect, calls = _mock_pg_connect()
        result = sync_one_order(redis_client, store, connect, "nonexistent", ttl_after_sync_seconds=None)
        assert result is False
        assert len(calls) == 0

    def test_sync_one_order_sets_ttl_when_requested(self, redis_client, store):
        store.stage_order("ord-ttl", {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001})
        store.update_fill_status("ord-ttl", "filled")
        connect, _ = _mock_pg_connect()
        sync_one_order(redis_client, store, connect, "ord-ttl", ttl_after_sync_seconds=300)
        assert redis_client.ttl("orders:ord-ttl") == 300


class TestSyncTerminalOrders:
    def test_sync_terminal_orders_calls_sync_per_order(self, redis_client, store):
        store.stage_order("t1", {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001})
        store.update_fill_status("t1", "filled")
        store.stage_order("t2", {"broker": "binance", "symbol": "ETHUSDT", "side": "SELL", "quantity": 0.01})
        store.update_fill_status("t2", "cancelled")
        connect, calls = _mock_pg_connect()
        count = sync_terminal_orders(redis_client, store, connect, ttl_after_sync_seconds=None)
        assert count == 2
        assert len(calls) == 2
        ids = {params.get("internal_id") for _sql, params in calls}
        assert ids == {"t1", "t2"}

    def test_sync_terminal_orders_empty_when_no_terminal(self, redis_client, store):
        store.stage_order("p1", {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001})
        connect, calls = _mock_pg_connect()
        count = sync_terminal_orders(redis_client, store, connect, ttl_after_sync_seconds=None)
        assert count == 0
        assert len(calls) == 0
