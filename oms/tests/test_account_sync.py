"""
Unit tests for OMS → Postgres account sync (task 12.2.7).

Tests sync_accounts_to_postgres (mapping, UPSERT accounts/balances, balance cleanup),
write_balance_change, get_account_pk_by_broker_and_id. Uses fakeredis and mock Postgres.
"""

import pytest
from decimal import Decimal
from fakeredis import FakeRedis

from oms.account_sync import (
    _account_to_row,
    _balance_to_row,
    _discover_brokers,
    _normalize_event_time,
    get_account_pk_by_broker_and_id,
    sync_accounts_to_postgres,
    write_balance_change,
)
from oms.storage.redis_account_store import RedisAccountStore


@pytest.fixture
def redis_client():
    return FakeRedis(decode_responses=True)


@pytest.fixture
def account_store(redis_client):
    return RedisAccountStore(redis_client)


def _mock_pg_connect(execute_calls=None):
    """Return (connect_callable, list that collects (sql, params))."""
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


class TestMappers:
    def test_account_to_row(self):
        row = _account_to_row(
            {"broker": "binance", "account_id": "a1", "updated_at": "2026-01-01T00:00:00Z"},
            "binance",
            "a1",
        )
        assert row["broker"] == "binance"
        assert row["account_id"] == "a1"
        assert row["name"] == "binance:a1"

    def test_balance_to_row(self):
        row = _balance_to_row(
            {"asset": "USDT", "available": "1000.5", "locked": "0"},
            42,
            "2026-01-01T00:00:00Z",
        )
        assert row["account_id"] == 42
        assert row["asset"] == "USDT"
        assert row["available"] == Decimal("1000.5")
        assert row["locked"] == Decimal("0")


class TestDiscoverBrokers:
    def test_no_brokers_returns_empty(self, redis_client):
        assert _discover_brokers(redis_client) == []

    def test_discovers_broker_after_apply(self, redis_client, account_store):
        account_store.apply_account_position(
            "binance", "acc1",
            [{"asset": "USDT", "available": "1000", "locked": "0"}],
            [],
        )
        assert _discover_brokers(redis_client) == ["binance"]


class TestSyncAccountsToPostgres:
    def test_sync_upserts_account_and_balances(self, redis_client, account_store):
        account_store.apply_account_position(
            "binance", "acc1",
            [
                {"asset": "USDT", "available": "1000", "locked": "0"},
                {"asset": "BTC", "available": "0.01", "locked": "0"},
            ],
            [],
        )
        execute_calls = []
        fetchone_results = [1]  # account id for RETURNING id

        class MockCursor:
            def execute(self, sql, params=None):
                execute_calls.append((sql, params or {}))

            def fetchone(self):
                if fetchone_results:
                    return (fetchone_results.pop(0),)
                return None

        class MockConn:
            def cursor(self):
                return MockCursor()

            def commit(self):
                pass

        def connect():
            return MockConn()

        count = sync_accounts_to_postgres(redis_client, account_store, connect, ttl_after_sync_seconds=None)
        assert count == 1
        sqls = [c[0] for c in execute_calls]
        assert any("INSERT INTO accounts" in s for s in sqls)
        assert any("ON CONFLICT (broker, account_id)" in s for s in sqls)
        assert any("INSERT INTO balances" in s for s in sqls)
        assert any("ON CONFLICT (account_id, asset)" in s for s in sqls)
        assert any("DELETE FROM balances" in s for s in sqls)

    def test_sync_deletes_stale_balances(self, redis_client, account_store):
        account_store.apply_account_position(
            "binance", "acc1",
            [{"asset": "USDT", "available": "500", "locked": "0"}],
            [],
        )
        execute_calls = []

        class MockCursor:
            def execute(self, sql, params=None):
                execute_calls.append((sql, params or {}))

            def fetchone(self):
                return (1,)

        def connect():
            class MockConn:
                def cursor(self):
                    return MockCursor()
                def commit(self):
                    pass
            return MockConn()

        sync_accounts_to_postgres(redis_client, account_store, connect, ttl_after_sync_seconds=None)
        delete_sqls = [c[0] for c in execute_calls if "DELETE FROM balances" in c[0]]
        assert len(delete_sqls) >= 1

    def test_sync_no_brokers_returns_zero(self, redis_client, account_store):
        connect, calls = _mock_pg_connect()
        count = sync_accounts_to_postgres(redis_client, account_store, connect)
        assert count == 0
        assert len(calls) == 0

    def test_sync_sets_ttl_when_requested(self, redis_client, account_store):
        account_store.apply_account_position(
            "binance", "acc1",
            [{"asset": "USDT", "available": "100", "locked": "0"}],
            [],
        )
        class MockCursor:
            def execute(self, sql, params=None):
                pass
            def fetchone(self):
                return (1,)
        def connect():
            class MockConn:
                def cursor(self):
                    return MockCursor()
                def commit(self):
                    pass
            return MockConn()
        sync_accounts_to_postgres(redis_client, account_store, connect, ttl_after_sync_seconds=60)
        assert redis_client.ttl("account:binance:acc1") == 60
        assert redis_client.ttl("account:binance:acc1:balances") == 60


class TestWriteBalanceChange:
    def test_write_balance_change_executes_insert(self):
        connect, calls = _mock_pg_connect()
        write_balance_change(
            connect,
            account_id_pk=1,
            asset="USDT",
            change_type="deposit",
            delta=Decimal("100"),
            event_type="balanceUpdate",
            event_time=1704067200000,
            payload={"e": "balanceUpdate", "d": "100", "T": 1704067200000},
        )
        assert len(calls) == 1
        sql, params = calls[0]
        assert "INSERT INTO balance_changes" in sql
        assert params.get("asset") == "USDT"
        assert params.get("change_type") == "deposit"
        assert params.get("delta") == Decimal("100")


class TestNormalizeEventTime:
    def test_none_returns_now(self):
        t = _normalize_event_time(None)
        assert t.tzinfo is not None

    def test_ms_int_converted(self):
        t = _normalize_event_time(1704067200000)
        assert t.year == 2024

    def test_datetime_preserved(self):
        from datetime import datetime, timezone
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert _normalize_event_time(dt) == dt


class TestGetAccountPkByBrokerAndId:
    def test_returns_none_when_no_connection_callback(self):
        connect, calls = _mock_pg_connect()
        # Mock returns no row
        class MockCursor:
            def execute(self, sql, params=None):
                pass
            def fetchone(self):
                return None
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
        pk = get_account_pk_by_broker_and_id(connect, "binance", "acc1")
        assert pk is None

    def test_returns_id_when_row_exists(self):
        class MockCursor:
            def execute(self, sql, params=None):
                pass
            def fetchone(self):
                return (99,)
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
        pk = get_account_pk_by_broker_and_id(connect, "binance", "acc1")
        assert pk == 99
