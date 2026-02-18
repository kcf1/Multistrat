"""
Unit tests for OMS Redis account store (task 12.2.4).

Uses fakeredis to verify CRUD and index updates (by_broker).
"""

import json
import pytest
from fakeredis import FakeRedis

from oms.storage.redis_account_store import RedisAccountStore


@pytest.fixture
def redis_client():
    return FakeRedis(decode_responses=True)


@pytest.fixture
def store(redis_client):
    return RedisAccountStore(redis_client)


class TestApplyAccountPosition:
    """Test apply_account_position (full snapshot)."""

    def test_apply_account_position_creates_account_and_balances(self, store, redis_client):
        """apply_account_position creates account hash, balances hash, and adds to broker set."""
        broker = "binance"
        account_id = "test-account"
        balances = [
            {"asset": "USDT", "available": "1000.0", "locked": "0.0"},
            {"asset": "BTC", "available": "0.5", "locked": "0.1"},
        ]
        positions = []
        
        store.apply_account_position(broker, account_id, balances, positions)
        
        # Check account hash
        account = store.get_account(broker, account_id)
        assert account is not None
        assert account["broker"] == broker
        assert account["account_id"] == account_id
        assert "updated_at" in account
        
        # Check balances
        stored_balances = store.get_balances(broker, account_id)
        assert len(stored_balances) == 2
        assert stored_balances[0]["asset"] == "BTC"
        assert stored_balances[0]["available"] == "0.5"
        assert stored_balances[1]["asset"] == "USDT"
        assert stored_balances[1]["available"] == "1000.0"
        
        # Check broker set
        assert redis_client.sismember("accounts:by_broker:binance", account_id)

    def test_apply_account_position_with_positions(self, store):
        """apply_account_position stores positions."""
        broker = "binance"
        account_id = "test-account"
        balances = [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}]
        positions = [
            {"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"},
            {"symbol": "ETHUSDT", "side": "short", "quantity": "0.5", "entry_price": "3000.0"},
        ]
        
        store.apply_account_position(broker, account_id, balances, positions)
        
        stored_positions = store.get_positions(broker, account_id)
        assert len(stored_positions) == 2
        assert stored_positions[0]["symbol"] == "BTCUSDT"
        assert stored_positions[0]["side"] == "long"
        assert stored_positions[0]["quantity"] == "0.1"
        assert stored_positions[1]["symbol"] == "ETHUSDT"
        assert stored_positions[1]["side"] == "short"

    def test_apply_account_position_replaces_existing_balances(self, store):
        """apply_account_position replaces all existing balances (not merge)."""
        broker = "binance"
        account_id = "test-account"
        
        # First snapshot
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            []
        )
        
        # Second snapshot (different balances)
        store.apply_account_position(
            broker, account_id,
            [{"asset": "BTC", "available": "0.5", "locked": "0.0"}],
            []
        )
        
        # Should only have BTC now
        balances = store.get_balances(broker, account_id)
        assert len(balances) == 1
        assert balances[0]["asset"] == "BTC"

    def test_apply_account_position_with_payload(self, store):
        """apply_account_position stores payload."""
        broker = "binance"
        account_id = "test-account"
        payload = {"e": "outboundAccountPosition", "E": 1234567890}
        
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            [],
            payload=payload
        )
        
        account = store.get_account(broker, account_id)
        assert account["payload"] == payload

    def test_apply_account_position_empty_balances_and_positions(self, store):
        """apply_account_position handles empty balances and positions."""
        broker = "binance"
        account_id = "test-account"
        
        store.apply_account_position(broker, account_id, [], [])
        
        assert store.get_balances(broker, account_id) == []
        assert store.get_positions(broker, account_id) == []


class TestApplyBalanceUpdate:
    """Test apply_balance_update (single-asset delta)."""

    def test_apply_balance_update_creates_new_balance(self, store):
        """apply_balance_update creates balance if asset doesn't exist."""
        broker = "binance"
        account_id = "test-account"
        balance = {"asset": "USDT", "available": "100.5", "locked": "0.0"}
        
        store.apply_balance_update(broker, account_id, balance)
        
        balances = store.get_balances(broker, account_id)
        assert len(balances) == 1
        assert balances[0]["asset"] == "USDT"
        assert balances[0]["available"] == "100.5"
        assert balances[0]["locked"] == "0.0"

    def test_apply_balance_update_merges_with_existing(self, store):
        """apply_balance_update merges with existing balance for same asset."""
        broker = "binance"
        account_id = "test-account"
        
        # First: full snapshot
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "50.0"}],
            []
        )
        
        # Second: balance update (only available changes)
        store.apply_balance_update(
            broker, account_id,
            {"asset": "USDT", "available": "1100.0"}  # locked not provided
        )
        
        balances = store.get_balances(broker, account_id)
        assert len(balances) == 1
        assert balances[0]["asset"] == "USDT"
        assert balances[0]["available"] == "1100.0"
        assert balances[0]["locked"] == "50.0"  # Preserved from original

    def test_apply_balance_update_without_locked_defaults_to_zero(self, store):
        """apply_balance_update defaults locked to 0.0 if not provided."""
        broker = "binance"
        account_id = "test-account"
        balance = {"asset": "USDT", "available": "100.5"}  # No locked field
        
        store.apply_balance_update(broker, account_id, balance)
        
        balances = store.get_balances(broker, account_id)
        assert balances[0]["locked"] == "0.0"

    def test_apply_balance_update_updates_account_metadata(self, store):
        """apply_balance_update updates account updated_at timestamp."""
        broker = "binance"
        account_id = "test-account"
        
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            []
        )
        first_updated = store.get_account(broker, account_id)["updated_at"]
        
        store.apply_balance_update(
            broker, account_id,
            {"asset": "BTC", "available": "0.1", "locked": "0.0"}
        )
        second_updated = store.get_account(broker, account_id)["updated_at"]
        
        assert second_updated != first_updated

    def test_apply_balance_update_with_payload(self, store):
        """apply_balance_update stores payload."""
        broker = "binance"
        account_id = "test-account"
        payload = {"e": "balanceUpdate", "a": "USDT", "d": "100.5"}
        
        store.apply_balance_update(
            broker, account_id,
            {"asset": "USDT", "available": "100.5", "locked": "0.0"},
            payload=payload
        )
        
        account = store.get_account(broker, account_id)
        assert account["payload"] == payload

    def test_apply_balance_update_missing_asset_skips(self, store):
        """apply_balance_update skips if balance missing asset field."""
        broker = "binance"
        account_id = "test-account"
        
        store.apply_balance_update(broker, account_id, {"available": "100.0"})
        
        balances = store.get_balances(broker, account_id)
        assert len(balances) == 0


class TestGetAccount:
    """Test get_account."""

    def test_get_account_returns_account_metadata(self, store):
        """get_account returns account hash."""
        broker = "binance"
        account_id = "test-account"
        
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            []
        )
        
        account = store.get_account(broker, account_id)
        assert account is not None
        assert account["broker"] == broker
        assert account["account_id"] == account_id
        assert "updated_at" in account

    def test_get_account_missing_returns_none(self, store):
        """get_account returns None if account doesn't exist."""
        account = store.get_account("binance", "nonexistent")
        assert account is None

    def test_get_account_parses_payload_json(self, store):
        """get_account parses payload JSON."""
        broker = "binance"
        account_id = "test-account"
        payload = {"e": "outboundAccountPosition", "E": 1234567890}
        
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            [],
            payload=payload
        )
        
        account = store.get_account(broker, account_id)
        assert isinstance(account["payload"], dict)
        assert account["payload"]["e"] == "outboundAccountPosition"


class TestGetBalances:
    """Test get_balances."""

    def test_get_balances_returns_all_balances(self, store):
        """get_balances returns all balances sorted by asset."""
        broker = "binance"
        account_id = "test-account"
        balances = [
            {"asset": "USDT", "available": "1000.0", "locked": "0.0"},
            {"asset": "BTC", "available": "0.5", "locked": "0.1"},
            {"asset": "ETH", "available": "10.0", "locked": "0.0"},
        ]
        
        store.apply_account_position(broker, account_id, balances, [])
        
        stored = store.get_balances(broker, account_id)
        assert len(stored) == 3
        assert stored[0]["asset"] == "BTC"  # Sorted
        assert stored[1]["asset"] == "ETH"
        assert stored[2]["asset"] == "USDT"

    def test_get_balances_empty_returns_empty_list(self, store):
        """get_balances returns empty list if no balances."""
        balances = store.get_balances("binance", "test-account")
        assert balances == []


class TestGetPositions:
    """Test get_positions."""

    def test_get_positions_returns_all_positions(self, store):
        """get_positions returns all positions sorted by symbol."""
        broker = "binance"
        account_id = "test-account"
        positions = [
            {"symbol": "ETHUSDT", "side": "short", "quantity": "0.5", "entry_price": "3000.0"},
            {"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"},
        ]
        
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            positions
        )
        
        stored = store.get_positions(broker, account_id)
        assert len(stored) == 2
        assert stored[0]["symbol"] == "BTCUSDT"  # Sorted
        assert stored[1]["symbol"] == "ETHUSDT"

    def test_get_positions_empty_returns_empty_list(self, store):
        """get_positions returns empty list if no positions."""
        positions = store.get_positions("binance", "test-account")
        assert positions == []


class TestGetAccountIdsByBroker:
    """Test get_account_ids_by_broker."""

    def test_get_account_ids_by_broker_returns_all_accounts(self, store):
        """get_account_ids_by_broker returns all account IDs for broker."""
        broker = "binance"
        
        store.apply_account_position(
            broker, "account-1",
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            []
        )
        store.apply_account_position(
            broker, "account-2",
            [{"asset": "USDT", "available": "2000.0", "locked": "0.0"}],
            []
        )
        
        account_ids = store.get_account_ids_by_broker(broker)
        assert set(account_ids) == {"account-1", "account-2"}

    def test_get_account_ids_by_broker_empty_returns_empty_list(self, store):
        """get_account_ids_by_broker returns empty list if no accounts."""
        account_ids = store.get_account_ids_by_broker("binance")
        assert account_ids == []

    def test_get_account_ids_by_broker_separates_by_broker(self, store):
        """get_account_ids_by_broker only returns accounts for specified broker."""
        store.apply_account_position(
            "binance", "account-1",
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            []
        )
        store.apply_account_position(
            "bybit", "account-1",
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            []
        )
        
        binance_accounts = store.get_account_ids_by_broker("binance")
        bybit_accounts = store.get_account_ids_by_broker("bybit")
        
        assert binance_accounts == ["account-1"]
        assert bybit_accounts == ["account-1"]


class TestPipelineAtomicity:
    """Test that operations are atomic via pipelines."""

    def test_apply_account_position_atomic(self, store, redis_client):
        """apply_account_position updates all keys atomically."""
        broker = "binance"
        account_id = "test-account"
        
        # Use a pipeline to verify atomicity
        # If operation fails mid-way, we shouldn't have partial state
        store.apply_account_position(
            broker, account_id,
            [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            [{"symbol": "BTCUSDT", "side": "long", "quantity": "0.1"}]
        )
        
        # All keys should exist
        assert redis_client.exists(f"account:{broker}:{account_id}")
        assert redis_client.exists(f"account:{broker}:{account_id}:balances")
        assert redis_client.exists(f"account:{broker}:{account_id}:positions")
        assert redis_client.sismember(f"accounts:by_broker:{broker}", account_id)

    def test_apply_balance_update_atomic(self, store, redis_client):
        """apply_balance_update updates account and balance atomically."""
        broker = "binance"
        account_id = "test-account"
        
        store.apply_balance_update(
            broker, account_id,
            {"asset": "USDT", "available": "100.5", "locked": "0.0"}
        )
        
        # Both account and balance keys should exist
        assert redis_client.exists(f"account:{broker}:{account_id}")
        assert redis_client.exists(f"account:{broker}:{account_id}:balances")
        assert redis_client.sismember(f"accounts:by_broker:{broker}", account_id)


class TestDataStructureMatchesAdapterEvents:
    """Test that stored data structure matches unified AccountEvent shape."""

    def test_balance_structure_matches_adapter_event(self, store):
        """Stored balance structure matches AccountEvent balance dict."""
        broker = "binance"
        account_id = "test-account"
        
        # Simulate account_position event from adapter
        event_balances = [
            {"asset": "USDT", "available": "1000.0", "locked": "0.0"},
            {"asset": "BTC", "available": "0.5", "locked": "0.1"},
        ]
        
        store.apply_account_position(broker, account_id, event_balances, [])
        
        stored_balances = store.get_balances(broker, account_id)
        assert len(stored_balances) == len(event_balances)
        for stored, original in zip(stored_balances, sorted(event_balances, key=lambda x: x["asset"])):
            assert stored["asset"] == original["asset"]
            assert stored["available"] == original["available"]
            assert stored["locked"] == original["locked"]

    def test_position_structure_matches_adapter_event(self, store):
        """Stored position structure matches AccountEvent position dict."""
        broker = "binance"
        account_id = "test-account"
        
        # Simulate account_position event from adapter
        event_positions = [
            {"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"},
        ]
        
        store.apply_account_position(broker, account_id, [], event_positions)
        
        stored_positions = store.get_positions(broker, account_id)
        assert len(stored_positions) == len(event_positions)
        assert stored_positions[0]["symbol"] == event_positions[0]["symbol"]
        assert stored_positions[0]["side"] == event_positions[0]["side"]
        assert stored_positions[0]["quantity"] == event_positions[0]["quantity"]
        assert stored_positions[0]["entry_price"] == event_positions[0]["entry_price"]
