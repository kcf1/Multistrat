"""
Unit tests for account callback handler (task 12.2.5).

Tests make_account_callback: event validation, store updates, idempotency.
"""

from unittest.mock import MagicMock, call

import pytest
from fakeredis import FakeRedis
from pydantic import ValidationError

from oms.account_flow import make_account_callback
from oms.storage.redis_account_store import RedisAccountStore


@pytest.fixture
def redis_client():
    return FakeRedis(decode_responses=True)


@pytest.fixture
def account_store(redis_client):
    return RedisAccountStore(redis_client)


class TestMakeAccountCallbackValidation:
    """Test event validation in account callback."""

    def test_account_position_event_validated(self, account_store):
        """Valid account_position event is validated and processed."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {"e": "outboundAccountPosition"},
        }
        
        callback(event)
        
        # Verify account was stored
        account = account_store.get_account("binance", "test-account")
        assert account is not None
        assert account["broker"] == "binance"
        assert account["account_id"] == "test-account"

    def test_balance_update_event_validated(self, account_store):
        """Valid balance_update event is validated and processed."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "balance_update",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "100.5", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {"e": "balanceUpdate", "a": "USDT", "d": "100.5"},
        }
        
        callback(event)
        
        # Verify balance was stored
        balances = account_store.get_balances("binance", "test-account")
        assert len(balances) == 1
        assert balances[0]["asset"] == "USDT"
        assert balances[0]["available"] == "100.5"

    def test_invalid_event_type_skipped(self, account_store):
        """Unknown event_type is skipped."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "unknown_event",
            "broker": "binance",
            "account_id": "test-account",
        }
        
        callback(event)
        
        # Verify nothing was stored
        account = account_store.get_account("binance", "test-account")
        assert account is None

    def test_invalid_event_structure_skipped(self, account_store):
        """Invalid event structure (missing required fields) is skipped."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "account_position",
            # Missing required fields: broker, account_id, balances, updated_at
        }
        
        callback(event)
        
        # Verify nothing was stored
        account = account_store.get_account("binance", "test-account")
        assert account is None

    def test_missing_broker_or_account_id_skipped(self, account_store):
        """Event missing broker or account_id is skipped."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "account_position",
            "broker": "",  # Empty broker
            "account_id": "test-account",
            "balances": [],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        
        callback(event)
        
        # Verify nothing was stored
        account = account_store.get_account("", "test-account")
        assert account is None


class TestMakeAccountCallbackStoreUpdates:
    """Test that account callback updates Redis store correctly."""

    def test_account_position_updates_store(self, account_store):
        """account_position event calls apply_account_position."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [
                {"asset": "USDT", "available": "1000.0", "locked": "0.0"},
                {"asset": "BTC", "available": "0.5", "locked": "0.1"},
            ],
            "positions": [
                {"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"},
            ],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {"e": "outboundAccountPosition"},
        }
        
        callback(event)
        
        # Verify balances
        balances = account_store.get_balances("binance", "test-account")
        assert len(balances) == 2
        
        # Verify positions
        positions = account_store.get_positions("binance", "test-account")
        assert len(positions) == 1
        assert positions[0]["symbol"] == "BTCUSDT"

    def test_balance_update_updates_store(self, account_store):
        """balance_update event calls apply_balance_update."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "balance_update",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "100.5", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {"e": "balanceUpdate", "a": "USDT", "d": "100.5"},
        }
        
        callback(event)
        
        # Verify balance was updated
        balances = account_store.get_balances("binance", "test-account")
        assert len(balances) == 1
        assert balances[0]["asset"] == "USDT"
        assert balances[0]["available"] == "100.5"

    def test_balance_update_without_balances_skipped(self, account_store):
        """balance_update event with empty balances is skipped."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "balance_update",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [],  # Empty balances
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        
        callback(event)
        
        # Verify nothing was stored
        balances = account_store.get_balances("binance", "test-account")
        assert len(balances) == 0


class TestMakeAccountCallbackIdempotency:
    """Test idempotency using updated_at timestamp."""

    def test_newer_event_overwrites_older(self, account_store):
        """Newer event (by updated_at) overwrites older data."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        # First event (older timestamp)
        event1 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        callback(event1)
        
        # Second event (newer timestamp)
        event2 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "2000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-02T00:00:00Z",  # Newer
            "payload": {},
        }
        callback(event2)
        
        # Verify newer data is stored
        balances = account_store.get_balances("binance", "test-account")
        assert balances[0]["available"] == "2000.0"

    def test_older_event_skipped_if_newer_exists(self, account_store):
        """Older event is skipped if newer data already exists."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        # First event (newer timestamp)
        event1 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "2000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-02T00:00:00Z",
            "payload": {},
        }
        callback(event1)
        
        # Second event (older timestamp) - should be skipped
        event2 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",  # Older
            "payload": {},
        }
        callback(event2)
        
        # Verify older data was NOT stored (newer data remains)
        balances = account_store.get_balances("binance", "test-account")
        assert balances[0]["available"] == "2000.0"

    def test_same_timestamp_overwrites(self, account_store):
        """Events with same timestamp overwrite (last write wins)."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        # First event
        event1 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        callback(event1)
        
        # Second event (same timestamp)
        event2 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "2000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",  # Same timestamp
            "payload": {},
        }
        callback(event2)
        
        # Verify last write wins
        balances = account_store.get_balances("binance", "test-account")
        assert balances[0]["available"] == "2000.0"

    def test_no_existing_account_allows_first_event(self, account_store):
        """First event for an account is always processed (no existing data)."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        event = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "new-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        
        callback(event)
        
        # Verify account was created
        account = account_store.get_account("binance", "new-account")
        assert account is not None


class TestMakeAccountCallbackOnAccountUpdated:
    """Test on_account_updated callback invocation."""

    def test_on_account_updated_called_for_account_position(self, account_store):
        """on_account_updated callback is called for account_position events."""
        on_updated = MagicMock()
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=on_updated
        )
        
        event = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        
        callback(event)
        
        # Verify callback was called
        on_updated.assert_called_once_with("binance", "test-account")

    def test_on_account_updated_called_for_balance_update(self, account_store):
        """on_account_updated callback is called for balance_update events."""
        on_updated = MagicMock()
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=on_updated
        )
        
        event = {
            "event_type": "balance_update",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "100.5", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        
        callback(event)
        
        # Verify callback was called
        on_updated.assert_called_once_with("binance", "test-account")

    def test_on_account_updated_not_called_if_event_skipped(self, account_store):
        """on_account_updated callback is not called if event is skipped."""
        on_updated = MagicMock()
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=on_updated
        )
        
        # Invalid event (missing required fields)
        event = {
            "event_type": "account_position",
            # Missing required fields
        }
        
        callback(event)
        
        # Verify callback was NOT called
        on_updated.assert_not_called()

    def test_on_account_updated_not_called_if_older_event_skipped(self, account_store):
        """on_account_updated callback is not called if older event is skipped."""
        on_updated = MagicMock()
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=on_updated
        )
        
        # First event (newer)
        event1 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "2000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-02T00:00:00Z",
            "payload": {},
        }
        callback(event1)
        on_updated.reset_mock()  # Reset call count
        
        # Second event (older - should be skipped)
        event2 = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        callback(event2)
        
        # Verify callback was NOT called for older event
        on_updated.assert_not_called()

    def test_on_account_updated_error_does_not_crash(self, account_store):
        """on_account_updated callback errors are caught and logged."""
        def failing_callback(broker: str, account_id: str) -> None:
            raise ValueError("Test error")
        
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=failing_callback
        )
        
        event = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        
        # Should not raise exception
        callback(event)
        
        # Verify account was still stored despite callback error
        account = account_store.get_account("binance", "test-account")
        assert account is not None


class TestMakeAccountCallbackPayload:
    """Test that payload is stored correctly."""

    def test_payload_stored_for_account_position(self, account_store):
        """Payload is stored in account metadata."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        payload = {"e": "outboundAccountPosition", "E": 1234567890}
        event = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": payload,
        }
        
        callback(event)
        
        # Verify payload was stored
        account = account_store.get_account("binance", "test-account")
        assert account["payload"] == payload

    def test_payload_stored_for_balance_update(self, account_store):
        """Payload is stored in account metadata for balance_update."""
        callback = make_account_callback(
            MagicMock(), account_store, on_account_updated=None
        )
        
        payload = {"e": "balanceUpdate", "a": "USDT", "d": "100.5"}
        event = {
            "event_type": "balance_update",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "100.5", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": payload,
        }
        
        callback(event)
        
        # Verify payload was stored
        account = account_store.get_account("binance", "test-account")
        assert account["payload"] == payload

    def test_on_balance_change_called_for_balance_update(self, account_store):
        """When on_balance_change is provided, it is called with (broker, account_id, asset, delta, event_type, event_time, payload)."""
        calls = []
        def on_balance_change(broker, account_id, asset, delta, event_type, event_time, payload):
            calls.append((broker, account_id, asset, delta, event_type, event_time, payload))

        callback = make_account_callback(
            MagicMock(), account_store,
            on_account_updated=None,
            on_balance_change=on_balance_change,
        )
        payload = {"e": "balanceUpdate", "a": "USDT", "d": "50.25", "T": 1704067200000}
        event = {
            "event_type": "balance_update",
            "broker": "binance",
            "account_id": "test-account",
            "balances": [{"asset": "USDT", "available": "50.25", "locked": "0.0"}],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": payload,
        }
        callback(event)
        assert len(calls) == 1
        broker, account_id, asset, delta, event_type, event_time, pl = calls[0]
        assert broker == "binance"
        assert account_id == "test-account"
        assert asset == "USDT"
        assert delta == "50.25"
        assert event_type == "balanceUpdate"
        assert event_time == 1704067200000
        assert pl == payload
