"""
Unit tests for broker adapter base protocol.

Tests:
- Protocol definition: verify BrokerAdapter is a Protocol with runtime_checkable
- Method signatures: verify all required methods are defined (place_order, start_fill_listener,
  cancel_order, start_account_listener, get_account_snapshot, stop_account_listener)
- Documentation: verify docstrings are present for all methods
- AccountEvent type annotation: verify AccountEvent is documented
"""

from typing import Any, Callable, Dict
from unittest.mock import MagicMock

import pytest

from oms.brokers.base import AccountEvent, BrokerAdapter


class MockBrokerAdapter:
    """Mock broker adapter that implements BrokerAdapter protocol."""

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """Place an order."""
        return {"broker_order_id": "123", "status": "NEW"}

    def start_fill_listener(
        self,
        callback: Callable[[Dict[str, Any]], None],
        *,
        store: Any = None,
    ) -> None:
        """Start fill listener."""
        pass

    def cancel_order(self, broker_order_id: str, symbol: str) -> Dict[str, Any]:
        """Cancel an order."""
        return {"status": "CANCELED", "broker_order_id": broker_order_id}

    def start_account_listener(
        self,
        callback: Callable[[AccountEvent], None],
    ) -> None:
        """Start account listener."""
        pass

    def get_account_snapshot(self, account_id: str) -> AccountEvent:
        """Get account snapshot."""
        return {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": account_id,
            "balances": [],
            "positions": [],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }

    def stop_account_listener(self) -> None:
        """Stop account listener."""
        pass


class TestBrokerAdapterProtocol:
    """Test BrokerAdapter protocol definition."""

    def test_protocol_is_runtime_checkable(self):
        """BrokerAdapter protocol supports runtime checking."""
        adapter = MockBrokerAdapter()
        assert isinstance(adapter, BrokerAdapter), "Mock adapter should conform to BrokerAdapter protocol"

    def test_protocol_requires_place_order(self):
        """Protocol requires place_order method."""
        adapter = MockBrokerAdapter()
        assert hasattr(adapter, "place_order")
        assert callable(adapter.place_order)

    def test_protocol_requires_start_fill_listener(self):
        """Protocol requires start_fill_listener method."""
        adapter = MockBrokerAdapter()
        assert hasattr(adapter, "start_fill_listener")
        assert callable(adapter.start_fill_listener)

    def test_protocol_requires_cancel_order(self):
        """Protocol requires cancel_order method."""
        adapter = MockBrokerAdapter()
        assert hasattr(adapter, "cancel_order")
        assert callable(adapter.cancel_order)

    def test_protocol_requires_start_account_listener(self):
        """Protocol requires start_account_listener method."""
        adapter = MockBrokerAdapter()
        assert hasattr(adapter, "start_account_listener")
        assert callable(adapter.start_account_listener)

    def test_protocol_requires_get_account_snapshot(self):
        """Protocol requires get_account_snapshot method."""
        adapter = MockBrokerAdapter()
        assert hasattr(adapter, "get_account_snapshot")
        assert callable(adapter.get_account_snapshot)

    def test_protocol_requires_stop_account_listener(self):
        """Protocol requires stop_account_listener method."""
        adapter = MockBrokerAdapter()
        assert hasattr(adapter, "stop_account_listener")
        assert callable(adapter.stop_account_listener)

    def test_place_order_signature(self):
        """place_order accepts order dict and returns dict."""
        adapter = MockBrokerAdapter()
        order = {"symbol": "BTCUSDT", "side": "BUY", "quantity": 0.1}
        result = adapter.place_order(order)
        assert isinstance(result, dict)

    def test_start_fill_listener_signature(self):
        """start_fill_listener accepts callback and optional store."""
        adapter = MockBrokerAdapter()
        callback = MagicMock()
        adapter.start_fill_listener(callback, store=None)
        # Should not raise

    def test_cancel_order_signature(self):
        """cancel_order accepts broker_order_id and symbol, returns dict."""
        adapter = MockBrokerAdapter()
        result = adapter.cancel_order("12345", "BTCUSDT")
        assert isinstance(result, dict)

    def test_start_account_listener_signature(self):
        """start_account_listener accepts callback."""
        adapter = MockBrokerAdapter()
        callback = MagicMock()
        adapter.start_account_listener(callback)
        # Should not raise

    def test_get_account_snapshot_signature(self):
        """get_account_snapshot accepts account_id and returns AccountEvent."""
        adapter = MockBrokerAdapter()
        result = adapter.get_account_snapshot("account-1")
        assert isinstance(result, dict)
        assert "event_type" in result
        assert "broker" in result
        assert "account_id" in result
        assert "balances" in result
        assert "positions" in result
        assert "updated_at" in result
        assert "payload" in result

    def test_stop_account_listener_signature(self):
        """stop_account_listener takes no args and returns None."""
        adapter = MockBrokerAdapter()
        result = adapter.stop_account_listener()
        assert result is None


class TestBrokerAdapterDocumentation:
    """Test BrokerAdapter protocol documentation."""

    def test_place_order_has_docstring(self):
        """place_order method has docstring."""
        assert BrokerAdapter.place_order.__doc__ is not None
        assert len(BrokerAdapter.place_order.__doc__.strip()) > 0

    def test_start_fill_listener_has_docstring(self):
        """start_fill_listener method has docstring."""
        assert BrokerAdapter.start_fill_listener.__doc__ is not None
        assert len(BrokerAdapter.start_fill_listener.__doc__.strip()) > 0

    def test_cancel_order_has_docstring(self):
        """cancel_order method has docstring."""
        assert BrokerAdapter.cancel_order.__doc__ is not None
        assert len(BrokerAdapter.cancel_order.__doc__.strip()) > 0

    def test_start_account_listener_has_docstring(self):
        """start_account_listener method has docstring."""
        assert BrokerAdapter.start_account_listener.__doc__ is not None
        assert len(BrokerAdapter.start_account_listener.__doc__.strip()) > 0
        # Verify it documents AccountEvent structure
        doc = BrokerAdapter.start_account_listener.__doc__
        assert "event_type" in doc or "AccountEvent" in doc
        assert "balances" in doc or "balance" in doc
        assert "positions" in doc or "position" in doc

    def test_get_account_snapshot_has_docstring(self):
        """get_account_snapshot method has docstring."""
        assert BrokerAdapter.get_account_snapshot.__doc__ is not None
        assert len(BrokerAdapter.get_account_snapshot.__doc__.strip()) > 0
        # Verify it documents return structure
        doc = BrokerAdapter.get_account_snapshot.__doc__
        assert "AccountEvent" in doc or "event_type" in doc
        assert "balances" in doc or "balance" in doc
        assert "positions" in doc or "position" in doc

    def test_stop_account_listener_has_docstring(self):
        """stop_account_listener method has docstring."""
        assert BrokerAdapter.stop_account_listener.__doc__ is not None
        assert len(BrokerAdapter.stop_account_listener.__doc__.strip()) > 0


class TestAccountEventStructure:
    """Test AccountEvent type annotation and structure."""

    def test_account_event_has_required_fields(self):
        """AccountEvent dict has all required fields."""
        event: AccountEvent = {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": "account-1",
            "balances": [
                {"asset": "USDT", "available": "1000.0", "locked": "0.0"}
            ],
            "positions": [
                {"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"}
            ],
            "updated_at": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        assert event["event_type"] in ("balance_update", "account_position")
        assert isinstance(event["balances"], list)
        assert isinstance(event["positions"], list)

    def test_account_event_balance_structure(self):
        """AccountEvent balance dict has required fields."""
        balance = {"asset": "USDT", "available": "1000.0", "locked": "0.0"}
        assert "asset" in balance
        assert "available" in balance
        assert "locked" in balance

    def test_account_event_position_structure(self):
        """AccountEvent position dict has required fields."""
        position = {"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"}
        assert "symbol" in position
        assert "quantity" in position
        # side and entry_price are optional
        assert "side" in position or "entry_price" in position

    def test_account_event_from_get_account_snapshot(self):
        """get_account_snapshot returns AccountEvent structure."""
        adapter = MockBrokerAdapter()
        snapshot = adapter.get_account_snapshot("account-1")
        assert isinstance(snapshot, dict)
        assert snapshot["event_type"] == "account_position"
        assert snapshot["broker"] == "binance"
        assert snapshot["account_id"] == "account-1"
        assert isinstance(snapshot["balances"], list)
        assert isinstance(snapshot["positions"], list)
        assert "updated_at" in snapshot
        assert "payload" in snapshot
