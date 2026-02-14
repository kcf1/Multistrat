"""
Unit tests for Binance broker adapter.

Tests:
- place_order: mock Binance client; verify adapter calls client correctly and
  returns unified response or reject.
- start_fill_listener: mock create_fills_listener; verify adapter starts listener
  with callback and callback receives unified fill/reject format.
"""

from unittest.mock import MagicMock, patch

import pytest

from oms.brokers.binance.adapter import (
    BinanceBrokerAdapter,
    binance_order_response_to_unified,
)
from oms.brokers.binance.api_client import BinanceAPIError


# --- binance_order_response_to_unified ---


class TestBinanceOrderResponseToUnified:
    """Test conversion of Binance order response to unified format."""

    def test_market_order_response(self):
        """Binance market order response is converted to unified keys."""
        resp = {
            "orderId": 12345,
            "symbol": "BTCUSDT",
            "status": "FILLED",
            "clientOrderId": "internal-uuid-1",
            "side": "BUY",
            "type": "MARKET",
            "origQty": "0.001",
            "price": "0",
            "executedQty": "0.001",
            "transactTime": 1499405658658,
        }
        out = binance_order_response_to_unified(resp)
        assert out["broker_order_id"] == "12345"
        assert out["status"] == "FILLED"
        assert out["symbol"] == "BTCUSDT"
        assert out["side"] == "BUY"
        assert out["order_type"] == "MARKET"
        assert out["quantity"] == 0.001
        assert out["executed_qty"] == 0.001
        assert out["client_order_id"] == "internal-uuid-1"
        assert out["binance_transact_time"] == 1499405658658

    def test_limit_order_with_optional_fields(self):
        """Limit order with timeInForce and cumulativeQuoteQty."""
        resp = {
            "orderId": 999,
            "symbol": "ETHUSDT",
            "status": "NEW",
            "clientOrderId": "limit-xyz",
            "side": "SELL",
            "type": "LIMIT",
            "origQty": "1.5",
            "price": "3000",
            "executedQty": "0",
            "timeInForce": "GTC",
            "transactTime": 1600000000000,
            "cumulativeQuoteQty": "0",
        }
        out = binance_order_response_to_unified(resp)
        assert out["broker_order_id"] == "999"
        assert out["order_type"] == "LIMIT"
        assert out["quantity"] == 1.5
        assert out["price"] == 3000.0
        assert out["time_in_force"] == "GTC"
        assert out["binance_transact_time"] == 1600000000000
        assert out.get("binance_cumulative_quote_qty") == 0.0


# --- BinanceBrokerAdapter.place_order ---


class TestBinanceBrokerAdapterPlaceOrder:
    """Test adapter place_order with mocked Binance client."""

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def adapter(self, mock_client):
        return BinanceBrokerAdapter(client=mock_client)

    def test_place_order_calls_client_with_correct_args(self, adapter, mock_client):
        """Adapter passes symbol, side, order_type, quantity, price, time_in_force, client_order_id."""
        mock_client.place_order.return_value = {
            "orderId": 42,
            "symbol": "BTCUSDT",
            "status": "NEW",
            "clientOrderId": "oms-order-1",
            "side": "BUY",
            "type": "LIMIT",
            "origQty": "0.01",
            "price": "50000",
            "executedQty": "0",
            "transactTime": 1234567890,
        }
        order = {
            "order_id": "oms-order-1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.01,
            "order_type": "LIMIT",
            "price": 50000.0,
            "time_in_force": "GTC",
            "book": "ma_cross",
            "comment": "test",
        }
        result = adapter.place_order(order)
        mock_client.place_order.assert_called_once()
        call_kw = mock_client.place_order.call_args[1]
        assert call_kw["symbol"] == "BTCUSDT"
        assert call_kw["side"] == "BUY"
        assert call_kw["order_type"] == "LIMIT"
        assert call_kw["quantity"] == 0.01
        assert call_kw["price"] == 50000.0
        assert call_kw["time_in_force"] == "GTC"
        assert call_kw["client_order_id"] == "oms-order-1"
        assert result["broker_order_id"] == "42"
        assert result["status"] == "NEW"
        assert result["client_order_id"] == "oms-order-1"
        assert "rejected" not in result

    def test_place_order_market_no_price(self, adapter, mock_client):
        """Market order without price calls client without price/time_in_force."""
        mock_client.place_order.return_value = {
            "orderId": 1,
            "symbol": "BTCUSDT",
            "status": "FILLED",
            "clientOrderId": "m1",
            "side": "SELL",
            "type": "MARKET",
            "origQty": "0.001",
            "executedQty": "0.001",
            "transactTime": 1,
        }
        result = adapter.place_order({
            "order_id": "m1",
            "symbol": "BTCUSDT",
            "side": "SELL",
            "quantity": 0.001,
            "order_type": "MARKET",
        })
        call_kw = mock_client.place_order.call_args[1]
        assert call_kw.get("price") is None
        assert result["broker_order_id"] == "1"
        assert result["status"] == "FILLED"

    def test_place_order_api_error_returns_reject(self, adapter, mock_client):
        """BinanceAPIError results in reject dict, not raise."""
        mock_client.place_order.side_effect = BinanceAPIError("Invalid quantity.")
        order = {
            "order_id": "fail-1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.01,
            "order_type": "MARKET",
        }
        result = adapter.place_order(order)
        assert result["rejected"] is True
        assert result["order_id"] == "fail-1"
        assert "Invalid quantity" in result["reject_reason"]

    def test_place_order_invalid_order_returns_reject(self, adapter, mock_client):
        """Missing symbol/side or quantity <= 0 returns reject without calling client."""
        result = adapter.place_order({
            "order_id": "x",
            "symbol": "",
            "side": "BUY",
            "quantity": 0.01,
            "order_type": "MARKET",
        })
        assert result["rejected"] is True
        assert "reject_reason" in result
        mock_client.place_order.assert_not_called()

        result2 = adapter.place_order({
            "order_id": "y",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0,
            "order_type": "MARKET",
        })
        assert result2["rejected"] is True
        mock_client.place_order.assert_not_called()


# --- BinanceBrokerAdapter.cancel_order (12.1.9e) ---


class TestBinanceBrokerAdapterCancelOrder:
    """Test adapter cancel_order with mocked Binance client."""

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def adapter(self, mock_client):
        return BinanceBrokerAdapter(client=mock_client)

    def test_cancel_order_calls_client_with_order_id_and_symbol(self, adapter, mock_client):
        """Adapter cancel_order(broker_order_id, symbol) calls client.cancel_order(symbol=..., order_id=...)."""
        mock_client.cancel_order.return_value = {
            "orderId": 4293156,
            "symbol": "BTCUSDT",
            "status": "CANCELED",
            "clientOrderId": "internal-1",
            "side": "BUY",
            "type": "LIMIT",
            "origQty": "0.001",
            "price": "50000",
            "executedQty": "0",
            "transactTime": 1499405658800,
        }
        result = adapter.cancel_order("4293156", "BTCUSDT")
        mock_client.cancel_order.assert_called_once_with(symbol="BTCUSDT", order_id=4293156)
        assert result.get("rejected") is not True
        assert result["broker_order_id"] == "4293156"
        assert result["status"] == "CANCELED"
        assert result["symbol"] == "BTCUSDT"

    def test_cancel_order_client_order_id_string_calls_client_with_orig_client_order_id(self, adapter, mock_client):
        """When broker_order_id is not numeric, adapter uses client_order_id parameter."""
        mock_client.cancel_order.return_value = {
            "orderId": 999,
            "symbol": "ETHUSDT",
            "status": "CANCELED",
            "clientOrderId": "my-custom-id",
            "side": "SELL",
            "type": "LIMIT",
            "origQty": "0.01",
            "price": "3000",
            "executedQty": "0",
            "transactTime": 1499405658900,
        }
        result = adapter.cancel_order("my-custom-id", "ETHUSDT")
        mock_client.cancel_order.assert_called_once_with(symbol="ETHUSDT", client_order_id="my-custom-id")
        assert result["status"] == "CANCELED"
        assert result["broker_order_id"] == "999"

    def test_cancel_order_api_error_returns_reject(self, adapter, mock_client):
        """BinanceAPIError from cancel_order results in reject dict."""
        mock_client.cancel_order.side_effect = BinanceAPIError("Order does not exist.")
        result = adapter.cancel_order("12345", "BTCUSDT")
        assert result["rejected"] is True
        assert "reject_reason" in result
        assert "Order does not exist" in result["reject_reason"]

    def test_cancel_order_missing_args_returns_reject(self, adapter, mock_client):
        """Missing symbol or broker_order_id returns reject without calling client."""
        result = adapter.cancel_order("", "BTCUSDT")
        assert result["rejected"] is True
        assert "reject_reason" in result
        mock_client.cancel_order.assert_not_called()

        result2 = adapter.cancel_order("123", "")
        assert result2["rejected"] is True
        mock_client.cancel_order.assert_not_called()


# --- BinanceBrokerAdapter.start_fill_listener ---


class TestBinanceBrokerAdapterStartFillListener:
    """Test adapter start_fill_listener with mocked create_fills_listener."""

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def mock_listener(self):
        listener = MagicMock()
        return listener

    def test_start_fill_listener_creates_listener_and_starts_background(self, mock_client, mock_listener):
        """Adapter calls create_fills_listener and start_background(callback)."""
        with patch("oms.brokers.binance.adapter.create_fills_listener", return_value=mock_listener) as mock_create:
            adapter = BinanceBrokerAdapter(client=mock_client, use_ws_api=True)
            received = []

            def callback(event):
                received.append(event)

            adapter.start_fill_listener(callback)
            mock_create.assert_called_once_with(mock_client, use_ws_api=True)
            mock_listener.start_background.assert_called_once()
            # Callback passed to start_background is the one we gave
            call_callback = mock_listener.start_background.call_args[0][0]
            assert call_callback is callback
            # Invoke it with a unified fill event
            unified_fill = {
                "event_type": "fill",
                "order_id": "internal-1",
                "broker_order_id": "123",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.001,
                "price": 50000.0,
                "fee": 0.0,
                "executed_at": "2025-01-01T12:00:00Z",
                "fill_id": "trade-1",
            }
            call_callback(unified_fill)
            assert len(received) == 1
            assert received[0]["event_type"] == "fill"
            assert received[0]["broker_order_id"] == "123"
            assert received[0]["symbol"] == "BTCUSDT"

    def test_start_fill_listener_reject_event_format(self, mock_client, mock_listener):
        """Callback receives reject event in unified format."""
        with patch("oms.brokers.binance.adapter.create_fills_listener", return_value=mock_listener):
            adapter = BinanceBrokerAdapter(client=mock_client)
            received = []

            def callback(event):
                received.append(event)

            adapter.start_fill_listener(callback)
            call_callback = mock_listener.start_background.call_args[0][0]
            unified_reject = {
                "event_type": "reject",
                "order_id": "internal-2",
                "broker_order_id": "0",
                "symbol": "ETHUSDT",
                "side": "SELL",
                "quantity": 1.0,
                "price": 0.0,
                "reject_reason": "INSUFFICIENT_BALANCE",
            }
            call_callback(unified_reject)
            assert len(received) == 1
            assert received[0]["event_type"] == "reject"
            assert received[0]["reject_reason"] == "INSUFFICIENT_BALANCE"

    def test_stop_fill_listener_calls_listener_stop_and_clears(self, mock_client, mock_listener):
        """After start_fill_listener, stop_fill_listener() calls listener.stop() and sets _listener to None."""
        with patch("oms.brokers.binance.adapter.create_fills_listener", return_value=mock_listener):
            adapter = BinanceBrokerAdapter(client=mock_client)
            adapter.start_fill_listener(lambda e: None)
            assert adapter._listener is mock_listener
            adapter.stop_fill_listener()
            mock_listener.stop.assert_called_once()
            assert adapter._listener is None

    def test_stop_fill_listener_idempotent(self, mock_client, mock_listener):
        """stop_fill_listener() when no listener, or called twice, does not raise."""
        with patch("oms.brokers.binance.adapter.create_fills_listener", return_value=mock_listener):
            adapter = BinanceBrokerAdapter(client=mock_client)
            adapter.stop_fill_listener()  # no listener started
            assert adapter._listener is None
            adapter.start_fill_listener(lambda e: None)
            adapter.stop_fill_listener()
            adapter.stop_fill_listener()  # second call is no-op
            mock_listener.stop.assert_called_once()
            assert adapter._listener is None
