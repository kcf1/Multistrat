"""
Unit tests for Binance account listener (task 12.2.2).

Tests:
- parse_account_event: mock WebSocket messages; verify parsing, event type routing,
  unified shape conversion for outboundAccountPosition and balanceUpdate.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from oms.brokers.binance.account_listener import (
    BinanceAccountListener,
    BinanceAccountListenerWsApi,
    create_account_listener,
    parse_account_event,
)


class TestParseAccountEvent:
    """Test parsing of Binance account events."""

    def test_parse_outbound_account_position(self):
        """outboundAccountPosition event is parsed to unified AccountPositionEvent."""
        payload = {
            "e": "outboundAccountPosition",
            "E": 1564034571105,
            "u": 1564034571073,
            "B": [
                {"a": "ETH", "f": "10000.000000", "l": "0.000000"},
                {"a": "BTC", "f": "1.500000", "l": "0.100000"},
            ],
        }
        result = parse_account_event(payload, account_id="test-account")
        assert result is not None
        assert result["event_type"] == "account_position"
        assert result["broker"] == "binance"
        assert result["account_id"] == "test-account"
        assert len(result["balances"]) == 2
        assert result["balances"][0]["asset"] == "ETH"
        assert result["balances"][0]["available"] == "10000.000000"
        assert result["balances"][0]["locked"] == "0.000000"
        assert result["balances"][1]["asset"] == "BTC"
        assert result["balances"][1]["available"] == "1.500000"
        assert result["balances"][1]["locked"] == "0.100000"
        assert result["positions"] == []
        assert "updated_at" in result
        assert result["payload"] == payload

    def test_parse_balance_update(self):
        """balanceUpdate event is parsed to unified BalanceUpdateEvent."""
        payload = {
            "e": "balanceUpdate",
            "E": 1564034571105,
            "a": "USDT",
            "d": "100.500000",
            "T": 1564034571073,
        }
        result = parse_account_event(payload, account_id="test-account")
        assert result is not None
        assert result["event_type"] == "balance_update"
        assert result["broker"] == "binance"
        assert result["account_id"] == "test-account"
        assert len(result["balances"]) == 1
        assert result["balances"][0]["asset"] == "USDT"
        assert result["balances"][0]["available"] == "100.500000"
        assert result["balances"][0]["locked"] == "0.0"
        assert result["positions"] == []
        assert "updated_at" in result
        assert result["payload"] == payload

    def test_parse_wrapped_payload(self):
        """Wrapped payload with 'event' key is unwrapped."""
        payload = {
            "event": {
                "e": "outboundAccountPosition",
                "E": 1564034571105,
                "u": 1564034571073,
                "B": [{"a": "ETH", "f": "1000.0", "l": "0.0"}],
            }
        }
        result = parse_account_event(payload, account_id="test")
        assert result is not None
        assert result["event_type"] == "account_position"

    def test_parse_ignores_execution_report(self):
        """executionReport events are ignored (handled by fills listener)."""
        payload = {"e": "executionReport", "x": "TRADE", "s": "BTCUSDT"}
        result = parse_account_event(payload)
        assert result is None

    def test_parse_invalid_event_type(self):
        """Invalid event types return None."""
        payload = {"e": "unknownEvent"}
        result = parse_account_event(payload)
        assert result is None

    def test_parse_invalid_outbound_account_position(self):
        """Invalid outboundAccountPosition returns None."""
        payload = {"e": "outboundAccountPosition", "B": "invalid"}
        result = parse_account_event(payload)
        assert result is None

    def test_parse_invalid_balance_update(self):
        """Invalid balanceUpdate returns None."""
        payload = {"e": "balanceUpdate"}  # Missing required fields
        result = parse_account_event(payload)
        assert result is None


class TestBinanceAccountListener:
    """Test BinanceAccountListener class."""

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    def test_listener_init(self, mock_client_class):
        """Listener initializes with client and account_id."""
        mock_client = MagicMock()
        listener = BinanceAccountListener(mock_client, account_id="test-account")
        assert listener._client == mock_client
        assert listener._account_id == "test-account"
        assert listener._listen_key is None
        assert not listener.stream_connected

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    def test_listener_init_with_listen_key(self, mock_client_class):
        """Listener can reuse existing listenKey."""
        mock_client = MagicMock()
        listener = BinanceAccountListener(
            mock_client, account_id="test", listen_key="existing-key"
        )
        assert listener._listen_key == "existing-key"

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    def test_start_background_requires_callback(self, mock_client_class):
        """start_background raises ValueError if no callback provided."""
        mock_client = MagicMock()
        listener = BinanceAccountListener(mock_client)
        with pytest.raises(ValueError, match="No callback provided"):
            listener.start_background()

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    @patch("oms.brokers.binance.account_listener.websocket")
    def test_start_background_creates_listen_key(self, mock_ws, mock_client_class):
        """start_background creates listenKey if not provided."""
        mock_client = MagicMock()
        mock_client.start_user_data_stream.return_value = "test-key"
        mock_client.base_url = "https://testnet.binance.vision"
        listener = BinanceAccountListener(mock_client)
        callback = MagicMock()

        # Mock WebSocketApp to avoid actual connection
        mock_ws_app = MagicMock()
        mock_ws.WebSocketApp.return_value = mock_ws_app

        listener.start_background(callback)
        mock_client.start_user_data_stream.assert_called_once()
        assert listener._listen_key == "test-key"

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    @patch("oms.brokers.binance.account_listener.websocket")
    def test_start_background_reuses_listen_key(self, mock_ws, mock_client_class):
        """start_background reuses listenKey if provided."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = BinanceAccountListener(mock_client, listen_key="existing-key")
        callback = MagicMock()

        mock_ws_app = MagicMock()
        mock_ws.WebSocketApp.return_value = mock_ws_app

        listener.start_background(callback)
        # Should not create new listenKey
        mock_client.start_user_data_stream.assert_not_called()
        assert listener._listen_key == "existing-key"

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    def test_stop_closes_websocket(self, mock_client_class):
        """stop() closes WebSocket."""
        mock_client = MagicMock()
        listener = BinanceAccountListener(mock_client)
        mock_ws = MagicMock()
        listener._ws = mock_ws
        listener.stop()
        mock_ws.close.assert_called_once()
        assert listener._ws is None


class TestBinanceAccountListenerWsApi:
    """Test BinanceAccountListenerWsApi class (WebSocket API method)."""

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    def test_listener_init(self, mock_client_class):
        """Listener initializes with client and account_id."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = BinanceAccountListenerWsApi(mock_client, account_id="test-account")
        assert listener._client == mock_client
        assert listener._account_id == "test-account"
        assert listener._subscription_id is None
        assert not listener.stream_connected

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    def test_start_background_requires_callback(self, mock_client_class):
        """start_background raises ValueError if no callback provided."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = BinanceAccountListenerWsApi(mock_client)
        with pytest.raises(ValueError, match="No callback provided"):
            listener.start_background()

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    @patch("oms.brokers.binance.account_listener.websocket")
    def test_start_background_sends_subscribe(self, mock_ws, mock_client_class):
        """start_background sends userDataStream.subscribe.signature."""
        import time
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        mock_client.api_key = "test-key"
        mock_client._ensure_time_sync = MagicMock()
        mock_client._time_offset_ms = 0
        mock_client._sign_request = MagicMock(return_value="test-sig")
        listener = BinanceAccountListenerWsApi(mock_client)
        callback = MagicMock()

        mock_ws_app = MagicMock()
        mock_ws.WebSocketApp.return_value = mock_ws_app

        listener.start_background(callback)
        # Wait briefly for background thread to start and create WebSocketApp
        time.sleep(0.1)
        # WebSocketApp should be created
        mock_ws.WebSocketApp.assert_called_once()

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    def test_stop_closes_websocket(self, mock_client_class):
        """stop() closes WebSocket and unsubscribes."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = BinanceAccountListenerWsApi(mock_client)
        mock_ws = MagicMock()
        listener._ws = mock_ws
        listener._subscription_id = 123
        listener.stop()
        # Should send unsubscribe and close
        assert mock_ws.send.called
        mock_ws.close.assert_called_once()
        assert listener._ws is None
        assert listener._subscription_id is None


class TestCreateAccountListener:
    """Test create_account_listener factory function."""

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    @patch.dict("os.environ", {}, clear=True)
    def test_create_defaults_to_ws_api(self, mock_client_class):
        """create_account_listener defaults to WebSocket API."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = create_account_listener(mock_client)
        assert isinstance(listener, BinanceAccountListenerWsApi)

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    @patch.dict("os.environ", {"BINANCE_ACCOUNT_STREAM": "listenkey"})
    def test_create_respects_env_listenkey(self, mock_client_class):
        """create_account_listener respects BINANCE_ACCOUNT_STREAM=listenkey."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = create_account_listener(mock_client)
        assert isinstance(listener, BinanceAccountListener)

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    @patch.dict("os.environ", {}, clear=True)
    def test_create_explicit_ws_api(self, mock_client_class):
        """create_account_listener with use_ws_api=True returns WS API listener."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = create_account_listener(mock_client, use_ws_api=True)
        assert isinstance(listener, BinanceAccountListenerWsApi)

    @patch("oms.brokers.binance.account_listener.BinanceAPIClient")
    @patch.dict("os.environ", {}, clear=True)
    def test_create_explicit_listenkey(self, mock_client_class):
        """create_account_listener with use_ws_api=False returns listenKey listener."""
        mock_client = MagicMock()
        mock_client.base_url = "https://testnet.binance.vision"
        listener = create_account_listener(mock_client, use_ws_api=False)
        assert isinstance(listener, BinanceAccountListener)
