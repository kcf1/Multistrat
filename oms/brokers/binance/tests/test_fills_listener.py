"""
Unit tests for Binance fills listener.

Tests:
- parse_execution_report: fill/reject parsing from mock executionReport payloads
- BinanceFillsListener: mock WebSocket messages; verify callback receives unified events
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from oms.brokers.binance.api_client import BinanceAPIClient, BinanceAPIError
from oms.brokers.binance.fills_listener import (
    STREAM_URL_MAIN,
    STREAM_URL_TESTNET,
    BinanceFillsListener,
    parse_execution_report,
    _stream_url_from_base_url,
)


# --- executionReport payloads (Binance-style) ---

EXECUTION_REPORT_TRADE = {
    "e": "executionReport",
    "E": 1499405658658,
    "s": "ETHBTC",
    "c": "my-internal-order-uuid-123",
    "S": "BUY",
    "o": "LIMIT",
    "f": "GTC",
    "q": "1.00000000",
    "p": "0.10264410",
    "P": "0.00000000",
    "F": "0.00000000",
    "g": -1,
    "C": "",
    "x": "TRADE",
    "X": "PARTIALLY_FILLED",
    "r": "NONE",
    "i": 4293153,
    "l": "0.50000000",
    "z": "0.50000000",
    "L": "0.10264410",
    "n": "0.0001",
    "N": "BNB",
    "T": 1499405658657,
    "t": 12345,
    "v": 0,
    "I": 8641984,
    "w": True,
    "m": False,
    "M": False,
    "O": 1499405658657,
    "Z": "0.05132205",
    "Y": "0.05132205",
    "Q": "0.00000000",
    "W": 1499405658657,
    "V": "NONE",
}

EXECUTION_REPORT_REJECTED = {
    "e": "executionReport",
    "E": 1499405658660,
    "s": "ETHBTC",
    "c": "rejected-order-uuid",
    "S": "SELL",
    "o": "MARKET",
    "f": "GTC",
    "q": "100.00000000",
    "p": "0.00000000",
    "P": "0.00000000",
    "F": "0.00000000",
    "g": -1,
    "C": "",
    "x": "REJECTED",
    "X": "REJECTED",
    "r": "INSUFFICIENT_BALANCES",
    "i": 0,
    "l": "0.00000000",
    "z": "0.00000000",
    "L": "0.00000000",
    "n": "0",
    "N": None,
    "T": 1499405658659,
    "t": -1,
    "v": 0,
    "I": 0,
    "w": False,
    "m": False,
    "M": False,
    "O": 1499405658658,
    "Z": "0.00000000",
    "Y": "0.00000000",
    "Q": "0.00000000",
    "W": 1499405658658,
    "V": "NONE",
}

EXECUTION_REPORT_TRADE_FILLED = {
    "e": "executionReport",
    "E": 1499405658659,
    "s": "ETHBTC",
    "c": "my-internal-order-uuid-123",
    "S": "BUY",
    "o": "LIMIT",
    "f": "GTC",
    "q": "1.00000000",
    "p": "0.10264410",
    "x": "TRADE",
    "X": "FILLED",
    "r": "NONE",
    "i": 4293153,
    "l": "0.50000000",
    "z": "1.00000000",
    "L": "0.10264410",
    "n": "0.0001",
    "N": "BNB",
    "T": 1499405658658,
    "t": 12346,
}

EXECUTION_REPORT_NEW = {
    "e": "executionReport",
    "E": 1499405658650,
    "s": "BTCUSDT",
    "c": "client-id-456",
    "S": "BUY",
    "o": "LIMIT",
    "x": "NEW",
    "X": "NEW",
    "i": 999,
    "l": "0",
    "L": "0",
    "q": "0.001",
    "p": "50000",
    "T": 1499405658649,
    "t": -1,
    "r": "NONE",
}

EXECUTION_REPORT_CANCELED = {
    "e": "executionReport",
    "E": 1499405658700,
    "s": "ETHBTC",
    "c": "internal-order-789",
    "S": "SELL",
    "o": "LIMIT",
    "x": "CANCELED",
    "X": "CANCELED",
    "r": "USER_CANCEL",
    "i": 4293154,
    "l": "0.00000000",
    "z": "0.00000000",
    "L": "0.00000000",
    "q": "1.00000000",
    "p": "0.10264410",
    "T": 1499405658699,
    "t": -1,
}

EXECUTION_REPORT_EXPIRED = {
    "e": "executionReport",
    "E": 1499405658750,
    "s": "BTCUSDT",
    "c": "internal-order-exp-1",
    "S": "BUY",
    "o": "LIMIT",
    "f": "GTX",
    "x": "EXPIRED",
    "X": "EXPIRED",
    "r": "GTX",
    "i": 4293155,
    "l": "0.00000000",
    "z": "0.00000000",
    "q": "0.001",
    "p": "45000",
    "T": 1499405658749,
    "t": -1,
}


class TestParseExecutionReport:
    """Test fill/reject parsing from executionReport payloads."""

    def test_trade_returns_fill(self):
        """TRADE execution type produces event_type=fill with correct fields."""
        out = parse_execution_report(EXECUTION_REPORT_TRADE)
        assert out is not None
        assert out["event_type"] == "fill"
        assert out["order_id"] == "my-internal-order-uuid-123"
        assert out["broker_order_id"] == "4293153"
        assert out["symbol"] == "ETHBTC"
        assert out["side"] == "BUY"
        assert out["quantity"] == 0.5
        assert out["price"] == 0.10264410
        assert out["fee"] == 0.0001
        assert out["fee_asset"] == "BNB"
        assert out["fill_id"] == "12345"
        assert "executed_at" in out
        assert out["executed_at"].endswith("Z") or "+" in out["executed_at"]
        assert out.get("order_status") == "PARTIALLY_FILLED"
        assert out.get("executed_qty_cumulative") == 0.5

    def test_rejected_returns_reject(self):
        """REJECTED execution type produces event_type=reject with reject_reason."""
        out = parse_execution_report(EXECUTION_REPORT_REJECTED)
        assert out is not None
        assert out["event_type"] == "reject"
        assert out["order_id"] == "rejected-order-uuid"
        assert out["broker_order_id"] == "0"
        assert out["symbol"] == "ETHBTC"
        assert out["side"] == "SELL"
        assert out["reject_reason"] == "INSUFFICIENT_BALANCES"
        assert out["quantity"] == 100.0
        assert out["price"] == 0.0
        assert out["fee"] == 0.0
        assert out["fee_asset"] is None

    def test_trade_filled_returns_order_status_and_cumulative(self):
        """TRADE with X=FILLED returns order_status FILLED and executed_qty_cumulative."""
        out = parse_execution_report(EXECUTION_REPORT_TRADE_FILLED)
        assert out is not None
        assert out["event_type"] == "fill"
        assert out.get("order_status") == "FILLED"
        assert out.get("executed_qty_cumulative") == 1.0
        assert out["quantity"] == 0.5
        assert out["broker_order_id"] == "4293153"

    def test_new_returns_none(self):
        """NEW execution type is not a fill or reject -> None."""
        assert parse_execution_report(EXECUTION_REPORT_NEW) is None

    def test_wrapped_event_payload(self):
        """Payload with top-level 'event' key is unwrapped and parsed."""
        wrapped = {"subscriptionId": 0, "event": EXECUTION_REPORT_TRADE}
        out = parse_execution_report(wrapped)
        assert out is not None
        assert out["event_type"] == "fill"
        assert out["order_id"] == "my-internal-order-uuid-123"

    def test_non_execution_report_returns_none(self):
        """Non-executionReport event returns None."""
        assert parse_execution_report({"e": "outboundAccountPosition"}) is None
        assert parse_execution_report({}) is None
        assert parse_execution_report({"e": "balanceUpdate"}) is None

    def test_trade_with_zero_last_qty_returns_none(self):
        """TRADE with last executed quantity 0 does not emit fill."""
        payload = {**EXECUTION_REPORT_TRADE, "l": "0.00000000", "z": "0"}
        assert parse_execution_report(payload) is None

    def test_canceled_returns_cancelled_event(self):
        """CANCELED exec_type / order_status produces event_type=cancelled (12.1.9c)."""
        out = parse_execution_report(EXECUTION_REPORT_CANCELED)
        assert out is not None
        assert out["event_type"] == "cancelled"
        assert out["order_id"] == "internal-order-789"
        assert out["broker_order_id"] == "4293154"
        assert out["symbol"] == "ETHBTC"
        assert out["side"] == "SELL"
        assert out["reject_reason"] == "USER_CANCEL"
        assert out["quantity"] == 1.0
        assert out["price"] == 0.10264410

    def test_expired_returns_expired_event(self):
        """EXPIRED exec_type / order_status produces event_type=expired (12.1.9c)."""
        out = parse_execution_report(EXECUTION_REPORT_EXPIRED)
        assert out is not None
        assert out["event_type"] == "expired"
        assert out["order_id"] == "internal-order-exp-1"
        assert out["broker_order_id"] == "4293155"
        assert out["symbol"] == "BTCUSDT"
        assert out["side"] == "BUY"
        assert out["reject_reason"] == "GTX"
        assert out["quantity"] == 0.001
        assert out["price"] == 45000.0


class TestStreamUrlFromBaseUrl:
    """Test derivation of WebSocket URL from REST base URL."""

    def test_testnet(self):
        assert _stream_url_from_base_url("https://testnet.binance.vision") == STREAM_URL_TESTNET

    def test_main(self):
        assert _stream_url_from_base_url("https://api.binance.com") == STREAM_URL_MAIN


class TestBinanceFillsListener:
    """Test listener with mocked WebSocket and API client."""

    @pytest.fixture
    def client(self):
        return BinanceAPIClient(
            api_key="key",
            api_secret="secret",
            base_url="https://testnet.binance.vision",
            testnet=True,
        )

    def test_start_requires_callback(self, client):
        """start() without callback in constructor or arg raises."""
        listener = BinanceFillsListener(client)
        with pytest.raises(ValueError, match="No callback"):
            listener.start()

    def test_start_background_requires_callback(self, client):
        """start_background() without callback raises."""
        listener = BinanceFillsListener(client)
        with pytest.raises(ValueError, match="No callback"):
            listener.start_background()

    def test_listener_callback_receives_unified_fill(self, client):
        """When WebSocket receives executionReport TRADE, callback gets unified fill."""
        received = []

        def capture(ev):
            received.append(ev)

        with patch.object(client, "start_user_data_stream", return_value="fake-listen-key"):
            with patch("oms.brokers.binance.fills_listener.websocket") as mock_ws_module:
                ws_app_instance = MagicMock()

                def run_forever():
                    # Call the on_message callback that was passed to WebSocketApp(...)
                    call_kwargs = mock_ws_module.WebSocketApp.call_args[1]
                    on_message = call_kwargs.get("on_message")
                    if on_message:
                        on_message(ws_app_instance, json.dumps(EXECUTION_REPORT_TRADE))

                ws_app_instance.run_forever.side_effect = run_forever
                mock_ws_module.WebSocketApp.return_value = ws_app_instance

                listener = BinanceFillsListener(client, on_fill_or_reject=capture)
                listener.start()

        assert len(received) == 1
        assert received[0]["event_type"] == "fill"
        assert received[0]["order_id"] == "my-internal-order-uuid-123"
        assert received[0]["broker_order_id"] == "4293153"
        assert received[0]["quantity"] == 0.5
        assert received[0]["fill_id"] == "12345"

    def test_listener_callback_receives_unified_reject(self, client):
        """When WebSocket receives executionReport REJECTED, callback gets unified reject."""
        received = []

        def capture(ev):
            received.append(ev)

        with patch.object(client, "start_user_data_stream", return_value="fake-listen-key"):
            with patch("oms.brokers.binance.fills_listener.websocket") as mock_ws_module:
                ws_app_instance = MagicMock()

                def run_forever():
                    call_kwargs = mock_ws_module.WebSocketApp.call_args[1]
                    on_message = call_kwargs.get("on_message")
                    if on_message:
                        on_message(ws_app_instance, json.dumps(EXECUTION_REPORT_REJECTED))

                ws_app_instance.run_forever.side_effect = run_forever
                mock_ws_module.WebSocketApp.return_value = ws_app_instance

                listener = BinanceFillsListener(client, on_fill_or_reject=capture)
                listener.start()

        assert len(received) == 1
        assert received[0]["event_type"] == "reject"
        assert received[0]["order_id"] == "rejected-order-uuid"
        assert received[0]["reject_reason"] == "INSUFFICIENT_BALANCES"

    def test_listener_ignores_new_execution_report(self, client):
        """NEW execution report does not invoke callback."""
        received = []

        def capture(ev):
            received.append(ev)

        with patch.object(client, "start_user_data_stream", return_value="fake-listen-key"):
            with patch("oms.brokers.binance.fills_listener.websocket") as mock_ws_module:
                ws_app_instance = MagicMock()

                def run_forever():
                    call_kwargs = mock_ws_module.WebSocketApp.call_args[1]
                    on_message = call_kwargs.get("on_message")
                    if on_message:
                        on_message(ws_app_instance, json.dumps(EXECUTION_REPORT_NEW))

                ws_app_instance.run_forever.side_effect = run_forever
                mock_ws_module.WebSocketApp.return_value = ws_app_instance

                listener = BinanceFillsListener(client, on_fill_or_reject=capture)
                listener.start()

        assert len(received) == 0

    def test_listener_invalid_json_does_not_raise(self, client):
        """Invalid JSON in WebSocket message does not raise; callback not called."""
        received = []

        def capture(ev):
            received.append(ev)

        with patch.object(client, "start_user_data_stream", return_value="fake-listen-key"):
            with patch("oms.brokers.binance.fills_listener.websocket") as mock_ws_module:
                ws_app_instance = MagicMock()

                def run_forever():
                    call_kwargs = mock_ws_module.WebSocketApp.call_args[1]
                    on_message = call_kwargs.get("on_message")
                    if on_message:
                        on_message(ws_app_instance, "not json {{{")

                ws_app_instance.run_forever.side_effect = run_forever
                mock_ws_module.WebSocketApp.return_value = ws_app_instance

                listener = BinanceFillsListener(client, on_fill_or_reject=capture)
                listener.start()

        assert len(received) == 0
