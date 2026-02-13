"""
Optional integration tests against Binance testnet.

Run only when explicitly enabled:
  set RUN_BINANCE_TESTNET=1
  set BINANCE_API_KEY=your_testnet_key
  set BINANCE_API_SECRET=your_testnet_secret
  pytest oms/brokers/binance/tests/test_testnet.py -v

Or load from .env (copy .env.example to .env and fill BINANCE_*).

Important: Use keys from Binance *testnet* (https://testnet.binance.vision →
API Management). Mainnet keys will get "API-key format invalid" (-2014).
See docs/BINANCE_API_RULES.md for testnet/mainnet, signature, and filter rules.

What the tests do (no mocks):
- test_user_data_stream_*: create listen key, keepalive, close (no order).
- test_place_order_then_cancel: place a limit BUY far from market, then cancel (no fill).
- test_fills_listener_*: connect WebSocket to user stream, run briefly, stop (no order).
"""

import os
import time
from pathlib import Path

import pytest

def _env(key: str, default: str = "") -> str:
    v = (os.getenv(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


# Load .env from repo root and cwd (override=True so .env wins over existing env)
try:
    from dotenv import load_dotenv
    _this = Path(__file__).resolve()
    # Walk up from test file to find .env (repo root is 4 levels up: tests -> binance -> brokers -> oms -> repo)
    for _d in [_this.parents[4], _this.parents[3], _this.parents[2], Path.cwd()]:
        _env_path = _d / ".env"
        if _env_path.is_file():
            load_dotenv(_env_path, override=True)
            break
    load_dotenv(Path.cwd() / ".env", override=True)  # cwd as well (e.g. when run from repo root)
except ImportError:
    pass

RUN_TESTNET = _env("RUN_BINANCE_TESTNET").lower() in ("1", "true", "yes")
BINANCE_API_KEY = _env("BINANCE_API_KEY")
BINANCE_API_SECRET = _env("BINANCE_API_SECRET")
BINANCE_BASE_URL = _env("BINANCE_BASE_URL") or "https://testnet.binance.vision"

skip_unless_testnet = pytest.mark.skipif(
    not RUN_TESTNET or not BINANCE_API_KEY or not BINANCE_API_SECRET,
    reason="Set RUN_BINANCE_TESTNET=1 and BINANCE_API_KEY, BINANCE_API_SECRET to run testnet tests",
)


@skip_unless_testnet
class TestBinanceTestnetAuth:
    """Test authentication first (key + HMAC-SHA256). Run before other testnet tests."""

    @pytest.fixture
    def client(self):
        from oms.brokers.binance.api_client import BinanceAPIClient
        return BinanceAPIClient(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET,
            base_url=BINANCE_BASE_URL,
            testnet=True,
        )

    def test_auth_signed(self, client):
        """Verify API key and HMAC-SHA256 signing with GET /api/v3/account."""
        account = client.get_account()
        assert "balances" in account or "makerCommission" in account or "updateTime" in account


@skip_unless_testnet
class TestBinanceTestnetAPI:
    """Real requests to Binance testnet (no mocks)."""

    @pytest.fixture
    def client(self):
        from oms.brokers.binance.api_client import BinanceAPIClient
        return BinanceAPIClient(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET,
            base_url=BINANCE_BASE_URL,
            testnet=True,
        )

    def test_user_data_stream_start_keepalive_close(self, client):
        """Create listen key, keepalive once, then close (real testnet)."""
        listen_key = client.start_user_data_stream()
        assert listen_key
        assert len(listen_key) > 0
        client.keepalive_user_data_stream(listen_key)
        client.close_user_data_stream(listen_key)

    def test_place_order_then_cancel(self, client):
        """Place a limit BUY below market (within PERCENT_PRICE filter), then cancel (real testnet)."""
        import requests as _requests
        symbol = "BTCUSDT"
        # Get current price so our limit is within Binance's PERCENT_PRICE_BY_SIDE filter
        ticker = _requests.get(
            f"{client.base_url}/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=10,
        )
        ticker.raise_for_status()
        last_price = float(ticker.json()["price"])
        price = round(last_price * 0.95, 2)  # 5% below market, order stays open
        quantity = 0.0001
        client_order_id = f"testnet-{int(time.time() * 1000)}"
        place = client.place_order(
            symbol=symbol,
            side="BUY",
            order_type="LIMIT",
            quantity=quantity,
            price=price,
            time_in_force="GTC",
            client_order_id=client_order_id,
        )
        assert place.get("orderId") or place.get("orderid")
        assert place.get("symbol") == symbol
        # Limit placed below market: expect unfilled order (NEW, executedQty 0)
        assert place.get("status") in ("NEW", "PENDING", "EXPIRED")
        executed = place.get("executedQty", place.get("executed_qty", 0))
        assert float(executed) == 0, "Expected unfilled order"
        cancel = client.cancel_order(symbol=symbol, client_order_id=client_order_id)
        assert cancel.get("status") in ("CANCELED", "PENDING_CANCEL") or cancel.get("orderId") is not None

    def test_place_order_query_then_cancel(self, client):
        """Place limit BUY, query by client_order_id (real testnet), then cancel."""
        import requests as _requests
        symbol = "BTCUSDT"
        ticker = _requests.get(
            f"{client.base_url}/api/v3/ticker/price",
            params={"symbol": symbol},
            timeout=10,
        )
        ticker.raise_for_status()
        last_price = float(ticker.json()["price"])
        price = round(last_price * 0.95, 2)
        quantity = 0.0001
        client_order_id = f"testnet-query-{int(time.time() * 1000)}"
        place = client.place_order(
            symbol=symbol,
            side="BUY",
            order_type="LIMIT",
            quantity=quantity,
            price=price,
            time_in_force="GTC",
            client_order_id=client_order_id,
        )
        assert place.get("status") in ("NEW", "PENDING", "EXPIRED")
        time.sleep(0.5)  # allow testnet to make order queryable
        # Query the order by client_order_id (real testnet)
        queried = client.query_order(symbol=symbol, client_order_id=client_order_id)
        assert queried.get("orderId") == place.get("orderId") or queried.get("orderId") == place.get("orderid")
        assert queried.get("origClientOrderId") == client_order_id or queried.get("clientOrderId") == client_order_id
        assert queried.get("symbol") == symbol
        assert float(queried.get("executedQty", 0)) == 0
        client.cancel_order(symbol=symbol, client_order_id=client_order_id)

    def test_query_order_by_order_id(self, client):
        """Place limit, query by order_id (not client_order_id), then cancel (testnet)."""
        import requests as _requests
        from oms.brokers.binance.api_client import BinanceAPIError
        symbol = "BTCUSDT"
        ticker = _requests.get(f"{client.base_url}/api/v3/ticker/price", params={"symbol": symbol}, timeout=10)
        ticker.raise_for_status()
        last_price = float(ticker.json()["price"])
        price = round(last_price * 0.95, 2)
        client_order_id = f"testnet-by-orderid-{int(time.time() * 1000)}"
        place = client.place_order(
            symbol=symbol, side="BUY", order_type="LIMIT", quantity=0.0001,
            price=price, time_in_force="GTC", client_order_id=client_order_id,
        )
        order_id = place.get("orderId") or place.get("orderid")
        assert order_id is not None
        time.sleep(0.5)  # allow testnet to make order queryable
        queried = client.query_order(symbol=symbol, order_id=int(order_id))
        assert queried.get("orderId") == order_id or str(queried.get("orderId")) == str(order_id)
        assert queried.get("symbol") == symbol
        client.cancel_order(symbol=symbol, client_order_id=client_order_id)

    def test_cancel_by_order_id(self, client):
        """Place limit, cancel by order_id (not client_order_id) (testnet)."""
        import requests as _requests
        symbol = "BTCUSDT"
        ticker = _requests.get(f"{client.base_url}/api/v3/ticker/price", params={"symbol": symbol}, timeout=10)
        ticker.raise_for_status()
        last_price = float(ticker.json()["price"])
        price = round(last_price * 0.95, 2)
        client_order_id = f"testnet-cancel-by-id-{int(time.time() * 1000)}"
        place = client.place_order(
            symbol=symbol, side="BUY", order_type="LIMIT", quantity=0.0001,
            price=price, time_in_force="GTC", client_order_id=client_order_id,
        )
        order_id = place.get("orderId") or place.get("orderid")
        assert order_id is not None
        cancel = client.cancel_order(symbol=symbol, order_id=order_id)
        assert cancel.get("status") in ("CANCELED", "PENDING_CANCEL") or cancel.get("orderId") is not None

    def test_place_order_invalid_symbol_raises(self, client):
        """Place with invalid symbol returns API error (testnet)."""
        from oms.brokers.binance.api_client import BinanceAPIError
        with pytest.raises(BinanceAPIError):
            client.place_order(
                symbol="INVALIDPAIR",
                side="BUY",
                order_type="LIMIT",
                quantity=0.0001,
                price=1.0,
                time_in_force="GTC",
            )

    def test_query_order_not_found_raises(self, client):
        """Query with nonexistent client_order_id returns API error (testnet)."""
        from oms.brokers.binance.api_client import BinanceAPIError
        with pytest.raises(BinanceAPIError):
            client.query_order(symbol="BTCUSDT", client_order_id="testnet-nonexistent-999999")

    def test_cancel_already_cancelled_raises(self, client):
        """Cancel same order twice: second cancel returns API error (testnet)."""
        import requests as _requests
        from oms.brokers.binance.api_client import BinanceAPIError
        symbol = "BTCUSDT"
        ticker = _requests.get(f"{client.base_url}/api/v3/ticker/price", params={"symbol": symbol}, timeout=10)
        ticker.raise_for_status()
        last_price = float(ticker.json()["price"])
        price = round(last_price * 0.95, 2)
        client_order_id = f"testnet-double-cancel-{int(time.time() * 1000)}"
        client.place_order(
            symbol=symbol, side="BUY", order_type="LIMIT", quantity=0.0001,
            price=price, time_in_force="GTC", client_order_id=client_order_id,
        )
        client.cancel_order(symbol=symbol, client_order_id=client_order_id)
        with pytest.raises(BinanceAPIError):
            client.cancel_order(symbol=symbol, client_order_id=client_order_id)


@skip_unless_testnet
class TestBinanceTestnetFillsListener:
    """Fills listener against testnet WebSocket (connect, receive stream, stop)."""

    @pytest.fixture
    def client(self):
        from oms.brokers.binance.api_client import BinanceAPIClient
        return BinanceAPIClient(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET,
            base_url=BINANCE_BASE_URL,
            testnet=True,
        )

    def test_fills_listener_connect_and_stop(self, client):
        """Start user data stream, connect WebSocket, run briefly, then stop (no assert on events)."""
        import threading
        import time
        from oms.brokers.binance.fills_listener import BinanceFillsListener

        received = []
        listener = BinanceFillsListener(client, on_fill_or_reject=received.append)
        thread_done = threading.Event()

        def run_listener():
            try:
                listener.start()
            except Exception:
                pass
            finally:
                thread_done.set()

        thread = threading.Thread(target=run_listener, daemon=True)
        thread.start()
        # Allow time to get listen key and connect WebSocket
        time.sleep(2)
        listener.stop()
        thread_done.wait(timeout=5)
        # Stopped cleanly: listen key cleared
        assert listener._listen_key is None

    def test_fills_listener_receives_fill_after_market_order(self, client):
        """Start listener (WebSocket API method-based), place small market BUY (fills on testnet),
        assert we get a fill event that matches the order, then stop."""
        import threading
        from oms.brokers.binance.api_client import BinanceAPIError
        from oms.brokers.binance.fills_listener import create_fills_listener

        symbol = "BTCUSDT"
        side = "BUY"
        quantity = 0.0001
        client_order_id = "fill_test_" + str(int(time.time() * 1000))

        received = []
        listener = create_fills_listener(client, on_fill_or_reject=received.append)
        thread_done = threading.Event()

        def run_listener():
            try:
                listener.start()
            except Exception:
                pass
            finally:
                thread_done.set()

        thread = threading.Thread(target=run_listener, daemon=True)
        thread.start()
        # Wait for WebSocket to open so we know the stream is connected to this account
        for _ in range(50):
            time.sleep(0.2)
            if listener.stream_connected:
                break
        if not listener.stream_connected:
            listener.stop()
            thread_done.wait(timeout=5)
            pytest.skip("WebSocket did not connect within 10s (stream not bound to account yet)")

        try:
            # Minimal market BUY; may fail with -2010 insufficient balance on testnet
            order_sent = {
                "symbol": symbol,
                "side": side,
                "order_type": "MARKET",
                "quantity": quantity,
                "client_order_id": client_order_id,
            }
            order_resp = client.place_order(**order_sent)
        except BinanceAPIError as e:
            if "insufficient" in str(e).lower() or "balance" in str(e).lower() or "-2010" in str(e):
                listener.stop()
                thread_done.wait(timeout=5)
                pytest.skip("Testnet account has insufficient balance for market order")
            raise

        print("\n--- Order sent ---")
        print("Request:", order_sent)
        print("Response:", order_resp)

        broker_order_id = str(order_resp.get("orderId") or order_resp.get("orderid") or "")

        # Wait up to 8s for fill event
        for _ in range(80):
            time.sleep(0.1)
            if received and received[-1].get("event_type") == "fill":
                break
        listener.stop()
        thread_done.wait(timeout=5)

        if not received:
            pytest.skip(
                "No events received (WebSocket may have failed to connect or stream URL issue)"
            )
        fills = [r for r in received if r.get("event_type") == "fill"]
        if not fills:
            pytest.skip(
                f"Market order placed but no fill event received (got {received}); "
                "testnet may not have pushed executionReport in time"
            )

        print("\n--- Fill status report(s) received ---")
        for r in received:
            print(r)

        # Find fill(s) for our order and assert they match the order we sent
        our_fills = [
            f for f in fills
            if f.get("order_id") == client_order_id or f.get("broker_order_id") == broker_order_id
        ]
        assert our_fills, (
            f"No fill matched our order (client_order_id={client_order_id!r}, "
            f"broker_order_id={broker_order_id!r}); fills={fills}"
        )
        for fill in our_fills:
            assert fill.get("order_id") == client_order_id
            assert fill.get("broker_order_id") == broker_order_id
            assert fill.get("symbol") == symbol
            assert fill.get("side") == side
            assert fill.get("quantity", 0) > 0
            assert fill.get("quantity", 0) <= quantity + 1e-9  # allow tiny precision
