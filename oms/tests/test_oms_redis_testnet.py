"""
Integration test: trigger Binance testnet through Redis (risk_approved) and assert response from Redis (oms_fills).

Requires:
  RUN_BINANCE_TESTNET=1
  BINANCE_API_KEY, BINANCE_API_SECRET (testnet)
  REDIS_URL (e.g. redis://localhost:6379)

Flow: XADD risk_approved -> OMS process_one (stage, place_order, update_status) -> fill listener
      callback updates store and XADD oms_fills -> test reads oms_fills and asserts.
"""

import os
import time
from pathlib import Path

import pytest

# Load .env from repo root
try:
    from dotenv import load_dotenv
    for _d in [Path(__file__).resolve().parents[2], Path.cwd()]:
        _env_path = _d / ".env"
        if _env_path.is_file():
            load_dotenv(_env_path, override=True)
            break
    load_dotenv(Path.cwd() / ".env", override=True)
except ImportError:
    pass


def _env(key: str, default: str = "") -> str:
    v = (os.getenv(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


RUN_TESTNET = _env("RUN_BINANCE_TESTNET").lower() in ("1", "true", "yes")
REDIS_URL = _env("REDIS_URL")

skip_unless_testnet = pytest.mark.skipif(
    not RUN_TESTNET or not _env("BINANCE_API_KEY") or not _env("BINANCE_API_SECRET"),
    reason="Set RUN_BINANCE_TESTNET=1 and BINANCE_API_KEY, BINANCE_API_SECRET",
)
skip_unless_redis = pytest.mark.skipif(
    not REDIS_URL,
    reason="Set REDIS_URL to run Redis-through-testnet test",
)


def _redis_available() -> bool:
    try:
        from redis import Redis
        r = Redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        return True
    except Exception:
        return False


@skip_unless_testnet
@skip_unless_redis
@pytest.mark.skipif(not _redis_available(), reason="Redis not reachable at REDIS_URL")
class TestOmsRedisTestnet:
    """Trigger testnet via Redis risk_approved; assert response on Redis oms_fills."""

    @pytest.fixture
    def redis_client(self):
        from redis import Redis
        return Redis.from_url(REDIS_URL, decode_responses=True)

    @pytest.fixture
    def store(self, redis_client):
        from oms.storage.redis_order_store import RedisOrderStore
        return RedisOrderStore(redis_client)

    @pytest.fixture
    def adapter(self):
        from oms.brokers.binance.api_client import BinanceAPIClient
        from oms.brokers.binance.adapter import BinanceBrokerAdapter
        client = BinanceAPIClient(
            api_key=_env("BINANCE_API_KEY"),
            api_secret=_env("BINANCE_API_SECRET"),
            base_url=_env("BINANCE_BASE_URL") or "https://testnet.binance.vision",
            testnet=True,
        )
        return BinanceBrokerAdapter(client=client)

    def test_trigger_testnet_via_redis_listen_oms_fills(self, redis_client, store, adapter):
        """Inject one market order into risk_approved; process via OMS; assert fill appears on oms_fills."""
        from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
        from oms.redis_flow import make_fill_callback, process_one_risk_approved
        from oms.streams import add_message, read_latest

        # Clean streams so we only see our message and our fill
        redis_client.delete(RISK_APPROVED_STREAM)
        redis_client.delete(OMS_FILLS_STREAM)

        order_id = f"redis-test-{int(time.time() * 1000)}"
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "MARKET",
            "book": "redis_test",
            "comment": "oms redis testnet",
        }

        # Start fill listener so fills are written to oms_fills
        fill_cb = make_fill_callback(redis_client, store)
        adapter.start_fill_listener(fill_cb)

        # Wait for WebSocket to connect
        for _ in range(125):
            time.sleep(0.2)
            if adapter._listener and getattr(adapter._listener, "stream_connected", False):
                break
        if not adapter._listener or not getattr(adapter._listener, "stream_connected", False):
            adapter.stop_fill_listener()
            pytest.skip("Fill listener WebSocket did not connect within 25s")

        try:
            # Trigger: inject into risk_approved
            add_message(redis_client, RISK_APPROVED_STREAM, risk_order)

            # OMS: read one and process (stage -> place_order -> update_status)
            def get_adapter(broker: str):
                return adapter if broker == "binance" else None

            result = process_one_risk_approved(redis_client, store, get_adapter)
            assert result is not None, "No message read from risk_approved"
            assert result.get("rejected") is not True, (
                f"Order rejected: {result.get('reject_reason', result)}"
            )
            assert result.get("broker_order_id"), "Missing broker_order_id"

            # Listen: poll oms_fills for up to 15s
            fills_entries = []
            for _ in range(150):
                time.sleep(0.1)
                fills_entries = read_latest(redis_client, OMS_FILLS_STREAM, count=10)
                if fills_entries:
                    break

            adapter.stop_fill_listener()
        except Exception:
            adapter.stop_fill_listener()
            raise

        assert fills_entries, "No message on oms_fills stream (timeout)"
        # Last entry is most recent; we may have one fill
        _eid, fields = fills_entries[-1]
        event_type = fields.get("event_type", "")
        assert event_type in ("fill", "reject"), f"Expected fill or reject, got {event_type}"
        assert fields.get("order_id") == order_id
        assert fields.get("broker_order_id") == result.get("broker_order_id")
        assert fields.get("symbol") == "BTCUSDT"
        assert fields.get("side") == "BUY"
        assert fields.get("book") == "redis_test"
        assert fields.get("comment") == "oms redis testnet"

        if event_type == "reject":
            pytest.skip(f"Order rejected on testnet: {fields.get('reject_reason', '')}")

        # Fill: quantity present
        qty = fields.get("quantity")
        assert qty, "Fill should have quantity"
        try:
            assert float(qty) > 0
        except (TypeError, ValueError):
            assert float(qty) > 0
