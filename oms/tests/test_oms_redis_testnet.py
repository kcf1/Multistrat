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
DATABASE_URL = _env("DATABASE_URL")

skip_unless_testnet = pytest.mark.skipif(
    not RUN_TESTNET or not _env("BINANCE_API_KEY") or not _env("BINANCE_API_SECRET"),
    reason="Set RUN_BINANCE_TESTNET=1 and BINANCE_API_KEY, BINANCE_API_SECRET",
)
skip_unless_redis = pytest.mark.skipif(
    not REDIS_URL,
    reason="Set REDIS_URL to run Redis-through-testnet test",
)
skip_unless_database = pytest.mark.skipif(
    not DATABASE_URL,
    reason="Set DATABASE_URL to run full pipeline test with Postgres sync",
)


def _pg_available() -> bool:
    try:
        import psycopg2
        c = psycopg2.connect(DATABASE_URL)
        c.close()
        return True
    except Exception:
        return False


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

    def test_full_pipeline_place_then_cancel_via_redis(self, redis_client, store, adapter):
        """Full pipeline: risk_approved -> place order -> cancel_requested -> cancel at broker -> store updated."""
        import requests as _requests
        from oms.registry import AdapterRegistry
        from oms.schemas import CANCEL_REQUESTED_STREAM, RISK_APPROVED_STREAM
        from oms.redis_flow import process_one_cancel, process_one_risk_approved
        from oms.streams import add_message, read_latest

        symbol = "BTCUSDT"
        # Limit price below market so order stays open (won't fill)
        try:
            ticker = _requests.get(
                f"{_env('BINANCE_BASE_URL') or 'https://testnet.binance.vision'}/api/v3/ticker/price",
                params={"symbol": symbol},
                timeout=10,
            )
            ticker.raise_for_status()
            last_price = float(ticker.json()["price"])
            price = round(last_price * 0.95, 2)
        except Exception as e:
            pytest.skip(f"Could not get ticker for limit price: {e}")

        redis_client.delete(RISK_APPROVED_STREAM)
        redis_client.delete(CANCEL_REQUESTED_STREAM)

        order_id = f"redis-cancel-{int(time.time() * 1000)}"
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": symbol,
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "LIMIT",
            "price": price,
            "time_in_force": "GTC",
            "book": "redis_cancel_test",
        }
        add_message(redis_client, RISK_APPROVED_STREAM, risk_order)

        registry = AdapterRegistry()
        registry.register("binance", adapter)

        # Place order
        result = process_one_risk_approved(redis_client, store, registry)
        assert result is not None, "No message read from risk_approved"
        assert result.get("rejected") is not True, (
            f"Place rejected: {result.get('reject_reason', result)}"
        )
        broker_order_id = result.get("broker_order_id")
        assert broker_order_id, "Missing broker_order_id after place"

        order_before = store.get_order(order_id)
        assert order_before is not None
        assert order_before.get("status") == "sent"

        # Request cancel via Redis
        add_message(redis_client, CANCEL_REQUESTED_STREAM, {"order_id": order_id, "broker": "binance"})

        # Cancel (consumer group so message consumed once)
        cancel_result = process_one_cancel(
            redis_client, store, registry,
            consumer_group="oms", consumer_name="oms-1",
        )
        assert cancel_result is not None, "No message read from cancel_requested"
        assert cancel_result.get("cancelled") is True, (
            f"Cancel failed: {cancel_result.get('reject_reason', cancel_result)}"
        )
        assert cancel_result.get("order_id") == order_id
        assert cancel_result.get("broker_order_id") == broker_order_id

        order_after = store.get_order(order_id)
        assert order_after is not None
        assert order_after.get("status") == "cancelled", (
            f"Expected status cancelled, got {order_after.get('status')}"
        )

        # Second process_one_cancel should get no message (consumer group already acked)
        no_msg = process_one_cancel(redis_client, store, registry, consumer_group="oms", consumer_name="oms-1")
        assert no_msg is None

    def test_place_order_raises_retry_then_reject_consumer_group(self, redis_client, store):
        """12.1.9d with real Redis: consumer group, place_order raises -> no XACK until max retries, then reject + XACK."""
        from typing import Any, Dict

        from oms.consumer import ensure_risk_approved_consumer_group
        from oms.registry import AdapterRegistry
        from oms.redis_flow import process_one
        from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
        from oms.streams import add_message, read_latest

        redis_client.delete(RISK_APPROVED_STREAM)
        redis_client.delete(OMS_FILLS_STREAM)
        # Clear any leftover retry keys and order from previous runs
        for key in redis_client.scan_iter("oms:retry:risk_approved:*"):
            redis_client.delete(key)
        order_id = f"ord-retry-redis-{int(time.time() * 1000)}"
        redis_client.delete(f"orders:{order_id}")

        # Consumer group created before add so ">" sees the message (real Redis)
        ensure_risk_approved_consumer_group(redis_client, "oms", "0")
        add_message(redis_client, RISK_APPROVED_STREAM, {
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": "0.001",
            "order_type": "MARKET",
            "order_id": order_id,
        })

        class RaisingAdapter:
            def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
                raise RuntimeError("broker down")
            def start_fill_listener(self, callback: Any) -> None:
                pass
            def cancel_order(self, broker_order_id: str, symbol: str) -> Dict[str, Any]:
                return {"rejected": True}

        registry = AdapterRegistry()
        registry.register("binance", RaisingAdapter())

        # First two: retry_later, no ack (message stays in PEL)
        for _ in range(2):
            result = process_one(
                redis_client, store, registry,
                consumer_group="oms", consumer_name="oms-1",
            )
            assert result is not None
            assert result.get("retry_later") is True
            assert result.get("rejected") is False
            order = store.get_order(order_id)
            assert order is not None
            assert order["status"] == "pending"

        # Third: reject + ack (Option B)
        result3 = process_one(
            redis_client, store, registry,
            consumer_group="oms", consumer_name="oms-1",
        )
        assert result3 is not None
        assert result3.get("rejected") is True
        assert "retries" in result3.get("reject_reason", "").lower()
        order = store.get_order(order_id)
        assert order is not None
        assert order["status"] == "rejected"

        entries = read_latest(redis_client, OMS_FILLS_STREAM, count=5)
        assert any(e[1].get("event_type") == "reject" and e[1].get("order_id") == order_id for e in entries)

        # Fourth: no message (acked)
        result4 = process_one(
            redis_client, store, registry,
            consumer_group="oms", consumer_name="oms-1",
        )
        assert result4 is None

    def test_error_order_rejected_via_redis_testnet(self, redis_client, store, adapter):
        """Send an order that testnet will reject (invalid symbol); assert reject flows to store and oms_fills."""
        from oms.consumer import ensure_risk_approved_consumer_group
        from oms.registry import AdapterRegistry
        from oms.redis_flow import process_one
        from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
        from oms.streams import add_message, read_latest

        redis_client.delete(RISK_APPROVED_STREAM)
        redis_client.delete(OMS_FILLS_STREAM)
        order_id = f"ord-error-redis-{int(time.time() * 1000)}"
        redis_client.delete(f"orders:{order_id}")

        ensure_risk_approved_consumer_group(redis_client, "oms", "0")
        add_message(redis_client, RISK_APPROVED_STREAM, {
            "broker": "binance",
            "symbol": "INVALIDPAIR",
            "side": "BUY",
            "quantity": "0.0001",
            "order_type": "MARKET",
            "order_id": order_id,
            "book": "redis_error_test",
            "comment": "error order test",
        })

        registry = AdapterRegistry()
        registry.register("binance", adapter)

        result = process_one(
            redis_client, store, registry,
            consumer_group="oms", consumer_name="oms-1",
        )
        assert result is not None
        assert result.get("rejected") is True
        assert result.get("order_id") == order_id
        assert result.get("reject_reason")

        order = store.get_order(order_id)
        assert order is not None
        assert order["status"] == "rejected"

        entries = read_latest(redis_client, OMS_FILLS_STREAM, count=5)
        assert any(
            e[1].get("event_type") == "reject" and e[1].get("order_id") == order_id
            for e in entries
        ), f"Expected reject on oms_fills, got {[e[1].get('event_type') for e in entries]}"
        reject_entry = next(e for e in entries if e[1].get("order_id") == order_id and e[1].get("event_type") == "reject")
        assert "INVALIDPAIR" in str(reject_entry[1].get("reject_reason", "")) or "symbol" in str(reject_entry[1].get("reject_reason", "")).lower()

        # Message was acked (reject path acks)
        result2 = process_one(redis_client, store, registry, consumer_group="oms", consumer_name="oms-1")
        assert result2 is None

    @skip_unless_testnet
    @skip_unless_redis
    @skip_unless_database
    @pytest.mark.skipif(not _redis_available(), reason="Redis not reachable at REDIS_URL")
    @pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable at DATABASE_URL")
    def test_full_pipeline_redis_order_testnet_status_sync_to_postgres(self, redis_client, store, adapter):
        """Full pipeline: risk_approved -> testnet -> Redis order status -> sync to Postgres (12.1.10)."""
        from oms.redis_flow import TERMINAL_STATUSES, make_fill_callback, process_one_risk_approved
        from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
        from oms.streams import add_message, read_latest
        from oms.sync import sync_one_order, sync_terminal_orders

        redis_client.delete(RISK_APPROVED_STREAM)
        redis_client.delete(OMS_FILLS_STREAM)

        order_id = f"redis-pg-{int(time.time() * 1000)}"
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "MARKET",
            "book": "full_pipeline",
            "comment": "redis testnet postgres sync",
        }

        def on_terminal_sync(oid: str) -> None:
            sync_one_order(redis_client, store, DATABASE_URL, oid, ttl_after_sync_seconds=60)

        fill_cb = make_fill_callback(redis_client, store, on_terminal_sync=on_terminal_sync)
        adapter.start_fill_listener(fill_cb)

        for _ in range(125):
            time.sleep(0.2)
            if adapter._listener and getattr(adapter._listener, "stream_connected", False):
                break
        if not adapter._listener or not getattr(adapter._listener, "stream_connected", False):
            adapter.stop_fill_listener()
            pytest.skip("Fill listener WebSocket did not connect within 25s")

        try:
            add_message(redis_client, RISK_APPROVED_STREAM, risk_order)

            def get_adapter(broker: str):
                return adapter if broker == "binance" else None

            result = process_one_risk_approved(
                redis_client, store, get_adapter, on_terminal_sync=on_terminal_sync,
            )
            assert result is not None
            if result.get("rejected"):
                # Rejected by testnet: sync runs in process_one via on_terminal_sync
                pass
            else:
                assert result.get("broker_order_id")

            entries = []
            for _ in range(150):
                time.sleep(0.1)
                entries = read_latest(redis_client, OMS_FILLS_STREAM, count=10)
                if entries:
                    break

            adapter.stop_fill_listener()
        except Exception:
            adapter.stop_fill_listener()
            raise

        assert entries, "No message on oms_fills (timeout)"
        _eid, fields = entries[-1]
        assert fields.get("order_id") == order_id

        # Ensure any terminal order is synced (callback may have run; periodic catch-up)
        sync_terminal_orders(redis_client, store, DATABASE_URL, ttl_after_sync_seconds=None)

        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT internal_id, status, symbol, side, book, comment FROM orders WHERE internal_id = %s",
                (order_id,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, f"Expected one order row in Postgres for {order_id}, got {len(rows)}"
        row = rows[0]
        assert row[0] == order_id
        assert row[1] in TERMINAL_STATUSES, f"Expected terminal status in Postgres, got {row[1]}"
        assert row[2] == "BTCUSDT"
        assert row[3] == "BUY"
        assert row[4] == "full_pipeline"
        assert "postgres sync" in (row[5] or "")

    @skip_unless_testnet
    @skip_unless_redis
    @skip_unless_database
    @pytest.mark.skipif(not _redis_available(), reason="Redis not reachable at REDIS_URL")
    @pytest.mark.skipif(not _pg_available(), reason="Postgres not reachable at DATABASE_URL")
    def test_full_pipeline_with_main_loop(self, redis_client, store, adapter):
        """Full pipeline via main loop: start_fill_listeners + run_oms_loop; inject order; assert Redis and DB."""
        from oms.main import (
            start_fill_listeners,
            stop_fill_listeners,
            run_oms_loop,
        )
        from oms.redis_flow import TERMINAL_STATUSES
        from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
        from oms.streams import add_message, read_latest
        from oms.sync import sync_one_order, sync_terminal_orders

        from oms.registry import AdapterRegistry

        redis_client.delete(RISK_APPROVED_STREAM)
        redis_client.delete(OMS_FILLS_STREAM)

        registry = AdapterRegistry()
        registry.register("binance", adapter)

        def on_terminal_sync(oid: str) -> None:
            sync_one_order(redis_client, store, DATABASE_URL, oid, ttl_after_sync_seconds=60)

        start_fill_listeners(redis_client, store, registry, on_terminal_sync=on_terminal_sync)

        for _ in range(125):
            time.sleep(0.2)
            if adapter._listener and getattr(adapter._listener, "stream_connected", False):
                break
        if not adapter._listener or not getattr(adapter._listener, "stream_connected", False):
            stop_fill_listeners(registry)
            pytest.skip("Fill listener WebSocket did not connect within 25s")

        order_id = f"mainloop-{int(time.time() * 1000)}"
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "MARKET",
            "book": "main_loop_pipeline",
            "comment": "test with main loop",
        }
        add_message(redis_client, RISK_APPROVED_STREAM, risk_order)

        try:
            processed = run_oms_loop(
                redis_client,
                store,
                registry,
                block_ms=500,
                trim_every_n=100,
                stop_after_n=1,
                on_terminal_sync=on_terminal_sync,
            )
            assert processed == 1

            entries = []
            for _ in range(150):
                time.sleep(0.1)
                entries = read_latest(redis_client, OMS_FILLS_STREAM, count=10)
                if entries:
                    break
        finally:
            stop_fill_listeners(registry)

        assert entries, "No message on oms_fills (timeout)"
        _eid, fields = entries[-1]
        assert fields.get("order_id") == order_id
        assert fields.get("symbol") == "BTCUSDT"
        assert fields.get("book") == "main_loop_pipeline"

        # Fill callback may still be updating store; poll for terminal status
        order = None
        for _ in range(50):
            order = store.get_order(order_id)
            if order and order.get("status") in TERMINAL_STATUSES:
                break
            time.sleep(0.2)
        assert order is not None, f"Order {order_id} not in store"
        assert order["status"] in TERMINAL_STATUSES, (
            f"Order not terminal after 10s: status={order.get('status')}"
        )
        assert order["symbol"] == "BTCUSDT"

        sync_terminal_orders(redis_client, store, DATABASE_URL, ttl_after_sync_seconds=None)

        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT internal_id, status, symbol, side, book, comment FROM orders WHERE internal_id = %s",
                (order_id,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()

        assert len(rows) == 1, f"Expected one order row in Postgres for {order_id}, got {len(rows)}"
        row = rows[0]
        assert row[0] == order_id
        assert row[1] in TERMINAL_STATUSES
        assert row[2] == "BTCUSDT"
        assert row[3] == "BUY"
        assert row[4] == "main_loop_pipeline"
        assert "main loop" in (row[5] or "")
