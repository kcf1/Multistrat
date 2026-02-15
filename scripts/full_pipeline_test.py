#!/usr/bin/env python3
"""
Full pipeline test: inject one order to Redis (risk_approved), then poll until
the OMS service (e.g. in Docker) processes it and check downstreams (oms_fills
stream, Redis order store, Postgres orders table).

Requires: REDIS_URL. Optional: DATABASE_URL for Postgres check.
OMS must be running separately (e.g. docker compose up -d oms) with testnet config.
Usage: from repo root, python scripts/full_pipeline_test.py [--market]
       or: python -m scripts.full_pipeline_test
       --market: inject MARKET order (fills immediately); default: LIMIT (unfillable) + cancel
"""

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

# Repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
except ImportError:
    pass

# Quiet loguru when running as script (optional: set LOG_OMS=1 to keep logs)
if not os.getenv("LOG_OMS"):
    import logging
    logging.disable(logging.CRITICAL)
    try:
        from loguru import logger
        logger.disable("oms")
    except Exception:
        pass


def _env(key: str, default: str = "") -> str:
    v = (os.getenv(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


def _read_latest_fills(redis, stream: str, count: int = 50):
    """Return last `count` entries from stream (newest first via XREVRANGE)."""
    raw = redis.xrevrange(stream, max="+", min="-", count=count)
    out = []
    for eid, flds in raw:
        eid_str = eid.decode() if isinstance(eid, bytes) else eid
        decoded = {}
        for k, v in (flds or {}).items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            decoded[key] = val
        out.append((eid_str, decoded))
    return out


# Terminal statuses in Redis store (OMS marks order as one of these when done)
TERMINAL_STATUSES = {"filled", "canceled", "rejected", "expired"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Full pipeline test: inject order to Redis, check downstreams")
    parser.add_argument("--market", action="store_true", help="Inject MARKET order (fills immediately)")
    args = parser.parse_args()

    REDIS_URL = _env("REDIS_URL")
    DATABASE_URL = _env("DATABASE_URL")
    if not REDIS_URL:
        print("REDIS_URL not set", file=sys.stderr)
        return 1

    from redis import Redis
    from oms.schemas import CANCEL_REQUESTED_STREAM, OMS_FILLS_STREAM, RISK_APPROVED_STREAM
    from oms.storage.redis_order_store import RedisOrderStore
    from oms.streams import add_message

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    store = RedisOrderStore(redis)

    order_id = f"script-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"

    if args.market:
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "MARKET",
            "book": "full_pipeline_script",
            "comment": "full pipeline test market order",
        }
    else:
        try:
            import requests
            ticker = requests.get(
                f"{_env('BINANCE_BASE_URL') or 'https://testnet.binance.vision'}/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
                timeout=10,
            )
            ticker.raise_for_status()
            last_price = float(ticker.json()["price"])
            price = round(last_price * 0.95, 2)
        except Exception as e:
            print(f"Could not get ticker for limit price: {e}", file=sys.stderr)
            return 1
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "LIMIT",
            "price": price,
            "time_in_force": "GTC",
            "book": "full_pipeline_script",
            "comment": "full pipeline test limit order then check downstreams",
        }

    print("1. Injecting order to Redis risk_approved ...")
    add_message(redis, RISK_APPROVED_STREAM, risk_order)
    print(f"   order_id={order_id} order_type={risk_order['order_type']}")

    if args.market:
        print("2. Waiting for OMS to process (poll oms_fills + Redis store, up to 60s) ...")
        deadline = time.monotonic() + 60
        fill_seen = False
        while time.monotonic() < deadline:
            time.sleep(0.5)
            entries = _read_latest_fills(redis, OMS_FILLS_STREAM, count=30)
            if any(e[1].get("order_id") == order_id for e in entries):
                fill_seen = True
                break
            order = store.get_order(order_id)
            if order and order.get("status") in TERMINAL_STATUSES:
                fill_seen = True
                break
        if not fill_seen:
            print("   ERROR: Order not seen in oms_fills or terminal within 60s", file=sys.stderr)
            return 1
        print("   Order processed by OMS (filled).")
    else:
        print("2. Waiting for OMS to process (poll Redis store for status sent, up to 60s) ...")
        deadline = time.monotonic() + 60
        order_placed = False
        while time.monotonic() < deadline:
            time.sleep(0.5)
            order = store.get_order(order_id)
            if order and order.get("status") in ("sent", "pending", *TERMINAL_STATUSES):
                order_placed = True
                break
        if not order_placed:
            print("   ERROR: Order not in Redis store within 60s", file=sys.stderr)
            return 1
        print("   Order processed by OMS (placed, unfilled).")
        print("3. Injecting cancel_requested ...")
        add_message(redis, CANCEL_REQUESTED_STREAM, {"order_id": order_id, "broker": "binance"})
        print("   Waiting for OMS to cancel (up to 60s) ...")
        deadline = time.monotonic() + 60
        cancelled = False
        while time.monotonic() < deadline:
            time.sleep(0.5)
            order = store.get_order(order_id)
            if order and order.get("status") == "cancelled":
                cancelled = True
                break
        if not cancelled:
            print("   ERROR: Order not cancelled within 60s", file=sys.stderr)
            return 1
        print("   Order cancelled.")

    print("4. Checking downstreams ...")

    fills = _read_latest_fills(redis, OMS_FILLS_STREAM, count=30)
    fill_entry = next((e for e in fills if e[1].get("order_id") == order_id), None)
    if fill_entry:
        eid, fields = fill_entry
        print(f"   oms_fills: entry_id={eid} event_type={fields.get('event_type')} "
              f"broker_order_id={fields.get('broker_order_id')} symbol={fields.get('symbol')} "
              f"quantity={fields.get('quantity')}")
    else:
        print("   oms_fills: NO ENTRY for order_id")
        return 1

    order = store.get_order(order_id)
    if order:
        print(f"   Redis store: status={order.get('status')} symbol={order.get('symbol')} "
              f"broker_order_id={order.get('broker_order_id')} executed_qty={order.get('executed_qty')}")
    else:
        print("   Redis store: NO ORDER for order_id")
        return 1

    expected_status = "filled" if args.market else "cancelled"
    if DATABASE_URL:
        time.sleep(2)
        try:
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
        except Exception as e:
            print(f"   Postgres: error {e}", file=sys.stderr)
            return 1
        if rows:
            row = rows[0]
            print(f"   Postgres: internal_id={row[0]} status={row[1]} symbol={row[2]} side={row[3]} book={row[4]} comment={row[5] or ''}")
        else:
            print("   Postgres: NO ROW")
            return 1
    else:
        print("   Postgres: (DATABASE_URL not set, skip)")

    print("5. All downstreams OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
