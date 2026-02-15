#!/usr/bin/env python3
"""
Full pipeline test: inject one order to Redis (risk_approved), then poll until
the OMS service (e.g. in Docker) processes it and check downstreams (oms_fills
stream, Redis order store, Postgres orders table).

Requires: REDIS_URL. Optional: DATABASE_URL for Postgres check.
OMS must be running separately (e.g. docker compose up -d oms) with testnet config.
Usage: from repo root, python scripts/full_pipeline_test.py
       or: python -m scripts.full_pipeline_test
"""

import os
import sys
import time
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
    REDIS_URL = _env("REDIS_URL")
    DATABASE_URL = _env("DATABASE_URL")
    if not REDIS_URL:
        print("REDIS_URL not set", file=sys.stderr)
        return 1

    from redis import Redis
    from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
    from oms.storage.redis_order_store import RedisOrderStore
    from oms.streams import add_message

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    store = RedisOrderStore(redis)

    order_id = f"script-{int(time.time() * 1000)}"
    risk_order = {
        "order_id": order_id,
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.0001,
        "order_type": "MARKET",
        "book": "full_pipeline_script",
        "comment": "full pipeline test then check downstreams",
    }

    print("1. Injecting order to Redis risk_approved ...")
    add_message(redis, RISK_APPROVED_STREAM, risk_order)
    print(f"   order_id={order_id}")

    print("2. Waiting for OMS service to process (poll oms_fills + Redis store, up to 60s) ...")
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
        print("   ERROR: Order not seen in oms_fills or terminal in Redis store within 60s", file=sys.stderr)
        return 1
    print("   Order processed by OMS.")

    print("3. Checking downstreams ...")

    # Downstream 1: oms_fills
    fills = _read_latest_fills(redis, OMS_FILLS_STREAM, count=30)
    fill_entry = next((e for e in fills if e[1].get("order_id") == order_id), None)
    if fill_entry:
        eid, fields = fill_entry
        print(f"   oms_fills: entry_id={eid} event_type={fields.get('event_type')} "
              f"broker_order_id={fields.get('broker_order_id')} symbol={fields.get('symbol')} "
              f"quantity={fields.get('quantity')}")
    else:
        print("   oms_fills: NO ENTRY for order_id (timeout or missing)")
        return 1

    # Downstream 2: Redis order store
    order = store.get_order(order_id)
    if order:
        print(f"   Redis store: status={order.get('status')} symbol={order.get('symbol')} "
              f"broker_order_id={order.get('broker_order_id')} executed_qty={order.get('executed_qty')}")
    else:
        print("   Redis store: NO ORDER for order_id")
        return 1

    # Downstream 3: Postgres (OMS service syncs terminal orders; allow short delay)
    if DATABASE_URL:
        time.sleep(2)  # give OMS container time to run on_terminal_sync
        rows = []
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
            print("   Postgres: NO ROW for order_id")
            return 1
    else:
        print("   Postgres: (DATABASE_URL not set, skip)")

    print("4. All downstreams OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
