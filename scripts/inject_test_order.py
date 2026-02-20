#!/usr/bin/env python3
"""
Inject a test order into Redis risk_approved stream (task 12.4.1).

Use for manual E2E and automated tests. Connects to Redis via REDIS_URL,
XADDs one message to risk_approved with broker, symbol, side, quantity,
order_type, etc. Optionally asserts by polling oms_fills or Postgres.

See PHASE2_DETAILED_PLAN §16.1–16.4 and §12.4.1.

Usage:
    python scripts/inject_test_order.py [options]

Options:
    --broker BROKER       Broker (default: binance)
    --symbol SYMBOL       Symbol (default: BTCUSDT)
    --side SIDE           BUY or SELL (default: BUY)
    --quantity QTY        Quantity (default: 0.0001)
    --order-type TYPE     MARKET or LIMIT (default: MARKET)
    --limit-price P       Limit price for LIMIT orders (optional)
    --time-in-force TIF   GTC, IOC, FOK (optional)
    --book BOOK           Strategy/book id (default: inject_test_order)
    --comment COMMENT     Freetext comment (optional)
    --order-id ID         Override internal order_id (default: auto-generated)
    --assert              After inject, poll oms_fills until fill/reject or timeout
    --assert-timeout SEC  Timeout for --assert in seconds (default: 60)
    --assert-postgres     With --assert, also verify order in Postgres (needs DATABASE_URL)
    --dry-run             Print payload only, do not connect to Redis
"""

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
except ImportError:
    pass


def _env(key: str, default: str = "") -> str:
    v = (os.getenv(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


def build_risk_order(args: argparse.Namespace) -> dict:
    """Build risk_approved message dict from args."""
    order_id = args.order_id or f"inject-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    payload = {
        "order_id": order_id,
        "broker": args.broker,
        "symbol": args.symbol,
        "side": args.side,
        "quantity": args.quantity,
        "order_type": args.order_type,
        "book": args.book,
        "comment": args.comment or "inject_test_order script",
    }
    if args.order_type.upper() == "LIMIT" and args.limit_price is not None:
        payload["price"] = args.limit_price
    if args.time_in_force:
        payload["time_in_force"] = args.time_in_force
    return payload


def inject(redis_url: str, payload: dict) -> str:
    """XADD payload to risk_approved; return entry id."""
    from redis import Redis
    from oms.schemas import RISK_APPROVED_STREAM
    from oms.streams import add_message

    redis = Redis.from_url(redis_url, decode_responses=True)
    entry_id = add_message(redis, RISK_APPROVED_STREAM, payload)
    return entry_id


def assert_oms_fills(redis_url: str, order_id: str, timeout_sec: float) -> bool:
    """Poll oms_fills for order_id until fill/reject or timeout. Returns True if found."""
    from redis import Redis
    from oms.schemas import OMS_FILLS_STREAM

    redis = Redis.from_url(redis_url, decode_responses=True)
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        # XREVRANGE oms_fills, last N entries
        raw = redis.xrevrange(OMS_FILLS_STREAM, max="+", min="-", count=50)
        for eid, flds in raw:
            oid = flds.get("order_id")
            if oid == order_id:
                event = flds.get("event_type", "?")
                print(f"[assert] Found order_id={order_id} in oms_fills: event_type={event}", file=sys.stderr)
                return True
        time.sleep(0.2)
    return False


def assert_postgres_order(database_url: str, order_id: str, timeout_sec: float) -> bool:
    """Poll Postgres orders table for internal_id = order_id. Returns True if found."""
    try:
        import psycopg2
    except ImportError:
        print("[assert-postgres] psycopg2 not installed; skip Postgres check", file=sys.stderr)
        return False
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        try:
            conn = psycopg2.connect(database_url)
            try:
                cur = conn.cursor()
                cur.execute("SELECT id, status FROM orders WHERE internal_id = %s", (order_id,))
                row = cur.fetchone()
                if row:
                    print(f"[assert-postgres] Found order internal_id={order_id} in Postgres: id={row[0]} status={row[1]}", file=sys.stderr)
                    return True
            finally:
                conn.close()
        except Exception as e:
            print(f"[assert-postgres] {e}", file=sys.stderr)
        time.sleep(0.5)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inject test order to Redis risk_approved (12.4.1); optional assertions."
    )
    parser.add_argument("--broker", default="binance", help="Broker (default: binance)")
    parser.add_argument("--symbol", default="BTCUSDT", help="Symbol (default: BTCUSDT)")
    parser.add_argument("--side", default="BUY", choices=("BUY", "SELL"), help="Side (default: BUY)")
    parser.add_argument("--quantity", default="0.0001", help="Quantity (default: 0.0001)")
    parser.add_argument("--order-type", default="MARKET", choices=("MARKET", "LIMIT"), help="Order type (default: MARKET)")
    parser.add_argument("--limit-price", type=float, default=None, help="Limit price for LIMIT orders")
    parser.add_argument("--time-in-force", default="", help="GTC, IOC, FOK (optional)")
    parser.add_argument("--book", default="inject_test_order", help="Book/strategy id (default: inject_test_order)")
    parser.add_argument("--comment", default="", help="Comment (optional)")
    parser.add_argument("--order-id", default="", help="Override order_id (default: auto-generated)")
    parser.add_argument("--assert", dest="do_assert", action="store_true", help="Poll oms_fills until fill/reject or timeout")
    parser.add_argument("--assert-timeout", type=float, default=60, help="Timeout for --assert in seconds (default: 60)")
    parser.add_argument("--assert-postgres", action="store_true", help="With --assert, also verify order in Postgres")
    parser.add_argument("--dry-run", action="store_true", help="Print payload only, do not connect to Redis")
    args = parser.parse_args()

    # Normalize quantity to number for payload (consumer expects numeric)
    try:
        args.quantity = float(args.quantity)
    except ValueError:
        print(f"Invalid quantity: {args.quantity}", file=sys.stderr)
        return 1

    payload = build_risk_order(args)
    order_id = payload["order_id"]

    if args.dry_run:
        print("Dry run — payload (would XADD to risk_approved):")
        for k, v in sorted(payload.items()):
            print(f"  {k}: {v}")
        return 0

    redis_url = _env("REDIS_URL")
    if not redis_url:
        print("REDIS_URL not set", file=sys.stderr)
        return 1

    entry_id = inject(redis_url, payload)
    print(f"Injected order_id={order_id} to risk_approved -> entry_id={entry_id}")

    if not args.do_assert:
        return 0

    ok = assert_oms_fills(redis_url, order_id, args.assert_timeout)
    if not ok:
        print(f"[assert] Timeout: order_id={order_id} not seen in oms_fills within {args.assert_timeout}s", file=sys.stderr)
        return 1

    if args.assert_postgres:
        database_url = _env("DATABASE_URL")
        if not database_url:
            print("[assert-postgres] DATABASE_URL not set", file=sys.stderr)
            return 1
        if not assert_postgres_order(database_url, order_id, args.assert_timeout):
            print(f"[assert-postgres] Timeout: order_id={order_id} not found in Postgres orders", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
