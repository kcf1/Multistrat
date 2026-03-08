#!/usr/bin/env python3
"""
Inject 2 fake buys at $1, 1 fake sell at $2, then 2 fake sells at $0.5 into the orders table,
wait 10s for PMS to build positions, then check that positions match expected.

PMS derives positions at asset grain (base + quote legs). So one symbol BTCUSDT produces two
position rows: BTC (base) and USDT (quote). Expected: 2 rows — BTC open_qty=-1, position_side=short;
USDT open_qty=+1, position_side=long (quote legs: -1-1+2+0.5+0.5=+1).

Requires: DATABASE_URL; symbols table must contain BTCUSDT -> (BTC, USDT) for derivation to run.
Run from repo root.
  python scripts/inject_orders_and_check_pms.py
"""

import os
import sys
import time

# Load .env from repo root
try:
    import dotenv
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dotenv.load_dotenv(os.path.join(repo_root, ".env"))
except ImportError:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor


def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    account_id = "test-acc"
    book = ""
    symbol = "BTCUSDT"

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Insert 2 buys at $1, 1 sell at $2, then 2 sells at $0.5 (status=filled so PMS picks them up)
            orders = [
                {
                    "internal_id": "inj-1",
                    "broker": "binance",
                    "account_id": account_id,
                    "broker_order_id": "bo1",
                    "symbol": symbol,
                    "side": "BUY",
                    "order_type": "LIMIT",
                    "quantity": 1,
                    "price": 1,
                    "status": "filled",
                    "executed_qty": 1,
                    "book": book,
                },
                {
                    "internal_id": "inj-2",
                    "broker": "binance",
                    "account_id": account_id,
                    "broker_order_id": "bo2",
                    "symbol": symbol,
                    "side": "BUY",
                    "order_type": "LIMIT",
                    "quantity": 1,
                    "price": 1,
                    "status": "filled",
                    "executed_qty": 1,
                    "book": book,
                },
                {
                    "internal_id": "inj-3",
                    "broker": "binance",
                    "account_id": account_id,
                    "broker_order_id": "bo3",
                    "symbol": symbol,
                    "side": "SELL",
                    "order_type": "LIMIT",
                    "quantity": 1,
                    "price": 2,
                    "status": "filled",
                    "executed_qty": 1,
                    "book": book,
                },
                {
                    "internal_id": "inj-4",
                    "broker": "binance",
                    "account_id": account_id,
                    "broker_order_id": "bo4",
                    "symbol": symbol,
                    "side": "SELL",
                    "order_type": "LIMIT",
                    "quantity": 1,
                    "price": 0.5,
                    "status": "filled",
                    "executed_qty": 1,
                    "book": book,
                },
                {
                    "internal_id": "inj-5",
                    "broker": "binance",
                    "account_id": account_id,
                    "broker_order_id": "bo5",
                    "symbol": symbol,
                    "side": "SELL",
                    "order_type": "LIMIT",
                    "quantity": 1,
                    "price": 0.5,
                    "status": "filled",
                    "executed_qty": 1,
                    "book": book,
                },
            ]
            for o in orders:
                cur.execute(
                    """
                    INSERT INTO orders (
                        internal_id, broker, account_id, broker_order_id, symbol, side,
                        order_type, quantity, price, status, executed_qty, book
                    ) VALUES (
                        %(internal_id)s, %(broker)s, %(account_id)s, %(broker_order_id)s, %(symbol)s, %(side)s,
                        %(order_type)s, %(quantity)s, %(price)s, %(status)s, %(executed_qty)s, %(book)s
                    )
                    ON CONFLICT (internal_id) DO UPDATE SET
                        status = EXCLUDED.status, executed_qty = EXCLUDED.executed_qty, price = EXCLUDED.price
                    """,
                    o,
                )
        conn.commit()
        print("Inserted 5 orders (2 BUY @ $1, 1 SELL @ $2, 2 SELL @ $0.5). Waiting 10s for PMS...")
    finally:
        conn.close()

    time.sleep(10)

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT broker, account_id, book, asset, open_qty, position_side, usd_price
                FROM positions
                WHERE broker = %s AND account_id = %s AND book = %s AND asset IN ('BTC', 'USDT')
                ORDER BY asset
                """,
                ("binance", account_id, book),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    # Asset grain: one order symbol produces two legs (base + quote) → expect 2 rows
    if len(rows) != 2:
        print(f"FAIL: expected 2 position rows (BTC and USDT), got {len(rows)}: {rows}")
        sys.exit(1)

    by_asset = {r["asset"]: r for r in rows}
    if "BTC" not in by_asset or "USDT" not in by_asset:
        print(f"FAIL: expected assets BTC and USDT, got {list(by_asset.keys())}")
        sys.exit(1)

    btc = by_asset["BTC"]
    usdt = by_asset["USDT"]
    btc_open_qty = float(btc["open_qty"])
    btc_side = btc["position_side"]
    usdt_open_qty = float(usdt["open_qty"])
    usdt_side = usdt["position_side"]

    if abs(btc_open_qty - (-1.0)) > 1e-6:
        print(f"FAIL: BTC open_qty={btc_open_qty}, expected -1.0")
        sys.exit(1)
    if btc_side != "short":
        print(f"FAIL: BTC position_side={btc_side!r}, expected 'short'")
        sys.exit(1)
    if abs(usdt_open_qty - 1.0) > 1e-6:
        print(f"FAIL: USDT open_qty={usdt_open_qty}, expected +1.0 (quote legs)")
        sys.exit(1)
    if usdt_side != "long":
        print(f"FAIL: USDT position_side={usdt_side!r}, expected 'long'")
        sys.exit(1)

    print("PASS: positions match expected (asset grain: BTC -1 short, USDT +1 long).")
    print(f"  BTC:  open_qty={btc_open_qty}, position_side={btc_side!r}")
    print(f"  USDT: open_qty={usdt_open_qty}, position_side={usdt_side!r}")


if __name__ == "__main__":
    main()
