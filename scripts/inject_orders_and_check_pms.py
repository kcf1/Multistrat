#!/usr/bin/env python3
"""
Inject 2 fake buys at $1, 1 fake sell at $2, then 2 fake sells at $0.5 into the orders table,
wait 10s for PMS to build positions, then check that positions match expected.

Expected: one row in positions with open_qty=-1, position_side='short', entry_avg=0.5 (FIFO).

Run from repo root. Requires DATABASE_URL (e.g. from .env).
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
                SELECT account_id, book, symbol, open_qty, position_side, entry_avg, mark_price, notional, unrealized_pnl
                FROM positions
                WHERE account_id = %s AND book = %s AND symbol = %s
                """,
                (account_id, book, symbol),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if len(rows) != 1:
        print(f"FAIL: expected 1 position row, got {len(rows)}: {rows}")
        sys.exit(1)
    row = rows[0]
    open_qty = float(row["open_qty"])
    position_side = row["position_side"]
    entry_avg = float(row["entry_avg"]) if row["entry_avg"] is not None else None

    if abs(open_qty - (-1.0)) > 1e-6:
        print(f"FAIL: open_qty={open_qty}, expected -1.0")
        sys.exit(1)
    if position_side != "short":
        print(f"FAIL: position_side={position_side!r}, expected 'short'")
        sys.exit(1)
    if entry_avg is None or abs(entry_avg - 0.5) > 1e-6:
        print(f"FAIL: entry_avg={entry_avg}, expected 0.5 (FIFO)")
        sys.exit(1)

    print("PASS: positions match expected (open_qty=-1, position_side=short, entry_avg=0.5).")
    print(f"  Row: open_qty={open_qty}, position_side={position_side!r}, entry_avg={entry_avg}")


if __name__ == "__main__":
    main()
