#!/usr/bin/env python3
"""
Inject one fake balance_change via the same code path as the OMS service
(write_balance_change), then verify the row in the DB.

Flow: no Redis involved — balance_changes are written when the account listener
receives a balanceUpdate event from the broker; this script calls write_balance_change
directly (same as _on_balance_change in main.py) then checks the table.

Prereqs: (1) alembic upgrade head (so balance_changes.account_id is TEXT),
         (2) DATABASE_URL set.

  python scripts/inject_balance_change_and_check.py
  DATABASE_URL=postgres://... python scripts/inject_balance_change_and_check.py
"""

import os
import sys

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

try:
    import dotenv
    dotenv.load_dotenv(os.path.join(repo_root, ".env"))
except ImportError:
    pass

# OMS path used by the service when balanceUpdate events arrive
from oms.account_sync import write_balance_change
import psycopg2
from psycopg2.extras import RealDictCursor


def main():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    account_id = "test_balance_acc"
    asset = "USDT"
    broker = "binance"
    delta = 100
    event_time = 1704067200000  # ms

    print("Injecting one balance_change via write_balance_change(account_id=%r, broker=%r, ...)" % (account_id, broker))
    write_balance_change(
        database_url,
        account_id=account_id,
        asset=asset,
        change_type="deposit",
        delta=delta,
        event_type="balanceUpdate",
        event_time=event_time,
        payload={"e": "balanceUpdate", "a": asset, "d": str(delta), "E": event_time},
        book="default",
        broker=broker,
    )
    print("Insert done. Checking downstream balance_changes table...")

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, account_id, asset, book, broker, change_type, delta, event_type
                FROM balance_changes
                WHERE account_id = %s AND asset = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (account_id, asset),
            )
            row = cur.fetchone()
        if not row:
            print("FAIL: No row found in balance_changes for account_id=%r asset=%r" % (account_id, asset), file=sys.stderr)
            sys.exit(1)
        # account_id must be TEXT (broker id), not integer
        if row["account_id"] != account_id:
            print("FAIL: account_id mismatch: got %r (type %s), expected %r" % (row["account_id"], type(row["account_id"]).__name__, account_id), file=sys.stderr)
            sys.exit(1)
        if row["asset"] != asset or row["change_type"] != "deposit" or float(row["delta"]) != float(delta):
            print("FAIL: row data mismatch:", row, file=sys.stderr)
            sys.exit(1)
        if row["book"] != "default":
            print("FAIL: book expected 'default', got", row["book"], file=sys.stderr)
            sys.exit(1)
        if row.get("broker") != broker:
            print("FAIL: broker expected %r, got %r" % (broker, row.get("broker")), file=sys.stderr)
            sys.exit(1)
        print("OK: balance_changes row verified: account_id=%r (TEXT), asset=%r, book=%r, broker=%r, change_type=%r, delta=%s" % (
            row["account_id"], row["asset"], row["book"], row.get("broker"), row["change_type"], row["delta"]))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
