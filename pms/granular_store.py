"""
PMS granular store writes (12.3.4).

UPSERT into positions table at grain (broker, account_id, book, asset).
"""

from datetime import datetime, timezone
from typing import Any, Callable, List, Union

from pms.schemas_pydantic import DerivedPosition


def write_pms_positions(
    pg_connect: Union[str, Callable[[], Any]],
    positions: List[DerivedPosition],
) -> int:
    """
    UPSERT derived positions into positions table. Grain (broker, account_id, book, asset).
    Uses position.broker and position.asset.
    Returns number of rows upserted.
    """
    if not positions:
        return 0

    if callable(pg_connect):
        conn = pg_connect()
    else:
        import psycopg2
        conn = psycopg2.connect(pg_connect)

    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc)
        count = 0
        for p in positions:
            broker_val = (p.broker or "").strip()
            asset_val = (p.asset or "").strip()
            cur.execute(
                """
                INSERT INTO positions (
                    broker, account_id, book, asset, open_qty, position_side,
                    usd_price, updated_at
                ) VALUES (
                    %(broker)s, %(account_id)s, %(book)s, %(asset)s, %(open_qty)s, %(position_side)s,
                    %(usd_price)s, %(updated_at)s
                )
                ON CONFLICT (broker, account_id, book, asset) DO UPDATE SET
                    open_qty = EXCLUDED.open_qty,
                    position_side = EXCLUDED.position_side,
                    usd_price = EXCLUDED.usd_price,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "broker": broker_val,
                    "account_id": p.account_id,
                    "book": p.book or "",
                    "asset": asset_val,
                    "open_qty": p.open_qty,
                    "position_side": p.position_side,
                    "usd_price": p.usd_price,
                    "updated_at": now,
                },
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()
