"""
PMS granular store writes (12.3.4).

UPSERT into positions table at grain (account_id, book, symbol).
"""

from datetime import datetime, timezone
from typing import Any, Callable, List, Union

from pms.schemas_pydantic import DerivedPosition


def write_pms_positions(
    pg_connect: Union[str, Callable[[], Any]],
    positions: List[DerivedPosition],
) -> int:
    """
    UPSERT derived positions into positions table. Grain (account_id, book, symbol).
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
            cur.execute(
                """
                INSERT INTO positions (
                    account_id, book, symbol, open_qty, position_side,
                    entry_avg, mark_price, notional, unrealized_pnl, updated_at
                ) VALUES (
                    %(account_id)s, %(book)s, %(symbol)s, %(open_qty)s, %(position_side)s,
                    %(entry_avg)s, %(mark_price)s, %(notional)s, %(unrealized_pnl)s, %(updated_at)s
                )
                ON CONFLICT (account_id, book, symbol) DO UPDATE SET
                    open_qty = EXCLUDED.open_qty,
                    position_side = EXCLUDED.position_side,
                    entry_avg = EXCLUDED.entry_avg,
                    mark_price = EXCLUDED.mark_price,
                    notional = EXCLUDED.notional,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    updated_at = EXCLUDED.updated_at
                """,
                {
                    "account_id": p.account_id,
                    "book": p.book or "",
                    "symbol": p.symbol,
                    "open_qty": p.open_qty,
                    "position_side": p.position_side,
                    "entry_avg": p.entry_avg,
                    "mark_price": p.mark_price,
                    "notional": p.notional,
                    "unrealized_pnl": p.unrealized_pnl,
                    "updated_at": now,
                },
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()
