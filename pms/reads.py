"""
PMS Postgres reads and position derivation (12.3.3).

Query orders (filter partially_filled, filled); use executed_qty as executed_quantity.
Derive positions at grain (account_id, book, symbol) with signed open_qty, position_side, entry_avg (FIFO).
"""

from collections import defaultdict
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union

from pms.schemas_pydantic import DerivedPosition, OrderRow


# Statuses that contribute executed quantity to position
POSITION_ORDER_STATUSES = ("partially_filled", "filled")


def query_orders_for_positions(
    pg_connect: Union[str, Callable[[], Any]],
) -> List[Dict[str, Any]]:
    """
    Query orders that contribute to position (partially_filled, filled).
    Returns rows with account_id, book, symbol, side, executed_qty (as executed_quantity), price, created_at.
    """
    if callable(pg_connect):
        conn = pg_connect()
    else:
        import psycopg2
        conn = psycopg2.connect(pg_connect)

    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT account_id, book, symbol, side, executed_qty, price, created_at
            FROM orders
            WHERE status IN %s AND COALESCE(executed_qty, 0) > 0
            ORDER BY created_at ASC NULLS LAST
            """,
            (POSITION_ORDER_STATUSES,),
        )
        columns = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(columns, r))
            # Expose executed_qty as executed_quantity for position derivation
            row["executed_quantity"] = float(row.pop("executed_qty", 0) or 0)
            rows.append(row)
        return rows
    finally:
        conn.close()


def _fifo_open_qty_and_entry_avg(
    lots: List[tuple],
) -> tuple:
    """
    lots: list of (signed_qty, price) in time order. signed_qty > 0 = buy, < 0 = sell.
    Returns (open_qty_signed, entry_avg or None). Entry_avg = cost basis of open quantity only (FIFO).
    """
    if not lots:
        return (0.0, None)
    # FIFO: long_lots = [(qty, price)]; sells consume from front of long_lots; leftover sells = short_lots.
    long_lots: List[tuple] = []
    short_lots: List[tuple] = []
    for (signed_qty, price) in lots:
        p = float(price or 0)
        if signed_qty > 0:
            long_lots.append((signed_qty, p))
        else:
            q = -signed_qty
            while q > 1e-15 and long_lots:
                take = min(q, long_lots[0][0])
                long_lots[0] = (long_lots[0][0] - take, long_lots[0][1])
                if long_lots[0][0] < 1e-15:
                    long_lots.pop(0)
                q -= take
            if q > 1e-15:
                short_lots.append((q, p))
    long_remaining = sum(x[0] for x in long_lots)
    short_remaining = sum(x[0] for x in short_lots)
    open_qty = long_remaining - short_remaining
    if abs(open_qty) < 1e-15:
        return (0.0, None)
    if open_qty > 0:
        total = sum(x[0] * x[1] for x in long_lots)
        total_q = sum(x[0] for x in long_lots)
        entry_avg = total / total_q if total_q else None
    else:
        total = sum(x[0] * x[1] for x in short_lots)
        total_q = sum(x[0] for x in short_lots)
        entry_avg = total / total_q if total_q else None
    return (open_qty, entry_avg)


def derive_positions_from_orders(
    order_rows: List[Union[Dict[str, Any], OrderRow]],
) -> List[DerivedPosition]:
    """
    Derive positions at grain (account_id, book, symbol) from order rows.
    Uses executed_quantity (from orders.executed_qty). FIFO for entry_avg (cost basis of open only).
    Returns list of DerivedPosition with open_qty (signed), position_side, entry_avg.
    """
    # Normalize to dicts with executed_quantity
    rows = []
    for r in order_rows:
        if isinstance(r, OrderRow):
            rows.append({
                "account_id": r.account_id or "",
                "book": r.book or "",
                "symbol": (r.symbol or "").strip().upper(),
                "side": (r.side or "").strip().upper(),
                "executed_quantity": r.executed_quantity,
                "price": r.price,
                "created_at": r.created_at,
            })
        else:
            q = float((r.get("executed_quantity") or r.get("executed_qty") or 0))
            rows.append({
                "account_id": (r.get("account_id") or ""),
                "book": (r.get("book") or ""),
                "symbol": (r.get("symbol") or "").strip().upper(),
                "side": (r.get("side") or "").strip().upper(),
                "executed_quantity": q,
                "price": r.get("price"),
                "created_at": r.get("created_at"),
            })

    # Group by (account_id, book, symbol); collect lots in time order
    key_to_lots: Dict[tuple, List[tuple]] = defaultdict(list)
    for r in rows:
        if not r["executed_quantity"] or not r["symbol"]:
            continue
        key = (r["account_id"], r["book"] or "", r["symbol"])
        signed = r["executed_quantity"] if (r["side"] == "BUY") else -r["executed_quantity"]
        key_to_lots[key].append((signed, r["price"]))

    out: List[DerivedPosition] = []
    for (account_id, book, symbol), lots in key_to_lots.items():
        open_qty, entry_avg = _fifo_open_qty_and_entry_avg(lots)
        if open_qty > 0:
            position_side = "long"
        elif open_qty < 0:
            position_side = "short"
        else:
            position_side = "flat"
        entry_f = float(entry_avg) if entry_avg is not None else None
        out.append(DerivedPosition(
            account_id=account_id,
            book=book,
            symbol=symbol,
            open_qty=open_qty,
            position_side=position_side,
            entry_avg=entry_f,
            notional=None,
            unrealized_pnl=0.0,
        ))
    return out


def query_balances(
    pg_connect: Union[str, Callable[[], Any]],
    account_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    Query balances from Postgres. account_ids: optional list of accounts.id (bigint).
    Returns list of dicts with account_id, asset, available, locked, updated_at.
    """
    if callable(pg_connect):
        conn = pg_connect()
    else:
        import psycopg2
        conn = psycopg2.connect(pg_connect)

    try:
        cur = conn.cursor()
        if account_ids:
            cur.execute(
                """
                SELECT b.account_id, b.asset, b.available, b.locked, b.updated_at
                FROM balances b
                WHERE b.account_id = ANY(%s)
                """,
                (account_ids,),
            )
        else:
            cur.execute(
                """
                SELECT account_id, asset, available, locked, updated_at
                FROM balances
                """
            )
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, r)) for r in cur.fetchall()]
    finally:
        conn.close()
