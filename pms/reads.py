"""
PMS Postgres reads and position derivation (12.3.3).

Query orders (filter partially_filled, filled); use executed_qty as executed_quantity.
Derive positions at grain (account_id, book, symbol) with signed open_qty, position_side, entry_avg (FIFO).
"""

from collections import defaultdict
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from pms.schemas_pydantic import DerivedPosition, OrderRow

try:
    from pms.log import logger
except ImportError:
    logger = None  # allow tests to run without pms.log


# Statuses that contribute executed quantity to position
POSITION_ORDER_STATUSES = ("partially_filled", "filled")


def query_symbol_map(
    pg_connect: Union[str, Callable[[], Any]],
) -> Dict[str, Tuple[str, str]]:
    """
    Query symbols table and return symbol -> (base_asset, quote_asset).
    Used by order→legs conversion for position derivation. Symbol keys are normalized to uppercase.
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
            SELECT symbol, base_asset, quote_asset
            FROM symbols
            """
        )
        out: Dict[str, Tuple[str, str]] = {}
        for row in cur.fetchall():
            symbol = (row[0] or "").strip().upper()
            base = (row[1] or "").strip()
            quote = (row[2] or "").strip()
            if symbol and base and quote:
                out[symbol] = (base, quote)
        return out
    finally:
        conn.close()


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
            SELECT internal_id, account_id, book, broker, symbol, side, executed_qty, price, created_at,
                   binance_cumulative_quote_qty
            FROM orders
            WHERE LOWER(TRIM(status)) IN ('partially_filled', 'filled') AND COALESCE(executed_qty, 0) > 0
            ORDER BY created_at ASC NULLS LAST
            """,
        )
        columns = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(columns, r))
            # Expose executed_qty as executed_quantity for position derivation
            row["executed_quantity"] = float(row.pop("executed_qty", 0) or 0)
            # Keep binance_cumulative_quote_qty for quote leg (coerce to float when used)
            if "binance_cumulative_quote_qty" in row and row["binance_cumulative_quote_qty"] is not None:
                row["binance_cumulative_quote_qty"] = float(row["binance_cumulative_quote_qty"])
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


def derive_asset_positions_from_orders(
    order_rows: List[Dict[str, Any]],
    symbol_map: Dict[str, Tuple[str, str]],
) -> List[Dict[str, Any]]:
    """
    Derive positions at grain (broker, account_id, book, asset) from order rows using base/quote legs.
    Each order: BUY adds +base_qty to base_asset and -quote_qty to quote_asset; SELL the reverse.
    Uses binance_cumulative_quote_qty when present, else executed_qty * price for quote amount.
    Returns list of dicts with account_id, book, asset, open_qty, position_side, entry_avg.
    Orders whose symbol is not in symbol_map are skipped (no legs posted); a warning is logged (E.1).
    """
    key_to_lots: Dict[tuple, List[tuple]] = defaultdict(list)
    for r in order_rows:
        executed_qty = float(r.get("executed_quantity") or r.get("executed_qty") or 0)
        if executed_qty <= 0:
            continue
        symbol = (r.get("symbol") or "").strip().upper()
        if not symbol or symbol not in symbol_map:
            order_id = r.get("internal_id") or r.get("order_id") or "(unknown)"
            if logger:
                logger.warning(
                    "symbol {!r} not in symbols, skipping legs for order {!r}",
                    symbol or "(empty)",
                    order_id,
                )
            continue
        base_asset, quote_asset = symbol_map[symbol]
        side = (r.get("side") or "").strip().upper()
        price = r.get("price")
        price_f = float(price) if price is not None else 0.0
        broker = (r.get("broker") or "").strip()
        account_id = r.get("account_id") or ""
        book = (r.get("book") or "").strip()

        # Quote amount: prefer broker cumulative quote, else executed_qty * price
        quote_amount_raw = r.get("binance_cumulative_quote_qty")
        if quote_amount_raw is not None:
            quote_amount = float(quote_amount_raw)
        elif price_f and executed_qty:
            quote_amount = executed_qty * price_f
        else:
            quote_amount = 0.0

        if side == "BUY":
            base_signed = executed_qty
            quote_signed = -quote_amount
        else:
            base_signed = -executed_qty
            quote_signed = quote_amount

        # Base leg: (signed_qty, price per base unit for entry_avg)
        key_to_lots[(broker, account_id, book, base_asset)].append((base_signed, price_f))
        # Quote leg: (signed_qty in quote units, 1.0 so entry_avg in quote = 1 for stables)
        key_to_lots[(broker, account_id, book, quote_asset)].append((quote_signed, 1.0))

    out: List[Dict[str, Any]] = []
    for (broker, account_id, book, asset), lots in key_to_lots.items():
        open_qty, entry_avg = _fifo_open_qty_and_entry_avg(lots)
        if open_qty > 0:
            position_side = "long"
        elif open_qty < 0:
            position_side = "short"
        else:
            position_side = "flat"
        entry_f = float(entry_avg) if entry_avg is not None else None
        out.append({
            "broker": broker,
            "account_id": account_id,
            "book": book,
            "asset": asset,
            "open_qty": open_qty,
            "position_side": position_side,
            "entry_avg": entry_f,
        })
    return out


def derive_positions_from_orders(
    order_rows: List[Union[Dict[str, Any], OrderRow]],
) -> List[DerivedPosition]:
    """
    Derive positions at grain (account_id, book, symbol) from order rows.
    Uses executed_quantity (from orders.executed_qty). FIFO for entry_avg (cost basis of open only).
    Returns list of DerivedPosition with open_qty (signed), position_side, entry_avg.
    """
    # Normalize to dicts with executed_quantity and broker
    rows = []
    for r in order_rows:
        if isinstance(r, OrderRow):
            rows.append({
                "account_id": r.account_id or "",
                "book": r.book or "",
                "broker": (r.broker or ""),
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
                "broker": (r.get("broker") or ""),
                "symbol": (r.get("symbol") or "").strip().upper(),
                "side": (r.get("side") or "").strip().upper(),
                "executed_quantity": q,
                "price": r.get("price"),
                "created_at": r.get("created_at"),
            })

    # Group by (broker, account_id, book, symbol); collect lots in time order
    key_to_lots: Dict[tuple, List[tuple]] = defaultdict(list)
    for r in rows:
        if not r["executed_quantity"] or not r["symbol"]:
            continue
        key = (r["broker"], r["account_id"], r["book"] or "", r["symbol"])
        signed = r["executed_quantity"] if (r["side"] == "BUY") else -r["executed_quantity"]
        key_to_lots[key].append((signed, r["price"]))

    out: List[DerivedPosition] = []
    for (broker, account_id, book, symbol), lots in key_to_lots.items():
        open_qty, entry_avg = _fifo_open_qty_and_entry_avg(lots)
        if open_qty > 0:
            position_side = "long"
        elif open_qty < 0:
            position_side = "short"
        else:
            position_side = "flat"
        entry_f = float(entry_avg) if entry_avg is not None else None
        out.append(DerivedPosition(
            broker=broker,
            account_id=account_id,
            book=book,
            asset=symbol,
            open_qty=open_qty,
            position_side=position_side,
        ))
    return out


def query_balance_changes_net_by_account_book_asset(
    pg_connect: Union[str, Callable[[], Any]],
    account_ids: Optional[List[str]] = None,
) -> Dict[Tuple[str, str, str, str], Decimal]:
    """
    Aggregate balance_changes by (broker, account_id, book, asset). No join to accounts;
    balance_changes.account_id is broker account id (TEXT, same as orders).
    Includes change_type IN (deposit, withdrawal, adjustment, transfer); excludes snapshot.
    Returns dict (broker, account_id, book, asset) -> net delta (positive = more cash/position).
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
                SELECT broker, account_id, book, asset, SUM(delta) AS net_delta
                FROM balance_changes
                WHERE change_type IN ('deposit', 'withdrawal', 'adjustment', 'transfer')
                  AND account_id = ANY(%s)
                GROUP BY broker, account_id, book, asset
                """,
                (account_ids,),
            )
        else:
            cur.execute(
                """
                SELECT broker, account_id, book, asset, SUM(delta) AS net_delta
                FROM balance_changes
                WHERE change_type IN ('deposit', 'withdrawal', 'adjustment', 'transfer')
                GROUP BY broker, account_id, book, asset
                """
            )
        out: Dict[Tuple[str, str, str, str], Decimal] = {}
        for row in cur.fetchall():
            broker = (row[0] or "").strip()
            acc = (row[1] or "").strip()
            book = (row[2] or "").strip()
            asset = (row[3] or "").strip()
            net = row[4]
            if net is not None:
                try:
                    out[(broker, acc, book, asset)] = Decimal(str(net))
                except (TypeError, ValueError):
                    out[(broker, acc, book, asset)] = Decimal("0")
        return out
    finally:
        conn.close()


def derive_positions_from_orders_and_balance_changes(
    pg_connect: Union[str, Callable[[], Any]],
) -> List[DerivedPosition]:
    """
    Derive positions at grain (broker, account_id, book, asset) from orders (base/quote legs)
    plus balance_changes (deposit/withdrawal). Final open_qty = order_derived_open_qty
    + balance_changes_net_delta per (broker, account_id, book, asset). entry_avg from orders only
    (FIFO); balance_changes do not affect entry_avg. Returns List[DerivedPosition] with
    Returns List[DerivedPosition] with asset set for use with asset-grain positions table.
    """
    order_rows = query_orders_for_positions(pg_connect)
    symbol_map = query_symbol_map(pg_connect)
    order_positions = derive_asset_positions_from_orders(order_rows, symbol_map)
    bc_net = query_balance_changes_net_by_account_book_asset(pg_connect)

    # Key -> order open_qty, entry_avg (key = broker, account_id, book, asset)
    key_to_order: Dict[Tuple[str, str, str, str], tuple] = {}
    for p in order_positions:
        key = (p.get("broker") or "", p["account_id"], p["book"] or "", p["asset"])
        key_to_order[key] = (p["open_qty"], p.get("entry_avg"))

    # All keys: from orders and from balance_changes (so we get positions that are only deposits)
    all_keys: set = set(key_to_order.keys()) | set(bc_net.keys())

    out: List[DerivedPosition] = []
    for (broker, account_id, book, asset) in all_keys:
        order_qty, entry_avg = key_to_order.get((broker, account_id, book, asset), (0.0, None))
        delta = bc_net.get((broker, account_id, book, asset), Decimal("0"))
        try:
            delta_f = float(delta)
        except (TypeError, ValueError):
            delta_f = 0.0
        final_open_qty = order_qty + delta_f
        if final_open_qty > 0:
            position_side = "long"
        elif final_open_qty < 0:
            position_side = "short"
        else:
            position_side = "flat"
        entry_f = float(entry_avg) if entry_avg is not None else None
        out.append(DerivedPosition(
            broker=broker,
            account_id=account_id,
            book=book,
            asset=asset,
            open_qty=final_open_qty,
            position_side=position_side,
        ))
    return out


def query_assets_usd_config(
    pg_connect: Union[str, Callable[[], Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Read assets table and return per-asset USD config: usd_price (fixed) or usd_symbol (to fetch).
    Returns dict asset -> {"usd_price": Decimal | None, "usd_symbol": str | None}.
    Used for position USD valuation: stables use usd_price; others (later) use usd_symbol + mark provider.
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
            SELECT asset, usd_symbol, usd_price
            FROM assets
            """
        )
        out: Dict[str, Dict[str, Any]] = {}
        for row in cur.fetchall():
            asset = (row[0] or "").strip()
            if not asset:
                continue
            usd_sym = (row[1] or "").strip() or None
            usd_pr = row[2]
            if usd_pr is not None:
                try:
                    usd_pr = Decimal(str(usd_pr))
                except (TypeError, ValueError):
                    usd_pr = None
            out[asset] = {"usd_price": usd_pr, "usd_symbol": usd_sym}
        return out
    finally:
        conn.close()


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
