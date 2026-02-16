"""
OMS order repairs: fix flawed Postgres order fields from payload.

Repairs are isolated (one per field) and run only for broker='binance'.
Scheduled periodically alongside sync in the OMS main loop.
"""

import json
from typing import Any, Callable, Dict, Optional, Union

from oms.log import logger


def _get_conn(pg_connect: Union[str, Callable[[], Any]]) -> Any:
    """Open connection from pg_connect (string or callable)."""
    if callable(pg_connect):
        return pg_connect()
    import psycopg2
    return psycopg2.connect(pg_connect)


def _extract_from_binance_payload(payload: Any) -> Dict[str, Any]:
    """
    Extract price, time_in_force, binance_cumulative_quote_qty from Binance payload.

    Payload shape: {"binance": {...}} with avgPrice, timeInForce, cumulativeQuoteQty,
    fills[0].price. Returns dict with only keys that have valid values.
    """
    out: Dict[str, Any] = {}
    if not isinstance(payload, dict):
        return out
    # Handle payload stored as JSON string (Redis store serializes it)
    if isinstance(payload.get("binance"), str):
        try:
            payload = {"binance": json.loads(payload["binance"])}
        except (json.JSONDecodeError, TypeError):
            return out
    binance = payload.get("binance")
    try:
        if isinstance(binance, dict):
            # Price: avgPrice, fills[0].price
            avg = binance.get("avgPrice")
            if avg is not None and str(avg).strip():
                out["price"] = float(avg)
            else:
                fills = binance.get("fills")
                if isinstance(fills, list) and fills:
                    first = fills[0]
                    if isinstance(first, dict):
                        p = first.get("price")
                        if p is not None and str(p).strip():
                            out["price"] = float(p)
            # Time in force
            tif = binance.get("timeInForce")
            if tif is not None and str(tif).strip():
                out["time_in_force"] = str(tif).strip()[:16]
            # Cumulative quote qty
            cq = binance.get("cumulativeQuoteQty")
            if cq is not None and str(cq).strip():
                try:
                    out["binance_cumulative_quote_qty"] = float(cq)
                except (TypeError, ValueError):
                    pass
        # Price fallback: payload.fill.price
        if "price" not in out:
            fill = payload.get("fill")
            if isinstance(fill, dict):
                p = fill.get("price")
                if p is not None and str(p).strip():
                    out["price"] = float(p)
    except (TypeError, ValueError):
        pass
    return out


def repair_binance_price_from_payload(
    pg_connect: Union[str, Callable[[], Any]],
) -> int:
    """
    Fix price (executed) for Binance orders where price is NULL/0 and payload has it.
    Returns number of rows updated.
    """
    conn = _get_conn(pg_connect)
    we_opened = not callable(pg_connect)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT internal_id, payload FROM orders
            WHERE broker = 'binance'
              AND (price IS NULL OR price = 0)
              AND payload IS NOT NULL
            """
        )
        rows = cur.fetchall()
        updated = 0
        for internal_id, payload in rows:
            vals = _extract_from_binance_payload(payload)
            price = vals.get("price")
            if price is not None and isinstance(price, (int, float)) and price > 0:
                cur.execute(
                    "UPDATE orders SET price = %s WHERE internal_id = %s",
                    (float(price), internal_id),
                )
                updated += cur.rowcount
        conn.commit()
        if updated:
            logger.info("repair_binance_price_from_payload: updated {} order(s)", updated)
        return updated
    finally:
        if we_opened:
            conn.close()


def repair_binance_time_in_force_from_payload(
    pg_connect: Union[str, Callable[[], Any]],
) -> int:
    """
    Fix time_in_force for Binance orders where it is NULL/empty and payload has it.
    Returns number of rows updated.
    """
    conn = _get_conn(pg_connect)
    we_opened = not callable(pg_connect)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT internal_id, payload FROM orders
            WHERE broker = 'binance'
              AND (time_in_force IS NULL OR time_in_force = '')
              AND payload IS NOT NULL
            """
        )
        rows = cur.fetchall()
        updated = 0
        for internal_id, payload in rows:
            vals = _extract_from_binance_payload(payload)
            tif = vals.get("time_in_force")
            if tif and isinstance(tif, str) and tif.strip():
                cur.execute(
                    "UPDATE orders SET time_in_force = %s WHERE internal_id = %s",
                    (tif.strip()[:16], internal_id),
                )
                updated += cur.rowcount
        conn.commit()
        if updated:
            logger.info(
                "repair_binance_time_in_force_from_payload: updated {} order(s)",
                updated,
            )
        return updated
    finally:
        if we_opened:
            conn.close()


def repair_binance_cumulative_quote_qty_from_payload(
    pg_connect: Union[str, Callable[[], Any]],
) -> int:
    """
    Fix binance_cumulative_quote_qty for Binance orders where it is NULL and payload has it.
    Returns number of rows updated.
    """
    conn = _get_conn(pg_connect)
    we_opened = not callable(pg_connect)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT internal_id, payload FROM orders
            WHERE broker = 'binance'
              AND binance_cumulative_quote_qty IS NULL
              AND payload IS NOT NULL
            """
        )
        rows = cur.fetchall()
        updated = 0
        for internal_id, payload in rows:
            vals = _extract_from_binance_payload(payload)
            cq = vals.get("binance_cumulative_quote_qty")
            if cq is not None and isinstance(cq, (int, float)):
                cur.execute(
                    "UPDATE orders SET binance_cumulative_quote_qty = %s WHERE internal_id = %s",
                    (float(cq), internal_id),
                )
                updated += cur.rowcount
        conn.commit()
        if updated:
            logger.info(
                "repair_binance_cumulative_quote_qty_from_payload: updated {} order(s)",
                updated,
            )
        return updated
    finally:
        if we_opened:
            conn.close()


def run_all_repairs(pg_connect: Union[str, Callable[[], Any]]) -> int:
    """
    Run all order repairs for Binance orders. Call from OMS loop alongside sync.
    Returns total number of rows updated across all repairs.
    """
    total = 0
    repairs = [
        repair_binance_price_from_payload,
        repair_binance_time_in_force_from_payload,
        repair_binance_cumulative_quote_qty_from_payload,
    ]
    for repair_fn in repairs:
        try:
            n = repair_fn(pg_connect)
            total += n
        except Exception as e:
            logger.warning("repair {} failed: {}", repair_fn.__name__, e)
    return total
