"""Resolve tradable symbols for base assets from OMS symbols table.

Market-data home schema uses search_path=market_data, but OMS tables must be fully qualified.
"""

from __future__ import annotations

from typing import Mapping, Sequence

from psycopg2.extensions import connection as PsycopgConnection

from pgconn import SCHEMA_OMS


def resolve_binance_usdt_spot_symbols_for_bases(
    conn: PsycopgConnection,
    *,
    base_assets: Sequence[str],
    quote_asset: str = "USDT",
    broker: str = "binance",
) -> Mapping[str, str]:
    """
    Return mapping base_asset -> symbol for rows in `oms.symbols` matching
    (broker, quote_asset). If multiple symbols exist for a base, an arbitrary one
    is returned (Binance spot generally has one BASEUSDT).
    """
    bases = sorted({(b or "").strip().upper() for b in base_assets if (b or "").strip()})
    if not bases:
        return {}
    q = (quote_asset or "USDT").strip().upper()
    br = (broker or "binance").strip().lower()
    if not q or not br:
        return {}

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT base_asset, symbol
            FROM {SCHEMA_OMS}.symbols
            WHERE LOWER(TRIM(broker)) = %s
              AND TRIM(UPPER(quote_asset)) = %s
              AND TRIM(base_asset) = ANY(%s)
            """,
            (br, q, bases),
        )
        rows = cur.fetchall()

    out: dict[str, str] = {}
    for base, symbol in rows:
        b = (base or "").strip().upper()
        s = (symbol or "").strip().upper()
        if b and s and b not in out:
            out[b] = s
    return out

