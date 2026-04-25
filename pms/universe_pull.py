"""PMS periodic universe pull + admission filter.

Universe ownership: market_data.universe_assets (asset-only).
Tradability mapping: oms.symbols (base_asset/quote_asset/broker -> symbol).
PMS admission: only admit assets that are tradable AND pricing-resolvable for the configured feed source.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence, Union

from pgconn import SCHEMA_MARKET_DATA, SCHEMA_OMS, configure_for_pms


@dataclass(frozen=True)
class PmsUniversePullResult:
    bases_seen: int
    bases_admitted: int
    assets_upserted: int
    skipped: dict[str, str]  # base_asset -> reason


def _pg_conn(pg_connect: Union[str, Callable[[], Any]]):
    if callable(pg_connect):
        conn = pg_connect()
        configure_for_pms(conn)
        return conn, False
    import psycopg2

    conn = psycopg2.connect(pg_connect)
    configure_for_pms(conn)
    return conn, True


def query_universe_base_assets(
    pg_connect: Union[str, Callable[[], Any]],
    *,
    mode: str = "ever_seen",
) -> list[str]:
    m = (mode or "ever_seen").strip()
    where = "was_ever_top100 = true"
    if m == "current_only":
        where = "is_current_top100 = true"
    conn, we_opened = _pg_conn(pg_connect)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT base_asset
                FROM {SCHEMA_MARKET_DATA}.universe_assets
                WHERE {where}
                ORDER BY base_asset ASC
                """
            )
            return [str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]]
    finally:
        if we_opened:
            conn.close()


def resolve_tradable_symbols_for_bases(
    pg_connect: Union[str, Callable[[], Any]],
    *,
    base_assets: Sequence[str],
    quote_asset: str = "USDT",
    broker: str = "binance",
) -> dict[str, str]:
    bases = sorted({(b or "").strip().upper() for b in base_assets if (b or "").strip()})
    if not bases:
        return {}
    q = (quote_asset or "USDT").strip().upper()
    br = (broker or "binance").strip().lower()
    conn, we_opened = _pg_conn(pg_connect)
    try:
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
            out: dict[str, str] = {}
            for base, symbol in cur.fetchall():
                b = (base or "").strip().upper()
                s = (symbol or "").strip().upper()
                if b and s and b not in out:
                    out[b] = s
            return out
    finally:
        if we_opened:
            conn.close()


def _pricing_resolvable(feed_source: str, *, base_asset: str, symbol: str) -> tuple[bool, str | None]:
    src = (feed_source or "").strip().lower()
    if not src:
        return False, "price_feed_disabled"
    if src == "binance":
        return True, None
    if src == "ohlcv_db":
        # Requires usd_symbol to read market_data.ohlcv. If we have a symbol mapping, we can set usd_symbol.
        return True, None
    return False, f"unknown_feed_source:{src}"


def compute_pms_admissions(
    base_assets: Iterable[str],
    *,
    base_to_symbol: dict[str, str],
    feed_source: str,
) -> tuple[list[str], dict[str, str]]:
    admitted: list[str] = []
    skipped: dict[str, str] = {}
    for b in base_assets:
        base = (b or "").strip().upper()
        if not base:
            continue
        sym = base_to_symbol.get(base)
        if not sym:
            skipped[base] = "no_tradable_symbol"
            continue
        ok, reason = _pricing_resolvable(feed_source, base_asset=base, symbol=sym)
        if not ok:
            skipped[base] = reason or "not_pricing_resolvable"
            continue
        admitted.append(base)
    return admitted, skipped


def upsert_assets_usd_symbols(
    pg_connect: Union[str, Callable[[], Any]],
    *,
    admitted_bases: Sequence[str],
    base_to_symbol: dict[str, str],
) -> int:
    if not admitted_bases:
        return 0
    conn, we_opened = _pg_conn(pg_connect)
    try:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        count = 0
        with conn.cursor() as cur:
            for base in admitted_bases:
                sym = base_to_symbol.get(base)
                if not sym:
                    continue
                cur.execute(
                    """
                    INSERT INTO assets (asset, usd_symbol, updated_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (asset) DO UPDATE SET
                        usd_symbol = EXCLUDED.usd_symbol,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (base, sym, now),
                )
                count += 1
        conn.commit()
        return count
    finally:
        if we_opened:
            conn.close()


def run_pms_universe_pull(
    pg_connect: Union[str, Callable[[], Any]],
    *,
    mode: str,
    quote_asset: str,
    broker: str,
    feed_source: str,
) -> tuple[list[str], PmsUniversePullResult]:
    bases = query_universe_base_assets(pg_connect, mode=mode)
    mapping = resolve_tradable_symbols_for_bases(
        pg_connect,
        base_assets=bases,
        quote_asset=quote_asset,
        broker=broker,
    )
    admitted, skipped = compute_pms_admissions(bases, base_to_symbol=mapping, feed_source=feed_source)
    upserted = upsert_assets_usd_symbols(pg_connect, admitted_bases=admitted, base_to_symbol=mapping)
    return admitted, PmsUniversePullResult(
        bases_seen=len(bases),
        bases_admitted=len(admitted),
        assets_upserted=upserted,
        skipped=skipped,
    )

