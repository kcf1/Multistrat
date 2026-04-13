"""
Postgres OHLCV writes and ingestion cursors (Phase 4 §9.4).

- **Upsert:** ``ON CONFLICT (symbol, interval, open_time)`` updates OHLCV fields and
  ``ingested_at`` (corrections / re-ingest).
- **Cursor:** ``ingestion_cursor.last_open_time`` is an **inclusive** high-water mark
  of bar ``open_time`` values successfully committed for that series. Jobs may set
  it after commit; if absent, use :func:`max_open_time_ohlcv` to derive a starting point.

Callers own transactions: these functions **do not** ``commit``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from psycopg2.extensions import connection as PsycopgConnection
from psycopg2.extras import execute_values

from market_data.schemas import (
    BasisPoint,
    OhlcvBar,
    OpenInterestPoint,
    TakerBuySellVolumePoint,
    TopTraderLongShortPoint,
)

_OHLCV_UPSERT_SQL = """
INSERT INTO ohlcv (
    symbol, interval, open_time, open, high, low, close, volume,
    quote_volume, taker_buy_base_volume, taker_buy_quote_volume, trades, close_time
) VALUES %s
ON CONFLICT (symbol, interval, open_time) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    quote_volume = EXCLUDED.quote_volume,
    taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
    taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
    trades = EXCLUDED.trades,
    close_time = EXCLUDED.close_time,
    ingested_at = now()
"""

_BASIS_UPSERT_SQL = """
INSERT INTO basis_rate (
    pair, contract_type, period, sample_time,
    basis, basis_rate, futures_price, index_price
) VALUES %s
ON CONFLICT (pair, contract_type, period, sample_time) DO UPDATE SET
    basis = EXCLUDED.basis,
    basis_rate = EXCLUDED.basis_rate,
    futures_price = EXCLUDED.futures_price,
    index_price = EXCLUDED.index_price,
    ingested_at = now()
"""

_OPEN_INTEREST_UPSERT_SQL = """
INSERT INTO open_interest (
    symbol, contract_type, period, sample_time,
    sum_open_interest, sum_open_interest_value, cmc_circulating_supply
) VALUES %s
ON CONFLICT (symbol, contract_type, period, sample_time) DO UPDATE SET
    sum_open_interest = EXCLUDED.sum_open_interest,
    sum_open_interest_value = EXCLUDED.sum_open_interest_value,
    cmc_circulating_supply = EXCLUDED.cmc_circulating_supply,
    ingested_at = now()
"""

_TAKER_BUYSELL_VOLUME_UPSERT_SQL = """
INSERT INTO taker_buy_sell_volume (
    symbol, period, sample_time,
    buy_sell_ratio, buy_vol, sell_vol
) VALUES %s
ON CONFLICT (symbol, period, sample_time) DO UPDATE SET
    buy_sell_ratio = EXCLUDED.buy_sell_ratio,
    buy_vol = EXCLUDED.buy_vol,
    sell_vol = EXCLUDED.sell_vol,
    ingested_at = now()
"""

_TOP_TRADER_LONG_SHORT_UPSERT_SQL = """
INSERT INTO top_trader_long_short (
    symbol, period, sample_time,
    long_short_position_ratio, long_account_ratio, short_account_ratio
) VALUES %s
ON CONFLICT (symbol, period, sample_time) DO UPDATE SET
    long_short_position_ratio = EXCLUDED.long_short_position_ratio,
    long_account_ratio = EXCLUDED.long_account_ratio,
    short_account_ratio = EXCLUDED.short_account_ratio,
    ingested_at = now()
"""


def _bar_row(b: OhlcvBar) -> tuple[Any, ...]:
    return (
        b.symbol,
        b.interval,
        b.open_time,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,
        b.quote_volume,
        b.taker_buy_base_volume,
        b.taker_buy_quote_volume,
        b.trades,
        b.close_time,
    )


def _basis_row(p: BasisPoint) -> tuple[Any, ...]:
    return (
        p.pair,
        p.contract_type,
        p.period,
        p.sample_time,
        p.basis,
        p.basis_rate,
        p.futures_price,
        p.index_price,
    )


def _open_interest_row(p: OpenInterestPoint) -> tuple[Any, ...]:
    return (
        p.symbol,
        p.contract_type,
        p.period,
        p.sample_time,
        p.sum_open_interest,
        p.sum_open_interest_value,
        p.cmc_circulating_supply,
    )


def _taker_buy_sell_volume_row(p: TakerBuySellVolumePoint) -> tuple[Any, ...]:
    return (
        p.symbol,
        p.period,
        p.sample_time,
        p.buy_sell_ratio,
        p.buy_vol,
        p.sell_vol,
    )


def _top_trader_long_short_row(p: TopTraderLongShortPoint) -> tuple[Any, ...]:
    return (
        p.symbol,
        p.period,
        p.sample_time,
        p.long_short_ratio,
        p.long_account_ratio,
        p.short_account_ratio,
    )


def upsert_ohlcv_bars(conn: PsycopgConnection, bars: Sequence[OhlcvBar]) -> int:
    """
    Bulk upsert OHLCV rows. Does not commit.

    Returns the number of rows passed (PostgreSQL may count inserts + updates as one batch).
    """
    if not bars:
        return 0
    rows = [_bar_row(b) for b in bars]
    with conn.cursor() as cur:
        execute_values(
            cur,
            _OHLCV_UPSERT_SQL,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            page_size=min(500, len(rows)),
        )
    return len(rows)


def upsert_basis_points(conn: PsycopgConnection, points: Sequence[BasisPoint]) -> int:
    """
    Bulk upsert basis rows. Does not commit.

    Returns the number of rows passed.
    """
    if not points:
        return 0
    rows = [_basis_row(p) for p in points]
    with conn.cursor() as cur:
        execute_values(
            cur,
            _BASIS_UPSERT_SQL,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s)",
            page_size=min(500, len(rows)),
        )
    return len(rows)


def upsert_open_interest_points(
    conn: PsycopgConnection,
    points: Sequence[OpenInterestPoint],
) -> int:
    """
    Bulk upsert open-interest rows. Does not commit.

    Returns the number of rows passed.
    """
    if not points:
        return 0
    rows = [_open_interest_row(p) for p in points]
    with conn.cursor() as cur:
        execute_values(
            cur,
            _OPEN_INTEREST_UPSERT_SQL,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s)",
            page_size=min(500, len(rows)),
        )
    return len(rows)


def upsert_taker_buy_sell_volume_points(
    conn: PsycopgConnection,
    points: Sequence[TakerBuySellVolumePoint],
) -> int:
    """
    Bulk upsert taker buy/sell volume rows. Does not commit.

    Returns the number of rows passed.
    """
    if not points:
        return 0
    rows = [_taker_buy_sell_volume_row(p) for p in points]
    with conn.cursor() as cur:
        execute_values(
            cur,
            _TAKER_BUYSELL_VOLUME_UPSERT_SQL,
            rows,
            template="(%s, %s, %s, %s, %s, %s)",
            page_size=min(500, len(rows)),
        )
    return len(rows)


def upsert_top_trader_long_short_points(
    conn: PsycopgConnection,
    points: Sequence[TopTraderLongShortPoint],
) -> int:
    """
    Bulk upsert top-trader long/short ratio rows. Does not commit.

    Returns the number of rows passed.
    """
    if not points:
        return 0
    rows = [_top_trader_long_short_row(p) for p in points]
    with conn.cursor() as cur:
        execute_values(
            cur,
            _TOP_TRADER_LONG_SHORT_UPSERT_SQL,
            rows,
            template="(%s, %s, %s, %s, %s, %s)",
            page_size=min(500, len(rows)),
        )
    return len(rows)


def fetch_ohlc_by_open_times(
    conn: PsycopgConnection,
    symbol: str,
    interval: str,
    open_times: Sequence[datetime],
) -> Mapping[
    datetime,
    tuple[
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal,
        Decimal | None,
        int | None,
        datetime | None,
        Decimal | None,
        Decimal | None,
    ],
]:
    """
    Return
    ``open_time -> (open, high, low, close, volume, quote_volume, trades, close_time, taker_buy_base_volume, taker_buy_quote_volume)``
    for rows that exist.
    Missing keys are omitted.
    """
    if not open_times:
        return {}
    sym = symbol.strip().upper()
    iv = interval.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                open_time, open, high, low, close, volume, quote_volume, trades, close_time,
                taker_buy_base_volume, taker_buy_quote_volume
            FROM ohlcv
            WHERE symbol = %s AND interval = %s AND open_time = ANY(%s)
            """,
            (sym, iv, list(open_times)),
        )
        rows = cur.fetchall()
    out: dict[
        datetime,
        tuple[
            Decimal,
            Decimal,
            Decimal,
            Decimal,
            Decimal,
            Decimal | None,
            int | None,
            datetime | None,
            Decimal | None,
            Decimal | None,
        ],
    ] = {}
    for open_time, o, h, l, c, v, qv, tr, ct, tbbv, tbqv in rows:
        out[open_time] = (o, h, l, c, v, qv, tr, ct, tbbv, tbqv)
    return out


def count_ohlcv_bars_in_window(
    conn: PsycopgConnection,
    symbol: str,
    interval: str,
    *,
    open_time_ge: datetime,
    open_time_le: datetime,
) -> int:
    """Count rows with ``open_time`` in ``[open_time_ge, open_time_le]`` (inclusive)."""
    n, _, _ = ohlcv_window_stats(conn, symbol, interval, open_time_ge=open_time_ge, open_time_le=open_time_le)
    return n


def ohlcv_window_stats(
    conn: PsycopgConnection,
    symbol: str,
    interval: str,
    *,
    open_time_ge: datetime,
    open_time_le: datetime,
) -> tuple[int, datetime | None, datetime | None]:
    """
    Return ``(row_count, min_open_time, max_open_time)`` for rows in the window (inclusive).

    ``min`` / ``max`` are ``None`` when the count is zero.
    """
    sym = symbol.strip().upper()
    iv = interval.strip()
    if open_time_ge.tzinfo is None or open_time_le.tzinfo is None:
        raise ValueError("open_time bounds must be timezone-aware")
    lo = open_time_ge.astimezone(timezone.utc)
    hi = open_time_le.astimezone(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), MIN(open_time), MAX(open_time) FROM ohlcv
            WHERE symbol = %s AND interval = %s
              AND open_time >= %s AND open_time <= %s
            """,
            (sym, iv, lo, hi),
        )
        row = cur.fetchone()
    if not row:
        return 0, None, None
    n = int(row[0]) if row[0] is not None else 0
    return n, row[1], row[2]


def max_open_time_ohlcv(conn: PsycopgConnection, symbol: str, interval: str) -> datetime | None:
    """Latest ``open_time`` in ``ohlcv`` for the series, or ``None`` if empty."""
    sym = symbol.strip().upper()
    iv = interval.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(open_time) FROM ohlcv
            WHERE symbol = %s AND interval = %s
            """,
            (sym, iv),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def max_sample_time_basis(
    conn: PsycopgConnection,
    pair: str,
    contract_type: str,
    period: str,
) -> datetime | None:
    """Latest ``sample_time`` in ``basis_rate`` for the series, or ``None`` if empty."""
    p = pair.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(sample_time) FROM basis_rate
            WHERE pair = %s AND contract_type = %s AND period = %s
            """,
            (p, ct, pd),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def max_sample_time_open_interest(
    conn: PsycopgConnection,
    symbol: str,
    contract_type: str,
    period: str,
) -> datetime | None:
    """Latest ``sample_time`` in ``open_interest`` for the series, or ``None`` if empty."""
    sym = symbol.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(sample_time) FROM open_interest
            WHERE symbol = %s AND contract_type = %s AND period = %s
            """,
            (sym, ct, pd),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def max_sample_time_taker_buy_sell_volume(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
) -> datetime | None:
    """Latest ``sample_time`` in ``taker_buy_sell_volume`` for the series, or ``None`` if empty."""
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(sample_time) FROM taker_buy_sell_volume
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def max_sample_time_top_trader_long_short(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
) -> datetime | None:
    """Latest ``sample_time`` in ``top_trader_long_short`` for the series, or ``None`` if empty."""
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(sample_time) FROM top_trader_long_short
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return row[0]


def get_ingestion_cursor(conn: PsycopgConnection, symbol: str, interval: str) -> datetime | None:
    """Return stored ``last_open_time`` for the series, or ``None`` if no row."""
    sym = symbol.strip().upper()
    iv = interval.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_open_time FROM ingestion_cursor
            WHERE symbol = %s AND interval = %s
            """,
            (sym, iv),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row[0]


def get_basis_cursor(
    conn: PsycopgConnection,
    pair: str,
    contract_type: str,
    period: str,
) -> datetime | None:
    """Return stored ``last_sample_time`` for basis series, or ``None`` if no row."""
    p = pair.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_sample_time FROM basis_cursor
            WHERE pair = %s AND contract_type = %s AND period = %s
            """,
            (p, ct, pd),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row[0]


def get_open_interest_cursor(
    conn: PsycopgConnection,
    symbol: str,
    contract_type: str,
    period: str,
) -> datetime | None:
    """Return stored ``last_sample_time`` for open-interest series, or ``None`` if no row."""
    sym = symbol.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_sample_time FROM open_interest_cursor
            WHERE symbol = %s AND contract_type = %s AND period = %s
            """,
            (sym, ct, pd),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row[0]


def upsert_ingestion_cursor(
    conn: PsycopgConnection,
    symbol: str,
    interval: str,
    last_open_time: datetime,
) -> None:
    """
    Set or update the ingestion high-water mark. Does not commit.

    ``last_open_time`` must be timezone-aware (UTC recommended).
    """
    if last_open_time.tzinfo is None:
        raise ValueError("last_open_time must be timezone-aware")
    sym = symbol.strip().upper()
    iv = interval.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion_cursor (symbol, interval, last_open_time, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (symbol, interval) DO UPDATE SET
                last_open_time = EXCLUDED.last_open_time,
                updated_at = now()
            """,
            (sym, iv, last_open_time.astimezone(timezone.utc)),
        )


def upsert_basis_cursor(
    conn: PsycopgConnection,
    pair: str,
    contract_type: str,
    period: str,
    last_sample_time: datetime,
) -> None:
    """
    Set or update basis ingestion high-water mark. Does not commit.

    ``last_sample_time`` must be timezone-aware (UTC recommended).
    """
    if last_sample_time.tzinfo is None:
        raise ValueError("last_sample_time must be timezone-aware")
    p = pair.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO basis_cursor (pair, contract_type, period, last_sample_time, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (pair, contract_type, period) DO UPDATE SET
                last_sample_time = EXCLUDED.last_sample_time,
                updated_at = now()
            """,
            (p, ct, pd, last_sample_time.astimezone(timezone.utc)),
        )


def upsert_open_interest_cursor(
    conn: PsycopgConnection,
    symbol: str,
    contract_type: str,
    period: str,
    last_sample_time: datetime,
) -> None:
    """
    Set or update open-interest ingestion high-water mark. Does not commit.

    ``last_sample_time`` must be timezone-aware (UTC recommended).
    """
    if last_sample_time.tzinfo is None:
        raise ValueError("last_sample_time must be timezone-aware")
    sym = symbol.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO open_interest_cursor (
                symbol, contract_type, period, last_sample_time, updated_at
            )
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (symbol, contract_type, period) DO UPDATE SET
                last_sample_time = EXCLUDED.last_sample_time,
                updated_at = now()
            """,
            (sym, ct, pd, last_sample_time.astimezone(timezone.utc)),
        )


def get_taker_buy_sell_volume_cursor(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
) -> datetime | None:
    """Return stored ``last_sample_time`` for taker buy/sell volume series, or ``None`` if no row."""
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_sample_time FROM taker_buy_sell_volume_cursor
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row[0]


def upsert_taker_buy_sell_volume_cursor(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
    last_sample_time: datetime,
) -> None:
    """
    Set or update taker buy/sell volume ingestion high-water mark. Does not commit.

    ``last_sample_time`` must be timezone-aware (UTC recommended).
    """
    if last_sample_time.tzinfo is None:
        raise ValueError("last_sample_time must be timezone-aware")
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO taker_buy_sell_volume_cursor (
                symbol, period, last_sample_time, updated_at
            )
            VALUES (%s, %s, %s, now())
            ON CONFLICT (symbol, period) DO UPDATE SET
                last_sample_time = EXCLUDED.last_sample_time,
                updated_at = now()
            """,
            (sym, pd, last_sample_time.astimezone(timezone.utc)),
        )


def get_top_trader_long_short_cursor(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
) -> datetime | None:
    """Return stored ``last_sample_time`` for top-trader long/short series, or ``None`` if no row."""
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_sample_time FROM top_trader_long_short_cursor
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )
        row = cur.fetchone()
    if not row:
        return None
    return row[0]


def upsert_top_trader_long_short_cursor(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
    last_sample_time: datetime,
) -> None:
    """
    Set or update top-trader long/short ingestion high-water mark. Does not commit.

    ``last_sample_time`` must be timezone-aware (UTC recommended).
    """
    if last_sample_time.tzinfo is None:
        raise ValueError("last_sample_time must be timezone-aware")
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO top_trader_long_short_cursor (
                symbol, period, last_sample_time, updated_at
            )
            VALUES (%s, %s, %s, now())
            ON CONFLICT (symbol, period) DO UPDATE SET
                last_sample_time = EXCLUDED.last_sample_time,
                updated_at = now()
            """,
            (sym, pd, last_sample_time.astimezone(timezone.utc)),
        )


def fetch_basis_rates_by_sample_times(
    conn: PsycopgConnection,
    pair: str,
    contract_type: str,
    period: str,
    sample_times: Sequence[datetime],
) -> Mapping[datetime, tuple[Decimal, Decimal, Decimal, Decimal]]:
    """
    Return ``sample_time -> (basis, basis_rate, futures_price, index_price)``
    for rows that exist. Missing keys are omitted.
    """
    if not sample_times:
        return {}
    p = pair.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sample_time, basis, basis_rate, futures_price, index_price
            FROM basis_rate
            WHERE pair = %s AND contract_type = %s AND period = %s AND sample_time = ANY(%s)
            """,
            (p, ct, pd, list(sample_times)),
        )
        rows = cur.fetchall()
    out: dict[datetime, tuple[Decimal, Decimal, Decimal, Decimal]] = {}
    for sample_time, basis, basis_rate, futures_price, index_price in rows:
        out[sample_time] = (
            basis,
            basis_rate,
            futures_price,
            index_price,
        )
    return out


def basis_window_stats(
    conn: PsycopgConnection,
    pair: str,
    contract_type: str,
    period: str,
    *,
    sample_time_ge: datetime,
    sample_time_le: datetime,
) -> tuple[int, datetime | None, datetime | None]:
    """
    Return ``(row_count, min_sample_time, max_sample_time)`` for basis rows in window (inclusive).

    ``min`` / ``max`` are ``None`` when the count is zero.
    """
    p = pair.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    if sample_time_ge.tzinfo is None or sample_time_le.tzinfo is None:
        raise ValueError("sample_time bounds must be timezone-aware")
    lo = sample_time_ge.astimezone(timezone.utc)
    hi = sample_time_le.astimezone(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), MIN(sample_time), MAX(sample_time) FROM basis_rate
            WHERE pair = %s AND contract_type = %s AND period = %s
              AND sample_time >= %s AND sample_time <= %s
            """,
            (p, ct, pd, lo, hi),
        )
        row = cur.fetchone()
    if not row:
        return 0, None, None
    n = int(row[0]) if row[0] is not None else 0
    return n, row[1], row[2]


def fetch_open_interest_by_sample_times(
    conn: PsycopgConnection,
    symbol: str,
    contract_type: str,
    period: str,
    sample_times: Sequence[datetime],
) -> Mapping[datetime, tuple[Decimal, Decimal, Decimal | None]]:
    """
    Return ``sample_time -> (sum_open_interest, sum_open_interest_value, cmc_circulating_supply)``
    for rows that exist. Missing keys are omitted.
    """
    if not sample_times:
        return {}
    sym = symbol.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sample_time, sum_open_interest, sum_open_interest_value, cmc_circulating_supply
            FROM open_interest
            WHERE symbol = %s AND contract_type = %s AND period = %s AND sample_time = ANY(%s)
            """,
            (sym, ct, pd, list(sample_times)),
        )
        rows = cur.fetchall()
    out: dict[datetime, tuple[Decimal, Decimal, Decimal | None]] = {}
    for sample_time, oi, oi_value, supply in rows:
        out[sample_time] = (oi, oi_value, supply)
    return out


def fetch_taker_buy_sell_volume_by_sample_times(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
    sample_times: Sequence[datetime],
) -> Mapping[datetime, tuple[Decimal, Decimal, Decimal]]:
    """
    Return ``sample_time -> (buy_sell_ratio, buy_vol, sell_vol)`` for rows that exist.

    Missing keys are omitted.
    """
    if not sample_times:
        return {}
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sample_time, buy_sell_ratio, buy_vol, sell_vol
            FROM taker_buy_sell_volume
            WHERE symbol = %s AND period = %s AND sample_time = ANY(%s)
            """,
            (sym, pd, list(sample_times)),
        )
        rows = cur.fetchall()
    out: dict[datetime, tuple[Decimal, Decimal, Decimal]] = {}
    for sample_time, ratio, buy_vol, sell_vol in rows:
        out[sample_time] = (ratio, buy_vol, sell_vol)
    return out


def fetch_top_trader_long_short_by_sample_times(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
    sample_times: Sequence[datetime],
) -> Mapping[datetime, tuple[Decimal, Decimal, Decimal]]:
    """
    Return ``sample_time -> (long_short_position_ratio, long_account_ratio, short_account_ratio)``
    for rows that exist. Missing keys are omitted.
    """
    if not sample_times:
        return {}
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sample_time, long_short_position_ratio, long_account_ratio, short_account_ratio
            FROM top_trader_long_short
            WHERE symbol = %s AND period = %s AND sample_time = ANY(%s)
            """,
            (sym, pd, list(sample_times)),
        )
        rows = cur.fetchall()
    out: dict[datetime, tuple[Decimal, Decimal, Decimal]] = {}
    for sample_time, ratio, long_acc, short_acc in rows:
        out[sample_time] = (ratio, long_acc, short_acc)
    return out


def open_interest_window_stats(
    conn: PsycopgConnection,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    sample_time_ge: datetime,
    sample_time_le: datetime,
) -> tuple[int, datetime | None, datetime | None]:
    """
    Return ``(row_count, min_sample_time, max_sample_time)`` for open-interest rows in window.

    ``min`` / ``max`` are ``None`` when the count is zero.
    """
    sym = symbol.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    if sample_time_ge.tzinfo is None or sample_time_le.tzinfo is None:
        raise ValueError("sample_time bounds must be timezone-aware")
    lo = sample_time_ge.astimezone(timezone.utc)
    hi = sample_time_le.astimezone(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), MIN(sample_time), MAX(sample_time) FROM open_interest
            WHERE symbol = %s AND contract_type = %s AND period = %s
              AND sample_time >= %s AND sample_time <= %s
            """,
            (sym, ct, pd, lo, hi),
        )
        row = cur.fetchone()
    if not row:
        return 0, None, None
    n = int(row[0]) if row[0] is not None else 0
    return n, row[1], row[2]


def taker_buy_sell_volume_window_stats(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
    *,
    sample_time_ge: datetime,
    sample_time_le: datetime,
) -> tuple[int, datetime | None, datetime | None]:
    """
    Return ``(row_count, min_sample_time, max_sample_time)`` for taker buy/sell volume rows in window (inclusive).

    ``min`` / ``max`` are ``None`` when the count is zero.
    """
    sym = symbol.strip().upper()
    pd = period.strip()
    if sample_time_ge.tzinfo is None or sample_time_le.tzinfo is None:
        raise ValueError("sample_time bounds must be timezone-aware")
    lo = sample_time_ge.astimezone(timezone.utc)
    hi = sample_time_le.astimezone(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), MIN(sample_time), MAX(sample_time) FROM taker_buy_sell_volume
            WHERE symbol = %s AND period = %s
              AND sample_time >= %s AND sample_time <= %s
            """,
            (sym, pd, lo, hi),
        )
        row = cur.fetchone()
    if not row:
        return 0, None, None
    n = int(row[0]) if row[0] is not None else 0
    return n, row[1], row[2]


def top_trader_long_short_window_stats(
    conn: PsycopgConnection,
    symbol: str,
    period: str,
    *,
    sample_time_ge: datetime,
    sample_time_le: datetime,
) -> tuple[int, datetime | None, datetime | None]:
    """
    Return ``(row_count, min_sample_time, max_sample_time)`` for top-trader long/short rows in window (inclusive).

    ``min`` / ``max`` are ``None`` when the count is zero.
    """
    sym = symbol.strip().upper()
    pd = period.strip()
    if sample_time_ge.tzinfo is None or sample_time_le.tzinfo is None:
        raise ValueError("sample_time bounds must be timezone-aware")
    lo = sample_time_ge.astimezone(timezone.utc)
    hi = sample_time_le.astimezone(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*), MIN(sample_time), MAX(sample_time) FROM top_trader_long_short
            WHERE symbol = %s AND period = %s
              AND sample_time >= %s AND sample_time <= %s
            """,
            (sym, pd, lo, hi),
        )
        row = cur.fetchone()
    if not row:
        return 0, None, None
    n = int(row[0]) if row[0] is not None else 0
    return n, row[1], row[2]
