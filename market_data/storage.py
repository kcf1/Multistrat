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

from datetime import date, datetime, timezone
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
from market_data.universe_schemas import UniverseRefreshResult, UniverseTopNMember

_DATASET_STATUS_UPSERT_SQL = """
INSERT INTO dataset_status (
    dataset, period, last_complete_time, status, notes, updated_at
) VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (dataset, period) DO UPDATE SET
    last_complete_time = EXCLUDED.last_complete_time,
    status = EXCLUDED.status,
    notes = EXCLUDED.notes,
    updated_at = now()
"""

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


def upsert_dataset_status(
    conn: PsycopgConnection,
    *,
    dataset: str,
    period: str,
    last_complete_time: datetime | None,
    status: str = "ok",
    notes: str | None = None,
) -> None:
    """
    Upsert one aggregated dataset readiness marker into ``market_data.dataset_status``.

    This is intended to be written **once per dataset job run** after the whole cycle
    completes (not per symbol/series).
    """
    ds = dataset.strip().lower()
    pd = period.strip()
    if not ds or not pd:
        raise ValueError("dataset and period must be non-empty")
    lct = None
    if last_complete_time is not None:
        if last_complete_time.tzinfo is None:
            raise ValueError("last_complete_time must be timezone-aware")
        lct = last_complete_time.astimezone(timezone.utc)
    st = (status or "").strip().lower() or "ok"
    with conn.cursor() as cur:
        cur.execute(_DATASET_STATUS_UPSERT_SQL, (ds, pd, lct, st, notes))


def get_dataset_status(
    conn: PsycopgConnection,
    *,
    dataset: str,
    period: str,
) -> tuple[datetime | None, datetime | None, str | None, str | None]:
    """
    Return `(last_complete_time, updated_at, status, notes)` for a dataset/period,
    or all Nones when no row exists.
    """
    ds = dataset.strip().lower()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_complete_time, updated_at, status, notes
            FROM dataset_status
            WHERE dataset = %s AND period = %s
            """,
            (ds, pd),
        )
        row = cur.fetchone()
    if not row:
        return (None, None, None, None)
    return (row[0], row[1], row[2], row[3])


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


_UNIVERSE_ASSETS_UPSERT_SQL = """
INSERT INTO universe_assets (
    base_asset, source, first_seen_date, last_seen_date,
    current_rank, is_current_top100, was_ever_top100, updated_at
) VALUES %s
ON CONFLICT (base_asset) DO UPDATE SET
    source = EXCLUDED.source,
    first_seen_date = COALESCE(universe_assets.first_seen_date, EXCLUDED.first_seen_date),
    last_seen_date = CASE
        WHEN universe_assets.last_seen_date IS NULL THEN EXCLUDED.last_seen_date
        WHEN EXCLUDED.last_seen_date IS NULL THEN universe_assets.last_seen_date
        ELSE GREATEST(universe_assets.last_seen_date, EXCLUDED.last_seen_date)
    END,
    current_rank = EXCLUDED.current_rank,
    is_current_top100 = EXCLUDED.is_current_top100,
    was_ever_top100 = EXCLUDED.was_ever_top100,
    updated_at = now()
"""


def upsert_universe_topn_members(
    conn: PsycopgConnection,
    *,
    as_of_date: date,
    source: str,
    members: Sequence[UniverseTopNMember],
) -> UniverseRefreshResult:
    """
    Upsert top-N members for *as_of_date* into `universe_assets`.

    - Members are normalized to uppercase base assets.
    - Marks present members `is_current_top100=true`, `was_ever_top100=true`.
    - Marks previously-current assets not present as `is_current_top100=false`, clears rank.

    This function does NOT commit; caller owns the transaction.
    """
    if not members:
        raise ValueError("members must be non-empty for refresh")
    src = (source or "cmc_top100").strip() or "cmc_top100"

    # Deduplicate by base_asset keeping best (lowest) rank.
    best: dict[str, int] = {}
    for m in members:
        b = (m.base_asset or "").strip().upper()
        if not b:
            continue
        r = int(m.rank)
        if r < 1:
            continue
        if b not in best or r < best[b]:
            best[b] = r

    base_assets = sorted(best.keys())
    rows = [
        (
            b,
            src,
            as_of_date,
            as_of_date,
            best[b],
            True,
            True,
            datetime.now(timezone.utc),
        )
        for b in base_assets
    ]

    with conn.cursor() as cur:
        execute_values(cur, _UNIVERSE_ASSETS_UPSERT_SQL, rows, page_size=1000)

        # Mark dropped assets (previously current, now absent).
        cur.execute(
            """
            UPDATE universe_assets
            SET is_current_top100 = false,
                current_rank = NULL,
                updated_at = now()
            WHERE is_current_top100 = true
              AND NOT (base_asset = ANY(%s))
            """,
            (base_assets,),
        )
        dropped = int(cur.rowcount or 0)

        # Count newly added (first_seen_date == as_of_date).
        cur.execute(
            """
            SELECT COUNT(*) FROM universe_assets
            WHERE first_seen_date = %s
            """,
            (as_of_date,),
        )
        newly = int((cur.fetchone() or [0])[0] or 0)

    return UniverseRefreshResult(
        as_of_date=as_of_date,
        requested=len(members),
        valid_members=len(base_assets),
        newly_added_base_assets=newly,
        dropped_base_assets=dropped,
    )


def upsert_universe_seed_base_assets(
    conn: PsycopgConnection,
    *,
    base_assets: Sequence[str],
    source: str = "static_seed",
) -> int:
    """
    Seed universe table with base assets (asset-only). Does not set top-100 flags.

    This function does NOT commit; caller owns the transaction.
    """
    if not base_assets:
        return 0
    src = (source or "static_seed").strip() or "static_seed"
    uniq = sorted({(b or "").strip().upper() for b in base_assets if (b or "").strip()})
    if not uniq:
        return 0
    now = datetime.now(timezone.utc)
    rows = [(b, src, None, None, None, False, False, now) for b in uniq]
    with conn.cursor() as cur:
        execute_values(cur, _UNIVERSE_ASSETS_UPSERT_SQL, rows, page_size=1000)
    return len(uniq)


def query_universe_base_assets(
    conn: PsycopgConnection,
    *,
    mode: str = "ever_seen",
) -> list[str]:
    """
    Return list of base assets from `universe_assets`.

    mode:
      - `current_only`: is_current_top100=true
      - `ever_seen`: was_ever_top100=true OR present in table (fallback)
    """
    m = (mode or "ever_seen").strip()
    where = "was_ever_top100 = true"
    if m == "current_only":
        where = "is_current_top100 = true"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT base_asset
            FROM universe_assets
            WHERE {where}
            ORDER BY base_asset ASC
            """
        )
        return [str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]]


_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def query_ohlcv_symbols_initial_backfill_complete(
    conn: PsycopgConnection, *, intervals: Sequence[str]
) -> set[str]:
    """
    Symbols for which every configured ``interval`` has an ``ingestion_cursor`` row with
    ``initial_backfill_done = true``.
    """
    ivs = sorted({(i or "").strip() for i in intervals if (i or "").strip()})
    if not ivs:
        return set()
    n = len(ivs)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol FROM ingestion_cursor
            WHERE interval = ANY(%s) AND initial_backfill_done = true
            GROUP BY symbol
            HAVING COUNT(DISTINCT interval) = %s
            """,
            (ivs, n),
        )
        return {str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]}


def mark_ohlcv_initial_backfill_done(
    conn: PsycopgConnection, symbol: str, interval: str
) -> None:
    """Set ``initial_backfill_done`` for one OHLCV series; upsert cursor row if missing."""
    sym = symbol.strip().upper()
    iv = interval.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND interval = %s
            """,
            (sym, iv),
        )
        if cur.rowcount:
            return
    last = max_open_time_ohlcv(conn, sym, iv) or _EPOCH_UTC
    upsert_ingestion_cursor(conn, sym, iv, last)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ingestion_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND interval = %s
            """,
            (sym, iv),
        )


def query_basis_pairs_initial_backfill_complete(
    conn: PsycopgConnection,
    *,
    contract_types: Sequence[str],
    periods: Sequence[str],
) -> set[str]:
    cts = sorted({(c or "").strip().upper() for c in contract_types if (c or "").strip()})
    pds = sorted({(p or "").strip() for p in periods if (p or "").strip()})
    keys = [(ct, pd) for ct in cts for pd in pds]
    if not keys:
        return set()
    expected = len(keys)
    flat: list[str] = [x for ct, pd in keys for x in (ct, pd)]
    placeholders = ", ".join(["(%s,%s)"] * len(keys))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT pair FROM basis_cursor
            WHERE initial_backfill_done = true
              AND (contract_type, period) IN ({placeholders})
            GROUP BY pair
            HAVING COUNT(DISTINCT (contract_type, period)) = %s
            """,
            (*flat, expected),
        )
        return {str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]}


def mark_basis_initial_backfill_done(
    conn: PsycopgConnection, pair: str, contract_type: str, period: str
) -> None:
    p = pair.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE basis_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE pair = %s AND contract_type = %s AND period = %s
            """,
            (p, ct, pd),
        )
        if cur.rowcount:
            return
    last = max_sample_time_basis(conn, p, ct, pd) or _EPOCH_UTC
    upsert_basis_cursor(conn, p, ct, pd, last)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE basis_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE pair = %s AND contract_type = %s AND period = %s
            """,
            (p, ct, pd),
        )


def query_open_interest_symbols_initial_backfill_complete(
    conn: PsycopgConnection,
    *,
    contract_types: Sequence[str],
    periods: Sequence[str],
) -> set[str]:
    cts = sorted({(c or "").strip().upper() for c in contract_types if (c or "").strip()})
    pds = sorted({(p or "").strip() for p in periods if (p or "").strip()})
    keys = [(ct, pd) for ct in cts for pd in pds]
    if not keys:
        return set()
    expected = len(keys)
    flat: list[str] = [x for ct, pd in keys for x in (ct, pd)]
    placeholders = ", ".join(["(%s,%s)"] * len(keys))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT symbol FROM open_interest_cursor
            WHERE initial_backfill_done = true
              AND (contract_type, period) IN ({placeholders})
            GROUP BY symbol
            HAVING COUNT(DISTINCT (contract_type, period)) = %s
            """,
            (*flat, expected),
        )
        return {str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]}


def mark_open_interest_initial_backfill_done(
    conn: PsycopgConnection, symbol: str, contract_type: str, period: str
) -> None:
    sym = symbol.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE open_interest_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND contract_type = %s AND period = %s
            """,
            (sym, ct, pd),
        )
        if cur.rowcount:
            return
    last = max_sample_time_open_interest(conn, sym, ct, pd) or _EPOCH_UTC
    upsert_open_interest_cursor(conn, sym, ct, pd, last)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE open_interest_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND contract_type = %s AND period = %s
            """,
            (sym, ct, pd),
        )


def query_taker_buy_sell_volume_symbols_initial_backfill_complete(
    conn: PsycopgConnection, *, periods: Sequence[str]
) -> set[str]:
    pds = sorted({(p or "").strip() for p in periods if (p or "").strip()})
    if not pds:
        return set()
    n = len(pds)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol FROM taker_buy_sell_volume_cursor
            WHERE period = ANY(%s) AND initial_backfill_done = true
            GROUP BY symbol
            HAVING COUNT(DISTINCT period) = %s
            """,
            (pds, n),
        )
        return {str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]}


def mark_taker_buy_sell_volume_initial_backfill_done(
    conn: PsycopgConnection, symbol: str, period: str
) -> None:
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE taker_buy_sell_volume_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )
        if cur.rowcount:
            return
    last = max_sample_time_taker_buy_sell_volume(conn, sym, pd) or _EPOCH_UTC
    upsert_taker_buy_sell_volume_cursor(conn, sym, pd, last)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE taker_buy_sell_volume_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )


def query_top_trader_long_short_symbols_initial_backfill_complete(
    conn: PsycopgConnection, *, periods: Sequence[str]
) -> set[str]:
    pds = sorted({(p or "").strip() for p in periods if (p or "").strip()})
    if not pds:
        return set()
    n = len(pds)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol FROM top_trader_long_short_cursor
            WHERE period = ANY(%s) AND initial_backfill_done = true
            GROUP BY symbol
            HAVING COUNT(DISTINCT period) = %s
            """,
            (pds, n),
        )
        return {str(r[0]).strip().upper() for r in cur.fetchall() if r and r[0]}


def mark_top_trader_long_short_initial_backfill_done(
    conn: PsycopgConnection, symbol: str, period: str
) -> None:
    sym = symbol.strip().upper()
    pd = period.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE top_trader_long_short_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )
        if cur.rowcount:
            return
    last = max_sample_time_top_trader_long_short(conn, sym, pd) or _EPOCH_UTC
    upsert_top_trader_long_short_cursor(conn, sym, pd, last)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE top_trader_long_short_cursor
            SET initial_backfill_done = true,
                initial_backfill_at = now(),
                updated_at = now()
            WHERE symbol = %s AND period = %s
            """,
            (sym, pd),
        )
