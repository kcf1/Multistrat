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

from market_data.schemas import OhlcvBar

_OHLCV_UPSERT_SQL = """
INSERT INTO ohlcv (
    symbol, interval, open_time, open, high, low, close, volume,
    quote_volume, trades, close_time
) VALUES %s
ON CONFLICT (symbol, interval, open_time) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    quote_volume = EXCLUDED.quote_volume,
    trades = EXCLUDED.trades,
    close_time = EXCLUDED.close_time,
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
        b.trades,
        b.close_time,
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
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            page_size=min(500, len(rows)),
        )
    return len(rows)


def fetch_ohlc_by_open_times(
    conn: PsycopgConnection,
    symbol: str,
    interval: str,
    open_times: Sequence[datetime],
) -> Mapping[datetime, tuple[Decimal, Decimal, Decimal, Decimal]]:
    """
    Return ``open_time -> (open, high, low, close)`` for rows that exist.
    Missing keys are omitted.
    """
    if not open_times:
        return {}
    sym = symbol.strip().upper()
    iv = interval.strip()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT open_time, open, high, low, close FROM ohlcv
            WHERE symbol = %s AND interval = %s AND open_time = ANY(%s)
            """,
            (sym, iv, list(open_times)),
        )
        rows = cur.fetchall()
    out: dict[datetime, tuple[Decimal, Decimal, Decimal, Decimal]] = {}
    for open_time, o, h, l, c in rows:
        out[open_time] = (o, h, l, c)
    return out


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
