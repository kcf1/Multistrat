"""Tests for market_data.storage (mocked + optional Postgres)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from market_data.schemas import OhlcvBar
from market_data.storage import (
    count_ohlcv_bars_in_window,
    get_ingestion_cursor,
    max_open_time_ohlcv,
    ohlcv_window_stats,
    upsert_ingestion_cursor,
    upsert_ohlcv_bars,
)


def _bar(
    *,
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    open_time: datetime | None = None,
    close: Decimal = Decimal("1"),
) -> OhlcvBar:
    ot = open_time or datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    return OhlcvBar(
        symbol=symbol,
        interval=interval,
        open_time=ot,
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=close,
        volume=Decimal("10"),
        quote_volume=Decimal("20"),
        trades=100,
        close_time=ot,
    )


def test_upsert_empty_no_cursor() -> None:
    conn = MagicMock()
    assert upsert_ohlcv_bars(conn, []) == 0
    conn.cursor.assert_not_called()


def test_upsert_uses_execute_values_with_on_conflict() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    bars = [_bar()]

    with patch("market_data.storage.execute_values") as ev:
        n = upsert_ohlcv_bars(conn, bars)

    assert n == 1
    ev.assert_called_once()
    args, kwargs = ev.call_args
    assert args[0] == cur
    sql = args[1]
    assert "ON CONFLICT (symbol, interval, open_time)" in sql
    assert "DO UPDATE SET" in sql
    assert "ingested_at = now()" in sql
    assert len(args[2]) == 1
    row = args[2][0]
    assert row[0] == "BTCUSDT"
    assert row[1] == "1m"


def test_get_ingestion_cursor_returns_none_when_missing() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = None
    conn.cursor.return_value = cur

    assert get_ingestion_cursor(conn, "ethusdt", "5m") is None
    cur.execute.assert_called_once()
    params = cur.execute.call_args[0][1]
    assert params == ("ETHUSDT", "5m")


def test_get_ingestion_cursor_returns_time() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    ts = datetime(2021, 6, 15, 12, 0, tzinfo=timezone.utc)
    cur.fetchone.return_value = (ts,)
    conn.cursor.return_value = cur

    assert get_ingestion_cursor(conn, "BTCUSDT", "1m") == ts


def test_count_ohlcv_bars_in_window_executes() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    lo = datetime(2020, 1, 1, tzinfo=timezone.utc)
    hi = datetime(2020, 1, 2, tzinfo=timezone.utc)
    cur.fetchone.return_value = (42, lo, hi)
    conn.cursor.return_value = cur
    assert count_ohlcv_bars_in_window(conn, "btcusdt", "1m", open_time_ge=lo, open_time_le=hi) == 42


def test_ohlcv_window_stats_empty() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = (0, None, None)
    conn.cursor.return_value = cur
    lo = datetime(2020, 1, 1, tzinfo=timezone.utc)
    hi = datetime(2020, 1, 2, tzinfo=timezone.utc)
    assert ohlcv_window_stats(conn, "BTCUSDT", "1h", open_time_ge=lo, open_time_le=hi) == (
        0,
        None,
        None,
    )


def test_count_ohlcv_bars_in_window_rejects_naive() -> None:
    conn = MagicMock()
    naive = datetime(2020, 1, 1)
    hi = datetime(2020, 1, 2, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone-aware"):
        ohlcv_window_stats(conn, "BTCUSDT", "1m", open_time_ge=naive, open_time_le=hi)


def test_max_open_time_ohlcv() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    ts = datetime(2022, 1, 1, tzinfo=timezone.utc)
    cur.fetchone.return_value = (ts,)
    conn.cursor.return_value = cur

    assert max_open_time_ohlcv(conn, "btcusdt", "1m") == ts


def test_upsert_ingestion_cursor_rejects_naive_datetime() -> None:
    conn = MagicMock()
    naive = datetime(2020, 1, 1)
    with pytest.raises(ValueError, match="timezone-aware"):
        upsert_ingestion_cursor(conn, "BTCUSDT", "1m", naive)


def test_upsert_ingestion_cursor_executes_upsert() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    upsert_ingestion_cursor(conn, "btcusdt", "1h", ts)
    cur.execute.assert_called_once()
    sql = cur.execute.call_args[0][0]
    assert "ON CONFLICT (symbol, interval)" in sql
    params = cur.execute.call_args[0][1]
    assert params[0] == "BTCUSDT"
    assert params[1] == "1h"
    assert params[2] == ts


def _postgres_url() -> str | None:
    return os.environ.get("MARKET_DATA_DATABASE_URL") or os.environ.get("DATABASE_URL")


def test_postgres_upsert_idempotent_and_cursor() -> None:
    """
    Requires migrated DB (``ohlcv`` + ``ingestion_cursor``). Uses ROLLBACK — no lasting data.
    """
    url = _postgres_url()
    if not url:
        pytest.skip("Set MARKET_DATA_DATABASE_URL or DATABASE_URL for integration test")

    import psycopg2

    sym = "ZZTESTMD94_OHLCV"
    iv = "1m"
    ot = datetime(2019, 7, 1, 0, 0, tzinfo=timezone.utc)

    bar_a = _bar(symbol=sym, interval=iv, open_time=ot, close=Decimal("100"))
    bar_b = _bar(symbol=sym, interval=iv, open_time=ot, close=Decimal("200"))

    conn = psycopg2.connect(url)
    conn.autocommit = False
    try:
        upsert_ohlcv_bars(conn, [bar_a])
        upsert_ohlcv_bars(conn, [bar_b])
        with conn.cursor() as c:
            c.execute(
                "SELECT close FROM ohlcv WHERE symbol = %s AND interval = %s AND open_time = %s",
                (sym, iv, ot),
            )
            row = c.fetchone()
        assert row is not None
        assert row[0] == Decimal("200")

        assert max_open_time_ohlcv(conn, sym, iv) == ot
        assert get_ingestion_cursor(conn, sym, iv) is None

        upsert_ingestion_cursor(conn, sym, iv, ot)
        assert get_ingestion_cursor(conn, sym, iv) == ot

        conn.rollback()
    finally:
        conn.close()
