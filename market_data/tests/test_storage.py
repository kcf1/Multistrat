"""Tests for market_data.storage (mocked + optional Postgres)."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from market_data.schemas import BasisPoint, OhlcvBar, OpenInterestPoint
from market_data.storage import (
    basis_window_stats,
    count_ohlcv_bars_in_window,
    fetch_basis_rates_by_sample_times,
    fetch_open_interest_by_sample_times,
    get_basis_cursor,
    get_ingestion_cursor,
    get_open_interest_cursor,
    max_open_time_ohlcv,
    max_sample_time_basis,
    max_sample_time_open_interest,
    open_interest_window_stats,
    ohlcv_window_stats,
    upsert_basis_cursor,
    upsert_basis_points,
    upsert_ingestion_cursor,
    upsert_open_interest_cursor,
    upsert_open_interest_points,
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


def _basis_point(
    *,
    pair: str = "BTCUSDT",
    contract_type: str = "PERPETUAL",
    period: str = "1h",
    sample_time: datetime | None = None,
    basis_rate: Decimal = Decimal("0.001"),
) -> BasisPoint:
    st = sample_time or datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    return BasisPoint(
        pair=pair,
        contract_type=contract_type,
        period=period,
        sample_time=st,
        basis=Decimal("100"),
        basis_rate=basis_rate,
        futures_price=Decimal("50100"),
        index_price=Decimal("50000"),
    )


def _open_interest_point(
    *,
    symbol: str = "BTCUSDT",
    contract_type: str = "PERPETUAL",
    period: str = "1h",
    sample_time: datetime | None = None,
    sum_open_interest: Decimal = Decimal("12345.6789"),
) -> OpenInterestPoint:
    st = sample_time or datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    return OpenInterestPoint(
        symbol=symbol,
        contract_type=contract_type,
        period=period,
        sample_time=st,
        sum_open_interest=sum_open_interest,
        sum_open_interest_value=Decimal("987654321.123456"),
        cmc_circulating_supply=Decimal("19500000.0"),
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


def test_upsert_basis_empty_no_cursor() -> None:
    conn = MagicMock()
    assert upsert_basis_points(conn, []) == 0
    conn.cursor.assert_not_called()


def test_upsert_basis_uses_execute_values_with_on_conflict() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    points = [_basis_point()]

    with patch("market_data.storage.execute_values") as ev:
        n = upsert_basis_points(conn, points)

    assert n == 1
    ev.assert_called_once()
    args, _kwargs = ev.call_args
    sql = args[1]
    assert "ON CONFLICT (pair, contract_type, period, sample_time)" in sql
    assert "DO UPDATE SET" in sql
    assert "ingested_at = now()" in sql
    row = args[2][0]
    assert row[0] == "BTCUSDT"
    assert row[1] == "PERPETUAL"
    assert row[2] == "1h"


def test_upsert_open_interest_empty_no_cursor() -> None:
    conn = MagicMock()
    assert upsert_open_interest_points(conn, []) == 0
    conn.cursor.assert_not_called()


def test_upsert_open_interest_uses_execute_values_with_on_conflict() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    points = [_open_interest_point()]

    with patch("market_data.storage.execute_values") as ev:
        n = upsert_open_interest_points(conn, points)

    assert n == 1
    ev.assert_called_once()
    args, _kwargs = ev.call_args
    sql = args[1]
    assert "ON CONFLICT (symbol, contract_type, period, sample_time)" in sql
    assert "DO UPDATE SET" in sql
    assert "ingested_at = now()" in sql
    row = args[2][0]
    assert row[0] == "BTCUSDT"
    assert row[1] == "PERPETUAL"
    assert row[2] == "1h"


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


def test_get_basis_cursor_returns_none_when_missing() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = None
    conn.cursor.return_value = cur

    assert get_basis_cursor(conn, "btcusdt", "perpetual", "1h") is None
    params = cur.execute.call_args[0][1]
    assert params == ("BTCUSDT", "PERPETUAL", "1h")


def test_get_basis_cursor_returns_time() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    ts = datetime(2021, 6, 15, 12, 0, tzinfo=timezone.utc)
    cur.fetchone.return_value = (ts,)
    conn.cursor.return_value = cur

    assert get_basis_cursor(conn, "BTCUSDT", "PERPETUAL", "1h") == ts


def test_get_open_interest_cursor_returns_none_when_missing() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = None
    conn.cursor.return_value = cur

    assert get_open_interest_cursor(conn, "btcusdt", "perpetual", "1h") is None
    params = cur.execute.call_args[0][1]
    assert params == ("BTCUSDT", "PERPETUAL", "1h")


def test_get_open_interest_cursor_returns_time() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    ts = datetime(2021, 6, 15, 12, 0, tzinfo=timezone.utc)
    cur.fetchone.return_value = (ts,)
    conn.cursor.return_value = cur

    assert get_open_interest_cursor(conn, "BTCUSDT", "PERPETUAL", "1h") == ts


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


def test_max_sample_time_basis() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    ts = datetime(2022, 1, 1, tzinfo=timezone.utc)
    cur.fetchone.return_value = (ts,)
    conn.cursor.return_value = cur

    assert max_sample_time_basis(conn, "btcusdt", "perpetual", "1h") == ts


def test_max_sample_time_open_interest() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    ts = datetime(2022, 1, 1, tzinfo=timezone.utc)
    cur.fetchone.return_value = (ts,)
    conn.cursor.return_value = cur

    assert max_sample_time_open_interest(conn, "btcusdt", "perpetual", "1h") == ts


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


def test_upsert_basis_cursor_rejects_naive_datetime() -> None:
    conn = MagicMock()
    naive = datetime(2020, 1, 1)
    with pytest.raises(ValueError, match="timezone-aware"):
        upsert_basis_cursor(conn, "BTCUSDT", "PERPETUAL", "1h", naive)


def test_upsert_basis_cursor_executes_upsert() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    upsert_basis_cursor(conn, "btcusdt", "perpetual", "1h", ts)
    cur.execute.assert_called_once()
    sql = cur.execute.call_args[0][0]
    assert "ON CONFLICT (pair, contract_type, period)" in sql
    params = cur.execute.call_args[0][1]
    assert params[0] == "BTCUSDT"
    assert params[1] == "PERPETUAL"
    assert params[2] == "1h"
    assert params[3] == ts


def test_upsert_open_interest_cursor_rejects_naive_datetime() -> None:
    conn = MagicMock()
    naive = datetime(2020, 1, 1)
    with pytest.raises(ValueError, match="timezone-aware"):
        upsert_open_interest_cursor(conn, "BTCUSDT", "PERPETUAL", "1h", naive)


def test_upsert_open_interest_cursor_executes_upsert() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    upsert_open_interest_cursor(conn, "btcusdt", "perpetual", "1h", ts)
    cur.execute.assert_called_once()
    sql = cur.execute.call_args[0][0]
    assert "ON CONFLICT (symbol, contract_type, period)" in sql
    params = cur.execute.call_args[0][1]
    assert params[0] == "BTCUSDT"
    assert params[1] == "PERPETUAL"
    assert params[2] == "1h"
    assert params[3] == ts


def test_fetch_basis_rates_by_sample_times_empty_returns_empty() -> None:
    conn = MagicMock()
    assert fetch_basis_rates_by_sample_times(conn, "BTCUSDT", "PERPETUAL", "1h", []) == {}


def test_fetch_basis_rates_by_sample_times_executes() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    st = datetime(2020, 1, 1, tzinfo=timezone.utc)
    cur.fetchall.return_value = [
        (
            st,
            Decimal("100"),
            Decimal("0.001"),
            Decimal("50100"),
            Decimal("50000"),
        )
    ]
    conn.cursor.return_value = cur
    out = fetch_basis_rates_by_sample_times(conn, "btcusdt", "perpetual", "1h", [st])
    assert st in out
    assert out[st][1] == Decimal("0.001")


def test_basis_window_stats_empty() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = (0, None, None)
    conn.cursor.return_value = cur
    lo = datetime(2020, 1, 1, tzinfo=timezone.utc)
    hi = datetime(2020, 1, 2, tzinfo=timezone.utc)
    assert basis_window_stats(
        conn,
        "BTCUSDT",
        "PERPETUAL",
        "1h",
        sample_time_ge=lo,
        sample_time_le=hi,
    ) == (0, None, None)


def test_basis_window_stats_rejects_naive() -> None:
    conn = MagicMock()
    naive = datetime(2020, 1, 1)
    hi = datetime(2020, 1, 2, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone-aware"):
        basis_window_stats(
            conn,
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            sample_time_ge=naive,
            sample_time_le=hi,
        )


def test_fetch_open_interest_by_sample_times_empty_returns_empty() -> None:
    conn = MagicMock()
    assert fetch_open_interest_by_sample_times(conn, "BTCUSDT", "PERPETUAL", "1h", []) == {}


def test_fetch_open_interest_by_sample_times_executes() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    st = datetime(2020, 1, 1, tzinfo=timezone.utc)
    cur.fetchall.return_value = [
        (
            st,
            Decimal("12345.6789"),
            Decimal("987654321.123456"),
            Decimal("19500000.0"),
        )
    ]
    conn.cursor.return_value = cur
    out = fetch_open_interest_by_sample_times(conn, "btcusdt", "perpetual", "1h", [st])
    assert st in out
    assert out[st][0] == Decimal("12345.6789")


def test_open_interest_window_stats_empty() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = (0, None, None)
    conn.cursor.return_value = cur
    lo = datetime(2020, 1, 1, tzinfo=timezone.utc)
    hi = datetime(2020, 1, 2, tzinfo=timezone.utc)
    assert open_interest_window_stats(
        conn,
        "BTCUSDT",
        "PERPETUAL",
        "1h",
        sample_time_ge=lo,
        sample_time_le=hi,
    ) == (0, None, None)


def test_open_interest_window_stats_rejects_naive() -> None:
    conn = MagicMock()
    naive = datetime(2020, 1, 1)
    hi = datetime(2020, 1, 2, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone-aware"):
        open_interest_window_stats(
            conn,
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            sample_time_ge=naive,
            sample_time_le=hi,
        )


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
