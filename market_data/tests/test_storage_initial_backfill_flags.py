"""Optional Postgres tests for initial-backfill cursor helpers."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg2")

from pgconn import configure_for_market_data
from market_data.storage import (
    mark_ohlcv_initial_backfill_done,
    query_ohlcv_symbols_initial_backfill_complete,
    upsert_ingestion_cursor,
)


def _postgres_url() -> str | None:
    return os.environ.get("MARKET_DATA_DATABASE_URL") or os.environ.get("DATABASE_URL")


@pytest.mark.skipif(not _postgres_url(), reason="Set MARKET_DATA_DATABASE_URL or DATABASE_URL")
def test_query_ohlcv_initial_backfill_complete_and_mark() -> None:
    import psycopg2

    url = _postgres_url()
    assert url
    conn = psycopg2.connect(url)
    configure_for_market_data(conn)
    sym = "ZZZTESTUSDT"
    iv = "1m"
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ingestion_cursor WHERE symbol = %s AND interval = %s",
                (sym, iv),
            )
        conn.commit()

        done = query_ohlcv_symbols_initial_backfill_complete(conn, intervals=("1m",))
        assert sym not in done

        upsert_ingestion_cursor(
            conn, sym, iv, datetime(2020, 1, 1, tzinfo=timezone.utc)
        )
        conn.commit()

        mark_ohlcv_initial_backfill_done(conn, sym, iv)
        conn.commit()

        done2 = query_ohlcv_symbols_initial_backfill_complete(conn, intervals=("1m",))
        assert sym in done2
    finally:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM ingestion_cursor WHERE symbol = %s AND interval = %s",
                (sym, iv),
            )
        conn.commit()
        conn.close()
