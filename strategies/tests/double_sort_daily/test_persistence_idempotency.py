"""Upsert idempotency (requires Postgres + migration + DATABASE_URL)."""

from __future__ import annotations

import os

import pandas as pd
import pytest

from strategies.modules.double_sort_daily import persistence


def _pg_url() -> str | None:
    return (
        os.environ.get("STRATEGIES_PIPELINE_DATABASE_URL", "").strip()
        or os.environ.get("DATABASE_URL", "").strip()
        or None
    )


@pytest.mark.skipif(not _pg_url(), reason="DATABASE_URL / STRATEGIES_PIPELINE_DATABASE_URL not set")
def test_l1_upsert_twice_no_duplicate_rows():
    import psycopg2

    url = _pg_url()
    conn = psycopg2.connect(url)
    try:
        ts = pd.Timestamp("2099-01-01", tz="UTC")
        row = {
            "bar_ts": ts,
            "symbol": "PYTESTUSDT",
            "close": 1.0,
            "volume": 2.0,
            "quote_volume": 3.0,
            "taker_buy_base_volume": 1.0,
            "log_close": 0.0,
            "log_return": None,
            "ewvol_20": None,
            "norm_return": None,
            "norm_close": None,
            "market_return": None,
            "market_ewvol_20": None,
            "beta_250": None,
            "resid_return": None,
            "log_volume": 0.69,
            "log_quote_volume": 1.09,
            "vwap_250": None,
        }
        df = pd.DataFrame([row])
        persistence.upsert_l1feats(conn, df, pipeline_version="pytest", source="test")
        persistence.upsert_l1feats(conn, df, pipeline_version="pytest", source="test")
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                'SELECT COUNT(*) FROM strategies_daily.l1feats_daily WHERE bar_ts = %s AND symbol = %s',
                (ts.to_pydatetime(), "PYTESTUSDT"),
            )
            (n,) = cur.fetchone()
        assert n == 1
    finally:
        with conn.cursor() as cur:
            cur.execute(
                'DELETE FROM strategies_daily.l1feats_daily WHERE symbol = %s AND bar_ts >= %s',
                ("PYTESTUSDT", "2099-01-01"),
            )
        conn.commit()
        conn.close()
