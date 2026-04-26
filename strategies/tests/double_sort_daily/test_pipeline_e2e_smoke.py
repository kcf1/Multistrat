"""End-to-end smoke (DB optional when ``--no-persist``)."""

from __future__ import annotations

import os

import pandas as pd
import pytest

from strategies.modules.double_sort_daily import config
from strategies.modules.double_sort_daily.pipeline import run_pipeline


def _pg_url() -> str | None:
    return (
        os.environ.get("STRATEGIES_PIPELINE_DATABASE_URL", "").strip()
        or os.environ.get("DATABASE_URL", "").strip()
        or None
    )


@pytest.mark.skipif(not _pg_url(), reason="No Postgres URL for OHLCV load")
def test_run_pipeline_smoke_with_persist_off():
    """Uses real DB for intraday load; does not persist."""
    syms = list(config.MARKET_BASKET)[:2] + ["SOLUSDT"]
    end = pd.Timestamp.now(tz="UTC").normalize()
    start = end - pd.Timedelta(days=45)
    res = run_pipeline(
        symbols=syms,
        open_time_ge=start,
        open_time_lt=end,
        persist=False,
        interval=config.DEFAULT_OHLCV_INTERVAL,
    )
    if res.l1.empty:
        pytest.skip("No OHLCV rows in window for chosen symbols")
    assert len(res.factors) == len(res.l1)
