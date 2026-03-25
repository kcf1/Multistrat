"""
Rolling re-fetch of recent top trader long/short points for vendor drift checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping

import psycopg2
from loguru import logger

from market_data.config import (
    TOP_TRADER_LONG_SHORT_CORRECT_WINDOW_POINTS,
    TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
    TOP_TRADER_LONG_SHORT_PERIODS,
    TOP_TRADER_LONG_SHORT_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import chunk_fetch_top_trader_long_short_forward, utc_now_ms
from market_data.providers.base import TopTraderLongShortPositionRatioProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.schemas import TopTraderLongShortPoint
from market_data.storage import (
    fetch_top_trader_long_short_by_sample_times,
    upsert_top_trader_long_short_points,
)


@dataclass(frozen=True)
class CorrectTopTraderLongShortWindowResult:
    symbol: str
    period: str
    rows_fetched: int
    drift_rows: int


def _log_top_trader_long_short_drifts(
    existing: Mapping[datetime, tuple[Decimal, Decimal, Decimal]],
    rows: list[TopTraderLongShortPoint],
) -> int:
    n = 0
    for r in rows:
        old = existing.get(r.sample_time)
        if old is None:
            continue
        same = old == (r.long_short_ratio, r.long_account_ratio, r.short_account_ratio)
        if not same:
            n += 1
            logger.debug(
                "market_data top_trader_long_short drift symbol={} period={} sample_time={} "
                "db=({}, {}, {}) api=({}, {}, {})",
                r.symbol,
                r.period,
                r.sample_time,
                old[0],
                old[1],
                old[2],
                r.long_short_ratio,
                r.long_account_ratio,
                r.short_account_ratio,
            )
    return n


def run_correct_window_top_trader_long_short_series(
    conn,
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    *,
    lookback_points: int | None = None,
    now_ms: int | None = None,
    chunk_limit: int = TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
) -> CorrectTopTraderLongShortWindowResult:
    lookback = (
        lookback_points
        if lookback_points is not None
        else TOP_TRADER_LONG_SHORT_CORRECT_WINDOW_POINTS
    )
    pd_ms = interval_to_millis(period)
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    start_ms = end_ms - lookback * pd_ms

    rows = chunk_fetch_top_trader_long_short_forward(
        provider,
        symbol,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    )

    if not rows:
        return CorrectTopTraderLongShortWindowResult(symbol, period, 0, 0)

    times = [r.sample_time for r in rows]
    prev = fetch_top_trader_long_short_by_sample_times(conn, symbol, period, times)
    drifts = _log_top_trader_long_short_drifts(dict(prev), rows)
    upsert_top_trader_long_short_points(conn, rows)
    conn.commit()
    return CorrectTopTraderLongShortWindowResult(symbol, period, len(rows), drifts)


def run_correct_window_top_trader_long_short(
    settings: MarketDataSettings,
    *,
    provider: TopTraderLongShortPositionRatioProvider | None = None,
    lookback_points: int | None = None,
) -> list[CorrectTopTraderLongShortWindowResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[CorrectTopTraderLongShortWindowResult] = []
        for symbol in TOP_TRADER_LONG_SHORT_SYMBOLS:
            for period in TOP_TRADER_LONG_SHORT_PERIODS:
                out.append(
                    run_correct_window_top_trader_long_short_series(
                        conn,
                        prov,
                        symbol,
                        period,
                        lookback_points=lookback_points,
                    )
                )
        return out
    finally:
        conn.close()

