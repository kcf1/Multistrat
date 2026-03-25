"""
Rolling re-fetch of recent taker buy/sell volume points for vendor drift checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping

import psycopg2
from loguru import logger

from market_data.config import (
    TAKER_BUYSELL_VOLUME_CORRECT_WINDOW_POINTS,
    TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
    TAKER_BUYSELL_VOLUME_PERIODS,
    TAKER_BUYSELL_VOLUME_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import chunk_fetch_taker_buy_sell_volume_forward, utc_now_ms
from market_data.providers.base import TakerBuySellVolumeProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.schemas import TakerBuySellVolumePoint
from market_data.storage import (
    fetch_taker_buy_sell_volume_by_sample_times,
    upsert_taker_buy_sell_volume_points,
)


@dataclass(frozen=True)
class CorrectTakerBuySellVolumeWindowResult:
    symbol: str
    period: str
    rows_fetched: int
    drift_rows: int


def _log_taker_buy_sell_volume_drifts(
    existing: Mapping[datetime, tuple[Decimal, Decimal, Decimal]],
    rows: list[TakerBuySellVolumePoint],
) -> int:
    n = 0
    for r in rows:
        old = existing.get(r.sample_time)
        if old is None:
            continue
        same = old == (
            r.buy_sell_ratio,
            r.buy_vol,
            r.sell_vol,
        )
        if not same:
            n += 1
            logger.debug(
                "market_data taker_buy_sell_volume drift symbol={} period={} sample_time={} "
                "db=({}, {}, {}) api=({}, {}, {})",
                r.symbol,
                r.period,
                r.sample_time,
                old[0],
                old[1],
                old[2],
                r.buy_sell_ratio,
                r.buy_vol,
                r.sell_vol,
            )
    return n


def run_correct_window_taker_buy_sell_volume_series(
    conn,
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    *,
    lookback_points: int | None = None,
    now_ms: int | None = None,
    chunk_limit: int = TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
) -> CorrectTakerBuySellVolumeWindowResult:
    lookback = (
        lookback_points
        if lookback_points is not None
        else TAKER_BUYSELL_VOLUME_CORRECT_WINDOW_POINTS
    )
    pd_ms = interval_to_millis(period)
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    start_ms = end_ms - lookback * pd_ms

    rows = chunk_fetch_taker_buy_sell_volume_forward(
        provider,
        symbol,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    )
    if not rows:
        return CorrectTakerBuySellVolumeWindowResult(symbol, period, 0, 0)

    times = [r.sample_time for r in rows]
    prev = fetch_taker_buy_sell_volume_by_sample_times(conn, symbol, period, times)
    drifts = _log_taker_buy_sell_volume_drifts(dict(prev), rows)
    upsert_taker_buy_sell_volume_points(conn, rows)
    conn.commit()
    return CorrectTakerBuySellVolumeWindowResult(symbol, period, len(rows), drifts)


def run_correct_window_taker_buy_sell_volume(
    settings: MarketDataSettings,
    *,
    provider: TakerBuySellVolumeProvider | None = None,
    lookback_points: int | None = None,
) -> list[CorrectTakerBuySellVolumeWindowResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[CorrectTakerBuySellVolumeWindowResult] = []
        for symbol in TAKER_BUYSELL_VOLUME_SYMBOLS:
            for period in TAKER_BUYSELL_VOLUME_PERIODS:
                out.append(
                    run_correct_window_taker_buy_sell_volume_series(
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

