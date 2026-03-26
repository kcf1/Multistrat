"""
Rolling re-fetch of recent taker buy/sell volume points for vendor drift checks.
"""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import time
from typing import Mapping

import psycopg2
from loguru import logger

from market_data.config import (
    FUTURES_CORRECT_WINDOW_MAX_WORKERS,
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
from market_data.providers.executor import ProviderExecutor, ProviderExecutorConfig
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
    provider_executor: ProviderExecutor[CorrectTakerBuySellVolumeWindowResult] | None = None,
    lookback_points: int | None = None,
) -> list[CorrectTakerBuySellVolumeWindowResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    own_executor = False
    ex = provider_executor
    if ex is None:
        ex = ProviderExecutor[CorrectTakerBuySellVolumeWindowResult](
            ProviderExecutorConfig(max_workers=FUTURES_CORRECT_WINDOW_MAX_WORKERS)
        )
        own_executor = True

    tasks: list[tuple[str, str]] = [
        (symbol, period)
        for symbol in TAKER_BUYSELL_VOLUME_SYMBOLS
        for period in TAKER_BUYSELL_VOLUME_PERIODS
    ]

    def _run_task(symbol: str, period: str) -> CorrectTakerBuySellVolumeWindowResult:
        conn = psycopg2.connect(settings.database_url)
        try:
            return run_correct_window_taker_buy_sell_volume_series(
                conn,
                prov,
                symbol,
                period,
                lookback_points=lookback_points,
            )
        finally:
            conn.close()
    try:
        t0 = time.perf_counter()
        logger.info(
            "correct_window_taker_buy_sell_volume run start: tasks={} workers={}",
            len(tasks),
            ex.max_workers,
        )
        out: list[CorrectTakerBuySellVolumeWindowResult] = []
        if ex.max_workers <= 1:
            for symbol, period in tasks:
                out.append(_run_task(symbol, period))
            logger.info(
                "correct_window_taker_buy_sell_volume run done: submitted={} completed={} failed=0 wall_clock_s={:.3f}",
                len(tasks),
                len(tasks),
                time.perf_counter() - t0,
            )
            return out

        futures: list[Future[CorrectTakerBuySellVolumeWindowResult]] = []
        future_to_task: dict[Future[CorrectTakerBuySellVolumeWindowResult], tuple[str, str]] = {}
        for symbol, period in tasks:
            fut = ex.submit(_run_task, symbol, period)
            futures.append(fut)
            future_to_task[fut] = (symbol, period)

        failed_tasks: list[tuple[str, str]] = []
        for fut in futures:
            symbol, period = future_to_task[fut]
            try:
                out.append(fut.result())
            except Exception:
                failed_tasks.append((symbol, period))
                logger.exception(
                    "correct_window_taker_buy_sell_volume task failed: symbol={} period={}",
                    symbol,
                    period,
                )

        logger.info(
            "correct_window_taker_buy_sell_volume run done: submitted={} completed={} failed={} wall_clock_s={:.3f}",
            len(tasks),
            len(tasks) - len(failed_tasks),
            len(failed_tasks),
            time.perf_counter() - t0,
        )
        if failed_tasks:
            failed_labels = [f"{symbol}/{period}" for symbol, period in failed_tasks]
            raise RuntimeError(
                "correct_window_taker_buy_sell_volume failed for task(s): "
                + ", ".join(sorted(failed_labels))
            )
        return out
    finally:
        if own_executor and ex is not None:
            ex.shutdown(wait=True)

