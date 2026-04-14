"""
Targeted range repair and gap detection for top trader long/short snapshots.
"""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import time

import psycopg2
from loguru import logger

from pgconn import configure_for_market_data
from market_data.config import (
    FUTURES_REPAIR_GAP_MAX_WORKERS,
    OHLCV_SKIP_EXISTING_GAP_MULTIPLE,
    TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
    TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS,
    TOP_TRADER_LONG_SHORT_PERIODS,
    TOP_TRADER_LONG_SHORT_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import floor_align_ms_to_interval, utc_now_ms
from market_data.providers.base import TopTraderLongShortPositionRatioProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.providers.executor import ProviderExecutor, ProviderExecutorConfig
from market_data.storage import upsert_top_trader_long_short_points


def detect_top_trader_long_short_time_gaps(
    conn,
    symbol: str,
    period: str,
    range_start: datetime,
    range_end: datetime,
    *,
    gap_multiple: float = 1.5,
) -> list[tuple[datetime, datetime]]:
    """
    Detect missing time spans within ``[range_start, range_end]`` for a (symbol, period) series.
    """
    if range_start.tzinfo is None or range_end.tzinfo is None:
        raise ValueError("range_start and range_end must be timezone-aware")
    if range_end < range_start:
        return []

    sym = symbol.strip().upper()
    pd = period.strip()
    pd_ms = interval_to_millis(pd)
    pd_td = timedelta(milliseconds=pd_ms)
    threshold = pd_ms * gap_multiple

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sample_time FROM top_trader_long_short
            WHERE symbol = %s AND period = %s
              AND sample_time >= %s AND sample_time <= %s
            ORDER BY sample_time
            """,
            (sym, pd, range_start, range_end),
        )
        times = [r[0] for r in cur.fetchall()]

    if not times:
        return [(range_start, range_end)]

    gaps: list[tuple[datetime, datetime]] = []
    rs = range_start.astimezone(timezone.utc)
    re_ = range_end.astimezone(timezone.utc)

    first_ms = (times[0] - rs).total_seconds() * 1000.0
    if first_ms > threshold:
        gaps.append((rs, times[0] - pd_td))

    for i in range(len(times) - 1):
        delta_ms = (times[i + 1] - times[i]).total_seconds() * 1000.0
        if delta_ms > threshold:
            g0 = times[i] + pd_td
            g1 = times[i + 1] - pd_td
            if g0 <= g1:
                gaps.append((g0, g1))

    last_ms = (re_ - times[-1]).total_seconds() * 1000.0
    if last_ms > threshold:
        g0 = times[-1] + pd_td
        if g0 <= re_:
            gaps.append((g0, re_))

    return gaps


def run_repair_top_trader_long_short_gap(
    conn,
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    *,
    start_time_ms: int,
    end_time_ms: int,
    chunk_limit: int = TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
) -> int:
    """Repair gaps by re-fetching and upserting data for ``[start_time_ms, end_time_ms]``."""
    if start_time_ms >= end_time_ms:
        return 0

    from market_data.jobs.common import iter_top_trader_long_short_batches_forward

    total = 0
    for batch in iter_top_trader_long_short_batches_forward(
        provider,
        symbol,
        period,
        start_ms=start_time_ms,
        end_ms=end_time_ms,
        chunk_limit=chunk_limit,
    ):
        upsert_top_trader_long_short_points(conn, batch)
        conn.commit()
        total += len(batch)
    return total


@dataclass(frozen=True)
class PolicyTopTraderLongShortRepairSeriesResult:
    symbol: str
    period: str
    gap_spans: int
    rows_upserted: int


def run_repair_detected_top_trader_long_short_gaps(
    conn,
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    gaps: list[tuple[datetime, datetime]],
    *,
    chunk_limit: int = TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
) -> int:
    total = 0
    for g0, g1 in gaps:
        total += run_repair_top_trader_long_short_gap(
            conn,
            provider,
            symbol,
            period,
            start_time_ms=int(g0.timestamp() * 1000),
            end_time_ms=int(g1.timestamp() * 1000),
            chunk_limit=chunk_limit,
        )
    return total


def run_repair_top_trader_long_short_gaps_policy_window_all_series(
    settings: MarketDataSettings,
    *,
    provider: TopTraderLongShortPositionRatioProvider | None = None,
    provider_executor: ProviderExecutor[PolicyTopTraderLongShortRepairSeriesResult] | None = None,
    backfill_days: int | None = None,
    gap_multiple: float | None = None,
) -> list[PolicyTopTraderLongShortRepairSeriesResult]:
    """
    Policy-window repair for all configured top trader long/short series.
    """
    days = (
        TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS
        if backfill_days is None
        else backfill_days
    )
    gm = OHLCV_SKIP_EXISTING_GAP_MULTIPLE if gap_multiple is None else gap_multiple

    end_ms = utc_now_ms()
    range_end = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

    prov = provider if provider is not None else build_binance_perps_provider(settings)
    own_executor = False
    ex = provider_executor
    if ex is None:
        ex = ProviderExecutor[PolicyTopTraderLongShortRepairSeriesResult](
            ProviderExecutorConfig(max_workers=FUTURES_REPAIR_GAP_MAX_WORKERS)
        )
        own_executor = True

    tasks: list[tuple[str, str]] = [
        (symbol, period)
        for symbol in TOP_TRADER_LONG_SHORT_SYMBOLS
        for period in TOP_TRADER_LONG_SHORT_PERIODS
    ]

    def _run_task(symbol: str, period: str) -> PolicyTopTraderLongShortRepairSeriesResult:
        conn = psycopg2.connect(settings.database_url)
        configure_for_market_data(conn)
        try:
            start_ms = end_ms - days * 86_400_000
            start_ms = floor_align_ms_to_interval(start_ms, period)
            range_start = datetime.fromtimestamp(
                start_ms / 1000.0,
                tz=timezone.utc,
            )

            gaps = detect_top_trader_long_short_time_gaps(
                conn,
                symbol,
                period,
                range_start,
                range_end,
                gap_multiple=gm,
            )
            if not gaps:
                return PolicyTopTraderLongShortRepairSeriesResult(symbol, period, 0, 0)

            n = run_repair_detected_top_trader_long_short_gaps(
                conn,
                prov,
                symbol,
                period,
                gaps,
            )
            logger.info(
                "market_data top trader long/short gap repair symbol={} period={} gap_spans={} rows_upserted={}",
                symbol,
                period,
                len(gaps),
                n,
            )
            return PolicyTopTraderLongShortRepairSeriesResult(
                symbol, period, len(gaps), n
            )
        finally:
            conn.close()

    out: list[PolicyTopTraderLongShortRepairSeriesResult] = []
    try:
        t0 = time.perf_counter()
        logger.info(
            "repair_gap_top_trader_long_short run start: tasks={} workers={}",
            len(tasks),
            ex.max_workers,
        )
        if ex.max_workers <= 1:
            for symbol, period in tasks:
                out.append(_run_task(symbol, period))
            logger.info(
                "repair_gap_top_trader_long_short run done: submitted={} completed={} failed=0 wall_clock_s={:.3f}",
                len(tasks),
                len(tasks),
                time.perf_counter() - t0,
            )
            return out

        futures: list[Future[PolicyTopTraderLongShortRepairSeriesResult]] = []
        future_to_task: dict[Future[PolicyTopTraderLongShortRepairSeriesResult], tuple[str, str]] = {}
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
                    "repair_gap_top_trader_long_short task failed: symbol={} period={}",
                    symbol,
                    period,
                )

        logger.info(
            "repair_gap_top_trader_long_short run done: submitted={} completed={} failed={} wall_clock_s={:.3f}",
            len(tasks),
            len(tasks) - len(failed_tasks),
            len(failed_tasks),
            time.perf_counter() - t0,
        )
        if failed_tasks:
            failed_labels = [f"{symbol}/{period}" for symbol, period in failed_tasks]
            raise RuntimeError(
                "repair_gap_top_trader_long_short failed for task(s): "
                + ", ".join(sorted(failed_labels))
            )
        return out
    finally:
        if own_executor and ex is not None:
            ex.shutdown(wait=True)

