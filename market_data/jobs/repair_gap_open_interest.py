"""Targeted range repair and gap detection for open-interest snapshots."""

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
    OPEN_INTEREST_CONTRACT_TYPES,
    OPEN_INTEREST_FETCH_CHUNK_LIMIT,
    OPEN_INTEREST_INITIAL_BACKFILL_DAYS,
    OPEN_INTEREST_PERIODS,
    OPEN_INTEREST_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import (
    floor_align_ms_to_interval,
    iter_open_interest_batches_forward,
    utc_now_ms,
)
from market_data.providers.base import OpenInterestProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.providers.executor import ProviderExecutor, ProviderExecutorConfig
from market_data.storage import upsert_open_interest_points


def run_repair_open_interest_gap(
    conn,
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    start_time_ms: int,
    end_time_ms: int,
    chunk_limit: int = OPEN_INTEREST_FETCH_CHUNK_LIMIT,
) -> int:
    if start_time_ms >= end_time_ms:
        return 0
    total = 0
    for batch in iter_open_interest_batches_forward(
        provider,
        symbol,
        contract_type,
        period,
        start_ms=start_time_ms,
        end_ms=end_time_ms,
        chunk_limit=chunk_limit,
    ):
        upsert_open_interest_points(conn, batch)
        conn.commit()
        total += len(batch)
    return total


def detect_open_interest_time_gaps(
    conn,
    symbol: str,
    contract_type: str,
    period: str,
    range_start: datetime,
    range_end: datetime,
    *,
    gap_multiple: float = 1.5,
) -> list[tuple[datetime, datetime]]:
    if range_start.tzinfo is None or range_end.tzinfo is None:
        raise ValueError("range_start and range_end must be timezone-aware")
    if range_end < range_start:
        return []

    sym = symbol.strip().upper()
    ct = contract_type.strip().upper()
    pd = period.strip()
    pd_ms = interval_to_millis(pd)
    pd_td = timedelta(milliseconds=pd_ms)
    threshold = pd_ms * gap_multiple

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sample_time FROM open_interest
            WHERE symbol = %s AND contract_type = %s AND period = %s
              AND sample_time >= %s AND sample_time <= %s
            ORDER BY sample_time
            """,
            (sym, ct, pd, range_start, range_end),
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


@dataclass(frozen=True)
class PolicyOpenInterestRepairSeriesResult:
    symbol: str
    contract_type: str
    period: str
    gap_spans: int
    rows_upserted: int


def run_repair_detected_open_interest_gaps(
    conn,
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    gaps: list[tuple[datetime, datetime]],
    *,
    chunk_limit: int = OPEN_INTEREST_FETCH_CHUNK_LIMIT,
) -> int:
    total = 0
    for g0, g1 in gaps:
        total += run_repair_open_interest_gap(
            conn,
            provider,
            symbol,
            contract_type,
            period,
            start_time_ms=int(g0.timestamp() * 1000),
            end_time_ms=int(g1.timestamp() * 1000),
            chunk_limit=chunk_limit,
        )
    return total


def run_repair_open_interest_gaps_policy_window_all_series(
    settings: MarketDataSettings,
    *,
    provider: OpenInterestProvider | None = None,
    provider_executor: ProviderExecutor[PolicyOpenInterestRepairSeriesResult] | None = None,
    backfill_days: int | None = None,
    gap_multiple: float | None = None,
) -> list[PolicyOpenInterestRepairSeriesResult]:
    days = OPEN_INTEREST_INITIAL_BACKFILL_DAYS if backfill_days is None else backfill_days
    gm = OHLCV_SKIP_EXISTING_GAP_MULTIPLE if gap_multiple is None else gap_multiple
    end_ms = utc_now_ms()
    range_end = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

    prov = provider if provider is not None else build_binance_perps_provider(settings)
    own_executor = False
    ex = provider_executor
    if ex is None:
        ex = ProviderExecutor[PolicyOpenInterestRepairSeriesResult](
            ProviderExecutorConfig(max_workers=FUTURES_REPAIR_GAP_MAX_WORKERS)
        )
        own_executor = True

    tasks: list[tuple[str, str, str]] = [
        (symbol, contract_type, period)
        for symbol in OPEN_INTEREST_SYMBOLS
        for contract_type in OPEN_INTEREST_CONTRACT_TYPES
        for period in OPEN_INTEREST_PERIODS
    ]

    def _run_task(
        symbol: str,
        contract_type: str,
        period: str,
    ) -> PolicyOpenInterestRepairSeriesResult:
        conn = psycopg2.connect(settings.database_url)
        configure_for_market_data(conn)
        try:
            task_start_ms = end_ms - days * 86_400_000
            task_start_ms = floor_align_ms_to_interval(task_start_ms, period)
            range_start = datetime.fromtimestamp(task_start_ms / 1000.0, tz=timezone.utc)
            gaps = detect_open_interest_time_gaps(
                conn,
                symbol,
                contract_type,
                period,
                range_start,
                range_end,
                gap_multiple=gm,
            )
            if not gaps:
                return PolicyOpenInterestRepairSeriesResult(symbol, contract_type, period, 0, 0)
            n = run_repair_detected_open_interest_gaps(
                conn,
                prov,
                symbol,
                contract_type,
                period,
                gaps,
            )
            logger.info(
                "market_data open_interest gap repair symbol={} contract_type={} period={} gap_spans={} rows_upserted={}",
                symbol,
                contract_type,
                period,
                len(gaps),
                n,
            )
            return PolicyOpenInterestRepairSeriesResult(
                symbol, contract_type, period, len(gaps), n
            )
        finally:
            conn.close()

    out: list[PolicyOpenInterestRepairSeriesResult] = []
    try:
        t0 = time.perf_counter()
        logger.info(
            "repair_gap_open_interest run start: tasks={} workers={}",
            len(tasks),
            ex.max_workers,
        )
        if ex.max_workers <= 1:
            for symbol, contract_type, period in tasks:
                out.append(_run_task(symbol, contract_type, period))
            logger.info(
                "repair_gap_open_interest run done: submitted={} completed={} failed=0 wall_clock_s={:.3f}",
                len(tasks),
                len(tasks),
                time.perf_counter() - t0,
            )
            return out

        futures: list[Future[PolicyOpenInterestRepairSeriesResult]] = []
        future_to_task: dict[Future[PolicyOpenInterestRepairSeriesResult], tuple[str, str, str]] = {}
        for symbol, contract_type, period in tasks:
            fut = ex.submit(_run_task, symbol, contract_type, period)
            futures.append(fut)
            future_to_task[fut] = (symbol, contract_type, period)

        failed_tasks: list[tuple[str, str, str]] = []
        for fut in futures:
            symbol, contract_type, period = future_to_task[fut]
            try:
                out.append(fut.result())
            except Exception:
                failed_tasks.append((symbol, contract_type, period))
                logger.exception(
                    "repair_gap_open_interest task failed: symbol={} contract_type={} period={}",
                    symbol,
                    contract_type,
                    period,
                )

        logger.info(
            "repair_gap_open_interest run done: submitted={} completed={} failed={} wall_clock_s={:.3f}",
            len(tasks),
            len(tasks) - len(failed_tasks),
            len(failed_tasks),
            time.perf_counter() - t0,
        )
        if failed_tasks:
            failed_labels = [
                f"{symbol}/{contract_type}/{period}"
                for symbol, contract_type, period in failed_tasks
            ]
            raise RuntimeError(
                "repair_gap_open_interest failed for task(s): "
                + ", ".join(sorted(failed_labels))
            )
        return out
    finally:
        if own_executor and ex is not None:
            ex.shutdown(wait=True)
