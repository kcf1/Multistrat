"""
Incremental top trader long/short ratio ingest.

Mirrors the taker buy/sell volume ingest pipeline, keyed by:
- (symbol, period, sample_time)
- cursor table keyed by (symbol, period)
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, timezone
import time

import psycopg2
from loguru import logger

from pgconn import configure_for_market_data
from market_data.config import (
    GLOBAL_PROVIDER_MAX_WORKERS,
    OHLCV_SKIP_EXISTING_GAP_MULTIPLE,
    TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
    TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS,
    TOP_TRADER_LONG_SHORT_PERIODS,
    TOP_TRADER_LONG_SHORT_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import (
    floor_align_ms_to_interval,
    open_time_plus_interval_ms,
    utc_now_ms,
    iter_top_trader_long_short_batches_forward,
)
from market_data.jobs.repair_gap_top_trader_long_short import (
    detect_top_trader_long_short_time_gaps,
)
from market_data.providers.base import TopTraderLongShortPositionRatioProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.providers.executor import ProviderExecutor, ProviderExecutorConfig
from market_data.schemas import TopTraderLongShortPoint
from market_data.storage import (
    get_top_trader_long_short_cursor,
    max_sample_time_top_trader_long_short,
    upsert_top_trader_long_short_cursor,
    upsert_top_trader_long_short_points,
)


@dataclass(frozen=True)
class IngestTopTraderLongShortSeriesResult:
    symbol: str
    period: str
    rows_upserted: int
    chunks: int
    fetch_give_ups: tuple[str, ...] = ()


def resolve_top_trader_long_short_ingest_start_ms(
    conn,
    symbol: str,
    period: str,
    *,
    now_ms: int,
    backfill_days: int,
    use_watermark: bool = True,
) -> int:
    pd_ms = interval_to_millis(period)
    horizon_ms = now_ms - backfill_days * 86_400_000
    horizon_ms = floor_align_ms_to_interval(horizon_ms, period)
    if not use_watermark:
        return horizon_ms

    c = get_top_trader_long_short_cursor(conn, symbol, period)
    m = max_sample_time_top_trader_long_short(conn, symbol, period)

    ref = None
    if c is not None and m is not None:
        ref = max(c, m, key=lambda t: t.timestamp())
    elif c is not None:
        ref = c
    elif m is not None:
        ref = m

    if ref is not None:
        return open_time_plus_interval_ms(ref, pd_ms)
    return horizon_ms


def _ingest_top_trader_long_short_forward_segment(
    conn,
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[TopTraderLongShortPoint]], None] | None,
) -> tuple[int, int]:
    total = 0
    chunks = 0
    if start_ms >= end_ms:
        return 0, 0

    for batch in iter_top_trader_long_short_batches_forward(
        provider,
        symbol,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        chunks += 1
        upsert_top_trader_long_short_points(conn, batch)
        max_st = max(p.sample_time for p in batch)
        upsert_top_trader_long_short_cursor(conn, symbol, period, max_st)
        conn.commit()
        total += len(batch)
        if chunk_progress is not None:
            chunk_progress(batch)

    return total, chunks


def _ingest_top_trader_long_short_skip_existing_gap_mode(
    conn,
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    *,
    end_ms: int,
    backfill_days: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[TopTraderLongShortPoint]], None] | None,
    give_before: int,
) -> IngestTopTraderLongShortSeriesResult:
    """
    Fill missing spans within the policy horizon via detect+segment gap repair.
    """
    pd_ms = interval_to_millis(period)
    horizon_ms = end_ms - backfill_days * 86_400_000
    horizon_ms = floor_align_ms_to_interval(horizon_ms, period)

    if horizon_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestTopTraderLongShortSeriesResult(symbol, period, 0, 0, give_msgs)

    horizon_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    gaps = detect_top_trader_long_short_time_gaps(
        conn,
        symbol,
        period,
        horizon_dt,
        end_dt,
        gap_multiple=OHLCV_SKIP_EXISTING_GAP_MULTIPLE,
    )

    total = 0
    chunks = 0
    for g0, g1 in gaps:
        seg_start = max(horizon_ms, int(g0.timestamp() * 1000))
        seg_end = min(end_ms, int(g1.timestamp() * 1000))
        if seg_start >= seg_end:
            continue
        t, c = _ingest_top_trader_long_short_forward_segment(
            conn,
            provider,
            symbol,
            period,
            start_ms=seg_start,
            end_ms=seg_end,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
        )
        total += t
        chunks += c

    # Tail catch-up
    m = max_sample_time_top_trader_long_short(conn, symbol, period)
    tail_start = (
        max(horizon_ms, open_time_plus_interval_ms(m, pd_ms))
        if m is not None
        else horizon_ms
    )
    if tail_start < end_ms:
        t, c = _ingest_top_trader_long_short_forward_segment(
            conn,
            provider,
            symbol,
            period,
            start_ms=tail_start,
            end_ms=end_ms,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
        )
        total += t
        chunks += c

    give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
    return IngestTopTraderLongShortSeriesResult(symbol, period, total, chunks, give_msgs)


def ingest_top_trader_long_short_series(
    conn,
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    *,
    now_ms: int | None = None,
    chunk_limit: int = TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
    backfill_days: int = TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS,
    chunk_progress: Callable[[Sequence[TopTraderLongShortPoint]], None] | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> IngestTopTraderLongShortSeriesResult:
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    give_before = len(getattr(provider, "fetch_give_ups", []))

    if not use_watermark and skip_existing_when_no_watermark:
        return _ingest_top_trader_long_short_skip_existing_gap_mode(
            conn,
            provider,
            symbol,
            period,
            end_ms=end_ms,
            backfill_days=backfill_days,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
            give_before=give_before,
        )

    start_ms = resolve_top_trader_long_short_ingest_start_ms(
        conn,
        symbol,
        period,
        now_ms=end_ms,
        backfill_days=backfill_days,
        use_watermark=use_watermark,
    )

    if start_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestTopTraderLongShortSeriesResult(symbol, period, 0, 0, give_msgs)

    total, chunks = _ingest_top_trader_long_short_forward_segment(
        conn,
        provider,
        symbol,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
        chunk_progress=chunk_progress,
    )

    give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
    return IngestTopTraderLongShortSeriesResult(symbol, period, total, chunks, give_msgs)


def run_ingest_top_trader_long_short(
    settings: MarketDataSettings,
    *,
    provider: TopTraderLongShortPositionRatioProvider | None = None,
    provider_executor: ProviderExecutor[IngestTopTraderLongShortSeriesResult] | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> list[IngestTopTraderLongShortSeriesResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    own_executor = False
    ex = provider_executor
    if ex is None:
        ex = ProviderExecutor[IngestTopTraderLongShortSeriesResult](
            ProviderExecutorConfig(max_workers=GLOBAL_PROVIDER_MAX_WORKERS)
        )
        own_executor = True

    tasks: list[tuple[str, str]] = [
        (symbol, period)
        for symbol in TOP_TRADER_LONG_SHORT_SYMBOLS
        for period in TOP_TRADER_LONG_SHORT_PERIODS
    ]

    def _ingest_task(symbol: str, period: str) -> IngestTopTraderLongShortSeriesResult:
        conn = psycopg2.connect(settings.database_url)
        configure_for_market_data(conn)
        try:
            return ingest_top_trader_long_short_series(
                conn,
                prov,
                symbol,
                period,
                chunk_limit=TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT,
                backfill_days=TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS,
                use_watermark=use_watermark,
                skip_existing_when_no_watermark=skip_existing_when_no_watermark,
            )
        finally:
            conn.close()

    try:
        t0 = time.perf_counter()
        logger.info(
            "ingest_top_trader_long_short run start: tasks={} workers={}",
            len(tasks),
            ex.max_workers,
        )
        out: list[IngestTopTraderLongShortSeriesResult] = []
        if ex.max_workers <= 1:
            for symbol, period in tasks:
                out.append(_ingest_task(symbol, period))
            logger.info(
                "ingest_top_trader_long_short run done: submitted={} completed={} failed=0 wall_clock_s={:.3f}",
                len(tasks),
                len(tasks),
                time.perf_counter() - t0,
            )
            return out

        futures: list[Future[IngestTopTraderLongShortSeriesResult]] = []
        future_to_task: dict[Future[IngestTopTraderLongShortSeriesResult], tuple[str, str]] = {}
        for symbol, period in tasks:
            fut = ex.submit(_ingest_task, symbol, period)
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
                    "ingest_top_trader_long_short task failed: symbol={} period={}",
                    symbol,
                    period,
                )

        logger.info(
            "ingest_top_trader_long_short run done: submitted={} completed={} failed={} wall_clock_s={:.3f}",
            len(tasks),
            len(tasks) - len(failed_tasks),
            len(failed_tasks),
            time.perf_counter() - t0,
        )
        if failed_tasks:
            failed_labels = [f"{symbol}/{period}" for symbol, period in failed_tasks]
            raise RuntimeError(
                "ingest_top_trader_long_short failed for task(s): "
                + ", ".join(sorted(failed_labels))
            )
        return out
    finally:
        if own_executor and ex is not None:
            ex.shutdown(wait=True)

