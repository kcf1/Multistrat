"""
Incremental taker buy/sell volume ingest.

Uses ``taker_buy_sell_volume_cursor`` and ``MAX(sample_time)`` to choose the next
``startTime`` for each (symbol, period) series; empty series starts from
``TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS`` horizon.
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
    TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
    TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS,
    TAKER_BUYSELL_VOLUME_PERIODS,
    TAKER_BUYSELL_VOLUME_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import (
    floor_align_ms_to_interval,
    open_time_plus_interval_ms,
    utc_now_ms,
    iter_taker_buy_sell_volume_batches_forward,
)
from market_data.jobs.repair_gap_taker_buy_sell_volume import (
    detect_taker_buy_sell_volume_time_gaps,
)
from market_data.providers.base import TakerBuySellVolumeProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.providers.executor import ProviderExecutor, ProviderExecutorConfig
from market_data.schemas import TakerBuySellVolumePoint
from market_data.storage import (
    get_taker_buy_sell_volume_cursor,
    max_sample_time_taker_buy_sell_volume,
    upsert_taker_buy_sell_volume_cursor,
    upsert_taker_buy_sell_volume_points,
)


@dataclass(frozen=True)
class IngestTakerBuySellVolumeSeriesResult:
    symbol: str
    period: str
    rows_upserted: int
    chunks: int
    fetch_give_ups: tuple[str, ...] = ()


def resolve_taker_buy_sell_volume_ingest_start_ms(
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

    c = get_taker_buy_sell_volume_cursor(conn, symbol, period)
    m = max_sample_time_taker_buy_sell_volume(conn, symbol, period)

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


def _ingest_taker_buy_sell_volume_forward_segment(
    conn,
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[TakerBuySellVolumePoint]], None] | None,
) -> tuple[int, int]:
    total = 0
    chunks = 0
    if start_ms >= end_ms:
        return 0, 0

    for batch in iter_taker_buy_sell_volume_batches_forward(
        provider,
        symbol,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        chunks += 1
        upsert_taker_buy_sell_volume_points(conn, batch)
        max_st = max(p.sample_time for p in batch)
        upsert_taker_buy_sell_volume_cursor(conn, symbol, period, max_st)
        conn.commit()
        total += len(batch)
        if chunk_progress is not None:
            chunk_progress(batch)

    return total, chunks


def _ingest_taker_buy_sell_volume_skip_existing_gap_mode(
    conn,
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    *,
    end_ms: int,
    backfill_days: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[TakerBuySellVolumePoint]], None] | None,
    give_before: int,
) -> IngestTakerBuySellVolumeSeriesResult:
    """
    Fill missing spans within the policy horizon via detect+segment gap repair.
    """
    pd_ms = interval_to_millis(period)
    horizon_ms = end_ms - backfill_days * 86_400_000
    horizon_ms = floor_align_ms_to_interval(horizon_ms, period)

    if horizon_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestTakerBuySellVolumeSeriesResult(symbol, period, 0, 0, give_msgs)

    horizon_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    gaps = detect_taker_buy_sell_volume_time_gaps(
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
        t, c = _ingest_taker_buy_sell_volume_forward_segment(
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

    # Tail catch-up: always ingest forward from the last stored sample time
    # (after alignment) up to `end_ms`.
    m = max_sample_time_taker_buy_sell_volume(conn, symbol, period)
    tail_start = (
        max(horizon_ms, open_time_plus_interval_ms(m, pd_ms))
        if m is not None
        else horizon_ms
    )
    if tail_start < end_ms:
        t, c = _ingest_taker_buy_sell_volume_forward_segment(
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
    return IngestTakerBuySellVolumeSeriesResult(symbol, period, total, chunks, give_msgs)


def ingest_taker_buy_sell_volume_series(
    conn,
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    *,
    now_ms: int | None = None,
    chunk_limit: int = TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
    backfill_days: int = TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS,
    chunk_progress: Callable[[Sequence[TakerBuySellVolumePoint]], None] | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> IngestTakerBuySellVolumeSeriesResult:
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    give_before = len(getattr(provider, "fetch_give_ups", []))

    if not use_watermark and skip_existing_when_no_watermark:
        return _ingest_taker_buy_sell_volume_skip_existing_gap_mode(
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

    start_ms = resolve_taker_buy_sell_volume_ingest_start_ms(
        conn,
        symbol,
        period,
        now_ms=end_ms,
        backfill_days=backfill_days,
        use_watermark=use_watermark,
    )

    if start_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestTakerBuySellVolumeSeriesResult(symbol, period, 0, 0, give_msgs)

    total, chunks = _ingest_taker_buy_sell_volume_forward_segment(
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
    return IngestTakerBuySellVolumeSeriesResult(symbol, period, total, chunks, give_msgs)


def run_ingest_taker_buy_sell_volume(
    settings: MarketDataSettings,
    *,
    symbols: Sequence[str] | None = None,
    provider: TakerBuySellVolumeProvider | None = None,
    provider_executor: ProviderExecutor[IngestTakerBuySellVolumeSeriesResult] | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> list[IngestTakerBuySellVolumeSeriesResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    own_executor = False
    ex = provider_executor
    if ex is None:
        ex = ProviderExecutor[IngestTakerBuySellVolumeSeriesResult](
            ProviderExecutorConfig(max_workers=GLOBAL_PROVIDER_MAX_WORKERS)
        )
        own_executor = True

    sym_list = list(symbols) if symbols is not None else list(TAKER_BUYSELL_VOLUME_SYMBOLS)
    tasks: list[tuple[str, str]] = [
        (symbol, period)
        for symbol in sym_list
        for period in TAKER_BUYSELL_VOLUME_PERIODS
    ]

    def _ingest_task(symbol: str, period: str) -> IngestTakerBuySellVolumeSeriesResult:
        conn = psycopg2.connect(settings.database_url)
        configure_for_market_data(conn)
        try:
            return ingest_taker_buy_sell_volume_series(
                conn,
                prov,
                symbol,
                period,
                chunk_limit=TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
                backfill_days=TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS,
                use_watermark=use_watermark,
                skip_existing_when_no_watermark=skip_existing_when_no_watermark,
            )
        finally:
            conn.close()

    try:
        t0 = time.perf_counter()
        logger.info(
            "ingest_taker_buy_sell_volume run start: tasks={} workers={}",
            len(tasks),
            ex.max_workers,
        )
        out: list[IngestTakerBuySellVolumeSeriesResult] = []
        if ex.max_workers <= 1:
            for symbol, period in tasks:
                out.append(_ingest_task(symbol, period))
            logger.info(
                "ingest_taker_buy_sell_volume run done: submitted={} completed={} failed=0 wall_clock_s={:.3f}",
                len(tasks),
                len(tasks),
                time.perf_counter() - t0,
            )
            return out

        futures: list[Future[IngestTakerBuySellVolumeSeriesResult]] = []
        future_to_task: dict[Future[IngestTakerBuySellVolumeSeriesResult], tuple[str, str]] = {}
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
                    "ingest_taker_buy_sell_volume task failed: symbol={} period={}",
                    symbol,
                    period,
                )

        logger.info(
            "ingest_taker_buy_sell_volume run done: submitted={} completed={} failed={} wall_clock_s={:.3f}",
            len(tasks),
            len(tasks) - len(failed_tasks),
            len(failed_tasks),
            time.perf_counter() - t0,
        )
        if failed_tasks:
            failed_labels = [f"{symbol}/{period}" for symbol, period in failed_tasks]
            raise RuntimeError(
                "ingest_taker_buy_sell_volume failed for task(s): "
                + ", ".join(sorted(failed_labels))
            )
        return out
    finally:
        if own_executor and ex is not None:
            ex.shutdown(wait=True)

