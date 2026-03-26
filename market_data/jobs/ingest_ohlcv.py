"""
Incremental OHLCV ingest + backfill (Phase 4 §9.5 / §4.5.1).

Uses ``ingestion_cursor`` and ``MAX(open_time)`` to choose the next ``startTime``; empty
series starts ``OHLCV_INITIAL_BACKFILL_DAYS`` ago. Commits after each chunk (checkpoint).
``skip_existing_when_no_watermark`` refetches :func:`detect_ohlcv_time_gaps` inside the
policy window, not a bare ``max(open_time)`` resume.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2

from market_data.config import (
    OHLCV_INITIAL_BACKFILL_DAYS,
    OHLCV_KLINES_CHUNK_LIMIT,
    OHLCV_PROVIDER_MAX_WORKERS,
    OHLCV_SKIP_EXISTING_GAP_MULTIPLE,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import (
    iter_kline_batches_forward,
    open_time_plus_interval_ms,
    utc_now_ms,
)
from market_data.jobs.repair_gap import detect_ohlcv_time_gaps
from market_data.schemas import OhlcvBar
from market_data.providers.base import KlinesProvider
from market_data.providers.binance_spot import build_binance_spot_provider
from market_data.providers.executor import ProviderExecutor, ProviderExecutorConfig
from market_data.storage import (
    get_ingestion_cursor,
    max_open_time_ohlcv,
    upsert_ingestion_cursor,
    upsert_ohlcv_bars,
)


@dataclass(frozen=True)
class IngestSeriesResult:
    symbol: str
    interval: str
    bars_upserted: int
    chunks: int
    fetch_give_ups: tuple[str, ...] = ()


def resolve_ingest_start_ms(
    conn,
    symbol: str,
    interval: str,
    *,
    now_ms: int,
    backfill_days: int,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> int:
    """
    First ``startTime`` for a **single** forward sweep (tqdm estimate, watermark path).

    With ``use_watermark=True`` (default): after ``max(ingestion_cursor, max(open_time))``
    or ``now_ms - backfill_days`` when empty.

    With ``use_watermark=False``: ``now_ms - backfill_days`` (``skip_existing_when_no_watermark``
    is ignored here; gap-based skip uses :func:`detect_ohlcv_time_gaps` in
    :func:`ingest_ohlcv_series`).
    """
    iv_ms = interval_to_millis(interval)
    horizon_ms = now_ms - backfill_days * 86_400_000

    if not use_watermark:
        return horizon_ms
    c = get_ingestion_cursor(conn, symbol, interval)
    m = max_open_time_ohlcv(conn, symbol, interval)
    ref = None
    if c is not None and m is not None:
        ref = max(c, m, key=lambda t: t.timestamp())
    elif c is not None:
        ref = c
    elif m is not None:
        ref = m
    if ref is not None:
        return open_time_plus_interval_ms(ref, iv_ms)
    return horizon_ms


def _ingest_forward_segment(
    conn,
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[OhlcvBar]], None] | None,
) -> tuple[int, int]:
    """Fetch ``[start_ms, end_ms)`` style paging toward ``end_ms``; upsert + cursor each chunk."""
    total = 0
    chunks = 0
    if start_ms >= end_ms:
        return 0, 0
    for batch in iter_kline_batches_forward(
        provider,
        symbol,
        interval,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        chunks += 1
        upsert_ohlcv_bars(conn, batch)
        max_ot = max(b.open_time for b in batch)
        upsert_ingestion_cursor(conn, symbol, interval, max_ot)
        conn.commit()
        total += len(batch)
        if chunk_progress is not None:
            chunk_progress(batch)
    return total, chunks


def _ingest_ohlcv_skip_existing_gap_mode(
    conn,
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    *,
    end_ms: int,
    backfill_days: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[OhlcvBar]], None] | None,
    give_before: int,
) -> IngestSeriesResult:
    """
    Fill missing spans in ``[horizon, end_ms]`` via :func:`detect_ohlcv_time_gaps`, then
    tail-catch-up from ``max(open_time)`` (policy-clamped).
    """
    iv_ms = interval_to_millis(interval)
    horizon_ms = end_ms - backfill_days * 86_400_000
    if horizon_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestSeriesResult(symbol, interval, 0, 0, give_msgs)

    horizon_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

    gaps = detect_ohlcv_time_gaps(
        conn,
        symbol,
        interval,
        horizon_dt,
        end_dt,
        gap_multiple=OHLCV_SKIP_EXISTING_GAP_MULTIPLE,
    )

    total = 0
    chunks = 0
    for g0, g1 in gaps:
        g0_ms = int(g0.timestamp() * 1000)
        g1_ms = int(g1.timestamp() * 1000)
        seg_start = max(horizon_ms, g0_ms)
        seg_end = min(end_ms, g1_ms)
        if seg_start >= seg_end:
            continue
        t, c = _ingest_forward_segment(
            conn,
            provider,
            symbol,
            interval,
            start_ms=seg_start,
            end_ms=seg_end,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
        )
        total += t
        chunks += c

    m = max_open_time_ohlcv(conn, symbol, interval)
    if m is not None:
        tail_start = max(horizon_ms, open_time_plus_interval_ms(m, iv_ms))
    else:
        tail_start = horizon_ms
    if tail_start < end_ms:
        t, c = _ingest_forward_segment(
            conn,
            provider,
            symbol,
            interval,
            start_ms=tail_start,
            end_ms=end_ms,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
        )
        total += t
        chunks += c

    give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
    return IngestSeriesResult(symbol, interval, total, chunks, give_msgs)


def ingest_ohlcv_series(
    conn,
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    *,
    now_ms: int | None = None,
    chunk_limit: int = OHLCV_KLINES_CHUNK_LIMIT,
    backfill_days: int = OHLCV_INITIAL_BACKFILL_DAYS,
    chunk_progress: Callable[[Sequence[OhlcvBar]], None] | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> IngestSeriesResult:
    """
    Catch up ``(symbol, interval)`` to ``now`` using chunked fetches + upsert + cursor.

    Commits after each successful chunk.

    ``chunk_progress``: optional callback invoked after each committed chunk with that batch
    (e.g. ``tqdm.update(len(batch))``).

    With ``use_watermark=False`` and ``skip_existing_when_no_watermark=True``, runs
    :func:`detect_ohlcv_time_gaps` on ``[now - backfill_days, now]`` and refetches each
    gap plus a final forward segment from ``max(open_time)``.

    Otherwise see :func:`resolve_ingest_start_ms` for the starting ``startTime``.
    """
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    give_before = len(getattr(provider, "fetch_give_ups", []))

    if not use_watermark and skip_existing_when_no_watermark:
        return _ingest_ohlcv_skip_existing_gap_mode(
            conn,
            provider,
            symbol,
            interval,
            end_ms=end_ms,
            backfill_days=backfill_days,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
            give_before=give_before,
        )

    start_ms = resolve_ingest_start_ms(
        conn,
        symbol,
        interval,
        now_ms=end_ms,
        backfill_days=backfill_days,
        use_watermark=use_watermark,
        skip_existing_when_no_watermark=False,
    )
    if start_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestSeriesResult(symbol, interval, 0, 0, give_msgs)

    total, chunks = _ingest_forward_segment(
        conn,
        provider,
        symbol,
        interval,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
        chunk_progress=chunk_progress,
    )

    give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
    return IngestSeriesResult(symbol, interval, total, chunks, give_msgs)


def run_ingest_ohlcv(
    settings: MarketDataSettings,
    *,
    provider: KlinesProvider | None = None,
    provider_executor: ProviderExecutor[IngestSeriesResult] | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> list[IngestSeriesResult]:
    """Run ingest for every ``(symbol, interval)`` in settings."""
    prov = provider if provider is not None else build_binance_spot_provider(settings)
    own_executor = False
    ex = provider_executor
    if ex is None:
        ex = ProviderExecutor[IngestSeriesResult](
            ProviderExecutorConfig(max_workers=OHLCV_PROVIDER_MAX_WORKERS)
        )
        own_executor = True

    def _ingest_symbol(symbol: str) -> list[IngestSeriesResult]:
        conn = psycopg2.connect(settings.database_url)
        try:
            out_symbol: list[IngestSeriesResult] = []
            for iv in settings.intervals:
                out_symbol.append(
                    ingest_ohlcv_series(
                        conn,
                        prov,
                        symbol,
                        iv,
                        use_watermark=use_watermark,
                        skip_existing_when_no_watermark=skip_existing_when_no_watermark,
                    )
                )
            return out_symbol
        finally:
            conn.close()

    try:
        out: list[IngestSeriesResult] = []
        if ex.max_workers <= 1:
            for sym in settings.symbols:
                out.extend(_ingest_symbol(sym))
            return out

        futures: list[Future[list[IngestSeriesResult]]] = []
        for sym in settings.symbols:
            futures.append(ex.submit(_ingest_symbol, sym))
        for fut in futures:
            out.extend(fut.result())
        return out
    finally:
        if own_executor and ex is not None:
            ex.shutdown(wait=True)
