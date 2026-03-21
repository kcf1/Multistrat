"""
Incremental OHLCV ingest + backfill (Phase 4 §9.5 / §4.5.1).

Uses ``ingestion_cursor`` and ``MAX(open_time)`` to choose the next ``startTime``; empty
series starts ``OHLCV_INITIAL_BACKFILL_DAYS`` ago. Commits after each chunk (checkpoint).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import psycopg2

from market_data.config import (
    OHLCV_INITIAL_BACKFILL_DAYS,
    OHLCV_KLINES_CHUNK_LIMIT,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import (
    iter_kline_batches_forward,
    open_time_plus_interval_ms,
    utc_now_ms,
)
from market_data.schemas import OhlcvBar
from market_data.providers.base import KlinesProvider
from market_data.providers.binance_spot import build_binance_spot_provider
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


def resolve_ingest_start_ms(
    conn,
    symbol: str,
    interval: str,
    *,
    now_ms: int,
    backfill_days: int,
) -> int:
    """First ``startTime`` for the next REST page (after last stored bar, or backfill horizon)."""
    iv_ms = interval_to_millis(interval)
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
    return now_ms - backfill_days * 86_400_000


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
) -> IngestSeriesResult:
    """
    Catch up ``(symbol, interval)`` to ``now`` using chunked fetches + upsert + cursor.

    Commits after each successful chunk.

    ``chunk_progress``: optional callback invoked after each committed chunk with that batch
    (e.g. ``tqdm.update(len(batch))``).
    """
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    start_ms = resolve_ingest_start_ms(
        conn,
        symbol,
        interval,
        now_ms=end_ms,
        backfill_days=backfill_days,
    )
    if start_ms >= end_ms:
        return IngestSeriesResult(symbol, interval, 0, 0)

    total = 0
    chunks = 0
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

    return IngestSeriesResult(symbol, interval, total, chunks)


def run_ingest_ohlcv(
    settings: MarketDataSettings,
    *,
    provider: KlinesProvider | None = None,
) -> list[IngestSeriesResult]:
    """Run ingest for every ``(symbol, interval)`` in settings (sequential, shared limiter)."""
    prov = provider if provider is not None else build_binance_spot_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[IngestSeriesResult] = []
        for sym in settings.symbols:
            for iv in settings.intervals:
                out.append(ingest_ohlcv_series(conn, prov, sym, iv))
        return out
    finally:
        conn.close()
