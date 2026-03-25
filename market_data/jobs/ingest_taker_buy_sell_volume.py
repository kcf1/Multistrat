"""
Incremental taker buy/sell volume ingest.

Uses ``taker_buy_sell_volume_cursor`` and ``MAX(sample_time)`` to choose the next
``startTime`` for each (symbol, period) series; empty series starts from
``TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS`` horizon.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import psycopg2

from market_data.config import (
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
from market_data.providers.base import TakerBuySellVolumeProvider
from market_data.providers.binance_perps import build_binance_perps_provider
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

    # We don't implement a dedicated gap-based skip mode for this dataset yet.
    # Upserts keep runs idempotent even if overlapping windows are re-fetched.
    _ = skip_existing_when_no_watermark

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
    provider: TakerBuySellVolumeProvider | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> list[IngestTakerBuySellVolumeSeriesResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[IngestTakerBuySellVolumeSeriesResult] = []
        for symbol in TAKER_BUYSELL_VOLUME_SYMBOLS:
            for period in TAKER_BUYSELL_VOLUME_PERIODS:
                out.append(
                    ingest_taker_buy_sell_volume_series(
                        conn,
                        prov,
                        symbol,
                        period,
                        chunk_limit=TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
                        backfill_days=TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS,
                        use_watermark=use_watermark,
                        skip_existing_when_no_watermark=skip_existing_when_no_watermark,
                    )
                )
        return out
    finally:
        conn.close()

