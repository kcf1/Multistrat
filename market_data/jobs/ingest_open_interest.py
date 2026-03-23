"""Incremental open-interest ingest + backfill."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2

from market_data.config import (
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
    iter_open_interest_batches_forward,
    open_time_plus_interval_ms,
    utc_now_ms,
)
from market_data.jobs.repair_gap_open_interest import detect_open_interest_time_gaps
from market_data.providers.base import OpenInterestProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.schemas import OpenInterestPoint
from market_data.storage import (
    get_open_interest_cursor,
    max_sample_time_open_interest,
    upsert_open_interest_cursor,
    upsert_open_interest_points,
)


@dataclass(frozen=True)
class IngestOpenInterestSeriesResult:
    symbol: str
    contract_type: str
    period: str
    rows_upserted: int
    chunks: int
    fetch_give_ups: tuple[str, ...] = ()


def resolve_open_interest_ingest_start_ms(
    conn,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    now_ms: int,
    backfill_days: int,
    use_watermark: bool = True,
) -> int:
    pd_ms = interval_to_millis(period)
    horizon_ms = now_ms - backfill_days * 86_400_000
    if not use_watermark:
        return horizon_ms
    c = get_open_interest_cursor(conn, symbol, contract_type, period)
    m = max_sample_time_open_interest(conn, symbol, contract_type, period)
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


def _ingest_open_interest_forward_segment(
    conn,
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[OpenInterestPoint]], None] | None,
) -> tuple[int, int]:
    total = 0
    chunks = 0
    if start_ms >= end_ms:
        return 0, 0
    for batch in iter_open_interest_batches_forward(
        provider,
        symbol,
        contract_type,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        chunks += 1
        upsert_open_interest_points(conn, batch)
        max_st = max(p.sample_time for p in batch)
        upsert_open_interest_cursor(conn, symbol, contract_type, period, max_st)
        conn.commit()
        total += len(batch)
        if chunk_progress is not None:
            chunk_progress(batch)
    return total, chunks


def _ingest_open_interest_skip_existing_gap_mode(
    conn,
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    end_ms: int,
    backfill_days: int,
    chunk_limit: int,
    chunk_progress: Callable[[Sequence[OpenInterestPoint]], None] | None,
    give_before: int,
) -> IngestOpenInterestSeriesResult:
    pd_ms = interval_to_millis(period)
    horizon_ms = end_ms - backfill_days * 86_400_000
    if horizon_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestOpenInterestSeriesResult(symbol, contract_type, period, 0, 0, give_msgs)

    horizon_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    gaps = detect_open_interest_time_gaps(
        conn,
        symbol,
        contract_type,
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
        t, c = _ingest_open_interest_forward_segment(
            conn,
            provider,
            symbol,
            contract_type,
            period,
            start_ms=seg_start,
            end_ms=seg_end,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
        )
        total += t
        chunks += c

    m = max_sample_time_open_interest(conn, symbol, contract_type, period)
    tail_start = max(horizon_ms, open_time_plus_interval_ms(m, pd_ms)) if m is not None else horizon_ms
    if tail_start < end_ms:
        t, c = _ingest_open_interest_forward_segment(
            conn,
            provider,
            symbol,
            contract_type,
            period,
            start_ms=tail_start,
            end_ms=end_ms,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
        )
        total += t
        chunks += c

    give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
    return IngestOpenInterestSeriesResult(symbol, contract_type, period, total, chunks, give_msgs)


def ingest_open_interest_series(
    conn,
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    now_ms: int | None = None,
    chunk_limit: int = OPEN_INTEREST_FETCH_CHUNK_LIMIT,
    backfill_days: int = OPEN_INTEREST_INITIAL_BACKFILL_DAYS,
    chunk_progress: Callable[[Sequence[OpenInterestPoint]], None] | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> IngestOpenInterestSeriesResult:
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    give_before = len(getattr(provider, "fetch_give_ups", []))

    if not use_watermark and skip_existing_when_no_watermark:
        return _ingest_open_interest_skip_existing_gap_mode(
            conn,
            provider,
            symbol,
            contract_type,
            period,
            end_ms=end_ms,
            backfill_days=backfill_days,
            chunk_limit=chunk_limit,
            chunk_progress=chunk_progress,
            give_before=give_before,
        )

    start_ms = resolve_open_interest_ingest_start_ms(
        conn,
        symbol,
        contract_type,
        period,
        now_ms=end_ms,
        backfill_days=backfill_days,
        use_watermark=use_watermark,
    )
    if start_ms >= end_ms:
        give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
        return IngestOpenInterestSeriesResult(symbol, contract_type, period, 0, 0, give_msgs)

    total, chunks = _ingest_open_interest_forward_segment(
        conn,
        provider,
        symbol,
        contract_type,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
        chunk_progress=chunk_progress,
    )
    give_msgs = tuple(getattr(provider, "fetch_give_ups", [])[give_before:])
    return IngestOpenInterestSeriesResult(symbol, contract_type, period, total, chunks, give_msgs)


def run_ingest_open_interest(
    settings: MarketDataSettings,
    *,
    provider: OpenInterestProvider | None = None,
    use_watermark: bool = True,
    skip_existing_when_no_watermark: bool = False,
) -> list[IngestOpenInterestSeriesResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[IngestOpenInterestSeriesResult] = []
        for symbol in OPEN_INTEREST_SYMBOLS:
            for contract_type in OPEN_INTEREST_CONTRACT_TYPES:
                for period in OPEN_INTEREST_PERIODS:
                    out.append(
                        ingest_open_interest_series(
                            conn,
                            prov,
                            symbol,
                            contract_type,
                            period,
                            use_watermark=use_watermark,
                            skip_existing_when_no_watermark=skip_existing_when_no_watermark,
                        )
                    )
        return out
    finally:
        conn.close()
