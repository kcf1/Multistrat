"""
Targeted range repair and gap detection for taker buy/sell volume snapshots.

This mirrors the basis/open-interest gap repair pattern, but for taker data keyed by
``(symbol, period, sample_time)``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg2
from loguru import logger

from market_data.config import (
    OHLCV_SKIP_EXISTING_GAP_MULTIPLE,
    TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
    TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS,
    TAKER_BUYSELL_VOLUME_PERIODS,
    TAKER_BUYSELL_VOLUME_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import floor_align_ms_to_interval, utc_now_ms
from market_data.providers.base import TakerBuySellVolumeProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.storage import upsert_taker_buy_sell_volume_points


def detect_taker_buy_sell_volume_time_gaps(
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

    Returns empty when no gaps are detected.
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
            SELECT sample_time FROM taker_buy_sell_volume
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


def run_repair_taker_buy_sell_volume_gap(
    conn,
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    *,
    start_time_ms: int,
    end_time_ms: int,
    chunk_limit: int = TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
) -> int:
    """Repair gaps by re-fetching and upserting data for ``[start_time_ms, end_time_ms]``."""
    if start_time_ms >= end_time_ms:
        return 0

    total = 0
    # For taker snapshots we have forward paging (ascending timestamps), so "forward"
    # batch paging matches the basis-style ingest.
    from market_data.jobs.common import iter_taker_buy_sell_volume_batches_forward

    for batch in iter_taker_buy_sell_volume_batches_forward(
        provider,
        symbol,
        period,
        start_ms=start_time_ms,
        end_ms=end_time_ms,
        chunk_limit=chunk_limit,
    ):
        upsert_taker_buy_sell_volume_points(conn, batch)
        conn.commit()
        total += len(batch)
    return total


@dataclass(frozen=True)
class PolicyTakerBuySellVolumeRepairSeriesResult:
    symbol: str
    period: str
    gap_spans: int
    rows_upserted: int


def run_repair_detected_taker_buy_sell_volume_gaps(
    conn,
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    gaps: list[tuple[datetime, datetime]],
    *,
    chunk_limit: int = TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT,
) -> int:
    total = 0
    for g0, g1 in gaps:
        total += run_repair_taker_buy_sell_volume_gap(
            conn,
            provider,
            symbol,
            period,
            start_time_ms=int(g0.timestamp() * 1000),
            end_time_ms=int(g1.timestamp() * 1000),
            chunk_limit=chunk_limit,
        )
    return total


def run_repair_taker_buy_sell_volume_gaps_policy_window_all_series(
    settings: MarketDataSettings,
    *,
    provider: TakerBuySellVolumeProvider | None = None,
    backfill_days: int | None = None,
    gap_multiple: float | None = None,
) -> list[PolicyTakerBuySellVolumeRepairSeriesResult]:
    """Policy-window repair for all configured taker buy/sell volume series."""
    days = TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS if backfill_days is None else backfill_days
    gm = OHLCV_SKIP_EXISTING_GAP_MULTIPLE if gap_multiple is None else gap_multiple

    end_ms = utc_now_ms()
    range_end = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    out: list[PolicyTakerBuySellVolumeRepairSeriesResult] = []
    try:
        for symbol in TAKER_BUYSELL_VOLUME_SYMBOLS:
            for period in TAKER_BUYSELL_VOLUME_PERIODS:
                start_ms = end_ms - days * 86_400_000
                start_ms = floor_align_ms_to_interval(start_ms, period)
                range_start = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)

                gaps = detect_taker_buy_sell_volume_time_gaps(
                    conn,
                    symbol,
                    period,
                    range_start,
                    range_end,
                    gap_multiple=gm,
                )
                if not gaps:
                    out.append(PolicyTakerBuySellVolumeRepairSeriesResult(symbol, period, 0, 0))
                    continue
                n = run_repair_detected_taker_buy_sell_volume_gaps(
                    conn,
                    prov,
                    symbol,
                    period,
                    gaps,
                )
                logger.info(
                    "market_data taker buy/sell volume gap repair symbol={} period={} gap_spans={} rows_upserted={}",
                    symbol,
                    period,
                    len(gaps),
                    n,
                )
                out.append(
                    PolicyTakerBuySellVolumeRepairSeriesResult(symbol, period, len(gaps), n)
                )
        return out
    finally:
        conn.close()

