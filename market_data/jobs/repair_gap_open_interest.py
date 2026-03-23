"""Targeted range repair and gap detection for open-interest snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg2
from loguru import logger

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
    floor_align_ms_to_interval,
    iter_open_interest_batches_forward,
    utc_now_ms,
)
from market_data.providers.base import OpenInterestProvider
from market_data.providers.binance_perps import build_binance_perps_provider
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
    backfill_days: int | None = None,
    gap_multiple: float | None = None,
) -> list[PolicyOpenInterestRepairSeriesResult]:
    days = OPEN_INTEREST_INITIAL_BACKFILL_DAYS if backfill_days is None else backfill_days
    gm = OHLCV_SKIP_EXISTING_GAP_MULTIPLE if gap_multiple is None else gap_multiple
    end_ms = utc_now_ms()
    range_end = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    out: list[PolicyOpenInterestRepairSeriesResult] = []
    try:
        for symbol in OPEN_INTEREST_SYMBOLS:
            for contract_type in OPEN_INTEREST_CONTRACT_TYPES:
                for period in OPEN_INTEREST_PERIODS:
                    start_ms = end_ms - days * 86_400_000
                    start_ms = floor_align_ms_to_interval(start_ms, period)
                    range_start = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
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
                        out.append(
                            PolicyOpenInterestRepairSeriesResult(symbol, contract_type, period, 0, 0)
                        )
                        continue
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
                    out.append(
                        PolicyOpenInterestRepairSeriesResult(symbol, contract_type, period, len(gaps), n)
                    )
        return out
    finally:
        conn.close()
