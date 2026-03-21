"""
Targeted range repair and simple gap detection (Phase 4 §9.5 / §4.5.3).

``run_repair_gap`` refetches a closed ``[start_time_ms, end_time_ms]`` window in chunks,
upserts after each chunk, and **does not** change ``ingestion_cursor`` (historical fill).

``detect_ohlcv_time_gaps`` finds missing spans **inside the given**
``[range_start, range_end]`` by walking ordered ``open_time`` rows in that window
(head gap before the first bar, oversized steps between bars, tail after the last).
It does **not** infer gaps from global ``min``/``max(open_time)`` alone—that would
drop the prefix ``[range_start, first_bar)`` and any interior holes if you only
compared endpoint extrema.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg2
from loguru import logger

from market_data.config import (
    OHLCV_INITIAL_BACKFILL_DAYS,
    OHLCV_KLINES_CHUNK_LIMIT,
    OHLCV_SKIP_EXISTING_GAP_MULTIPLE,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import iter_kline_batches_forward, utc_now_ms
from market_data.providers.base import KlinesProvider
from market_data.providers.binance_spot import build_binance_spot_provider
from market_data.storage import upsert_ohlcv_bars


def run_repair_gap(
    conn,
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    *,
    start_time_ms: int,
    end_time_ms: int,
    chunk_limit: int = OHLCV_KLINES_CHUNK_LIMIT,
) -> int:
    """
    Refetch and upsert klines in ``[start_time_ms, end_time_ms]`` (inclusive intent via API).

    Commits once per chunk. Does **not** update ``ingestion_cursor``.
    """
    if start_time_ms >= end_time_ms:
        return 0
    total = 0
    for batch in iter_kline_batches_forward(
        provider,
        symbol,
        interval,
        start_ms=start_time_ms,
        end_ms=end_time_ms,
        chunk_limit=chunk_limit,
    ):
        upsert_ohlcv_bars(conn, batch)
        conn.commit()
        total += len(batch)
    return total


def detect_ohlcv_time_gaps(
    conn,
    symbol: str,
    interval: str,
    range_start: datetime,
    range_end: datetime,
    *,
    gap_multiple: float = 1.5,
) -> list[tuple[datetime, datetime]]:
    """
    Return sub-ranges ``(gap_start, gap_end)`` (inclusive) **within**
    ``[range_start, range_end]`` where stored bars are missing or skip at least
    ``gap_multiple`` bar lengths. If no rows exist in that window, returns the whole
    window. Gaps are **sub-intervals of that fixed window** (what the policy range
    requires minus what rows exist on the grid)—not ``min``/``max(open_time)``
    endpoint tricks that omit the head or interior.

    ``range_*`` must be timezone-aware (UTC recommended).
    """
    if range_start.tzinfo is None or range_end.tzinfo is None:
        raise ValueError("range_start and range_end must be timezone-aware")
    if range_end < range_start:
        return []

    sym = symbol.strip().upper()
    iv = interval.strip()
    iv_ms = interval_to_millis(interval)
    iv_td = timedelta(milliseconds=iv_ms)
    threshold = iv_ms * gap_multiple

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT open_time FROM ohlcv
            WHERE symbol = %s AND interval = %s
              AND open_time >= %s AND open_time <= %s
            ORDER BY open_time
            """,
            (sym, iv, range_start, range_end),
        )
        times = [r[0] for r in cur.fetchall()]

    gaps: list[tuple[datetime, datetime]] = []
    if not times:
        return [(range_start, range_end)]

    rs = range_start.astimezone(timezone.utc)
    re_ = range_end.astimezone(timezone.utc)

    first_ms = (times[0] - rs).total_seconds() * 1000.0
    if first_ms > threshold:
        gaps.append((rs, times[0] - iv_td))

    for i in range(len(times) - 1):
        delta_ms = (times[i + 1] - times[i]).total_seconds() * 1000.0
        if delta_ms > threshold:
            g0 = times[i] + iv_td
            g1 = times[i + 1] - iv_td
            if g0 <= g1:
                gaps.append((g0, g1))

    last_ms = (re_ - times[-1]).total_seconds() * 1000.0
    if last_ms > threshold:
        g0 = times[-1] + iv_td
        if g0 <= re_:
            gaps.append((g0, re_))

    return gaps


@dataclass(frozen=True)
class PolicyRepairSeriesResult:
    symbol: str
    interval: str
    gap_spans: int
    bars_upserted: int


def run_repair_gaps_policy_window_all_series(
    settings: MarketDataSettings,
    *,
    provider: KlinesProvider | None = None,
    backfill_days: int | None = None,
    gap_multiple: float | None = None,
) -> list[PolicyRepairSeriesResult]:
    """
    For each configured ``(symbol, interval)``, detect gaps in
    ``[now - backfill_days, now]`` (same policy window as ingest) and refetch each gap.

    Uses one Postgres connection for the whole run. Can be API-heavy when many gaps exist;
    prefer initial history from :func:`run_ingest_ohlcv` before relying on this.
    """
    days = OHLCV_INITIAL_BACKFILL_DAYS if backfill_days is None else backfill_days
    gm = OHLCV_SKIP_EXISTING_GAP_MULTIPLE if gap_multiple is None else gap_multiple
    end_ms = utc_now_ms()
    start_ms = end_ms - days * 86_400_000
    range_start = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
    range_end = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)

    prov = provider if provider is not None else build_binance_spot_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    out: list[PolicyRepairSeriesResult] = []
    try:
        for sym in settings.symbols:
            for iv in settings.intervals:
                gaps = detect_ohlcv_time_gaps(
                    conn, sym, iv, range_start, range_end, gap_multiple=gm
                )
                if not gaps:
                    out.append(PolicyRepairSeriesResult(sym, iv, 0, 0))
                    continue
                n = run_repair_detected_gaps(conn, prov, sym, iv, gaps)
                logger.info(
                    "market_data gap repair symbol={} interval={} gap_spans={} bars_upserted={}",
                    sym,
                    iv,
                    len(gaps),
                    n,
                )
                out.append(PolicyRepairSeriesResult(sym, iv, len(gaps), n))
        return out
    finally:
        conn.close()


def run_repair_detected_gaps(
    conn,
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    gaps: list[tuple[datetime, datetime]],
    *,
    chunk_limit: int = OHLCV_KLINES_CHUNK_LIMIT,
) -> int:
    """Run :func:`run_repair_gap` for each gap window (ms bounds from datetimes)."""
    total = 0
    for g0, g1 in gaps:
        start_ms = int(g0.timestamp() * 1000)
        end_ms = int(g1.timestamp() * 1000)
        total += run_repair_gap(
            conn,
            provider,
            symbol,
            interval,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            chunk_limit=chunk_limit,
        )
    return total


def run_repair_gaps_in_window(
    settings: MarketDataSettings,
    symbol: str,
    interval: str,
    range_start: datetime,
    range_end: datetime,
    *,
    provider: KlinesProvider | None = None,
    gap_multiple: float = 1.5,
) -> tuple[list[tuple[datetime, datetime]], int]:
    """
    Detect gaps in ``[range_start, range_end]`` then repair them.

    Returns ``(gaps, bars_upserted)``.
    """
    prov = provider if provider is not None else build_binance_spot_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        gaps = detect_ohlcv_time_gaps(
            conn,
            symbol,
            interval,
            range_start,
            range_end,
            gap_multiple=gap_multiple,
        )
        n = run_repair_detected_gaps(conn, prov, symbol, interval, gaps)
        return gaps, n
    finally:
        conn.close()
