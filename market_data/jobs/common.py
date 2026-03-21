"""Shared helpers for OHLCV jobs (Phase 4 §9.5)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

from market_data.intervals import interval_to_millis
from market_data.providers.base import KlinesProvider
from market_data.schemas import OhlcvBar


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def open_time_plus_interval_ms(open_time: datetime, interval_ms: int) -> int:
    nxt = open_time + timedelta(milliseconds=interval_ms)
    return int(nxt.timestamp() * 1000)


def filter_bars_not_after_ms(bars: list[OhlcvBar], end_ms: int) -> list[OhlcvBar]:
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    return [b for b in bars if b.open_time <= end_dt]


def iter_kline_batches_forward(
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 1000,
) -> Iterator[list[OhlcvBar]]:
    """
    Yield each ``fetch_klines`` page from ``start_ms`` toward ``end_ms`` (oldest batch first).
    Stops when a page is empty or the next ``startTime`` would be past ``end_ms``.
    (A short page does **not** imply completion—only advancing past ``end_ms`` does.)
    """
    iv_ms = interval_to_millis(interval)
    cur = start_ms
    safety = 0
    while cur < end_ms and safety < 100_000:
        safety += 1
        batch = provider.fetch_klines(
            symbol,
            interval,
            start_time_ms=cur,
            end_time_ms=end_ms,
            limit=chunk_limit,
        )
        if not batch:
            break
        batch = filter_bars_not_after_ms(batch, end_ms)
        if not batch:
            break
        yield batch
        last = batch[-1]
        cur = open_time_plus_interval_ms(last.open_time, iv_ms)


def chunk_fetch_forward(
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 1000,
) -> list[OhlcvBar]:
    """Concatenate all batches from :func:`iter_kline_batches_forward` (oldest first)."""
    out: list[OhlcvBar] = []
    for batch in iter_kline_batches_forward(
        provider,
        symbol,
        interval,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        out.extend(batch)
    return out
