"""Shared helpers for OHLCV jobs (Phase 4 §9.5)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone

from market_data.intervals import floor_align_ms_to_interval, interval_to_millis
from market_data.providers.base import (
    BasisProvider,
    KlinesProvider,
    OpenInterestProvider,
    TopTraderLongShortPositionRatioProvider,
    TakerBuySellVolumeProvider,
)
from market_data.schemas import (
    BasisPoint,
    OhlcvBar,
    OpenInterestPoint,
    TakerBuySellVolumePoint,
    TopTraderLongShortPoint,
)


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def expected_ohlcv_slots(start_ms: int, end_ms: int, iv_ms: int) -> int:
    """Approximate number of interval buckets in ``[start_ms, end_ms]`` (inclusive-style)."""
    if end_ms <= start_ms or iv_ms <= 0:
        return 0
    return (end_ms - start_ms) // iv_ms + 1


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


def iter_basis_batches_forward(
    provider: BasisProvider,
    pair: str,
    contract_type: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> Iterator[list[BasisPoint]]:
    """
    Yield each ``fetch_basis`` page from ``start_ms`` toward ``end_ms`` (oldest first).
    Stops when page empty or next ``startTime`` would be past ``end_ms``.
    """
    pd_ms = interval_to_millis(period)
    cur = start_ms
    safety = 0
    while cur < end_ms and safety < 100_000:
        safety += 1
        batch = provider.fetch_basis(
            pair,
            contract_type,
            period,
            start_time_ms=cur,
            end_time_ms=end_ms,
            limit=chunk_limit,
        )
        if not batch:
            break
        batch = filter_basis_not_after_ms(batch, end_ms)
        if not batch:
            break
        yield batch
        last = batch[-1]
        cur = open_time_plus_interval_ms(last.sample_time, pd_ms)


def filter_basis_not_after_ms(points: list[BasisPoint], end_ms: int) -> list[BasisPoint]:
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    return [p for p in points if p.sample_time <= end_dt]


def chunk_fetch_basis_forward(
    provider: BasisProvider,
    pair: str,
    contract_type: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> list[BasisPoint]:
    """Concatenate all batches from :func:`iter_basis_batches_forward` (oldest first)."""
    out: list[BasisPoint] = []
    for batch in iter_basis_batches_forward(
        provider,
        pair,
        contract_type,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        out.extend(batch)
    return out


def filter_open_interest_in_ms_window(
    points: list[OpenInterestPoint],
    start_ms: int,
    end_ms: int,
) -> list[OpenInterestPoint]:
    start_dt = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    return [p for p in points if start_dt <= p.sample_time <= end_dt]


def iter_open_interest_batches_forward(
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> Iterator[list[OpenInterestPoint]]:
    """
    Yield each ``fetch_open_interest_hist`` page covering ``[start_ms, end_ms]``, **oldest
    chunk first** (chronological ingest / cursor checkpoints).

    Unlike ``/basis`` (klines-style paging), Binance ``openInterestHist`` with both
    ``startTime`` and ``endTime`` returns the **latest** ``limit`` rows in that window, not
    the earliest from ``startTime``. We therefore page **backward** by lowering ``endTime`` to
    just before the current page's oldest timestamp, then :func:`reversed` the fetch stack
    so callers still receive batches from oldest wall-clock segment to newest.
    """
    pd_ms = interval_to_millis(period)
    lo = int(start_ms)
    hi = int(end_ms)
    if lo >= hi:
        return

    stack: list[list[OpenInterestPoint]] = []
    cur_end = hi
    safety = 0
    while cur_end > lo and safety < 100_000:
        safety += 1
        batch = provider.fetch_open_interest_hist(
            symbol,
            contract_type,
            period,
            start_time_ms=lo,
            end_time_ms=cur_end,
            limit=chunk_limit,
        )
        if not batch:
            break
        batch = filter_open_interest_in_ms_window(batch, lo, hi)
        if not batch:
            break
        stack.append(batch)
        first_ms = int(batch[0].sample_time.timestamp() * 1000)
        next_end = first_ms - pd_ms
        if next_end < lo:
            break
        cur_end = next_end

    for batch in reversed(stack):
        yield batch


def filter_open_interest_not_after_ms(
    points: list[OpenInterestPoint],
    end_ms: int,
) -> list[OpenInterestPoint]:
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    return [p for p in points if p.sample_time <= end_dt]


def chunk_fetch_open_interest_forward(
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> list[OpenInterestPoint]:
    """Concatenate all batches from :func:`iter_open_interest_batches_forward`."""
    out: list[OpenInterestPoint] = []
    for batch in iter_open_interest_batches_forward(
        provider,
        symbol,
        contract_type,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        out.extend(batch)
    return out


def iter_taker_buy_sell_volume_batches_forward(
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> Iterator[list[TakerBuySellVolumePoint]]:
    """
    Yield each ``fetch_taker_buy_sell_volume`` page covering ``[start_ms, end_ms]``, **oldest
    chunk first** (chronological ingest / cursor checkpoints).

    Binance ``takerlongshortRatio`` behaves like ``openInterestHist`` here: when both
    ``startTime`` and ``endTime`` are provided, the response is effectively the **latest**
    ``limit`` rows within that window. We therefore page **backward** by lowering
    ``endTime`` to just before the current page's oldest timestamp, then reverse the
    fetch stack so callers still receive batches from oldest wall-clock segment to newest.
    """
    pd_ms = interval_to_millis(period)
    lo = int(start_ms)
    hi = int(end_ms)
    if lo >= hi:
        return

    def filter_taker_buy_sell_volume_in_ms_window(
        points: list[TakerBuySellVolumePoint],
        start_ms: int,
        end_ms: int,
    ) -> list[TakerBuySellVolumePoint]:
        start_dt = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
        return [p for p in points if start_dt <= p.sample_time <= end_dt]

    stack: list[list[TakerBuySellVolumePoint]] = []
    safety = 0
    cur_end = hi
    while cur_end > lo and safety < 100_000:
        safety += 1
        batch = provider.fetch_taker_buy_sell_volume(
            symbol,
            period,
            start_time_ms=lo,
            end_time_ms=cur_end,
            limit=chunk_limit,
        )
        if not batch:
            break
        # Filter to the overall requested ingest window, mirroring open interest behavior.
        batch = filter_taker_buy_sell_volume_in_ms_window(batch, lo, hi)
        if not batch:
            break

        first = batch[0].sample_time

        stack.append(batch)

        next_end = int(first.timestamp() * 1000) - pd_ms
        if next_end < lo:
            break
        cur_end = next_end

    for batch in reversed(stack):
        yield batch


def chunk_fetch_taker_buy_sell_volume_forward(
    provider: TakerBuySellVolumeProvider,
    symbol: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> list[TakerBuySellVolumePoint]:
    """Concatenate all batches from :func:`iter_taker_buy_sell_volume_batches_forward` (oldest first)."""
    out: list[TakerBuySellVolumePoint] = []
    for batch in iter_taker_buy_sell_volume_batches_forward(
        provider,
        symbol,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        out.extend(batch)
    return out


def iter_top_trader_long_short_batches_forward(
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> Iterator[list[TopTraderLongShortPoint]]:
    """
    Yield each ``fetch_top_trader_long_short_position_ratio`` page covering ``[start_ms, end_ms]``, **oldest
    chunk first** (chronological ingest / cursor checkpoints).

    Pagination strategy mirrors ``openInterestHist`` / ``takerlongshortRatio``-style limited retention
    endpoints: when both ``startTime`` and ``endTime`` are provided, the response is effectively the
    **latest** ``limit`` rows within that window, so we page backward by lowering ``endTime`` to just
    before the current page's oldest timestamp and then reverse the fetch stack.
    """
    pd_ms = interval_to_millis(period)
    lo = int(start_ms)
    hi = int(end_ms)
    if lo >= hi:
        return

    def filter_top_trader_long_short_in_ms_window(
        points: list[TopTraderLongShortPoint],
        start_ms: int,
        end_ms: int,
    ) -> list[TopTraderLongShortPoint]:
        start_dt = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
        return [p for p in points if start_dt <= p.sample_time <= end_dt]

    stack: list[list[TopTraderLongShortPoint]] = []
    safety = 0
    cur_end = hi
    while cur_end > lo and safety < 100_000:
        safety += 1
        batch = provider.fetch_top_trader_long_short_position_ratio(
            symbol,
            period,
            start_time_ms=lo,
            end_time_ms=cur_end,
            limit=chunk_limit,
        )
        if not batch:
            break

        batch = filter_top_trader_long_short_in_ms_window(batch, lo, hi)
        if not batch:
            break

        first = batch[0].sample_time
        stack.append(batch)

        next_end = int(first.timestamp() * 1000) - pd_ms
        if next_end < lo:
            break
        cur_end = next_end

    for batch in reversed(stack):
        yield batch


def chunk_fetch_top_trader_long_short_forward(
    provider: TopTraderLongShortPositionRatioProvider,
    symbol: str,
    period: str,
    *,
    start_ms: int,
    end_ms: int,
    chunk_limit: int = 500,
) -> list[TopTraderLongShortPoint]:
    """Concatenate all batches from :func:`iter_top_trader_long_short_batches_forward` (oldest first)."""
    out: list[TopTraderLongShortPoint] = []
    for batch in iter_top_trader_long_short_batches_forward(
        provider,
        symbol,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    ):
        out.extend(batch)
    return out
