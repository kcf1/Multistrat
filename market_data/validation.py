"""
Klines response validation after HTTP (Phase 4).

Binance may return **HTTP 200** with malformed rows under stress / rate limits. We parse
per-row, enforce OHLC invariants on :class:`~market_data.schemas.OhlcvBar`, check batch
shape (monotonic ``open_time``, no duplicates), **interior grid gaps**, and when
``startTime`` / ``endTime`` / ``limit`` are known, **tail shortfall**, **head slack**, and
**expected bar count** vs window span.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from market_data.config import (
    OHLCV_KLINES_GRID_MAX_STEP_RATIO,
    OHLCV_KLINES_GRID_MIN_STEP_RATIO,
    OHLCV_KLINES_HEAD_MAX_SLACK_INTERVALS,
    OHLCV_KLINES_SPAN_CHECK_MIN_INTERVALS,
)
from market_data.intervals import interval_to_millis
from market_data.schemas import OhlcvBar, parse_binance_kline


def theoretical_max_bars_in_window(
    start_ms: int,
    end_ms: int,
    iv_ms: int,
    limit: int,
) -> int:
    """Upper bound on bars that can fit in ``[start_ms, end_ms]``, capped by ``limit``."""
    if end_ms <= start_ms or iv_ms <= 0:
        return 0
    span = end_ms - start_ms
    return min(limit, span // iv_ms + 2)


def scan_bar_series_grid_gaps(bars: Sequence[OhlcvBar], interval: str) -> list[str]:
    """Public: interior step scan for an in-memory series (same rules as klines payload)."""
    iv_ms = interval_to_millis(interval)
    return _interior_grid_issues(list(bars), iv_ms)


def process_binance_klines_payload(
    raw: Any,
    *,
    symbol: str,
    interval: str,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    request_limit: int | None = None,
) -> list[OhlcvBar]:
    """
    Validate JSON shape, parse each row, run model/batch checks, optional span/gap checks.

    When ``start_time_ms``, ``end_time_ms``, and ``request_limit`` are all set, validates
    tail coverage and expected count vs span (in addition to interior grid gaps).

    Returns an empty list for an empty array. Raises ``ValueError`` if anything is wrong
    (caller retries the HTTP call).
    """
    if not isinstance(raw, list):
        raise ValueError("Binance klines response must be a JSON array")
    sym = symbol.strip().upper()
    iv = interval.strip()
    iv_ms = interval_to_millis(iv)
    row_errors: list[str] = []
    bars: list[OhlcvBar] = []
    for i, row in enumerate(raw):
        if not isinstance(row, list):
            row_errors.append(f"row[{i}] is not a list")
            continue
        try:
            bars.append(parse_binance_kline(row, symbol=sym, interval=iv))
        except Exception as e:
            row_errors.append(f"row[{i}]: {e}")
    if row_errors:
        head = "; ".join(row_errors[:15])
        more = f" … (+{len(row_errors) - 15} more)" if len(row_errors) > 15 else ""
        raise ValueError(f"klines row errors ({len(row_errors)}): {head}{more}")

    batch_issues = _batch_integrity_issues(bars)
    if batch_issues:
        raise ValueError("klines batch integrity: " + "; ".join(batch_issues))

    interior = _interior_grid_issues(bars, iv_ms)
    if interior:
        raise ValueError("klines interior gap scan: " + "; ".join(interior))

    if (
        start_time_ms is not None
        and end_time_ms is not None
        and request_limit is not None
        and bars
    ):
        span_issues = _span_coverage_issues(
            bars,
            start_ms=start_time_ms,
            end_ms=end_time_ms,
            iv_ms=iv_ms,
            request_limit=request_limit,
        )
        if span_issues:
            raise ValueError("klines span/coverage: " + "; ".join(span_issues))

    return bars


def _interior_grid_issues(bars: list[OhlcvBar], iv_ms: int) -> list[str]:
    if len(bars) < 2:
        return []
    issues: list[str] = []
    min_step = iv_ms * OHLCV_KLINES_GRID_MIN_STEP_RATIO
    max_step = iv_ms * OHLCV_KLINES_GRID_MAX_STEP_RATIO
    for i in range(1, len(bars)):
        delta_ms = (bars[i].open_time - bars[i - 1].open_time).total_seconds() * 1000.0
        if delta_ms < min_step:
            issues.append(
                f"step too short at index {i}: delta_ms={delta_ms:.0f}, expected ~{iv_ms}"
            )
        elif delta_ms > max_step:
            issues.append(
                f"interior gap at index {i}: delta_ms={delta_ms:.0f}, expected ~{iv_ms}"
            )
    return issues


def _span_coverage_issues(
    bars: list[OhlcvBar],
    *,
    start_ms: int,
    end_ms: int,
    iv_ms: int,
    request_limit: int,
) -> list[str]:
    issues: list[str] = []
    span = end_ms - start_ms
    if span < iv_ms * OHLCV_KLINES_SPAN_CHECK_MIN_INTERVALS:
        return issues

    theoretical = theoretical_max_bars_in_window(start_ms, end_ms, iv_ms, request_limit)
    first_ms = int(bars[0].open_time.timestamp() * 1000)
    last_ms = int(bars[-1].open_time.timestamp() * 1000)
    n = len(bars)

    head_slack = first_ms - start_ms
    max_head = OHLCV_KLINES_HEAD_MAX_SLACK_INTERVALS * iv_ms
    if head_slack > max_head:
        issues.append(
            f"head slack: first_open_ms={first_ms}, start_ms={start_ms}, slack_ms={head_slack} "
            f"(max allowed {max_head})"
        )

    next_open = last_ms + iv_ms
    room_after_next_start = end_ms - next_open
    if n < request_limit and room_after_next_start >= iv_ms:
        issues.append(
            f"tail shortfall: got {n} bars (theoretical≤{theoretical}), last_open_ms={last_ms}, "
            f"end_ms={end_ms}, room_after_next_start_ms={room_after_next_start} "
            f"(>= one interval {iv_ms}ms) but under limit={request_limit}"
        )

    return issues


def _batch_integrity_issues(bars: list[OhlcvBar]) -> list[str]:
    if len(bars) <= 1:
        return []
    issues: list[str] = []
    seen: set = set()
    for i, b in enumerate(bars):
        ot = b.open_time
        if ot in seen:
            issues.append(f"duplicate open_time at index {i}: {ot}")
        seen.add(ot)
        if i > 0 and bars[i - 1].open_time >= ot:
            issues.append(
                f"non-increasing open_time at index {i}: {bars[i - 1].open_time} then {ot}"
            )
    return issues
