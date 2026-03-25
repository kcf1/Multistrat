"""Tests for scheduler wall-clock alignment (Phase 5 runner)."""

from __future__ import annotations

from datetime import datetime, timezone

from scheduler.config import SCHEDULER_HOURLY_INTERVAL_SECONDS
from scheduler.runner import _next_deadline_after_run, _next_utc_hourly_at_minute_past


def _ts(y: int, m: int, d: int, hh: int, mm: int, ss: int = 0) -> float:
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp()


def test_next_hourly_align_same_hour() -> None:
    """10:03 → next 10:05."""
    after = _ts(2026, 3, 26, 10, 3, 0)
    nxt = _next_utc_hourly_at_minute_past(after, 5)
    assert nxt == _ts(2026, 3, 26, 10, 5, 0)


def test_next_hourly_align_next_hour() -> None:
    """10:07 → next 11:05."""
    after = _ts(2026, 3, 26, 10, 7, 0)
    nxt = _next_utc_hourly_at_minute_past(after, 5)
    assert nxt == _ts(2026, 3, 26, 11, 5, 0)


def test_next_hourly_align_exact_slot_moves_forward() -> None:
    """10:05:00 → 11:05 (strictly after)."""
    after = _ts(2026, 3, 26, 10, 5, 0)
    nxt = _next_utc_hourly_at_minute_past(after, 5)
    assert nxt == _ts(2026, 3, 26, 11, 5, 0)


def test_next_deadline_hourly_uses_align() -> None:
    after = _ts(2026, 3, 26, 10, 3, 0)
    nxt = _next_deadline_after_run(after, SCHEDULER_HOURLY_INTERVAL_SECONDS)
    assert nxt == _ts(2026, 3, 26, 10, 5, 0)


def test_next_deadline_non_hourly_uses_epoch_grid() -> None:
    """300 s period: matches floor rule (same as market_data style)."""
    after = 1234.0
    nxt = _next_deadline_after_run(after, 300)
    expected = 1234.0 // 300 * 300 + 300
    assert nxt == expected
