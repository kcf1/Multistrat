"""Tests for shared job helpers in ``market_data.jobs.common``."""

from market_data.jobs.common import floor_align_ms_to_interval


def test_floor_align_ms_to_interval_1h() -> None:
    pd_ms = 3_600_000
    raw = 1_234_567_890
    assert floor_align_ms_to_interval(raw, "1h") == (raw // pd_ms) * pd_ms


def test_floor_align_ms_to_interval_already_aligned() -> None:
    ms = 10 * 3_600_000
    assert floor_align_ms_to_interval(ms, "1h") == ms
