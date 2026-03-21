"""Tests for market_data.intervals."""

import pytest

from market_data.intervals import interval_to_millis


def test_interval_to_millis_common() -> None:
    assert interval_to_millis("1m") == 60_000
    assert interval_to_millis(" 1h ") == 3_600_000


def test_interval_1m_uppercase_M_month() -> None:
    assert interval_to_millis("1M") == 30 * 86_400_000


def test_unknown_interval() -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        interval_to_millis("7m")
