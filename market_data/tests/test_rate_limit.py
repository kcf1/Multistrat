"""Tests for ProviderRateLimiter."""

import time
from unittest.mock import patch

from market_data.rate_limit import ProviderRateLimiter


def test_unlimited_no_sleep_many_acquires() -> None:
    lim = ProviderRateLimiter(min_interval_seconds=None)
    t0 = time.perf_counter()
    for _ in range(200):
        lim.acquire()
    assert time.perf_counter() - t0 < 0.1


def test_unlimited_zero_or_negative_interval() -> None:
    for v in (0.0, -1.0):
        lim = ProviderRateLimiter(min_interval_seconds=v)
        t0 = time.perf_counter()
        for _ in range(50):
            lim.acquire()
        assert time.perf_counter() - t0 < 0.05


def test_limited_calls_sleep() -> None:
    lim = ProviderRateLimiter(min_interval_seconds=1.0)
    with patch("market_data.rate_limit.time.sleep") as mock_sleep:
        lim.acquire()
        lim.acquire()
    assert mock_sleep.call_count == 1
    (wait_arg,), _ = mock_sleep.call_args
    assert wait_arg > 0
