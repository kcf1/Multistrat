"""
Per-provider request spacing (Phase 4 §9.3).

``min_interval_seconds=None`` (default) means **no delay** — unlimited until venue
limits are documented and set in ``market_data.config`` (micro constant).
"""

from __future__ import annotations

import threading
import time
from typing import Optional


class ProviderRateLimiter:
    """
    Thread-safe minimum interval between ``acquire()`` calls.

    - ``min_interval_seconds`` is ``None`` or ``<= 0``: **no-op** (unlimited throughput).
    - Positive value: block until at least that many seconds have passed since the last acquire.
    """

    __slots__ = ("_min_interval", "_lock", "_next_allowed_monotonic")

    def __init__(self, min_interval_seconds: Optional[float] = None) -> None:
        self._min_interval = min_interval_seconds
        self._lock = threading.Lock()
        self._next_allowed_monotonic = 0.0

    def acquire(self) -> None:
        if self._min_interval is None or self._min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed_monotonic - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._next_allowed_monotonic = now + self._min_interval
