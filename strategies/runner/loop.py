"""Minimal strategy runner loop scaffold."""

from __future__ import annotations

import time

from strategies.config import DEFAULT_ENABLED_STRATEGIES, DEFAULT_REBALANCE_INTERVAL_SECONDS
from strategies.registry import get_enabled_pipelines


def run_once() -> int:
    """Execute one iteration across enabled strategy pipelines."""
    pipelines = get_enabled_pipelines(DEFAULT_ENABLED_STRATEGIES)
    total_intents = 0
    for pipeline in pipelines:
        intents = pipeline.run_once()
        total_intents += len(intents)
    return total_intents


def run_forever(interval_seconds: int = DEFAULT_REBALANCE_INTERVAL_SECONDS) -> None:
    """Simple polling loop; stream publication will be added later."""
    while True:
        run_once()
        time.sleep(interval_seconds)

