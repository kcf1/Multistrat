"""
Strategies runner entrypoint: long-lived loop, one tick per registered ``StrategyModule``.

Run: ``python -m strategies.runner`` or ``python -m strategies.runner.main``
"""

from __future__ import annotations

import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from strategies import config as strategies_config
from strategies.runner.module_registry import RunnerModuleRegistry
from strategies.runner.bootstrap import build_runner_registry
from strategies.runner.types import RunnerContext


def _repo_root() -> Path:
    # strategies/runner/main.py -> parents[2] == repo root (Multistrat)
    return Path(__file__).resolve().parents[2]


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(_repo_root() / ".env", override=False)
    except ImportError:
        pass


def _run_loop(registry: RunnerModuleRegistry, stop: threading.Event, interval_seconds: float) -> None:
    """One process loop until ``stop`` is set; used by ``main`` and tests."""
    ids = registry.module_ids()
    if not ids:
        logger.warning("strategies_runner: no modules registered; exiting")
        return

    logger.info("strategies_runner modules={} tick_interval_s={}", ids, interval_seconds)

    while not stop.is_set():
        ctx = RunnerContext(now_utc=datetime.now(timezone.utc))
        for mid in ids:
            if stop.is_set():
                break
            mod = registry.get(mid)
            if mod is None:
                continue
            try:
                mod.run_tick(ctx)
            except Exception:
                logger.exception("strategies_runner module_id={} run_tick failed", mid)

        if stop.wait(timeout=interval_seconds):
            break

    logger.info("strategies_runner loop ended")


def main() -> int:
    _load_dotenv()
    logger.info("strategies_runner starting (Milestone 1 scaffold)")

    stop = threading.Event()

    def _handle_signal(_signo: int, _frame: object | None) -> None:
        logger.info("strategies_runner received shutdown signal")
        stop.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    registry = build_runner_registry()
    interval = float(strategies_config.RUNNER_TICK_INTERVAL_SECONDS)
    _run_loop(registry, stop, interval)

    logger.info("strategies_runner stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
