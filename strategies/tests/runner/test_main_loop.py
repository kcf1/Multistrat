from __future__ import annotations

import threading

from strategies.runner.main import _run_loop
from strategies.runner.module_registry import RunnerModuleRegistry


def test_run_loop_empty_registry_returns_immediately() -> None:
    stop = threading.Event()
    reg = RunnerModuleRegistry()
    _run_loop(reg, stop, interval_seconds=0.01)
    assert not stop.is_set()


def test_run_loop_one_tick_then_stop() -> None:
    from strategies.runner.noop_module import NoopStrategyModule

    stop = threading.Event()
    reg = RunnerModuleRegistry()
    reg.register(NoopStrategyModule())

    def _set_stop() -> None:
        stop.set()

    t = threading.Timer(0.15, _set_stop)
    t.start()
    try:
        _run_loop(reg, stop, interval_seconds=0.05)
    finally:
        t.cancel()
