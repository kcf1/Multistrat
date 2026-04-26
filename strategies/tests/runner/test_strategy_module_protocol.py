from __future__ import annotations

from datetime import datetime, timezone

from strategies.runner.noop_module import NoopStrategyModule
from strategies.runner.types import RunnerContext, StrategyModule


def test_noop_is_strategy_module_runtime_check() -> None:
    n = NoopStrategyModule()
    assert isinstance(n, StrategyModule)


def test_noop_book_id_defaults_to_module_id() -> None:
    n = NoopStrategyModule()
    assert n.book_id == n.module_id == "noop"


def test_noop_run_tick_does_not_raise() -> None:
    n = NoopStrategyModule()
    ctx = RunnerContext(now_utc=datetime(2026, 1, 1, tzinfo=timezone.utc))
    n.run_tick(ctx)
