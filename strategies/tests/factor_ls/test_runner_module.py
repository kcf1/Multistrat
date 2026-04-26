from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from strategies.modules.factor_ls.runner_module import FactorLsRunnerModule
from strategies.runner.types import RunnerContext


def test_factor_ls_runs_at_most_once_per_utc_day(monkeypatch) -> None:
    m = FactorLsRunnerModule(updated_at_cushion_seconds=0)

    # Gate ready immediately.
    monkeypatch.setattr(m, "_ohlcv_ready_for_previous_utc_day", lambda _now: True)

    calls: list[datetime] = []

    def _run_pipeline(*, run_at_utc, **_kwargs):
        calls.append(run_at_utc)

    monkeypatch.setattr("strategies.modules.factor_ls.runner_module.run_pipeline", _run_pipeline)

    t0 = datetime(2026, 4, 27, 0, 0, 0, tzinfo=timezone.utc)
    m.run_tick(RunnerContext(now_utc=t0))
    m.run_tick(RunnerContext(now_utc=t0 + timedelta(minutes=10)))

    assert len(calls) == 1


def test_factor_ls_skips_when_not_ready(monkeypatch) -> None:
    m = FactorLsRunnerModule()
    monkeypatch.setattr(m, "_ohlcv_ready_for_previous_utc_day", lambda _now: False)

    def _run_pipeline(**_kwargs):
        raise AssertionError("should not run_pipeline when gate is false")

    monkeypatch.setattr("strategies.modules.factor_ls.runner_module.run_pipeline", _run_pipeline)

    t0 = datetime(2026, 4, 27, 0, 0, 0, tzinfo=timezone.utc)
    m.run_tick(RunnerContext(now_utc=t0))


def test_factor_ls_runs_again_next_day(monkeypatch) -> None:
    m = FactorLsRunnerModule(updated_at_cushion_seconds=0)
    monkeypatch.setattr(m, "_ohlcv_ready_for_previous_utc_day", lambda _now: True)

    n = 0

    def _run_pipeline(**_kwargs):
        nonlocal n
        n += 1

    monkeypatch.setattr("strategies.modules.factor_ls.runner_module.run_pipeline", _run_pipeline)

    t0 = datetime(2026, 4, 27, 0, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=1, minutes=1)
    m.run_tick(RunnerContext(now_utc=t0))
    m.run_tick(RunnerContext(now_utc=t1))
    assert n == 2

