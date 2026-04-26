"""Build the runner ``RunnerModuleRegistry`` (wire concrete ``StrategyModule`` instances)."""

from __future__ import annotations

from strategies.runner.module_registry import RunnerModuleRegistry
from strategies.runner.noop_module import NoopStrategyModule


def build_runner_registry() -> RunnerModuleRegistry:
    """
    Milestone 1: register ``NoopStrategyModule`` only.

    Later: register ``factor_ls`` and others when implementations exist.
    """
    reg = RunnerModuleRegistry()
    reg.register(NoopStrategyModule())
    return reg
