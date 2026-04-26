"""Build the runner ``RunnerModuleRegistry`` (wire concrete ``StrategyModule`` instances)."""

from __future__ import annotations

from strategies.runner.module_registry import RunnerModuleRegistry
from strategies.runner.noop_module import NoopStrategyModule
from strategies.modules.factor_ls.runner_module import FactorLsRunnerModule


def build_runner_registry() -> RunnerModuleRegistry:
    """
    Register runner modules.

    Currently: Noop + factor_ls.
    """
    reg = RunnerModuleRegistry()
    reg.register(NoopStrategyModule())
    reg.register(FactorLsRunnerModule())
    return reg
