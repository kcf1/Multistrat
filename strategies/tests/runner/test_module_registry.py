from __future__ import annotations

import pytest

from strategies.runner.module_registry import RunnerModuleRegistry
from strategies.runner.noop_module import NoopStrategyModule


def test_register_get_module_ids() -> None:
    reg = RunnerModuleRegistry()
    reg.register(NoopStrategyModule())
    assert reg.module_ids() == ["noop"]
    assert reg.get("noop") is not None
    assert reg.get("NOOP") is not None
    assert "noop" in reg
    assert reg.get("missing") is None


def test_register_duplicate_raises() -> None:
    reg = RunnerModuleRegistry()
    reg.register(NoopStrategyModule())
    with pytest.raises(ValueError, match="already registered"):
        reg.register(NoopStrategyModule())
