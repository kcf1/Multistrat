"""
Registry of ``module_id`` -> ``StrategyModule`` (OMS ``AdapterRegistry``-shaped).
"""

from __future__ import annotations

from typing import Optional

from strategies.runner.types import StrategyModule


class RunnerModuleRegistry:
    """Register strategy modules by ``module_id``; runner iterates ``module_ids()``."""

    __slots__ = ("_modules",)

    def __init__(self) -> None:
        self._modules: dict[str, StrategyModule] = {}

    def register(self, module: StrategyModule) -> None:
        """Register ``module`` under ``module.module_id`` (normalized to lower case)."""
        key = module.module_id.strip().lower()
        if not key:
            raise ValueError("module.module_id must be non-empty")
        if key in self._modules:
            raise ValueError(f"module_id {key!r} is already registered")
        self._modules[key] = module

    def get(self, module_id: str) -> Optional[StrategyModule]:
        if not module_id:
            return None
        return self._modules.get(module_id.strip().lower())

    def module_ids(self) -> list[str]:
        """Stable iteration order for the main loop."""
        return sorted(self._modules.keys())

    def __contains__(self, module_id: str) -> bool:
        return self.get(module_id) is not None
