"""
Contracts for the strategies runner (Milestone 1): one process, many ``StrategyModule`` instances.

Phase 1 data jobs (e.g. future ``factor_ls``) and trading strategies may share this interface
or split later; the surface is intentionally small.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class RunnerContext:
    """Per-tick context passed to each ``StrategyModule``; extend with DB/Redis when needed."""

    now_utc: datetime


@runtime_checkable
class StrategyModule(Protocol):
    """
    Pluggable unit of work inside the strategies runner (OMS ``BrokerAdapter``-shaped).

    - ``module_id``: unique registry key (lowercased when stored).
    - ``book_id``: accounting / routing book for OMS-PMS-risk; defaults to ``module_id`` when
      the module owns a single book 1:1. Override when several modules share one book.
    """

    @property
    def module_id(self) -> str: ...

    @property
    def book_id(self) -> str: ...

    def run_tick(self, ctx: RunnerContext) -> None:
        """One unit of work per runner loop iteration."""
        ...
