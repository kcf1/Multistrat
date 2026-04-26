"""Milestone 1 placeholder module (no DB, no side effects beyond a debug log)."""

from __future__ import annotations

from loguru import logger

from strategies.runner.types import RunnerContext


class NoopStrategyModule:
    """Registers as ``noop``; ``book_id`` follows ``module_id`` (single-book default)."""

    __slots__ = ()

    @property
    def module_id(self) -> str:
        return "noop"

    @property
    def book_id(self) -> str:
        return self.module_id

    def run_tick(self, ctx: RunnerContext) -> None:
        logger.debug(
            "strategies_runner noop tick module_id={} book_id={} now_utc={}",
            self.module_id,
            self.book_id,
            ctx.now_utc.isoformat(),
        )
