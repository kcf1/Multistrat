"""Placeholder job to validate registry wiring (Phase 5 §4.1)."""

from __future__ import annotations

from loguru import logger

from scheduler.types import JobContext


class NoopJob:
    """Logs at debug; replace with real work under ``jobs/reports``, ``jobs/reconciliation``, etc."""

    __slots__ = ("_job_id",)

    def __init__(self, job_id: str) -> None:
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    def run(self, ctx: JobContext) -> None:
        logger.debug("noop job run job_id={}", ctx.job_id)
