"""
No-op **heartbeat** job (Phase 5 §4.7 / §5.7.1).

Does no external work; emits one **info** log line per run so operators can confirm the runner
is ticking and the registry dispatches non-report/recon jobs. Replace or extend with housekeeping
(e.g. prune ``scheduler_runs``) when needed.
"""

from __future__ import annotations

from loguru import logger

from scheduler.types import JobContext


class NoopHeartbeatJob:
    """Idempotent placeholder; safe to run on any cadence."""

    __slots__ = ("_job_id",)

    def __init__(self, job_id: str = "noop_heartbeat") -> None:
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    def run(self, ctx: JobContext) -> None:
        logger.info(
            "scheduler misc job noop_heartbeat ok job_id={} (no work; Phase 5 §4.7)",
            ctx.job_id,
        )
