"""Job contract (Phase 5 §4.1 / §5.1.2): context, spec, and runnable protocol."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class JobContext:
    """Per-run context passed to ``Job.run``; extend with DB/Redis handles in later tasks."""

    job_id: str


@dataclass(frozen=True, slots=True)
class JobSpec:
    """
    Schedule and flags for one job (Phase 5 §5.1.3).

    An **enabled** job must specify at least one of:

    - ``interval_seconds > 0`` — fixed cadence (runner implements in §4.3), or
    - non-empty ``cron_expression`` — cron cadence (runner wiring in §4.3; optional ``croniter`` later).
    """

    job_id: str
    enabled: bool
    interval_seconds: int | None = None
    cron_expression: str | None = None
    timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        if not self.job_id or not str(self.job_id).strip():
            raise ValueError("JobSpec.job_id must be non-empty")
        jid = str(self.job_id).strip()
        object.__setattr__(self, "job_id", jid)
        if not self.enabled:
            return
        has_interval = self.interval_seconds is not None and self.interval_seconds > 0
        has_cron = bool(self.cron_expression and str(self.cron_expression).strip())
        if not has_interval and not has_cron:
            raise ValueError(
                f"JobSpec {jid!r}: enabled job requires interval_seconds > 0 "
                "and/or a non-empty cron_expression"
            )
        if self.interval_seconds is not None and self.interval_seconds < 0:
            raise ValueError(f"JobSpec {jid!r}: interval_seconds must be >= 0 or None")
        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError(f"JobSpec {jid!r}: timeout_seconds must be > 0 or None")


@runtime_checkable
class Job(Protocol):
    """Runnable job: sync ``run``; timeouts enforce in the runner (§4.3)."""

    @property
    def job_id(self) -> str: ...

    def run(self, ctx: JobContext) -> None:
        """Execute one scheduled invocation."""
        ...
