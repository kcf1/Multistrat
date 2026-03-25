"""
Job registry (Phase 5 §5.1.3): bind ``JobSpec`` rows from ``config`` to callables.

Add new jobs by:
1. Define a ``JobSpec`` in ``scheduler.config.JOB_SPECS``.
2. Register a factory in ``_JOB_FACTORIES`` returning a ``Job`` implementation.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass

from scheduler.config import JOB_SPECS
from scheduler.jobs.noop import NoopJob
from scheduler.types import Job, JobSpec


@dataclass(frozen=True, slots=True)
class RegisteredJob:
    spec: JobSpec
    job: Job


_JOB_FACTORIES: dict[str, Callable[[], Job]] = {
    "noop_heartbeat": lambda: NoopJob("noop_heartbeat"),
}


def iter_registered_jobs(include_disabled: bool = False) -> Iterator[RegisteredJob]:
    """Yield jobs wired from ``JOB_SPECS``. Disabled specs are skipped unless ``include_disabled``."""
    for spec in JOB_SPECS:
        if not spec.enabled and not include_disabled:
            continue
        if not spec.enabled:
            yield RegisteredJob(spec=spec, job=_build_placeholder_job(spec))
            continue
        factory = _JOB_FACTORIES.get(spec.job_id)
        if factory is None:
            raise KeyError(
                f"No job factory registered for job_id={spec.job_id!r}; "
                "add it to scheduler.registry._JOB_FACTORIES"
            )
        job = factory()
        if job.job_id != spec.job_id:
            raise ValueError(
                f"Factory for {spec.job_id!r} produced job with job_id={job.job_id!r}"
            )
        yield RegisteredJob(spec=spec, job=job)


def _build_placeholder_job(spec: JobSpec) -> Job:
    """Disabled specs still need a job only when iterating with ``include_disabled``."""
    return NoopJob(spec.job_id)
