"""
Scheduler entrypoint (Phase 5 §5.1.1).

Usage::

    python -m scheduler
    python -m scheduler --list-jobs

The long-running loop (§4.3) is not implemented yet; this module lists the registry
and exits so CI and Docker can verify the package imports cleanly.
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger

from scheduler.registry import iter_registered_jobs
from scheduler.types import JobContext


def main() -> None:
    parser = argparse.ArgumentParser(description="Multistrat batch scheduler (Phase 5)")
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="Print registered jobs (including disabled) and exit",
    )
    parser.add_argument(
        "--dry-run-job",
        metavar="JOB_ID",
        default=None,
        help="Run a single enabled job once by id (for debugging; noop only today)",
    )
    args = parser.parse_args()

    if args.list_jobs:
        _print_job_table(include_disabled=True)
        return

    if args.dry_run_job:
        _dry_run_job(args.dry_run_job)
        return

    _print_job_table(include_disabled=False)
    logger.info(
        "Scheduler skeleton: no loop yet (Phase 5 §4.3). "
        "Use --list-jobs or --dry-run-job JOB_ID."
    )


def _print_job_table(include_disabled: bool) -> None:
    rows = list(iter_registered_jobs(include_disabled=include_disabled))
    if not rows:
        logger.warning("No jobs in registry (check JOB_SPECS and enabled flags)")
        return
    logger.info(
        "Registered jobs (include_disabled={}): count={}",
        include_disabled,
        len(rows),
    )
    for r in rows:
        spec = r.spec
        interval = spec.interval_seconds
        cron = spec.cron_expression if spec.cron_expression else "-"
        to = spec.timeout_seconds
        logger.info(
            "  job_id={} enabled={} interval_s={} cron={} timeout_s={}",
            spec.job_id,
            spec.enabled,
            interval,
            cron,
            to,
        )


def _dry_run_job(job_id: str) -> None:
    for r in iter_registered_jobs(include_disabled=False):
        if r.spec.job_id == job_id:
            logger.info("Dry-run job_id={}", job_id)
            r.job.run(JobContext(job_id=job_id))
            return
    logger.error("No enabled job with job_id={!r}", job_id)
    sys.exit(1)


if __name__ == "__main__":
    main()
