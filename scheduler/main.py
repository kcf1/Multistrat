"""
Scheduler entrypoint (Phase 5).

Usage::

    python -m scheduler                  # long-running loop (§4.3)
    python -m scheduler --list-jobs
    python -m scheduler --dry-run-job ID
    python -m scheduler --once           # all enabled interval jobs once, then exit
"""

from __future__ import annotations

import argparse
import sys

from loguru import logger

from scheduler.registry import iter_registered_jobs
from scheduler.runner import run_forever, run_job_isolated, run_job_once


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
        help="Run a single enabled job once by id (with timeout; errors propagate)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run each enabled job with interval_seconds once, then exit",
    )
    args = parser.parse_args()

    if args.list_jobs:
        _print_job_table(include_disabled=True)
        return

    if args.dry_run_job:
        _dry_run_job(args.dry_run_job)
        return

    if args.once:
        _run_all_once()
        return

    run_forever()


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
            run_job_once(r)
            return
    logger.error("No enabled job with job_id={!r}", job_id)
    sys.exit(1)


def _run_all_once() -> None:
    """Run every enabled job once (for CI/smoke); isolation matches the loop (errors do not stop others)."""
    jobs = list(iter_registered_jobs(include_disabled=False))
    if not jobs:
        logger.warning("No enabled jobs")
        return
    logger.info("--once: running {} enabled job(s)", len(jobs))
    for r in jobs:
        run_job_isolated(r)


if __name__ == "__main__":
    main()
