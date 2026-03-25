"""
Long-running scheduler loop (Phase 5 §4.3): interval cadence, per-job isolation, timeouts, SIGTERM.
"""

from __future__ import annotations

import math
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError

from loguru import logger

from scheduler.config import (
    DEFAULT_JOB_TIMEOUT_SECONDS,
    SCHEDULER_HOURLY_ALIGN_MINUTE,
    SCHEDULER_HOURLY_INTERVAL_SECONDS,
    SCHEDULER_LOOP_POLL_SECONDS,
    load_scheduler_settings,
)
from scheduler.registry import RegisteredJob, iter_registered_jobs
from scheduler.run_history import RunStatus, record_run_end, record_run_start
from scheduler.types import JobContext

_ERROR_TEXT_MAX_LEN = 8000


def _next_periodic_deadline_after(completed_at: float, period_seconds: int) -> float:
    """
    Next epoch-aligned tick strictly after ``completed_at`` (Unix period grid).

    Matches ``market_data.main`` semantics for non-hourly periods: ``floor(t / p) * p + p``.
    """
    if period_seconds < 1:
        raise ValueError("period_seconds must be >= 1")
    return math.floor(completed_at / period_seconds) * period_seconds + period_seconds


def _next_utc_hourly_at_minute_past(completed_at: float, minute_past: int) -> float:
    """
    Next time **strictly after** ``completed_at`` (epoch seconds, UTC) that falls on
    ``minute_past`` seconds-of-minute 0 (e.g. minute_past=5 → hh:05:00 UTC).
    """
    if not 0 <= minute_past <= 59:
        raise ValueError("minute_past must be 0..59")
    after = datetime.fromtimestamp(completed_at, tz=timezone.utc)
    cand = after.replace(minute=minute_past, second=0, microsecond=0)
    if cand <= after:
        cand = (after + timedelta(hours=1)).replace(minute=minute_past, second=0, microsecond=0)
    return cand.timestamp()


def _next_deadline_after_run(completed_at: float, interval_seconds: int) -> float:
    """Schedule next fire: hourly specs use UTC ``:MM`` alignment; other intervals use epoch grid."""
    if interval_seconds == SCHEDULER_HOURLY_INTERVAL_SECONDS:
        return _next_utc_hourly_at_minute_past(completed_at, SCHEDULER_HOURLY_ALIGN_MINUTE)
    return _next_periodic_deadline_after(completed_at, interval_seconds)


def _jobs_for_loop() -> list[RegisteredJob]:
    """Enabled jobs with ``interval_seconds > 0``. Cron-only specs are skipped with a warning."""
    out: list[RegisteredJob] = []
    for r in iter_registered_jobs(include_disabled=False):
        if r.spec.interval_seconds is not None and r.spec.interval_seconds > 0:
            out.append(r)
        elif r.spec.cron_expression:
            logger.warning(
                "job_id={} has cron_expression but no interval_seconds; "
                "not scheduled in loop (add interval_seconds or use --dry-run-job)",
                r.spec.job_id,
            )
    return out


def _resolve_database_url(explicit: str | None) -> str | None:
    """Return a non-empty DB URL from ``explicit`` or settings; ``None`` skips ``scheduler_runs``."""
    if explicit is not None:
        u = explicit.strip()
        return u if u else None
    u = load_scheduler_settings().database_url
    if u is None:
        return None
    u = str(u).strip()
    return u if u else None


def _try_record_run_start(database_url: str | None, job_id: str) -> int | None:
    if not database_url:
        return None
    try:
        return record_run_start(database_url, job_id)
    except Exception as e:
        logger.warning("scheduler_runs record_run_start failed (job continues): {}", e)
        return None


def _try_record_run_end(
    database_url: str | None,
    run_id: int | None,
    *,
    status: RunStatus,
    error: str | None = None,
    payload: dict | None = None,
) -> None:
    if not database_url or run_id is None:
        return
    if error is not None and len(error) > _ERROR_TEXT_MAX_LEN:
        error = error[: _ERROR_TEXT_MAX_LEN] + "…"
    try:
        record_run_end(database_url, run_id, status, error, payload)
    except Exception as e:
        logger.warning("scheduler_runs record_run_end failed: {}", e)


def _wait_until(wall_deadline: float, stop: threading.Event) -> None:
    """Sleep until ``time.time() >= wall_deadline`` or ``stop`` is set (polls for responsive shutdown)."""
    while not stop.is_set():
        now = time.time()
        if now >= wall_deadline:
            return
        remaining = wall_deadline - now
        time.sleep(min(SCHEDULER_LOOP_POLL_SECONDS, remaining))


def run_job_once(
    r: RegisteredJob,
    *,
    timeout_seconds: float | None = None,
    database_url: str | None = None,
) -> None:
    """
    Run a single job with timeout and logging. Exceptions propagate after logging.

    Used by the loop and ``--dry-run-job`` (via ``main``).
    """
    job_id = r.spec.job_id
    db_url = _resolve_database_url(database_url)
    run_id = _try_record_run_start(db_url, job_id)

    timeout = timeout_seconds
    if timeout is None:
        timeout = r.spec.timeout_seconds if r.spec.timeout_seconds is not None else DEFAULT_JOB_TIMEOUT_SECONDS

    ctx = JobContext(job_id=job_id)
    started = time.perf_counter()
    logger.info("job_start job_id={}", job_id)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(r.job.run, ctx)
            future.result(timeout=timeout)
    except FutureTimeoutError:
        logger.error("job_timeout job_id={} timeout_s={}", job_id, timeout)
        _try_record_run_end(
            db_url,
            run_id,
            status="error",
            error=f"timeout after {timeout}s",
        )
        raise
    except Exception as e:
        _try_record_run_end(db_url, run_id, status="error", error=str(e))
        raise
    else:
        _try_record_run_end(db_url, run_id, status="ok")
    elapsed = time.perf_counter() - started
    logger.info("job_end job_id={} duration_ms={:.1f}", job_id, elapsed * 1000.0)


def run_job_isolated(r: RegisteredJob, *, database_url: str | None = None) -> None:
    """
    Run one job; swallow exceptions and log with stack — does not re-raise (loop continues).
    """
    job_id = r.spec.job_id
    db_url = _resolve_database_url(database_url)
    run_id = _try_record_run_start(db_url, job_id)

    timeout = (
        r.spec.timeout_seconds if r.spec.timeout_seconds is not None else DEFAULT_JOB_TIMEOUT_SECONDS
    )
    ctx = JobContext(job_id=job_id)
    started = time.perf_counter()
    logger.info("job_start job_id={}", job_id)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(r.job.run, ctx)
            future.result(timeout=timeout)
    except FutureTimeoutError:
        logger.error("job_timeout job_id={} timeout_s={}", job_id, timeout)
        _try_record_run_end(
            db_url,
            run_id,
            status="error",
            error=f"timeout after {timeout}s",
        )
    except Exception as e:
        logger.exception("job_failed job_id={}", job_id)
        _try_record_run_end(db_url, run_id, status="error", error=str(e))
    else:
        _try_record_run_end(db_url, run_id, status="ok")
        elapsed = time.perf_counter() - started
        logger.info("job_end job_id={} duration_ms={:.1f}", job_id, elapsed * 1000.0)


def run_scheduled_loop(stop: threading.Event, *, database_url: str | None = None) -> None:
    """
    Interval-based scheduler until ``stop`` is set. First fire for each job is immediate; thereafter
    jobs with ``interval_seconds == SCHEDULER_HOURLY_INTERVAL_SECONDS`` run at **UTC**
    ``SCHEDULER_HOURLY_ALIGN_MINUTE`` past each hour (e.g. :05). Other intervals use the epoch grid
    (same formula as ``market_data`` periodic deadlines).

    Cron-only jobs (no ``interval_seconds``) are excluded; see logs at startup.
    """
    db_url = _resolve_database_url(database_url)

    jobs = _jobs_for_loop()
    if not jobs:
        logger.warning("No jobs with interval_seconds > 0; idle until process exit")
        while not stop.is_set():
            time.sleep(SCHEDULER_LOOP_POLL_SECONDS)
        return

    next_run: dict[str, float | None] = {r.spec.job_id: None for r in jobs}
    logger.info(
        "Scheduler loop: {} job(s); first run immediate; then hourly jobs at UTC :{:02d} each hour; "
        "SIGINT/SIGTERM to stop",
        len(jobs),
        SCHEDULER_HOURLY_ALIGN_MINUTE,
    )

    while not stop.is_set():
        now_wall = time.time()
        due = [r for r in jobs if next_run[r.spec.job_id] is None or now_wall >= next_run[r.spec.job_id]]

        if not due:
            earliest = min(next_run[j.spec.job_id] for j in jobs if next_run[j.spec.job_id] is not None)
            if earliest is not None:
                _wait_until(earliest, stop)
            continue

        for r in due:
            if stop.is_set():
                break
            run_job_isolated(r, database_url=db_url)
            completed_at = time.time()
            interval = r.spec.interval_seconds
            assert interval is not None and interval > 0
            next_run[r.spec.job_id] = _next_deadline_after_run(completed_at, interval)

        if stop.is_set():
            break


def install_signal_handlers(stop: threading.Event) -> None:
    """Register SIGINT / SIGTERM to set ``stop``."""

    def _handler(signum: int, _frame: object | None) -> None:
        logger.info("shutdown_signal signum={}", signum)
        stop.set()

    signal.signal(signal.SIGINT, _handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handler)


def run_forever() -> None:
    """
    Entry for ``python -m scheduler``: loop until SIGINT/SIGTERM.

    In-flight work: each job is already capped by its own ``timeout``; there is no second long-running
    step after ``stop`` besides finishing the current ``future.result`` wait.
    """
    stop = threading.Event()
    install_signal_handlers(stop)
    db_url = load_scheduler_settings().database_url
    try:
        run_scheduled_loop(stop, database_url=db_url)
    finally:
        logger.info("Scheduler loop exited")
