"""
Scheduler configuration: **micro** (this file) vs **macro** (env — §5.2).

- **Micro:** job cadence, timeouts, which jobs are registered/enabled, ``JOB_SPECS`` — not in ``.env``
  (see ``.cursor/rules/env-and-config.mdc``).
- **Macro:** shared ``DATABASE_URL`` and ``REDIS_URL`` (same as other services; no scheduler-specific DB/Redis env).
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from scheduler.types import JobSpec

# --- Micro (code constants; not .env) ---

# Default cap for a single job invocation when spec omits timeout (runner may use this).
DEFAULT_JOB_TIMEOUT_SECONDS: float = 300.0

# Sleep slice when waiting for the next tick so shutdown is responsive (seconds).
SCHEDULER_LOOP_POLL_SECONDS: float = 1.0

# CSV report outputs live under ``scheduler/reports_out/`` (listed in root ``.gitignore``).
SCHEDULER_REPORTS_CSV_DIRNAME: str = "reports_out"


def scheduler_reports_csv_dir() -> Path:
    """Package-relative directory for scheduler CSV exports; created on first write."""
    return Path(__file__).resolve().parent / SCHEDULER_REPORTS_CSV_DIRNAME

# Registry keys must match entries in ``scheduler.registry._JOB_FACTORIES``.
JOB_SPECS: tuple[JobSpec, ...] = (
    JobSpec(
        job_id="position_snapshot_hourly",
        enabled=True,
        interval_seconds=3600,
        cron_expression=None,
        timeout_seconds=DEFAULT_JOB_TIMEOUT_SECONDS,
    ),
    JobSpec(
        job_id="noop_heartbeat",
        enabled=False,
        interval_seconds=3600,
        cron_expression=None,
        timeout_seconds=DEFAULT_JOB_TIMEOUT_SECONDS,
    ),
)


class SchedulerSettings(BaseSettings):
    """
    Macro env: same **DATABASE_URL** / **REDIS_URL** as the rest of the stack (no separate scheduler URLs).

    Both are optional while jobs are no-ops; Postgres/Redis jobs should validate at runtime.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str | None = Field(default=None, description="Postgres URL (DATABASE_URL).")
    redis_url: str | None = Field(default=None, description="Redis URL (REDIS_URL); optional until locks.")


def load_scheduler_settings() -> SchedulerSettings:
    """Load from environment and optional ``.env`` file."""
    return SchedulerSettings()
