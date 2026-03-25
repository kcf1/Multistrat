"""
Scheduler configuration: **micro** (this file) vs **macro** (env — §5.2).

- **Micro:** job cadence, timeouts, which jobs are registered/enabled, ``JOB_SPECS`` — not in ``.env``
  (see ``.cursor/rules/env-and-config.mdc``).
- **Macro:** shared ``DATABASE_URL`` and ``REDIS_URL`` (same as other services; no scheduler-specific DB/Redis env).
"""

from __future__ import annotations

from decimal import Decimal
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

# Cadence for hourly jobs (order recon, position snapshot, etc.).
SCHEDULER_HOURLY_INTERVAL_SECONDS: int = 3600
# Wall-clock alignment (UTC): after each run, next fire is the **next** occurrence of this
# minute past the hour (e.g. ``5`` → …:04:59 → **10:05:00** UTC). Same idea as spacing off
# the hour in ``market_data`` (ingest ticks on 5-minute epoch grid), but **hourly at :05**, not every 5 minutes.
SCHEDULER_HOURLY_ALIGN_MINUTE: int = 5


def scheduler_reports_csv_dir() -> Path:
    """Package-relative directory for scheduler CSV exports; created on first write."""
    return Path(__file__).resolve().parent / SCHEDULER_REPORTS_CSV_DIRNAME


# Order reconciliation (Phase 5 §4.6): **hourly** — filled orders vs Binance ``myTrades`` over
# the last ``ORDER_RECON_TRADE_LOOKBACK_HOURS`` (2h window; job still runs every hour).
ORDER_RECON_BROKER: str = "binance"
ORDER_RECON_DB_FILLED_STATUSES: tuple[str, ...] = ("filled",)
ORDER_RECON_TRADE_TAIL_LIMIT: int = 100
ORDER_RECON_TRADE_LOOKBACK_HOURS: int = 2
# Restrict to one broker account id if set; ``None`` = all rows for ``ORDER_RECON_BROKER``.
ORDER_RECON_ACCOUNT_ID: str | None = None

# Position reconciliation (Phase 5 §5.6.2): PMS ``positions`` vs Binance spot wallet (hourly).
POSITION_RECON_BROKER: str = "binance"
POSITION_RECON_ACCOUNT_ID: str | None = None
POSITION_RECON_QTY_EPSILON: Decimal = Decimal("1e-8")

# Misc (Phase 5 §4.7): no-op heartbeat (hourly, same cadence as other scheduler jobs).
NOOP_HEARTBEAT_INTERVAL_SECONDS: int = SCHEDULER_HOURLY_INTERVAL_SECONDS

# Registry keys must match entries in ``scheduler.registry._JOB_FACTORIES``.
JOB_SPECS: tuple[JobSpec, ...] = (
    JobSpec(
        job_id="order_reconciliation_binance",
        enabled=True,
        interval_seconds=SCHEDULER_HOURLY_INTERVAL_SECONDS,
        cron_expression=None,
        timeout_seconds=DEFAULT_JOB_TIMEOUT_SECONDS,
    ),
    JobSpec(
        job_id="position_reconciliation_binance",
        enabled=True,
        interval_seconds=SCHEDULER_HOURLY_INTERVAL_SECONDS,
        cron_expression=None,
        timeout_seconds=DEFAULT_JOB_TIMEOUT_SECONDS,
    ),
    JobSpec(
        job_id="position_snapshot_hourly",
        enabled=True,
        interval_seconds=SCHEDULER_HOURLY_INTERVAL_SECONDS,
        cron_expression=None,
        timeout_seconds=DEFAULT_JOB_TIMEOUT_SECONDS,
    ),
    JobSpec(
        job_id="noop_heartbeat",
        enabled=True,
        interval_seconds=NOOP_HEARTBEAT_INTERVAL_SECONDS,
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
