"""Factor LS runner adapter: run Phase 1 pipeline from the strategies runner service."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import psycopg2
from loguru import logger

from pgconn import configure_for_market_data
from market_data.storage import get_dataset_status

from strategies.modules.factor_ls.pipeline import run_pipeline
from strategies.runner.types import RunnerContext, StrategyModule


@dataclass
class FactorLsRunnerModule:
    """
    `StrategyModule` wrapper around `strategies.modules.factor_ls.run_pipeline`.

    Runs at most once per UTC day, gated by market_data.dataset_status for OHLCV.
    """

    dataset_status_period: str = "1h"
    updated_at_cushion_seconds: int = 180  # 3 minutes

    _last_run_utc_date: object | None = None  # date

    @property
    def module_id(self) -> str:
        return "factor_ls"

    @property
    def book_id(self) -> str:
        return self.module_id

    def _db_url(self) -> str:
        # Reuse factor_ls DB resolution contract.
        from strategies.modules.factor_ls.data_loader import database_url

        return database_url()

    def _ohlcv_ready_for_previous_utc_day(self, now_utc: datetime) -> bool:
        """
        Gate only on dataset_status.updated_at (per plan).

        Require status computed after the UTC day boundary for the previous day.
        """
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        else:
            now_utc = now_utc.astimezone(timezone.utc)

        last_bar_ts = (now_utc.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1))
        need_updated_at = last_bar_ts + timedelta(days=1, seconds=self.updated_at_cushion_seconds)

        conn = psycopg2.connect(self._db_url())
        configure_for_market_data(conn)
        try:
            _, updated_at, status, _ = get_dataset_status(conn, dataset="ohlcv", period=self.dataset_status_period)
        finally:
            conn.close()

        if status and str(status).lower() not in ("ok", "partial"):
            logger.warning("factor_ls gate: dataset_status status={} (treating as not ready)", status)
            return False
        if updated_at is None:
            logger.info("factor_ls gate: no dataset_status row for ohlcv/{}", self.dataset_status_period)
            return False
        return updated_at >= need_updated_at

    def run_tick(self, ctx: RunnerContext) -> None:
        now = ctx.now_utc
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        else:
            now = now.astimezone(timezone.utc)

        today = now.date()
        if self._last_run_utc_date == today:
            return

        # Only attempt on/after the scheduled boundary (00:00 UTC). If runner starts later, still OK.
        if not self._ohlcv_ready_for_previous_utc_day(now):
            logger.info("factor_ls gate: waiting for OHLCV readiness (dataset_status.updated_at)")
            return

        logger.info("factor_ls run start run_at_utc={}", now.isoformat())
        run_pipeline(run_at_utc=now, persist=True, quiet=False)
        self._last_run_utc_date = today
        logger.info("factor_ls run done utc_date={}", today)

