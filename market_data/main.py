"""
Market data service entrypoint (Phase 4 §9.6): scheduler for §9.5 REST jobs.

Runs until SIGINT/SIGTERM (Docker stop) or forever:

- **ingest_ohlcv** — catch-up from cursor / ``max(open_time)`` (cadence: ``OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS`` in ``config.py``).
- **correct_window** — rolling drift check (``OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS``).
- **repair_gap** — policy-window detect+refetch (``OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS``; **0 = off**).
- **ingest_basis_rate** — catch-up from basis cursor / ``max(sample_time)``.
- **correct_window_basis_rate** — rolling basis drift check.
- **repair_gap_basis_rate** — policy-window basis detect+refetch (**0 = off**).

Each job **runs once immediately** on startup, then its **next** runs fall on **UTC-aligned** period
boundaries (Unix epoch): e.g. default ingest every **300 s** then fires at **:00, :05, :10, … UTC**.

Usage::

    python -m market_data.main
    python -m market_data.main --once
    python -m market_data.main --once --with-repair

Env: ``DATABASE_URL`` or ``MARKET_DATA_DATABASE_URL``, optional
``MARKET_DATA_BINANCE_BASE_URL`` and ``MARKET_DATA_BINANCE_PERPS_BASE_URL``.
"""

from __future__ import annotations

import argparse
import math
import signal
import sys
import threading
import time

from loguru import logger

from market_data.config import (
    BASIS_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    BASIS_SCHEDULER_INGEST_INTERVAL_SECONDS,
    BASIS_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    load_settings,
)
from market_data.jobs.correct_window_basis_rate import run_correct_window_basis_rate
from market_data.jobs.correct_window import run_correct_window
from market_data.jobs.ingest_basis_rate import run_ingest_basis_rate
from market_data.jobs.ingest_ohlcv import run_ingest_ohlcv
from market_data.jobs.repair_gap_basis_rate import run_repair_basis_gaps_policy_window_all_series
from market_data.jobs.repair_gap import run_repair_gaps_policy_window_all_series


def _next_periodic_deadline_after(completed_at: float, period_seconds: int) -> float:
    """
    Strictly future UTC-aligned tick (epoch seconds): ``floor(t / p) * p + p``.

    Skips missed slots in O(1) when a run finishes late.
    """
    if period_seconds < 1:
        raise ValueError("period_seconds must be >= 1")
    return math.floor(completed_at / period_seconds) * period_seconds + period_seconds


def _due(now: float, next_run: float | None) -> bool:
    """``None`` means “run once as soon as the loop starts” (immediate first fire)."""
    return next_run is None or now >= next_run


def _sleep_deadline(next_run: float | None) -> float:
    """Earliest wake time; ``None`` means no wait (run again this tick if still due)."""
    return 0.0 if next_run is None else next_run


def _run_ingest_step() -> None:
    settings = load_settings()
    results = run_ingest_ohlcv(settings)
    n = sum(r.bars_upserted for r in results)
    logger.info(
        "ingest_ohlcv: {} series, {} bars upserted",
        len(results),
        n,
    )


def _run_correct_step() -> None:
    settings = load_settings()
    results = run_correct_window(settings)
    d = sum(r.drift_rows for r in results)
    logger.info(
        "correct_window: {} series, {} drift row(s)",
        len(results),
        d,
    )


def _run_repair_step() -> None:
    settings = load_settings()
    results = run_repair_gaps_policy_window_all_series(settings)
    with_gaps = sum(1 for r in results if r.gap_spans > 0)
    bars = sum(r.bars_upserted for r in results)
    logger.info(
        "repair_gap: {} series, {} had gaps, {} bars upserted",
        len(results),
        with_gaps,
        bars,
    )


def _run_basis_ingest_step() -> None:
    settings = load_settings()
    results = run_ingest_basis_rate(settings)
    n = sum(r.rows_upserted for r in results)
    logger.info(
        "ingest_basis_rate: {} series, {} rows upserted",
        len(results),
        n,
    )


def _run_basis_correct_step() -> None:
    settings = load_settings()
    results = run_correct_window_basis_rate(settings)
    d = sum(r.drift_rows for r in results)
    logger.info(
        "correct_window_basis_rate: {} series, {} drift row(s)",
        len(results),
        d,
    )


def _run_basis_repair_step() -> None:
    settings = load_settings()
    results = run_repair_basis_gaps_policy_window_all_series(settings)
    with_gaps = sum(1 for r in results if r.gap_spans > 0)
    rows = sum(r.rows_upserted for r in results)
    logger.info(
        "repair_gap_basis_rate: {} series, {} had gaps, {} rows upserted",
        len(results),
        with_gaps,
        rows,
    )


def run_scheduler_loop(
    *,
    ingest_interval_seconds: int,
    correct_interval_seconds: int,
    repair_interval_seconds: int,
    basis_ingest_interval_seconds: int,
    basis_correct_interval_seconds: int,
    basis_repair_interval_seconds: int,
    stop_event: threading.Event,
) -> None:
    next_ingest: float | None = None
    next_correct: float | None = None
    next_repair: float | None = None if repair_interval_seconds > 0 else float("inf")
    next_basis_ingest: float | None = None
    next_basis_correct: float | None = None
    next_basis_repair: float | None = None if basis_repair_interval_seconds > 0 else float("inf")

    repair_cadence = (
        f"every {repair_interval_seconds}s"
        if repair_interval_seconds > 0
        else "off"
    )
    logger.info(
        "market_data scheduler: ingest every {}s (first run now, then UTC-aligned), "
        "correct_window every {}s, repair_gap {} ({}), "
        "ingest_basis_rate every {}s, correct_window_basis_rate every {}s, "
        "repair_gap_basis_rate {} ({})",
        ingest_interval_seconds,
        correct_interval_seconds,
        repair_cadence,
        "enabled" if repair_interval_seconds > 0 else "disabled",
        basis_ingest_interval_seconds,
        basis_correct_interval_seconds,
        (
            f"every {basis_repair_interval_seconds}s"
            if basis_repair_interval_seconds > 0
            else "off"
        ),
        "enabled" if basis_repair_interval_seconds > 0 else "disabled",
    )

    while not stop_event.is_set():
        now = time.time()
        if _due(now, next_ingest):
            try:
                _run_ingest_step()
            except Exception:
                logger.exception("ingest_ohlcv step failed")
            next_ingest = _next_periodic_deadline_after(time.time(), ingest_interval_seconds)

        now = time.time()
        if _due(now, next_correct):
            try:
                _run_correct_step()
            except Exception:
                logger.exception("correct_window step failed")
            next_correct = _next_periodic_deadline_after(time.time(), correct_interval_seconds)

        now = time.time()
        if repair_interval_seconds > 0 and _due(now, next_repair):
            try:
                _run_repair_step()
            except Exception:
                logger.exception("repair_gap step failed")
            next_repair = _next_periodic_deadline_after(time.time(), repair_interval_seconds)

        now = time.time()
        if _due(now, next_basis_ingest):
            try:
                _run_basis_ingest_step()
            except Exception:
                logger.exception("ingest_basis_rate step failed")
            next_basis_ingest = _next_periodic_deadline_after(time.time(), basis_ingest_interval_seconds)

        now = time.time()
        if _due(now, next_basis_correct):
            try:
                _run_basis_correct_step()
            except Exception:
                logger.exception("correct_window_basis_rate step failed")
            next_basis_correct = _next_periodic_deadline_after(time.time(), basis_correct_interval_seconds)

        now = time.time()
        if basis_repair_interval_seconds > 0 and _due(now, next_basis_repair):
            try:
                _run_basis_repair_step()
            except Exception:
                logger.exception("repair_gap_basis_rate step failed")
            next_basis_repair = _next_periodic_deadline_after(time.time(), basis_repair_interval_seconds)

        deadline = min(
            _sleep_deadline(next_ingest),
            _sleep_deadline(next_correct),
            next_repair,
            _sleep_deadline(next_basis_ingest),
            _sleep_deadline(next_basis_correct),
            next_basis_repair,
        )
        sleep_for = max(0.0, min(1.0, deadline - time.time()))
        if sleep_for > 0:
            stop_event.wait(sleep_for)

    logger.info("market_data scheduler stopped")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market data: scheduled OHLCV + basis jobs (REST -> Postgres)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run ingest + correct_window once and exit (optional --with-repair)",
    )
    parser.add_argument(
        "--with-repair",
        action="store_true",
        help="With --once: also run one policy-window gap repair pass",
    )
    args = parser.parse_args()

    settings = load_settings()
    if not (settings.database_url or "").strip():
        logger.error("DATABASE_URL / MARKET_DATA_DATABASE_URL is not set")
        sys.exit(1)

    if args.once:
        try:
            _run_ingest_step()
            _run_correct_step()
            _run_basis_ingest_step()
            _run_basis_correct_step()
            if args.with_repair:
                _run_repair_step()
                _run_basis_repair_step()
        except Exception:
            logger.exception("market_data --once failed")
            sys.exit(1)
        return

    stop = threading.Event()

    def _handle_stop(*_: object) -> None:
        logger.info("shutdown signal received")
        stop.set()

    signal.signal(signal.SIGINT, _handle_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_stop)

    run_scheduler_loop(
        ingest_interval_seconds=OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS,
        correct_interval_seconds=OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
        repair_interval_seconds=OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
        basis_ingest_interval_seconds=BASIS_SCHEDULER_INGEST_INTERVAL_SECONDS,
        basis_correct_interval_seconds=BASIS_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
        basis_repair_interval_seconds=BASIS_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
        stop_event=stop,
    )


if __name__ == "__main__":
    main()
