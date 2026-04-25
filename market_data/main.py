"""
Market data service entrypoint (Phase 4 §9.6): scheduler for §9.5 REST jobs.

Runs until SIGINT/SIGTERM (Docker stop) or forever:

- **ingest_ohlcv** — catch-up from cursor / ``max(open_time)`` (cadence: ``OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS`` in ``config.py``).
- **correct_window** — rolling drift check (``OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS``).
- **repair_gap** — policy-window detect+refetch (``OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS``; **0 = off**).
- **ingest_basis_rate** — catch-up from basis cursor / ``max(sample_time)``.
- **correct_window_basis_rate** — rolling basis drift check.
- **repair_gap_basis_rate** — policy-window basis detect+refetch (**0 = off**).
- **ingest_open_interest** — catch-up from open-interest cursor / ``max(sample_time)``.
- **correct_window_open_interest** — rolling open-interest drift check.
- **repair_gap_open_interest** — policy-window open-interest detect+refetch (**0 = off**).

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
from typing import Any

from loguru import logger

from market_data.config import (
    MarketDataSettings,
    BASIS_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    BASIS_SCHEDULER_INGEST_INTERVAL_SECONDS,
    BASIS_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    OPEN_INTEREST_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    OPEN_INTEREST_SCHEDULER_INGEST_INTERVAL_SECONDS,
    OPEN_INTEREST_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    TOP_TRADER_LONG_SHORT_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    TOP_TRADER_LONG_SHORT_SCHEDULER_INGEST_INTERVAL_SECONDS,
    TOP_TRADER_LONG_SHORT_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    TAKER_BUYSELL_VOLUME_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    TAKER_BUYSELL_VOLUME_SCHEDULER_INGEST_INTERVAL_SECONDS,
    TAKER_BUYSELL_VOLUME_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    UNIVERSE_REFRESH_INTERVAL_SECONDS,
    load_settings,
)
from market_data.jobs.correct_window_basis_rate import run_correct_window_basis_rate
from market_data.jobs.correct_window_open_interest import run_correct_window_open_interest
from market_data.jobs.correct_window import run_correct_window
from market_data.jobs.correct_window_taker_buy_sell_volume import (
    run_correct_window_taker_buy_sell_volume,
)
from market_data.jobs.ingest_basis_rate import (
    run_backfill_new_symbols_basis_rate,
    run_ingest_basis_rate,
)
from market_data.jobs.ingest_open_interest import run_ingest_open_interest
from market_data.jobs.ingest_top_trader_long_short import run_ingest_top_trader_long_short
from market_data.jobs.ingest_taker_buy_sell_volume import run_ingest_taker_buy_sell_volume
from market_data.jobs.ingest_ohlcv import run_ingest_ohlcv
from market_data.jobs.repair_gap_basis_rate import run_repair_basis_gaps_policy_window_all_series
from market_data.jobs.repair_gap_open_interest import (
    run_repair_open_interest_gaps_policy_window_all_series,
)
from market_data.jobs.repair_gap import run_repair_gaps_policy_window_all_series
from market_data.jobs.repair_gap_taker_buy_sell_volume import (
    run_repair_taker_buy_sell_volume_gaps_policy_window_all_series,
)
from market_data.jobs.correct_window_top_trader_long_short import (
    run_correct_window_top_trader_long_short,
)
from market_data.jobs.repair_gap_top_trader_long_short import (
    run_repair_top_trader_long_short_gaps_policy_window_all_series,
)
from market_data.jobs.ingest_ohlcv import run_backfill_new_symbols_ohlcv
from market_data.jobs.ingest_open_interest import run_backfill_new_symbols_open_interest
from market_data.jobs.ingest_taker_buy_sell_volume import (
    run_backfill_new_symbols_taker_buy_sell_volume,
)
from market_data.jobs.ingest_top_trader_long_short import (
    run_backfill_new_symbols_top_trader_long_short,
)
from market_data.jobs.universe_refresh_top100 import run_universe_refresh_top100
from market_data.universe_runtime import resolve_runtime_symbols


def _run_per_dataset_initial_backfills(settings: MarketDataSettings) -> None:
    """Each dataset tracks its own ``initial_backfill_done`` on its cursor table."""
    for name, fn in (
        ("ohlcv", run_backfill_new_symbols_ohlcv),
        ("basis_rate", run_backfill_new_symbols_basis_rate),
        ("open_interest", run_backfill_new_symbols_open_interest),
        ("taker_buy_sell_volume", run_backfill_new_symbols_taker_buy_sell_volume),
        ("top_trader_long_short", run_backfill_new_symbols_top_trader_long_short),
    ):
        try:
            n = fn(settings)
            if n:
                logger.info("{} initial backfill: {} symbol(s)/pair(s) committed", name, n)
        except Exception:
            logger.exception("{} initial backfill step failed", name)


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


def _log_series_keys_summary(step_name: str, results: list[Any]) -> None:
    """One-line summary of series keys processed/retrieved."""
    if not results:
        logger.info("{} series keys: none", step_name)
        return

    keys: set[str] = set()
    for r in results:
        if hasattr(r, "pair"):
            pair = getattr(r, "pair")
            contract_type = getattr(r, "contract_type", None)
            period = getattr(r, "period", None)
            keys.add("/".join([str(x) for x in (pair, contract_type, period) if x is not None]))
            continue
        symbol = getattr(r, "symbol", None)
        if symbol is None:
            continue
        interval = getattr(r, "interval", None)
        contract_type = getattr(r, "contract_type", None)
        period = getattr(r, "period", None)
        parts = [str(symbol)]
        if contract_type is not None:
            parts.append(str(contract_type))
        if interval is not None:
            parts.append(str(interval))
        elif period is not None:
            parts.append(str(period))
        keys.add("/".join(parts))

    if not keys:
        logger.info("{} series keys: none", step_name)
        return

    shown = sorted(keys)
    max_show = 12
    preview = ", ".join(shown[:max_show])
    if len(shown) > max_show:
        preview += f", ... (+{len(shown) - max_show} more)"
    logger.info("{} series keys: {}", step_name, preview)


def _run_ingest_step() -> None:
    settings = load_settings()
    syms = resolve_runtime_symbols(settings)
    results = run_ingest_ohlcv(settings, symbols=syms)
    n = sum(r.bars_upserted for r in results)
    logger.info(
        "ingest_ohlcv: {} series, {} bars upserted",
        len(results),
        n,
    )
    _log_series_keys_summary("ingest_ohlcv", results)


def _run_correct_step() -> None:
    settings = load_settings()
    # correct_window currently reads symbols from settings; keep static list during cutover.
    results = run_correct_window(settings)
    d = sum(r.drift_rows for r in results)
    logger.info(
        "correct_window: {} series, {} drift row(s)",
        len(results),
        d,
    )
    _log_series_keys_summary("correct_window", results)


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
    _log_series_keys_summary("repair_gap", results)


def _run_basis_ingest_step() -> None:
    settings = load_settings()
    pairs = resolve_runtime_symbols(settings)
    results = run_ingest_basis_rate(settings, pairs=pairs)
    n = sum(r.rows_upserted for r in results)
    logger.info(
        "ingest_basis_rate: {} series, {} rows upserted",
        len(results),
        n,
    )
    _log_series_keys_summary("ingest_basis_rate", results)


def _run_basis_correct_step() -> None:
    settings = load_settings()
    results = run_correct_window_basis_rate(settings)
    d = sum(r.drift_rows for r in results)
    logger.info(
        "correct_window_basis_rate: {} series, {} drift row(s)",
        len(results),
        d,
    )
    _log_series_keys_summary("correct_window_basis_rate", results)


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
    _log_series_keys_summary("repair_gap_basis_rate", results)


def _run_open_interest_ingest_step() -> None:
    settings = load_settings()
    syms = resolve_runtime_symbols(settings)
    results = run_ingest_open_interest(settings, symbols=syms)
    n = sum(r.rows_upserted for r in results)
    logger.info(
        "ingest_open_interest: {} series, {} rows upserted",
        len(results),
        n,
    )
    _log_series_keys_summary("ingest_open_interest", results)


def _run_taker_ingest_step() -> None:
    settings = load_settings()
    syms = resolve_runtime_symbols(settings)
    results = run_ingest_taker_buy_sell_volume(settings, symbols=syms)
    n = sum(r.rows_upserted for r in results)
    logger.info(
        "ingest_taker_buy_sell_volume: {} series, {} rows upserted",
        len(results),
        n,
    )
    _log_series_keys_summary("ingest_taker_buy_sell_volume", results)


def _run_top_trader_ingest_step() -> None:
    settings = load_settings()
    syms = resolve_runtime_symbols(settings)
    results = run_ingest_top_trader_long_short(settings, symbols=syms)
    n = sum(r.rows_upserted for r in results)
    logger.info(
        "ingest_top_trader_long_short: {} series, {} rows upserted",
        len(results),
        n,
    )
    _log_series_keys_summary("ingest_top_trader_long_short", results)


def _run_open_interest_correct_step() -> None:
    settings = load_settings()
    results = run_correct_window_open_interest(settings)
    d = sum(r.drift_rows for r in results)
    logger.info(
        "correct_window_open_interest: {} series, {} drift row(s)",
        len(results),
        d,
    )
    _log_series_keys_summary("correct_window_open_interest", results)


def _run_taker_correct_step() -> None:
    settings = load_settings()
    results = run_correct_window_taker_buy_sell_volume(settings)
    d = sum(r.drift_rows for r in results)
    logger.info(
        "correct_window_taker_buy_sell_volume: {} series, {} drift row(s)",
        len(results),
        d,
    )
    _log_series_keys_summary("correct_window_taker_buy_sell_volume", results)


def _run_top_trader_correct_step() -> None:
    settings = load_settings()
    results = run_correct_window_top_trader_long_short(settings)
    d = sum(r.drift_rows for r in results)
    logger.info(
        "correct_window_top_trader_long_short: {} series, {} drift row(s)",
        len(results),
        d,
    )
    _log_series_keys_summary("correct_window_top_trader_long_short", results)


def _run_taker_repair_step() -> None:
    settings = load_settings()
    results = run_repair_taker_buy_sell_volume_gaps_policy_window_all_series(settings)
    with_gaps = sum(1 for r in results if r.gap_spans > 0)
    rows = sum(r.rows_upserted for r in results)
    logger.info(
        "repair_gap_taker_buy_sell_volume: {} series, {} had gaps, {} rows upserted",
        len(results),
        with_gaps,
        rows,
    )
    _log_series_keys_summary("repair_gap_taker_buy_sell_volume", results)


def _run_top_trader_repair_step() -> None:
    settings = load_settings()
    results = run_repair_top_trader_long_short_gaps_policy_window_all_series(settings)
    with_gaps = sum(1 for r in results if r.gap_spans > 0)
    rows = sum(r.rows_upserted for r in results)
    logger.info(
        "repair_gap_top_trader_long_short: {} series, {} had gaps, {} rows upserted",
        len(results),
        with_gaps,
        rows,
    )
    _log_series_keys_summary("repair_gap_top_trader_long_short", results)


def _run_open_interest_repair_step() -> None:
    settings = load_settings()
    results = run_repair_open_interest_gaps_policy_window_all_series(settings)
    with_gaps = sum(1 for r in results if r.gap_spans > 0)
    rows = sum(r.rows_upserted for r in results)
    logger.info(
        "repair_gap_open_interest: {} series, {} had gaps, {} rows upserted",
        len(results),
        with_gaps,
        rows,
    )
    _log_series_keys_summary("repair_gap_open_interest", results)


def run_scheduler_loop(
    *,
    ingest_interval_seconds: int,
    correct_interval_seconds: int,
    repair_interval_seconds: int,
    basis_ingest_interval_seconds: int,
    basis_correct_interval_seconds: int,
    basis_repair_interval_seconds: int,
    open_interest_ingest_interval_seconds: int,
    open_interest_correct_interval_seconds: int,
    open_interest_repair_interval_seconds: int,
    top_trader_ingest_interval_seconds: int,
    top_trader_correct_interval_seconds: int,
    top_trader_repair_interval_seconds: int,
    taker_ingest_interval_seconds: int,
    taker_correct_interval_seconds: int,
    taker_repair_interval_seconds: int,
    universe_refresh_interval_seconds: int,
    stop_event: threading.Event,
) -> None:
    next_ingest: float | None = None
    next_correct: float | None = None
    next_repair: float | None = None if repair_interval_seconds > 0 else float("inf")
    next_basis_ingest: float | None = None
    next_basis_correct: float | None = None
    next_basis_repair: float | None = None if basis_repair_interval_seconds > 0 else float("inf")
    next_oi_ingest: float | None = None
    next_oi_correct: float | None = None
    next_oi_repair: float | None = (
        None if open_interest_repair_interval_seconds > 0 else float("inf")
    )
    next_top_ingest: float | None = None
    next_top_correct: float | None = None
    next_top_repair: float | None = (
        None if top_trader_repair_interval_seconds > 0 else float("inf")
    )
    next_taker_ingest: float | None = None
    next_taker_correct: float | None = None
    next_taker_repair: float | None = (
        None if taker_repair_interval_seconds > 0 else float("inf")
    )

    next_universe_refresh: float | None = None

    repair_cadence = (
        f"every {repair_interval_seconds}s"
        if repair_interval_seconds > 0
        else "off"
    )
    oi_repair_cadence = (
        f"every {open_interest_repair_interval_seconds}s"
        if open_interest_repair_interval_seconds > 0
        else "off"
    )
    logger.info(
        "market_data scheduler: ingest every {}s (first run now, then UTC-aligned), "
        "correct_window every {}s, repair_gap {} ({}), "
        "ingest_basis_rate every {}s, correct_window_basis_rate every {}s, "
        "repair_gap_basis_rate {} ({}), "
        "ingest_open_interest every {}s, correct_window_open_interest every {}s, "
        "repair_gap_open_interest {} ({}), "
        "ingest_top_trader_long_short every {}s, correct_window_top_trader_long_short every {}s, "
        "repair_gap_top_trader_long_short {} ({}), "
        "ingest_taker_buy_sell_volume every {}s, correct_window_taker_buy_sell_volume every {}s, "
        "universe_refresh every {}s (per-dataset initial backfills on same tick)",
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
        open_interest_ingest_interval_seconds,
        open_interest_correct_interval_seconds,
        oi_repair_cadence,
        "enabled" if open_interest_repair_interval_seconds > 0 else "disabled",
        top_trader_ingest_interval_seconds,
        top_trader_correct_interval_seconds,
        f"every {top_trader_repair_interval_seconds}s"
        if top_trader_repair_interval_seconds > 0
        else "off",
        "enabled" if top_trader_repair_interval_seconds > 0 else "disabled",
        taker_ingest_interval_seconds,
        taker_correct_interval_seconds,
        universe_refresh_interval_seconds,
    )

    while not stop_event.is_set():
        # Process-by-process ordering:
        # 1) run all due ingests (OHLCV -> basis -> open interest)
        # 2) then run correct+repair per dataset in the same order.
        now = time.time()
        if _due(now, next_ingest):
            try:
                _run_ingest_step()
            except Exception:
                logger.exception("ingest_ohlcv step failed")
            next_ingest = _next_periodic_deadline_after(time.time(), ingest_interval_seconds)

        now = time.time()
        if _due(now, next_basis_ingest):
            try:
                _run_basis_ingest_step()
            except Exception:
                logger.exception("ingest_basis_rate step failed")
            next_basis_ingest = _next_periodic_deadline_after(
                time.time(), basis_ingest_interval_seconds
            )

        now = time.time()
        if _due(now, next_oi_ingest):
            try:
                _run_open_interest_ingest_step()
            except Exception:
                logger.exception("ingest_open_interest step failed")
            next_oi_ingest = _next_periodic_deadline_after(
                time.time(), open_interest_ingest_interval_seconds
            )

        now = time.time()
        if _due(now, next_top_ingest):
            try:
                _run_top_trader_ingest_step()
            except Exception:
                logger.exception("ingest_top_trader_long_short step failed")
            next_top_ingest = _next_periodic_deadline_after(
                time.time(), top_trader_ingest_interval_seconds
            )

        now = time.time()
        if _due(now, next_taker_ingest):
            try:
                _run_taker_ingest_step()
            except Exception:
                logger.exception("ingest_taker_buy_sell_volume step failed")
            next_taker_ingest = _next_periodic_deadline_after(
                time.time(), taker_ingest_interval_seconds
            )

        now = time.time()
        if _due(now, next_universe_refresh):
            settings = load_settings()
            try:
                run_universe_refresh_top100(settings)
            except Exception:
                logger.exception("universe_refresh_top100 step failed")
            _run_per_dataset_initial_backfills(settings)
            next_universe_refresh = _next_periodic_deadline_after(
                time.time(), universe_refresh_interval_seconds
            )

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
        if _due(now, next_basis_correct):
            try:
                _run_basis_correct_step()
            except Exception:
                logger.exception("correct_window_basis_rate step failed")
            next_basis_correct = _next_periodic_deadline_after(
                time.time(), basis_correct_interval_seconds
            )

        now = time.time()
        if basis_repair_interval_seconds > 0 and _due(now, next_basis_repair):
            try:
                _run_basis_repair_step()
            except Exception:
                logger.exception("repair_gap_basis_rate step failed")
            next_basis_repair = _next_periodic_deadline_after(
                time.time(), basis_repair_interval_seconds
            )

        now = time.time()
        if _due(now, next_oi_correct):
            try:
                _run_open_interest_correct_step()
            except Exception:
                logger.exception("correct_window_open_interest step failed")
            next_oi_correct = _next_periodic_deadline_after(
                time.time(), open_interest_correct_interval_seconds
            )

        now = time.time()
        if open_interest_repair_interval_seconds > 0 and _due(now, next_oi_repair):
            try:
                _run_open_interest_repair_step()
            except Exception:
                logger.exception("repair_gap_open_interest step failed")
            next_oi_repair = _next_periodic_deadline_after(
                time.time(), open_interest_repair_interval_seconds
            )

        now = time.time()
        if _due(now, next_top_correct):
            try:
                _run_top_trader_correct_step()
            except Exception:
                logger.exception("correct_window_top_trader_long_short step failed")
            next_top_correct = _next_periodic_deadline_after(
                time.time(), top_trader_correct_interval_seconds
            )

        now = time.time()
        if top_trader_repair_interval_seconds > 0 and _due(now, next_top_repair):
            try:
                _run_top_trader_repair_step()
            except Exception:
                logger.exception("repair_gap_top_trader_long_short step failed")
            next_top_repair = _next_periodic_deadline_after(
                time.time(), top_trader_repair_interval_seconds
            )

        now = time.time()
        if _due(now, next_taker_correct):
            try:
                _run_taker_correct_step()
            except Exception:
                logger.exception("correct_window_taker_buy_sell_volume step failed")
            next_taker_correct = _next_periodic_deadline_after(
                time.time(), taker_correct_interval_seconds
            )

        now = time.time()
        if taker_repair_interval_seconds > 0 and _due(now, next_taker_repair):
            try:
                _run_taker_repair_step()
            except Exception:
                logger.exception("repair_gap_taker_buy_sell_volume step failed")
            next_taker_repair = _next_periodic_deadline_after(
                time.time(), taker_repair_interval_seconds
            )

        deadline = min(
            _sleep_deadline(next_ingest),
            _sleep_deadline(next_correct),
            next_repair,
            _sleep_deadline(next_basis_ingest),
            _sleep_deadline(next_basis_correct),
            next_basis_repair,
            _sleep_deadline(next_oi_ingest),
            _sleep_deadline(next_oi_correct),
            next_oi_repair,
            _sleep_deadline(next_top_ingest),
            _sleep_deadline(next_top_correct),
            next_top_repair,
            _sleep_deadline(next_taker_ingest),
            _sleep_deadline(next_taker_correct),
            _sleep_deadline(next_taker_repair),
            _sleep_deadline(next_universe_refresh),
        )
        sleep_for = max(0.0, min(1.0, deadline - time.time()))
        if sleep_for > 0:
            stop_event.wait(sleep_for)

    logger.info("market_data scheduler stopped")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market data: scheduled OHLCV + basis + open-interest jobs (REST -> Postgres)"
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
            # Process-by-process ordering for --once:
            # 1) run all ingests
            # 2) then run correct+repair per dataset in order.
            _run_ingest_step()
            _run_basis_ingest_step()
            _run_open_interest_ingest_step()
            _run_top_trader_ingest_step()
            _run_taker_ingest_step()

            settings_once = load_settings()
            try:
                run_universe_refresh_top100(settings_once)
            except Exception:
                logger.exception("universe_refresh_top100 (--once) failed")
            _run_per_dataset_initial_backfills(settings_once)

            _run_correct_step()
            if args.with_repair:
                _run_repair_step()

            _run_basis_correct_step()
            if args.with_repair:
                _run_basis_repair_step()

            _run_open_interest_correct_step()
            if args.with_repair:
                _run_open_interest_repair_step()

            _run_top_trader_correct_step()
            if args.with_repair:
                _run_top_trader_repair_step()

            _run_taker_correct_step()
            if args.with_repair:
                _run_taker_repair_step()
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
        open_interest_ingest_interval_seconds=OPEN_INTEREST_SCHEDULER_INGEST_INTERVAL_SECONDS,
        open_interest_correct_interval_seconds=OPEN_INTEREST_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
        open_interest_repair_interval_seconds=OPEN_INTEREST_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
        top_trader_ingest_interval_seconds=TOP_TRADER_LONG_SHORT_SCHEDULER_INGEST_INTERVAL_SECONDS,
        top_trader_correct_interval_seconds=TOP_TRADER_LONG_SHORT_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
        top_trader_repair_interval_seconds=TOP_TRADER_LONG_SHORT_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
        taker_ingest_interval_seconds=TAKER_BUYSELL_VOLUME_SCHEDULER_INGEST_INTERVAL_SECONDS,
        taker_correct_interval_seconds=TAKER_BUYSELL_VOLUME_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
        taker_repair_interval_seconds=TAKER_BUYSELL_VOLUME_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
        universe_refresh_interval_seconds=UNIVERSE_REFRESH_INTERVAL_SECONDS,
        stop_event=stop,
    )


if __name__ == "__main__":
    main()
