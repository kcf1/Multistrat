"""
Time OHLCV ingest sub-steps (cursor -> API -> parsing/validation -> upsert -> commit).

This script mirrors the watermark-based ingest flow in ``market_data/jobs/ingest_ohlcv.py`` for
one ``(symbol, interval)`` and records durations for:
- DB cursor reads (``get_ingestion_cursor``, ``max_open_time_ohlcv``)
- REST call (HTTP GET duration)
- JSON decode duration (``resp.json()``)
- Parsing + validation split:
  - Parse (row -> ``OhlcvBar``; includes pydantic field/model checks)
  - Validate (batch/rule checks: ordering, overlap, span coverage)
- Filter step (drop bars with ``open_time`` after ``end_ms``)
- DB writes: ``upsert_ohlcv_bars``, ``upsert_ingestion_cursor``, and ``conn.commit()``

Run (example):
  python scripts/time_ohlcv_ingest.py --symbol BTCUSDT --interval 1h --max-pages 2 --write
"""

from __future__ import annotations

import os
import sys

# When executing `python scripts/<this_file>.py`, Python puts `scripts/` on `sys.path`
# (but not the repo root), so sibling packages like `market_data/` may not import.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import argparse
import csv
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

import psycopg2
import requests
from loguru import logger

from pgconn import configure_for_market_data
from market_data.config import (
    MARKET_DATA_MIN_REQUEST_INTERVAL_SEC,
    OHLCV_INITIAL_BACKFILL_DAYS,
    OHLCV_KLINES_CHUNK_LIMIT,
    OHLCV_KLINES_FETCH_MAX_ATTEMPTS,
    OHLCV_KLINES_FETCH_RETRY_BASE_SLEEP_SEC,
    load_settings,
)
from market_data.intervals import interval_to_millis
from market_data.intervals import floor_align_ms_to_interval
from market_data.jobs.common import open_time_plus_interval_ms, utc_now_ms
from market_data.schemas import OhlcvBar, parse_binance_kline
from market_data.storage import (
    get_ingestion_cursor,
    max_open_time_ohlcv,
    upsert_ohlcv_bars,
    upsert_ingestion_cursor,
)
from market_data.validation import (
    _batch_integrity_issues,
    _interior_overlap_issues,
    _span_coverage_issues,
    _warn_large_open_time_gaps,
)
from market_data.rate_limit import ProviderRateLimiter


BINANCE_SPOT_KLINES_PATH = "/api/v3/klines"


@dataclass
class AttemptStats:
    attempt: int
    ok: bool
    http_get_s: float = 0.0
    json_decode_s: float = 0.0
    parse_s: float = 0.0
    validate_s: float = 0.0
    sleep_s: float = 0.0
    total_s: float = 0.0
    error: Optional[str] = None
    rows_returned: int = 0


@dataclass
class PageStats:
    start_time_ms: int
    end_time_ms: int
    limit: int
    attempts: List[AttemptStats] = field(default_factory=list)
    filtered_out: int = 0
    bars_after_filter: int = 0
    fetch_s_total: float = 0.0  # includes retries for this page

    upsert_s: float = 0.0
    cursor_upsert_s: float = 0.0
    open_time_step_s: float = 0.0
    commit_s: float = 0.0


@dataclass
class IngestStats:
    symbol: str
    interval: str
    start_ms: int
    end_ms: int
    horizon_ms: int
    backfill_days: int
    use_watermark: bool

    cursor_read_s: float = 0.0
    max_open_time_s: float = 0.0

    pages: List[PageStats] = field(default_factory=list)
    bars_upserted: int = 0
    filter_s_total: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "horizon_ms": self.horizon_ms,
            "backfill_days": self.backfill_days,
            "use_watermark": self.use_watermark,
            "cursor_read_s": self.cursor_read_s,
            "max_open_time_s": self.max_open_time_s,
            "pages": len(self.pages),
            "bars_upserted": self.bars_upserted,
            "filter_s_total": self.filter_s_total,
        }


def _resolve_ingest_start_ms(
    conn: psycopg2.extensions.connection,
    symbol: str,
    interval: str,
    *,
    now_ms: int,
    backfill_days: int,
    use_watermark: bool,
) -> Tuple[int, int, float, float]:
    """
    Watermark path only (``use_watermark=True``).

    Returns:
      start_ms, horizon_ms, cursor_read_s, max_open_time_s
    """
    iv_ms = interval_to_millis(interval)
    horizon_ms = now_ms - backfill_days * 86_400_000
    if not use_watermark:
        return horizon_ms, horizon_ms, 0.0, 0.0

    t0 = time.perf_counter()
    c = get_ingestion_cursor(conn, symbol, interval)
    t1 = time.perf_counter()
    cursor_s = t1 - t0

    t2 = time.perf_counter()
    m = max_open_time_ohlcv(conn, symbol, interval)
    t3 = time.perf_counter()
    max_open_time_s = t3 - t2

    ref: Optional[datetime] = None
    if c is not None and m is not None:
        ref = max(c, m, key=lambda t: t.timestamp())
    elif c is not None:
        ref = c
    elif m is not None:
        ref = m

    if ref is not None:
        return open_time_plus_interval_ms(ref, iv_ms), horizon_ms, cursor_s, max_open_time_s

    return horizon_ms, horizon_ms, cursor_s, max_open_time_s


def _build_binance_spot_url(
    base_url: str,
    *,
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int,
) -> str:
    params: Dict[str, Any] = {
        "symbol": symbol.strip().upper(),
        "interval": interval.strip(),
        "startTime": int(start_time_ms),
        "limit": int(limit),
        "endTime": int(end_time_ms),
    }
    return f"{base_url.rstrip('/')}{BINANCE_SPOT_KLINES_PATH}?{urlencode(params)}"


def _filter_bars_not_after_end(
    bars: Sequence[OhlcvBar],
    end_ms: int,
) -> Tuple[List[OhlcvBar], int, float]:
    """
    Mirrors ``market_data/jobs/common.py::filter_bars_not_after_ms``.
    Returns: filtered_bars, filtered_out_count, filter_s
    """
    t0 = time.perf_counter()
    end_dt = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    out = [b for b in bars if b.open_time <= end_dt]
    t1 = time.perf_counter()
    return out, len(bars) - len(out), t1 - t0


def _parse_and_validate_klines_timed(
    raw: Any,
    *,
    symbol: str,
    interval: str,
    start_time_ms: int | None,
    end_time_ms: int | None,
    request_limit: int | None,
) -> tuple[list[OhlcvBar], float, float]:
    """
    Split parse vs validation timings:
    - parse: row decoding into OhlcvBar (includes pydantic field/model checks)
    - validate: batch/rule checks over parsed rows
    """
    if not isinstance(raw, list):
        raise ValueError("Binance klines response must be a JSON array")

    sym = symbol.strip().upper()
    iv = interval.strip()
    iv_ms = interval_to_millis(iv)

    t_parse0 = time.perf_counter()
    row_errors: list[str] = []
    bars: list[OhlcvBar] = []
    for i, row in enumerate(raw):
        if not isinstance(row, list):
            row_errors.append(f"row[{i}] is not a list")
            continue
        try:
            bars.append(parse_binance_kline(row, symbol=sym, interval=iv))
        except Exception as e:  # noqa: BLE001
            row_errors.append(f"row[{i}]: {e}")
    t_parse1 = time.perf_counter()
    parse_s = t_parse1 - t_parse0

    if row_errors:
        head = "; ".join(row_errors[:15])
        more = f" ... (+{len(row_errors) - 15} more)" if len(row_errors) > 15 else ""
        raise ValueError(f"klines row errors ({len(row_errors)}): {head}{more}")

    t_validate0 = time.perf_counter()
    batch_issues = _batch_integrity_issues(bars)
    if batch_issues:
        raise ValueError("klines batch integrity: " + "; ".join(batch_issues))

    overlaps = _interior_overlap_issues(bars, iv_ms)
    if overlaps:
        raise ValueError("klines open_time overlap: " + "; ".join(overlaps))

    _warn_large_open_time_gaps(bars, iv_ms, symbol=sym, interval=iv)

    if (
        start_time_ms is not None
        and end_time_ms is not None
        and request_limit is not None
        and bars
    ):
        span_issues = _span_coverage_issues(
            bars,
            start_ms=start_time_ms,
            end_ms=end_time_ms,
            iv_ms=iv_ms,
            request_limit=request_limit,
        )
        fatal = [
            m
            for m in span_issues
            if not (m.startswith("tail shortfall:") or m.startswith("head slack:"))
        ]
        if fatal:
            raise ValueError("klines span/coverage: " + "; ".join(fatal))
    t_validate1 = time.perf_counter()
    validate_s = t_validate1 - t_validate0

    return bars, parse_s, validate_s


def _fetch_ohlcv_page_timed(
    *,
    session: requests.Session,
    limiter: ProviderRateLimiter,
    base_url: str,
    symbol: str,
    interval: str,
    start_time_ms: int,
    end_time_ms: int,
    limit: int,
    timeout_s: int,
    fetch_max_attempts: int,
    fetch_retry_base_sleep_sec: float,
) -> Tuple[List[OhlcvBar], PageStats]:
    """
    Fetch one page [start_time_ms, end_time_ms] and parse/validate it.
    Includes retry/backoff timings.
    """
    page = PageStats(
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=limit,
    )
    url = _build_binance_spot_url(
        base_url,
        symbol=symbol,
        interval=interval,
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        limit=limit,
    )

    for attempt in range(1, fetch_max_attempts + 1):
        attempt_stats = AttemptStats(attempt=attempt, ok=False)
        t_attempt0 = time.perf_counter()
        try:
            limiter.acquire()

            t_http0 = time.perf_counter()
            resp = session.get(url, timeout=timeout_s)
            t_http1 = time.perf_counter()
            attempt_stats.http_get_s = t_http1 - t_http0

            if resp.status_code == 400:
                body = (resp.text or "").strip()
                snippet = body[:400] + ("..." if len(body) > 400 else "")
                attempt_stats.error = f"HTTP 400: {snippet or '(no body)'}"
                attempt_stats.total_s = time.perf_counter() - t_attempt0
                page.attempts.append(attempt_stats)
                page.fetch_s_total += attempt_stats.total_s
                return [], page

            resp.raise_for_status()

            t_json0 = time.perf_counter()
            raw = resp.json()
            t_json1 = time.perf_counter()
            attempt_stats.json_decode_s = t_json1 - t_json0

            bars, parse_s, validate_s = _parse_and_validate_klines_timed(
                raw,
                symbol=symbol,
                interval=interval,
                start_time_ms=int(start_time_ms),
                end_time_ms=int(end_time_ms),
                request_limit=int(limit),
            )
            attempt_stats.parse_s = parse_s
            attempt_stats.validate_s = validate_s

            attempt_stats.rows_returned = len(bars)
            attempt_stats.ok = True
            attempt_stats.total_s = time.perf_counter() - t_attempt0

            page.attempts.append(attempt_stats)
            page.fetch_s_total += attempt_stats.total_s
            return bars, page
        except Exception as e:  # noqa: BLE001 - mirrors provider retry semantics
            attempt_stats.error = str(e)
            attempt_stats.total_s = time.perf_counter() - t_attempt0
            page.attempts.append(attempt_stats)
            page.fetch_s_total += attempt_stats.total_s

            if attempt >= fetch_max_attempts:
                return [], page

            sleep_s = fetch_retry_base_sleep_sec * (2 ** (attempt - 1))
            t_sleep0 = time.perf_counter()
            time.sleep(sleep_s)
            t_sleep1 = time.perf_counter()
            attempt_stats.sleep_s = t_sleep1 - t_sleep0
            page.fetch_s_total += attempt_stats.sleep_s

    return [], page


def run_time_ohlcv_ingest_once(
    *,
    symbol: str,
    interval: str,
    now_ms: int | None,
    backfill_days: int,
    chunk_limit: int,
    use_watermark: bool,
    max_pages: int | None,
    write: bool,
    force_fetch_window_intervals: int = 0,
    override_start_ms: int | None = None,
    override_end_ms: int | None = None,
) -> IngestStats:
    settings = load_settings()
    conn = psycopg2.connect(settings.database_url)
    configure_for_market_data(conn)
    try:
        end_ms = override_end_ms if override_end_ms is not None else (now_ms if now_ms is not None else utc_now_ms())
        start_ms, horizon_ms, cursor_s, max_open_s = _resolve_ingest_start_ms(
            conn,
            symbol,
            interval,
            now_ms=end_ms,
            backfill_days=backfill_days,
            use_watermark=use_watermark,
        )

        if override_start_ms is not None:
            start_ms = override_start_ms
        elif force_fetch_window_intervals > 0 and start_ms >= end_ms:
            # Force a non-empty fetch window for timing comparisons.
            # This helps when the real ingest job would be a no-op due to cursor catching up.
            iv_ms = interval_to_millis(interval)
            forced_start = end_ms - int(force_fetch_window_intervals) * iv_ms
            start_ms = floor_align_ms_to_interval(forced_start, interval)

        stats = IngestStats(
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            horizon_ms=horizon_ms,
            backfill_days=backfill_days,
            use_watermark=use_watermark,
            cursor_read_s=cursor_s,
            max_open_time_s=max_open_s,
        )

        if start_ms >= end_ms:
            logger.info(
                "No-op ingest: start_ms >= end_ms (start_ms={}, end_ms={}); set --force-fetch-window-intervals to time REST+validation",
                start_ms,
                end_ms,
            )
            return stats

        iv_ms = interval_to_millis(interval)

        limiter = ProviderRateLimiter(MARKET_DATA_MIN_REQUEST_INTERVAL_SEC)
        session = requests.Session()

        cur = start_ms
        safety = 0
        while cur < end_ms and safety < 100_000:
            safety += 1

            bars, page_stats = _fetch_ohlcv_page_timed(
                session=session,
                limiter=limiter,
                base_url=settings.binance_rest_url,
                symbol=symbol,
                interval=interval,
                start_time_ms=cur,
                end_time_ms=end_ms,
                limit=chunk_limit,
                timeout_s=30,
                fetch_max_attempts=OHLCV_KLINES_FETCH_MAX_ATTEMPTS,
                fetch_retry_base_sleep_sec=OHLCV_KLINES_FETCH_RETRY_BASE_SLEEP_SEC,
            )

            # Even for empty pages, record attempts for comparison.
            if not bars:
                stats.pages.append(page_stats)
                break

            filtered_bars, filtered_out, filter_s = _filter_bars_not_after_end(bars, end_ms)
            stats.filter_s_total += filter_s
            page_stats.filtered_out = filtered_out
            page_stats.bars_after_filter = len(filtered_bars)

            if not filtered_bars:
                stats.pages.append(page_stats)
                break

            if write:
                t_up0 = time.perf_counter()
                upserted = upsert_ohlcv_bars(conn, filtered_bars)
                t_up1 = time.perf_counter()
                page_stats.upsert_s = t_up1 - t_up0

                t_open0 = time.perf_counter()
                max_ot = max(b.open_time for b in filtered_bars)
                t_open1 = time.perf_counter()
                page_stats.open_time_step_s = t_open1 - t_open0

                t_cur0 = time.perf_counter()
                upsert_ingestion_cursor(conn, symbol, interval, max_ot)
                t_cur1 = time.perf_counter()
                page_stats.cursor_upsert_s = t_cur1 - t_cur0

                t_commit0 = time.perf_counter()
                conn.commit()
                t_commit1 = time.perf_counter()
                page_stats.commit_s = t_commit1 - t_commit0

                stats.bars_upserted += upserted
            else:
                stats.bars_upserted += len(filtered_bars)

            stats.pages.append(page_stats)

            last = filtered_bars[-1]
            cur = open_time_plus_interval_ms(last.open_time, iv_ms)

            if max_pages is not None and len(stats.pages) >= max_pages:
                break

        return stats
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Time OHLCV ingest sub-steps (single series).")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="OHLCV symbol (e.g. BTCUSDT).")
    parser.add_argument("--all-symbols", action="store_true", help="Run timing for all configured OHLCV symbols.")
    parser.add_argument("--interval", type=str, default="1h", help="OHLCV interval (e.g. 1m, 1h).")
    parser.add_argument("--now-ms", type=int, default=None, help="Override 'now' (ms since epoch UTC).")
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=OHLCV_INITIAL_BACKFILL_DAYS,
        help="Used when watermark/cursors are missing (horizon start).",
    )
    parser.add_argument("--chunk-limit", type=int, default=OHLCV_KLINES_CHUNK_LIMIT, help="Binance kline request limit.")
    parser.add_argument("--no-watermark", action="store_true", help="Start from horizon_ms instead of watermark.")
    parser.add_argument("--max-pages", type=int, default=None, help="Stop after N REST pages.")
    parser.add_argument("--write", action="store_true", help="Upsert + commit to Postgres (default: off).")
    parser.add_argument(
        "--output-csv",
        type=str,
        default=None,
        help="Optional CSV path for per-symbol timing rows.",
    )
    parser.add_argument("--override-start-ms", type=int, default=None, help="Force start_ms (ms since epoch UTC) regardless of cursor.")
    parser.add_argument("--override-end-ms", type=int, default=None, help="Force end_ms (ms since epoch UTC) regardless of now.")
    parser.add_argument(
        "--force-fetch-window-intervals",
        type=int,
        default=0,
        help="If computed start_ms >= end_ms, force timing by fetching a non-empty window of N intervals (e.g. 1 for 1h).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel workers across symbols (default: 1, sequential).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Repeat the full symbol set N times and report average total wall-clock.",
    )
    args = parser.parse_args()

    settings = load_settings()
    symbols = list(settings.symbols) if args.all_symbols else [args.symbol]
    workers = max(1, int(args.workers))
    runs = max(1, int(args.runs))

    def _run_one_symbol(sym: str) -> dict[str, Any]:
        t0 = time.perf_counter()
        stats = run_time_ohlcv_ingest_once(
            symbol=sym,
            interval=args.interval,
            now_ms=args.now_ms,
            backfill_days=args.backfill_days,
            chunk_limit=args.chunk_limit,
            use_watermark=not args.no_watermark,
            max_pages=args.max_pages,
            write=args.write,
            force_fetch_window_intervals=args.force_fetch_window_intervals,
            override_start_ms=args.override_start_ms,
            override_end_ms=args.override_end_ms,
        )
        t1 = time.perf_counter()

        total_fetch_s = sum(p.fetch_s_total for p in stats.pages)
        total_http_get_s = sum(a.http_get_s for p in stats.pages for a in p.attempts)
        total_json_decode_s = sum(a.json_decode_s for p in stats.pages for a in p.attempts)
        total_parse_s = sum(a.parse_s for p in stats.pages for a in p.attempts)
        total_validate_s = sum(a.validate_s for p in stats.pages for a in p.attempts)
        total_upsert_s = sum(p.upsert_s for p in stats.pages)
        total_cursor_upsert_s = sum(p.cursor_upsert_s for p in stats.pages)
        total_commit_s = sum(p.commit_s for p in stats.pages)
        total_pages = len(stats.pages)
        total_attempts = sum(len(p.attempts) for p in stats.pages)
        return {
            "symbol": stats.symbol,
            "interval": stats.interval,
            "start_ms": stats.start_ms,
            "end_ms": stats.end_ms,
            "pages": total_pages,
            "attempts": total_attempts,
            "bars_upserted": stats.bars_upserted,
            "cursor_read_s": stats.cursor_read_s,
            "max_open_time_s": stats.max_open_time_s,
            "fetch_total_s": total_fetch_s,
            "http_get_s": total_http_get_s,
            "json_decode_s": total_json_decode_s,
            "parse_s": total_parse_s,
            "validate_s": total_validate_s,
            "filter_s": stats.filter_s_total,
            "upsert_s": total_upsert_s,
            "cursor_upsert_s": total_cursor_upsert_s,
            "commit_s": total_commit_s,
            "wall_clock_s": t1 - t0,
        }

    rows: list[dict[str, Any]] = []
    run_totals_s: list[float] = []
    for run_idx in range(1, runs + 1):
        logger.info(
            "Run {}/{} starting: symbols={} interval={} workers={} write={}",
            run_idx,
            runs,
            len(symbols),
            args.interval,
            workers,
            args.write,
        )
        t_run0 = time.perf_counter()
        run_rows: list[dict[str, Any]] = []
        if workers <= 1:
            for i, sym in enumerate(symbols, start=1):
                logger.info("Running timing for {}/{}: {} {}", i, len(symbols), sym, args.interval)
                row = _run_one_symbol(sym)
                run_rows.append(row)
                logger.info(
                    "done {}: pages={} attempts={} bars={} http={:.3f}s json={:.3f}s parse={:.3f}s validate={:.3f}s wall={:.3f}s",
                    row["symbol"],
                    row["pages"],
                    row["attempts"],
                    row["bars_upserted"],
                    row["http_get_s"],
                    row["json_decode_s"],
                    row["parse_s"],
                    row["validate_s"],
                    row["wall_clock_s"],
                )
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = {ex.submit(_run_one_symbol, sym): sym for sym in symbols}
                done_count = 0
                for fut in as_completed(futures):
                    done_count += 1
                    sym = futures[fut]
                    row = fut.result()
                    run_rows.append(row)
                    logger.info(
                        "done {}/{} {}: pages={} attempts={} bars={} http={:.3f}s parse={:.3f}s validate={:.3f}s wall={:.3f}s",
                        done_count,
                        len(symbols),
                        sym,
                        row["pages"],
                        row["attempts"],
                        row["bars_upserted"],
                        row["http_get_s"],
                        row["parse_s"],
                        row["validate_s"],
                        row["wall_clock_s"],
                    )
            run_rows.sort(key=lambda r: str(r["symbol"]))

        t_run1 = time.perf_counter()
        run_total_s = t_run1 - t_run0
        run_totals_s.append(run_total_s)
        rows.extend(run_rows)
        logger.info(
            "Run {}/{} finished: total_wall_clock={:.3f}s",
            run_idx,
            runs,
            run_total_s,
        )

    if args.output_csv:
        out_path = Path(args.output_csv)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "symbol",
            "interval",
            "start_ms",
            "end_ms",
            "pages",
            "attempts",
            "bars_upserted",
            "cursor_read_s",
            "max_open_time_s",
            "fetch_total_s",
            "http_get_s",
            "json_decode_s",
            "parse_s",
            "validate_s",
            "filter_s",
            "upsert_s",
            "cursor_upsert_s",
            "commit_s",
            "wall_clock_s",
        ]
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        logger.info("Wrote per-symbol timings to {}", out_path)

    if not rows:
        logger.warning("No symbols timed")
        return

    def _summary(label: str, values: list[float]) -> None:
        vals = sorted(values)
        n = len(vals)
        p95_idx = max(0, min(n - 1, int(round(0.95 * (n - 1)))))
        logger.info(
            "{}: n={} min={:.3f}s p50={:.3f}s p95={:.3f}s max={:.3f}s avg={:.3f}s",
            label,
            n,
            vals[0],
            statistics.median(vals),
            vals[p95_idx],
            vals[-1],
            statistics.mean(vals),
        )

    logger.info("=== OHLCV ingest timing aggregate summary ===")
    logger.info(
        "symbols_per_run={} runs={} interval={} workers={} write={}",
        len(symbols),
        runs,
        args.interval,
        workers,
        args.write,
    )
    _summary("run_total_wall_clock_s", run_totals_s)
    logger.info(
        "run_total_wall_clock_s_avg={:.3f}s",
        statistics.mean(run_totals_s),
    )
    _summary("fetch_total_s", [float(r["fetch_total_s"]) for r in rows])
    _summary("http_get_s", [float(r["http_get_s"]) for r in rows])
    _summary("json_decode_s", [float(r["json_decode_s"]) for r in rows])
    _summary("parse_s", [float(r["parse_s"]) for r in rows])
    _summary("validate_s", [float(r["validate_s"]) for r in rows])
    _summary("wall_clock_s", [float(r["wall_clock_s"]) for r in rows])
    _summary("cursor_read_s", [float(r["cursor_read_s"]) for r in rows])
    if args.write:
        _summary("upsert_s", [float(r["upsert_s"]) for r in rows])
        _summary("commit_s", [float(r["commit_s"]) for r in rows])


if __name__ == "__main__":
    main()

