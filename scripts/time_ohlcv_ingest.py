"""
Time OHLCV ingest sub-steps (cursor -> API -> parsing/validation -> upsert -> commit).

This script mirrors the watermark-based ingest flow in ``market_data/jobs/ingest_ohlcv.py`` for
one ``(symbol, interval)`` and records durations for:
- DB cursor reads (``get_ingestion_cursor``, ``max_open_time_ohlcv``)
- REST call (HTTP GET duration)
- JSON decode duration (``resp.json()``)
- Validation/parsing (``process_binance_klines_payload``; includes pydantic + rule checks)
- Filter step (drop bars with ``open_time`` after ``end_ms``)
- DB writes: ``upsert_ohlcv_bars``, ``upsert_ingestion_cursor``, and ``conn.commit()``

Run (example):
  python scripts/time_ohlcv_ingest.py --symbol BTCUSDT --interval 1h --max-pages 2 --write
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

import psycopg2
import requests
from loguru import logger

from market_data.config import (
    MARKET_DATA_MIN_REQUEST_INTERVAL_SEC,
    OHLCV_INITIAL_BACKFILL_DAYS,
    OHLCV_KLINES_CHUNK_LIMIT,
    OHLCV_KLINES_FETCH_MAX_ATTEMPTS,
    OHLCV_KLINES_FETCH_RETRY_BASE_SLEEP_SEC,
    load_settings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import open_time_plus_interval_ms, utc_now_ms
from market_data.schemas import OhlcvBar
from market_data.storage import (
    get_ingestion_cursor,
    max_open_time_ohlcv,
    upsert_ohlcv_bars,
    upsert_ingestion_cursor,
)
from market_data.validation import process_binance_klines_payload
from market_data.rate_limit import ProviderRateLimiter


BINANCE_SPOT_KLINES_PATH = "/api/v3/klines"


@dataclass
class AttemptStats:
    attempt: int
    ok: bool
    http_get_s: float = 0.0
    json_decode_s: float = 0.0
    process_s: float = 0.0
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

            t_proc0 = time.perf_counter()
            bars = process_binance_klines_payload(
                raw,
                symbol=symbol,
                interval=interval,
                start_time_ms=int(start_time_ms),
                end_time_ms=int(end_time_ms),
                request_limit=int(limit),
            )
            t_proc1 = time.perf_counter()
            attempt_stats.process_s = t_proc1 - t_proc0

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
) -> IngestStats:
    settings = load_settings()
    conn = psycopg2.connect(settings.database_url)
    try:
        end_ms = now_ms if now_ms is not None else utc_now_ms()
        start_ms, horizon_ms, cursor_s, max_open_s = _resolve_ingest_start_ms(
            conn,
            symbol,
            interval,
            now_ms=end_ms,
            backfill_days=backfill_days,
            use_watermark=use_watermark,
        )

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
            logger.info("No-op ingest: start_ms >= end_ms (start_ms={}, end_ms={})", start_ms, end_ms)
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
    args = parser.parse_args()

    t0 = time.perf_counter()
    stats = run_time_ohlcv_ingest_once(
        symbol=args.symbol,
        interval=args.interval,
        now_ms=args.now_ms,
        backfill_days=args.backfill_days,
        chunk_limit=args.chunk_limit,
        use_watermark=not args.no_watermark,
        max_pages=args.max_pages,
        write=args.write,
    )
    t1 = time.perf_counter()

    total_fetch_s = sum(p.fetch_s_total for p in stats.pages)
    total_upsert_s = sum(p.upsert_s for p in stats.pages)
    total_cursor_upsert_s = sum(p.cursor_upsert_s for p in stats.pages)
    total_commit_s = sum(p.commit_s for p in stats.pages)

    total_pages = len(stats.pages)
    total_attempts = sum(len(p.attempts) for p in stats.pages)

    logger.info("=== OHLCV ingest timing summary ===")
    logger.info(
        "series: {} {}  start_ms={}  end_ms={}  write={}",
        stats.symbol,
        stats.interval,
        stats.start_ms,
        stats.end_ms,
        args.write,
    )
    logger.info("pages: {} (attempts: {})  bars_upserted: {}", total_pages, total_attempts, stats.bars_upserted)
    logger.info(
        "cursor reads: {:.3f}s  max_open_time: {:.3f}s",
        stats.cursor_read_s,
        stats.max_open_time_s,
    )
    logger.info(
        "fetch+parse+validate: {:.3f}s  filter: {:.3f}s",
        total_fetch_s,
        stats.filter_s_total,
    )
    logger.info(
        "db: upsert={:.3f}s  cursor_upsert={:.3f}s  commit={:.3f}s  wall_clock={:.3f}s",
        total_upsert_s,
        total_cursor_upsert_s,
        total_commit_s,
        t1 - t0,
    )

    # Per-page detail (small output).
    for idx, p in enumerate(stats.pages[:5], start=1):
        last_attempt = p.attempts[-1] if p.attempts else None
        logger.info(
            "page#{} start_ms={} limit={} bars_after_filter={} filtered_out={} fetch_s_total={:.3f} upsert_s={:.3f} commit_s={:.3f} last_ok={}",
            idx,
            p.start_time_ms,
            p.limit,
            p.bars_after_filter,
            p.filtered_out,
            p.fetch_s_total,
            p.upsert_s,
            p.commit_s,
            getattr(last_attempt, "ok", None),
        )


if __name__ == "__main__":
    main()

