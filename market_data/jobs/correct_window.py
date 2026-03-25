"""
Rolling re-fetch of recent bars for vendor drift (Phase 4 §9.5 / §4.5.2).

Re-downloads the last ``OHLCV_CORRECT_WINDOW_BARS`` (or override) per series, logs when
stored OHLC differs from the API response, then upserts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping

import psycopg2
from loguru import logger

from market_data.config import (
    OHLCV_CORRECT_WINDOW_BARS,
    OHLCV_KLINES_CHUNK_LIMIT,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import chunk_fetch_forward, utc_now_ms
from market_data.providers.base import KlinesProvider
from market_data.providers.binance_spot import build_binance_spot_provider
from market_data.schemas import OhlcvBar
from market_data.storage import fetch_ohlc_by_open_times, upsert_ohlcv_bars


@dataclass(frozen=True)
class CorrectWindowResult:
    symbol: str
    interval: str
    bars_fetched: int
    drift_rows: int


def _log_drifts(
    existing: Mapping[datetime, tuple[Decimal, Decimal, Decimal, Decimal]],
    bars: list[OhlcvBar],
) -> int:
    n = 0
    for b in bars:
        old = existing.get(b.open_time)
        if old is None:
            continue
        o, h, l, c = old
        same = (
            o == b.open
            and h == b.high
            and l == b.low
            and c == b.close
        )
        if not same:
            n += 1
            logger.debug(
                "market_data OHLC drift symbol={} interval={} open_time={} "
                "db=({}, {}, {}, {}) api=({}, {}, {}, {})",
                b.symbol,
                b.interval,
                b.open_time,
                o,
                h,
                l,
                c,
                b.open,
                b.high,
                b.low,
                b.close,
            )
    return n


def run_correct_window_series(
    conn,
    provider: KlinesProvider,
    symbol: str,
    interval: str,
    *,
    lookback_bars: int | None = None,
    now_ms: int | None = None,
    chunk_limit: int = OHLCV_KLINES_CHUNK_LIMIT,
) -> CorrectWindowResult:
    lookback = lookback_bars if lookback_bars is not None else OHLCV_CORRECT_WINDOW_BARS
    iv_ms = interval_to_millis(interval)
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    start_ms = end_ms - lookback * iv_ms
    bars = chunk_fetch_forward(
        provider,
        symbol,
        interval,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    )
    if not bars:
        return CorrectWindowResult(symbol, interval, 0, 0)
    times = [b.open_time for b in bars]
    prev = fetch_ohlc_by_open_times(conn, symbol, interval, times)
    drifts = _log_drifts(dict(prev), bars)
    upsert_ohlcv_bars(conn, bars)
    conn.commit()
    return CorrectWindowResult(symbol, interval, len(bars), drifts)


def run_correct_window(
    settings: MarketDataSettings,
    *,
    provider: KlinesProvider | None = None,
    lookback_bars: int | None = None,
) -> list[CorrectWindowResult]:
    prov = provider if provider is not None else build_binance_spot_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[CorrectWindowResult] = []
        for sym in settings.symbols:
            for iv in settings.intervals:
                out.append(
                    run_correct_window_series(
                        conn,
                        prov,
                        sym,
                        iv,
                        lookback_bars=lookback_bars,
                    )
                )
        return out
    finally:
        conn.close()
