"""Rolling re-fetch of recent open-interest points for vendor drift checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping

import psycopg2
from loguru import logger

from market_data.config import (
    OPEN_INTEREST_CONTRACT_TYPES,
    OPEN_INTEREST_CORRECT_WINDOW_POINTS,
    OPEN_INTEREST_FETCH_CHUNK_LIMIT,
    OPEN_INTEREST_PERIODS,
    OPEN_INTEREST_SYMBOLS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import chunk_fetch_open_interest_forward, utc_now_ms
from market_data.providers.base import OpenInterestProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.schemas import OpenInterestPoint
from market_data.storage import fetch_open_interest_by_sample_times, upsert_open_interest_points


@dataclass(frozen=True)
class CorrectOpenInterestWindowResult:
    symbol: str
    contract_type: str
    period: str
    rows_fetched: int
    drift_rows: int


def _log_open_interest_drifts(
    existing: Mapping[datetime, tuple[Decimal, Decimal, Decimal | None]],
    rows: list[OpenInterestPoint],
) -> int:
    n = 0
    for r in rows:
        old = existing.get(r.sample_time)
        if old is None:
            continue
        same = old == (
            r.sum_open_interest,
            r.sum_open_interest_value,
            r.cmc_circulating_supply,
        )
        if not same:
            n += 1
            logger.debug(
                "market_data open_interest drift symbol={} contract_type={} period={} sample_time={} "
                "db=({}, {}, {}) api=({}, {}, {})",
                r.symbol,
                r.contract_type,
                r.period,
                r.sample_time,
                old[0],
                old[1],
                old[2],
                r.sum_open_interest,
                r.sum_open_interest_value,
                r.cmc_circulating_supply,
            )
    return n


def run_correct_window_open_interest_series(
    conn,
    provider: OpenInterestProvider,
    symbol: str,
    contract_type: str,
    period: str,
    *,
    lookback_points: int | None = None,
    now_ms: int | None = None,
    chunk_limit: int = OPEN_INTEREST_FETCH_CHUNK_LIMIT,
) -> CorrectOpenInterestWindowResult:
    lookback = (
        lookback_points if lookback_points is not None else OPEN_INTEREST_CORRECT_WINDOW_POINTS
    )
    pd_ms = interval_to_millis(period)
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    start_ms = end_ms - lookback * pd_ms
    rows = chunk_fetch_open_interest_forward(
        provider,
        symbol,
        contract_type,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    )
    if not rows:
        return CorrectOpenInterestWindowResult(symbol, contract_type, period, 0, 0)
    times = [r.sample_time for r in rows]
    prev = fetch_open_interest_by_sample_times(conn, symbol, contract_type, period, times)
    drifts = _log_open_interest_drifts(dict(prev), rows)
    upsert_open_interest_points(conn, rows)
    conn.commit()
    return CorrectOpenInterestWindowResult(symbol, contract_type, period, len(rows), drifts)


def run_correct_window_open_interest(
    settings: MarketDataSettings,
    *,
    provider: OpenInterestProvider | None = None,
    lookback_points: int | None = None,
) -> list[CorrectOpenInterestWindowResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[CorrectOpenInterestWindowResult] = []
        for symbol in OPEN_INTEREST_SYMBOLS:
            for contract_type in OPEN_INTEREST_CONTRACT_TYPES:
                for period in OPEN_INTEREST_PERIODS:
                    out.append(
                        run_correct_window_open_interest_series(
                            conn,
                            prov,
                            symbol,
                            contract_type,
                            period,
                            lookback_points=lookback_points,
                        )
                    )
        return out
    finally:
        conn.close()
