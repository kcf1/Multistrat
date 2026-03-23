"""Rolling re-fetch of recent basis points for vendor drift checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping

import psycopg2
from loguru import logger

from market_data.config import (
    BASIS_CONTRACT_TYPES,
    BASIS_CORRECT_WINDOW_POINTS,
    BASIS_FETCH_CHUNK_LIMIT,
    BASIS_PAIRS,
    BASIS_PERIODS,
    MarketDataSettings,
)
from market_data.intervals import interval_to_millis
from market_data.jobs.common import chunk_fetch_basis_forward, utc_now_ms
from market_data.providers.base import BasisProvider
from market_data.providers.binance_perps import build_binance_perps_provider
from market_data.schemas import BasisPoint
from market_data.storage import fetch_basis_rates_by_sample_times, upsert_basis_points


@dataclass(frozen=True)
class CorrectBasisWindowResult:
    pair: str
    contract_type: str
    period: str
    rows_fetched: int
    drift_rows: int


def _log_basis_drifts(
    existing: Mapping[datetime, tuple[Decimal, Decimal, Decimal, Decimal, Decimal]],
    rows: list[BasisPoint],
) -> int:
    n = 0
    for r in rows:
        old = existing.get(r.sample_time)
        if old is None:
            continue
        same = old == (
            r.basis,
            r.basis_rate,
            r.annualized_basis_rate,
            r.futures_price,
            r.index_price,
        )
        if not same:
            n += 1
            logger.warning(
                "market_data basis drift pair={} contract_type={} period={} sample_time={} "
                "db=({}, {}, {}, {}, {}) api=({}, {}, {}, {}, {})",
                r.pair,
                r.contract_type,
                r.period,
                r.sample_time,
                old[0],
                old[1],
                old[2],
                old[3],
                old[4],
                r.basis,
                r.basis_rate,
                r.annualized_basis_rate,
                r.futures_price,
                r.index_price,
            )
    return n


def run_correct_window_basis_series(
    conn,
    provider: BasisProvider,
    pair: str,
    contract_type: str,
    period: str,
    *,
    lookback_points: int | None = None,
    now_ms: int | None = None,
    chunk_limit: int = BASIS_FETCH_CHUNK_LIMIT,
) -> CorrectBasisWindowResult:
    lookback = lookback_points if lookback_points is not None else BASIS_CORRECT_WINDOW_POINTS
    pd_ms = interval_to_millis(period)
    end_ms = now_ms if now_ms is not None else utc_now_ms()
    start_ms = end_ms - lookback * pd_ms
    rows = chunk_fetch_basis_forward(
        provider,
        pair,
        contract_type,
        period,
        start_ms=start_ms,
        end_ms=end_ms,
        chunk_limit=chunk_limit,
    )
    if not rows:
        return CorrectBasisWindowResult(pair, contract_type, period, 0, 0)
    times = [r.sample_time for r in rows]
    prev = fetch_basis_rates_by_sample_times(conn, pair, contract_type, period, times)
    drifts = _log_basis_drifts(dict(prev), rows)
    upsert_basis_points(conn, rows)
    conn.commit()
    return CorrectBasisWindowResult(pair, contract_type, period, len(rows), drifts)


def run_correct_window_basis_rate(
    settings: MarketDataSettings,
    *,
    provider: BasisProvider | None = None,
    lookback_points: int | None = None,
) -> list[CorrectBasisWindowResult]:
    prov = provider if provider is not None else build_binance_perps_provider(settings)
    conn = psycopg2.connect(settings.database_url)
    try:
        out: list[CorrectBasisWindowResult] = []
        for pair in BASIS_PAIRS:
            for contract_type in BASIS_CONTRACT_TYPES:
                for period in BASIS_PERIODS:
                    out.append(
                        run_correct_window_basis_series(
                            conn,
                            prov,
                            pair,
                            contract_type,
                            period,
                            lookback_points=lookback_points,
                        )
                    )
        return out
    finally:
        conn.close()
