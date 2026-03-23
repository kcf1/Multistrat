"""
Binance futures market data provider for basis/open-interest endpoints.

Uses shared ``ProviderRateLimiter`` per provider instance. Retries transient failures
with exponential backoff. HTTP 400 is treated as non-retryable and returns [].
"""

from __future__ import annotations

import time
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from loguru import logger

from market_data.config import (
    BASIS_FETCH_MAX_ATTEMPTS,
    BASIS_FETCH_RETRY_BASE_SLEEP_SEC,
    MARKET_DATA_MIN_REQUEST_INTERVAL_SEC,
    OPEN_INTEREST_FETCH_MAX_ATTEMPTS,
    OPEN_INTEREST_FETCH_RETRY_BASE_SLEEP_SEC,
    MarketDataSettings,
)
from market_data.rate_limit import ProviderRateLimiter
from market_data.schemas import BasisPoint, OpenInterestPoint
from market_data.validation import (
    process_binance_basis_payload,
    process_binance_open_interest_payload,
)


class BinancePerpsMarketDataProvider:
    """REST basis data for Binance futures; one limiter shared by this instance."""

    BASIS_PATH = "/futures/data/basis"
    OPEN_INTEREST_PATH = "/futures/data/openInterestHist"

    def __init__(
        self,
        base_url: str,
        *,
        rate_limiter: Optional[ProviderRateLimiter] = None,
        session: Optional[requests.Session] = None,
        timeout: int = 30,
        fetch_max_attempts: int | None = None,
        fetch_retry_base_sleep_sec: float | None = None,
        open_interest_fetch_max_attempts: int | None = None,
        open_interest_fetch_retry_base_sleep_sec: float | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._limiter = rate_limiter if rate_limiter is not None else ProviderRateLimiter()
        self._session = session if session is not None else requests.Session()
        self._timeout = timeout
        self._fetch_max_attempts = (
            fetch_max_attempts
            if fetch_max_attempts is not None
            else BASIS_FETCH_MAX_ATTEMPTS
        )
        self._fetch_retry_base = (
            fetch_retry_base_sleep_sec
            if fetch_retry_base_sleep_sec is not None
            else BASIS_FETCH_RETRY_BASE_SLEEP_SEC
        )
        self._open_interest_fetch_max_attempts = (
            open_interest_fetch_max_attempts
            if open_interest_fetch_max_attempts is not None
            else OPEN_INTEREST_FETCH_MAX_ATTEMPTS
        )
        self._open_interest_fetch_retry_base = (
            open_interest_fetch_retry_base_sleep_sec
            if open_interest_fetch_retry_base_sleep_sec is not None
            else OPEN_INTEREST_FETCH_RETRY_BASE_SLEEP_SEC
        )
        self.fetch_give_ups: list[str] = []

    def fetch_basis(
        self,
        pair: str,
        contract_type: str,
        period: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[BasisPoint]:
        if limit < 1 or limit > 500:
            raise ValueError("limit must be between 1 and 500 for Binance basis")

        params: dict[str, Any] = {
            "pair": pair.strip().upper(),
            "contractType": contract_type.strip().upper(),
            "period": period.strip(),
            "startTime": int(start_time_ms),
            "limit": int(limit),
        }
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)

        url = f"{self._base}{self.BASIS_PATH}?{urlencode(params)}"
        p = params["pair"]
        ct = params["contractType"]
        pd = params["period"]

        last_exc: Exception | None = None
        for attempt in range(1, self._fetch_max_attempts + 1):
            try:
                self._limiter.acquire()
                resp = self._session.get(url, timeout=self._timeout)
                if resp.status_code == 400:
                    body = (resp.text or "").strip()
                    snippet = body[:400] + ("…" if len(body) > 400 else "")
                    detail = f"{p} {ct} {pd}: HTTP 400 {snippet or '(no body)'}"
                    self.fetch_give_ups.append(detail)
                    logger.warning(
                        "Binance basis {} {} {} HTTP 400 — not retrying; {}",
                        p,
                        ct,
                        pd,
                        snippet or "(no body)",
                    )
                    return []
                resp.raise_for_status()
                raw = resp.json()
                return process_binance_basis_payload(
                    raw,
                    pair=p,
                    contract_type=ct,
                    period=pd,
                )
            except Exception as e:
                last_exc = e
                if attempt >= self._fetch_max_attempts:
                    break
                sleep_s = self._fetch_retry_base * (2 ** (attempt - 1))
                logger.warning(
                    "Binance basis {} {} {} failed (attempt {}/{}): {}; retry in {:.2f}s",
                    p,
                    ct,
                    pd,
                    attempt,
                    self._fetch_max_attempts,
                    e,
                    sleep_s,
                )
                time.sleep(sleep_s)

        assert last_exc is not None
        detail = f"{p} {ct} {pd}: {last_exc!s}"
        self.fetch_give_ups.append(detail)
        logger.error(
            "Binance basis giving up after {} attempts — {}; returning empty page (ingest proceeds)",
            self._fetch_max_attempts,
            detail,
        )
        return []

    def fetch_open_interest_hist(
        self,
        symbol: str,
        contract_type: str,
        period: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[OpenInterestPoint]:
        if limit < 1 or limit > 500:
            raise ValueError("limit must be between 1 and 500 for Binance open interest")

        params: dict[str, Any] = {
            "symbol": symbol.strip().upper(),
            "contractType": contract_type.strip().upper(),
            "period": period.strip(),
            "startTime": int(start_time_ms),
            "limit": int(limit),
        }
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)

        url = f"{self._base}{self.OPEN_INTEREST_PATH}?{urlencode(params)}"
        sym = params["symbol"]
        ct = params["contractType"]
        pd = params["period"]

        last_exc: Exception | None = None
        for attempt in range(1, self._open_interest_fetch_max_attempts + 1):
            try:
                self._limiter.acquire()
                resp = self._session.get(url, timeout=self._timeout)
                if resp.status_code == 400:
                    body = (resp.text or "").strip()
                    snippet = body[:400] + ("…" if len(body) > 400 else "")
                    detail = f"{sym} {ct} {pd}: HTTP 400 {snippet or '(no body)'}"
                    self.fetch_give_ups.append(detail)
                    logger.warning(
                        "Binance open interest {} {} {} HTTP 400 — not retrying; {}",
                        sym,
                        ct,
                        pd,
                        snippet or "(no body)",
                    )
                    return []
                resp.raise_for_status()
                raw = resp.json()
                return process_binance_open_interest_payload(
                    raw,
                    symbol=sym,
                    contract_type=ct,
                    period=pd,
                )
            except Exception as e:
                last_exc = e
                if attempt >= self._open_interest_fetch_max_attempts:
                    break
                sleep_s = self._open_interest_fetch_retry_base * (2 ** (attempt - 1))
                logger.warning(
                    "Binance open interest {} {} {} failed (attempt {}/{}): {}; retry in {:.2f}s",
                    sym,
                    ct,
                    pd,
                    attempt,
                    self._open_interest_fetch_max_attempts,
                    e,
                    sleep_s,
                )
                time.sleep(sleep_s)

        assert last_exc is not None
        detail = f"{sym} {ct} {pd}: {last_exc!s}"
        self.fetch_give_ups.append(detail)
        logger.error(
            "Binance open interest giving up after {} attempts — {}; returning empty page (ingest proceeds)",
            self._open_interest_fetch_max_attempts,
            detail,
        )
        return []


def build_binance_perps_provider(settings: MarketDataSettings) -> BinancePerpsMarketDataProvider:
    """Factory for Binance perps market-data provider with shared service throttle."""
    limiter = ProviderRateLimiter(MARKET_DATA_MIN_REQUEST_INTERVAL_SEC)
    return BinancePerpsMarketDataProvider(
        settings.binance_perps_rest_url,
        rate_limiter=limiter,
    )
