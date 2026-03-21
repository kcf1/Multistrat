"""
Binance **spot** public klines (GET /api/v3/klines) — no API key.

Uses shared ``ProviderRateLimiter`` per provider instance (§6.0).

Fetches **retry** on HTTP errors or invalid / incomplete payloads (row parse failures,
OHLC invariants, non-monotonic ``open_time``) with exponential backoff. After the last
retry, logs an error, appends to ``fetch_give_ups``, and returns an **empty** list so
callers can continue pagination / other symbols.
"""

from __future__ import annotations

import time
from typing import Any, List, Optional
from urllib.parse import urlencode

import requests
from loguru import logger

from market_data.config import (
    MARKET_DATA_MIN_REQUEST_INTERVAL_SEC,
    OHLCV_KLINES_FETCH_MAX_ATTEMPTS,
    OHLCV_KLINES_FETCH_RETRY_BASE_SLEEP_SEC,
    MarketDataSettings,
)
from market_data.rate_limit import ProviderRateLimiter
from market_data.schemas import OhlcvBar
from market_data.validation import process_binance_klines_payload


class BinanceSpotKlinesProvider:
    """REST klines for Binance spot; one rate limiter shared by all calls on this instance."""

    KLINES_PATH = "/api/v3/klines"

    def __init__(
        self,
        base_url: str,
        *,
        rate_limiter: Optional[ProviderRateLimiter] = None,
        session: Optional[requests.Session] = None,
        timeout: int = 30,
        fetch_max_attempts: int | None = None,
        fetch_retry_base_sleep_sec: float | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._limiter = rate_limiter if rate_limiter is not None else ProviderRateLimiter()
        self._session = session if session is not None else requests.Session()
        self._timeout = timeout
        self._fetch_max_attempts = (
            fetch_max_attempts
            if fetch_max_attempts is not None
            else OHLCV_KLINES_FETCH_MAX_ATTEMPTS
        )
        self._fetch_retry_base = (
            fetch_retry_base_sleep_sec
            if fetch_retry_base_sleep_sec is not None
            else OHLCV_KLINES_FETCH_RETRY_BASE_SLEEP_SEC
        )
        self.fetch_give_ups: list[str] = []

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list[OhlcvBar]:
        if limit < 1 or limit > 1000:
            raise ValueError("limit must be between 1 and 1000 for Binance spot klines")

        params: dict[str, Any] = {
            "symbol": symbol.strip().upper(),
            "interval": interval.strip(),
            "startTime": int(start_time_ms),
            "limit": int(limit),
        }
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)

        url = f"{self._base}{self.KLINES_PATH}?{urlencode(params)}"
        sym = params["symbol"]
        iv = params["interval"]

        last_exc: Exception | None = None
        for attempt in range(1, self._fetch_max_attempts + 1):
            try:
                self._limiter.acquire()
                resp = self._session.get(url, timeout=self._timeout)
                resp.raise_for_status()
                raw: List[Any] = resp.json()
                return process_binance_klines_payload(
                    raw,
                    symbol=sym,
                    interval=iv,
                    start_time_ms=int(start_time_ms),
                    end_time_ms=int(end_time_ms) if end_time_ms is not None else None,
                    request_limit=int(limit),
                )
            except Exception as e:
                last_exc = e
                if attempt >= self._fetch_max_attempts:
                    break
                sleep_s = self._fetch_retry_base * (2 ** (attempt - 1))
                logger.warning(
                    "Binance klines {} {} failed (attempt {}/{}): {}; retry in {:.2f}s",
                    sym,
                    iv,
                    attempt,
                    self._fetch_max_attempts,
                    e,
                    sleep_s,
                )
                time.sleep(sleep_s)

        assert last_exc is not None
        detail = f"{sym} {iv}: {last_exc!s}"
        self.fetch_give_ups.append(detail)
        logger.error(
            "Binance klines giving up after {} attempts — {}; returning empty page (ingest proceeds)",
            self._fetch_max_attempts,
            detail,
        )
        return []


def build_binance_spot_provider(settings: MarketDataSettings) -> BinanceSpotKlinesProvider:
    """Factory using service REST URL and micro throttle from ``market_data.config``."""
    limiter = ProviderRateLimiter(MARKET_DATA_MIN_REQUEST_INTERVAL_SEC)
    return BinanceSpotKlinesProvider(
        settings.binance_rest_url,
        rate_limiter=limiter,
    )
