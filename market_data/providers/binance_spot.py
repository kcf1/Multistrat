"""
Binance **spot** public klines (GET /api/v3/klines) — no API key.

Uses shared ``ProviderRateLimiter`` per provider instance (§6.0).
"""

from __future__ import annotations

from typing import Any, List, Optional
from urllib.parse import urlencode

import requests

from market_data.config import MARKET_DATA_MIN_REQUEST_INTERVAL_SEC, MarketDataSettings
from market_data.rate_limit import ProviderRateLimiter
from market_data.schemas import OhlcvBar, parse_binance_klines


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
    ) -> None:
        self._base = base_url.rstrip("/")
        self._limiter = rate_limiter if rate_limiter is not None else ProviderRateLimiter()
        self._session = session if session is not None else requests.Session()
        self._timeout = timeout

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
        self._limiter.acquire()
        resp = self._session.get(url, timeout=self._timeout)
        resp.raise_for_status()
        raw: List[list[Any]] = resp.json()
        if not isinstance(raw, list):
            raise ValueError("Binance klines response must be a JSON array")
        return parse_binance_klines(raw, symbol=params["symbol"], interval=params["interval"])


def build_binance_spot_provider(settings: MarketDataSettings) -> BinanceSpotKlinesProvider:
    """Factory using service REST URL and micro throttle from ``market_data.config``."""
    limiter = ProviderRateLimiter(MARKET_DATA_MIN_REQUEST_INTERVAL_SEC)
    return BinanceSpotKlinesProvider(
        settings.binance_rest_url,
        rate_limiter=limiter,
    )
