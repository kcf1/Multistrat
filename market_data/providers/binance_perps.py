"""
Binance futures market data provider for basis/open-interest endpoints.

Uses shared ``ProviderRateLimiter`` per provider instance. Retries transient failures
with exponential backoff. HTTP 400 is treated as non-retryable and returns [].
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
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
    TAKER_BUYSELL_VOLUME_FETCH_MAX_ATTEMPTS,
    TAKER_BUYSELL_VOLUME_FETCH_RETRY_BASE_SLEEP_SEC,
    TOP_TRADER_LONG_SHORT_FETCH_MAX_ATTEMPTS,
    TOP_TRADER_LONG_SHORT_FETCH_RETRY_BASE_SLEEP_SEC,
    MarketDataSettings,
)
from market_data.intervals import floor_align_ms_to_interval, interval_to_millis
from market_data.rate_limit import ProviderRateLimiter
from market_data.schemas import (
    BasisPoint,
    OpenInterestPoint,
    TakerBuySellVolumePoint,
    TopTraderLongShortPoint,
)
from market_data.validation import (
    process_binance_basis_payload,
    process_binance_open_interest_payload,
    process_binance_taker_buy_sell_volume_payload,
    process_binance_top_trader_long_short_position_ratio_payload,
)


def _utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


class BinancePerpsMarketDataProvider:
    """REST basis data for Binance futures; one limiter shared by this instance."""

    BASIS_PATH = "/futures/data/basis"
    OPEN_INTEREST_PATH = "/futures/data/openInterestHist"
    TAKER_BUYSELL_VOLUME_PATH = "/futures/data/takerlongshortRatio"
    TOP_TRADER_LONG_SHORT_POSITION_RATIO_PATH = "/futures/data/topLongShortPositionRatio"

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
        taker_fetch_max_attempts: int | None = None,
        taker_fetch_retry_base_sleep_sec: float | None = None,
        top_trader_fetch_max_attempts: int | None = None,
        top_trader_fetch_retry_base_sleep_sec: float | None = None,
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
        self._taker_fetch_max_attempts = (
            taker_fetch_max_attempts
            if taker_fetch_max_attempts is not None
            else TAKER_BUYSELL_VOLUME_FETCH_MAX_ATTEMPTS
        )
        self._taker_fetch_retry_base = (
            taker_fetch_retry_base_sleep_sec
            if taker_fetch_retry_base_sleep_sec is not None
            else TAKER_BUYSELL_VOLUME_FETCH_RETRY_BASE_SLEEP_SEC
        )
        self._top_trader_fetch_max_attempts = (
            top_trader_fetch_max_attempts
            if top_trader_fetch_max_attempts is not None
            else TOP_TRADER_LONG_SHORT_FETCH_MAX_ATTEMPTS
        )
        self._top_trader_fetch_retry_base = (
            top_trader_fetch_retry_base_sleep_sec
            if top_trader_fetch_retry_base_sleep_sec is not None
            else TOP_TRADER_LONG_SHORT_FETCH_RETRY_BASE_SLEEP_SEC
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

        pd = period.strip()
        pd_ms = interval_to_millis(pd)
        # Binance /futures/data/basis treats endTime as mandatory; missing or off-grid
        # timestamps can yield -1102 ("not sent ... or malformed").
        end_raw = int(end_time_ms) if end_time_ms is not None else _utc_now_ms()
        start_eff = floor_align_ms_to_interval(int(start_time_ms), pd)
        end_eff = floor_align_ms_to_interval(end_raw, pd)
        if end_eff <= start_eff:
            end_eff = start_eff + pd_ms

        params: dict[str, Any] = {
            "pair": pair.strip().upper(),
            "contractType": contract_type.strip().upper(),
            "period": pd,
            "startTime": int(start_eff),
            "endTime": int(end_eff),
            "limit": int(limit),
        }

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

    def fetch_taker_buy_sell_volume(
        self,
        symbol: str,
        period: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[TakerBuySellVolumePoint]:
        if limit < 1 or limit > 500:
            raise ValueError("limit must be between 1 and 500 for Binance taker buy/sell volume")

        pd = period.strip()
        pd_ms = interval_to_millis(pd)

        start_eff = floor_align_ms_to_interval(int(start_time_ms), pd)
        end_eff: int | None
        if end_time_ms is not None:
            end_eff = floor_align_ms_to_interval(int(end_time_ms), pd)
            if end_eff <= start_eff:
                end_eff = start_eff + pd_ms
        else:
            end_eff = None

        params: dict[str, Any] = {
            "symbol": symbol.strip().upper(),
            "period": pd,
            "startTime": int(start_eff),
            "limit": int(limit),
        }
        if end_eff is not None:
            params["endTime"] = int(end_eff)

        url = f"{self._base}{self.TAKER_BUYSELL_VOLUME_PATH}?{urlencode(params)}"
        sym = params["symbol"]
        pd = params["period"]

        last_exc: Exception | None = None
        for attempt in range(1, self._taker_fetch_max_attempts + 1):
            try:
                self._limiter.acquire()
                resp = self._session.get(url, timeout=self._timeout)
                if resp.status_code == 400:
                    body = (resp.text or "").strip()
                    snippet = body[:400] + ("…" if len(body) > 400 else "")
                    detail = f"{sym} {pd}: HTTP 400 {snippet or '(no body)'}"
                    self.fetch_give_ups.append(detail)
                    logger.warning(
                        "Binance taker buy/sell volume {} {} HTTP 400 — not retrying; {}",
                        sym,
                        pd,
                        snippet or "(no body)",
                    )
                    return []
                resp.raise_for_status()
                raw = resp.json()
                return process_binance_taker_buy_sell_volume_payload(
                    raw,
                    symbol=sym,
                    period=pd,
                )
            except Exception as e:
                last_exc = e
                if attempt >= self._taker_fetch_max_attempts:
                    break
                sleep_s = self._taker_fetch_retry_base * (2 ** (attempt - 1))
                logger.warning(
                    "Binance taker buy/sell volume {} {} failed (attempt {}/{}): {}; retry in {:.2f}s",
                    sym,
                    pd,
                    attempt,
                    self._taker_fetch_max_attempts,
                    e,
                    sleep_s,
                )
                time.sleep(sleep_s)

        assert last_exc is not None
        detail = f"{sym} {pd}: {last_exc!s}"
        self.fetch_give_ups.append(detail)
        logger.error(
            "Binance taker buy/sell volume giving up after {} attempts — {}; returning empty page (ingest proceeds)",
            self._taker_fetch_max_attempts,
            detail,
        )
        return []

    def fetch_top_trader_long_short_position_ratio(
        self,
        symbol: str,
        period: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[TopTraderLongShortPoint]:
        if limit < 1 or limit > 500:
            raise ValueError(
                "limit must be between 1 and 500 for Binance top trader long/short position ratio"
            )

        pd = period.strip()
        pd_ms = interval_to_millis(pd)

        start_eff = floor_align_ms_to_interval(int(start_time_ms), pd)
        end_eff: int | None
        if end_time_ms is not None:
            end_eff = floor_align_ms_to_interval(int(end_time_ms), pd)
            if end_eff <= start_eff:
                end_eff = start_eff + pd_ms
        else:
            end_eff = None

        params: dict[str, Any] = {
            "symbol": symbol.strip().upper(),
            "period": pd,
            "startTime": int(start_eff),
            "limit": int(limit),
        }
        if end_eff is not None:
            params["endTime"] = int(end_eff)

        url = (
            f"{self._base}{self.TOP_TRADER_LONG_SHORT_POSITION_RATIO_PATH}?{urlencode(params)}"
        )
        sym = params["symbol"]
        pd = params["period"]

        last_exc: Exception | None = None
        for attempt in range(1, self._top_trader_fetch_max_attempts + 1):
            try:
                self._limiter.acquire()
                resp = self._session.get(url, timeout=self._timeout)
                if resp.status_code == 400:
                    body = (resp.text or "").strip()
                    snippet = body[:400] + ("…" if len(body) > 400 else "")
                    detail = f"{sym} {pd}: HTTP 400 {snippet or '(no body)'}"
                    self.fetch_give_ups.append(detail)
                    logger.warning(
                        "Binance top trader long/short ratio {} {} HTTP 400 — not retrying; {}",
                        sym,
                        pd,
                        snippet or "(no body)",
                    )
                    return []
                resp.raise_for_status()
                raw = resp.json()
                return process_binance_top_trader_long_short_position_ratio_payload(
                    raw,
                    symbol=sym,
                    period=pd,
                )
            except Exception as e:
                last_exc = e
                if attempt >= self._top_trader_fetch_max_attempts:
                    break
                sleep_s = self._top_trader_fetch_retry_base * (2 ** (attempt - 1))
                logger.warning(
                    "Binance top trader long/short ratio {} {} failed (attempt {}/{}): {}; retry in {:.2f}s",
                    sym,
                    pd,
                    attempt,
                    self._top_trader_fetch_max_attempts,
                    e,
                    sleep_s,
                )
                time.sleep(sleep_s)

        assert last_exc is not None
        detail = f"{sym} {pd}: {last_exc!s}"
        self.fetch_give_ups.append(detail)
        logger.error(
            "Binance top trader long/short ratio giving up after {} attempts — {}; returning empty page (ingest proceeds)",
            self._top_trader_fetch_max_attempts,
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
