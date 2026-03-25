"""Provider interfaces for market_data ingestion (Phase 4 §9.3 / §6.0)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.schemas import (
    BasisPoint,
    OhlcvBar,
    OpenInterestPoint,
    TopTraderLongShortPoint,
    TakerBuySellVolumePoint,
)


@runtime_checkable
class KlinesProvider(Protocol):
    """Fetch closed klines / candles for a symbol and interval."""

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> list[OhlcvBar]:
        """
        Return parsed bars (newest last, Binance order).

        ``limit``: max rows per HTTP request (Binance cap 1000 for /api/v3/klines).
        """
        ...


@runtime_checkable
class BasisProvider(Protocol):
    """Fetch basis snapshots for a pair, contract type, and period."""

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
        """
        Return parsed basis points (oldest first, Binance order).

        ``limit``: max rows per HTTP request (Binance cap 500 for /futures/data/basis).
        """
        ...


@runtime_checkable
class OpenInterestProvider(Protocol):
    """Fetch open-interest snapshots for a symbol, contract type, and period."""

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
        """
        Return parsed open-interest points (oldest first, Binance order).

        ``limit``: max rows per HTTP request (Binance cap 500 for /futures/data/openInterestHist).
        """
        ...


@runtime_checkable
class TakerBuySellVolumeProvider(Protocol):
    """Fetch taker buy/sell volume statistics for a symbol and period."""

    def fetch_taker_buy_sell_volume(
        self,
        symbol: str,
        period: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[TakerBuySellVolumePoint]:
        """
        Return parsed taker buy/sell volume points (oldest first, Binance order).

        ``limit``: max rows per HTTP request (Binance cap 500 for /futures/data/takerlongshortRatio).
        """
        ...


@runtime_checkable
class TopTraderLongShortPositionRatioProvider(Protocol):
    """Fetch top trader long/short position ratio positions for a symbol and period."""

    def fetch_top_trader_long_short_position_ratio(
        self,
        symbol: str,
        period: str,
        *,
        start_time_ms: int,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> list[TopTraderLongShortPoint]:
        """
        Return parsed top trader long/short position ratio points (oldest first, Binance order).

        ``limit``: max rows per HTTP request (Binance cap 500 for /futures/data/topLongShortPositionRatio).
        """
        ...
