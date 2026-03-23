"""Provider interfaces for market_data ingestion (Phase 4 §9.3 / §6.0)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.schemas import BasisPoint, OhlcvBar


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
