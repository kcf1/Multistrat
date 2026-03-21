"""Provider interface for OHLCV ingestion (Phase 4 §9.3 / §6.0)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from market_data.schemas import OhlcvBar


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
