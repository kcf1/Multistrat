"""
Asset price provider interface for the asset price feed.

Implementations return USD price per asset. Used by the feed to update assets.usd_price.
See docs/pms/ASSET_PRICE_FEED_PLAN.md §3.2, §4.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class AssetPriceProvider(ABC):
    """
    Interface for fetching USD price per asset.

    Used by the asset price feed to update assets.usd_price. Implementations
    are source-agnostic (Binance, CoinGecko, etc.).
    """

    @abstractmethod
    def get_prices(self, assets: List[str]) -> Dict[str, Optional[float]]:
        """
        Return current USD price for each requested asset.

        Missing or failed assets may be omitted or have value None; caller
        filters out None before writing to DB.

        Args:
            assets: List of asset codes (e.g. ["BTC", "ETH", "USDT"]).

        Returns:
            Dict mapping asset -> USD price (float) or None. Asset key
            should match requested (e.g. uppercase normalized).
        """
        ...
