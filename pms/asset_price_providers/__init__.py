"""
Asset price providers for the asset price feed.

Each provider implements AssetPriceProvider (get_prices(assets) -> Dict[asset, USD price]).
Used to update assets.usd_price from external sources (Binance first). See docs/pms/ASSET_PRICE_FEED_PLAN.md.
"""

from .binance import (
    AssetPriceProviderError,
    BinanceAssetPriceProvider,
    BINANCE_SPOT_BASE_URL,
    BINANCE_SPOT_TESTNET_BASE_URL,
)
from .interface import AssetPriceProvider

__all__ = [
    "AssetPriceProvider",
    "AssetPriceProviderError",
    "BinanceAssetPriceProvider",
    "BINANCE_SPOT_BASE_URL",
    "BINANCE_SPOT_TESTNET_BASE_URL",
]
