"""
Asset price provider registry: get provider by name from config.

See docs/pms/ASSET_PRICE_FEED_PLAN.md §3.4 step 10.
"""

from typing import Any, Optional

from .binance import BinanceAssetPriceProvider
from .interface import AssetPriceProvider


def get_asset_price_provider(
    name: str,
    *,
    base_url: Optional[str] = None,
    timeout: float = 10.0,
    use_testnet: bool = True,
    quote_asset: str = "USDT",
    **kwargs: Any,
) -> Optional[AssetPriceProvider]:
    """
    Return AssetPriceProvider for the given source name, or None if disabled/unknown.

    Args:
        name: From pms_asset_price_source (e.g. 'binance' or '' to disable).
        base_url: Binance REST base URL (when name=binance); overrides use_testnet if set.
        timeout: Request timeout in seconds.
        use_testnet: Use testnet when base_url not set (binance only).
        quote_asset: Quote for symbol building (default USDT).
        **kwargs: Ignored; for future provider options.

    Returns:
        Provider instance, or None if name is empty/unknown (feed disabled).
    """
    src = (name or "").strip().lower()
    if src == "binance":
        return BinanceAssetPriceProvider(
            base_url=base_url,
            timeout=timeout,
            use_testnet=use_testnet,
            quote_asset=quote_asset,
        )
    return None
