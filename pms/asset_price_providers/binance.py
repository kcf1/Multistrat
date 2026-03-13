"""
Binance asset price provider: REST ticker/price, returns USD price per asset.

Uses GET /api/v3/ticker/price; assumes quote is USDT (USD). Reuses BinanceTickerPriceItem
from pms.schemas_pydantic for response parsing. See docs/pms/ASSET_PRICE_FEED_PLAN.md §3.2.
"""

from typing import Dict, List, Optional

import requests

from pms.log import logger
from pms.schemas_pydantic import BinanceTickerPriceItem

from .interface import AssetPriceProvider


class AssetPriceProviderError(Exception):
    """Raised when asset price provider fails (e.g. network, API error)."""
    pass


# Default Binance spot URLs (public; no auth)
BINANCE_SPOT_BASE_URL = "https://api.binance.com"
BINANCE_SPOT_TESTNET_BASE_URL = "https://testnet.binance.vision"


class BinanceAssetPriceProvider(AssetPriceProvider):
    """
    Fetch USD price per asset via Binance spot ticker/price.

    Builds symbol as {asset}{quote_asset} (e.g. BTCUSDT). Fetches all ticker
    prices in one request, then filters to requested assets and strips quote
    to get asset key.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 10.0,
        use_testnet: bool = True,
        quote_asset: str = "USDT",
    ):
        """
        Args:
            base_url: Binance REST base (e.g. https://testnet.binance.vision).
                     If None, uses testnet or production from use_testnet.
            timeout: Request timeout in seconds.
            use_testnet: If True and base_url not set, use testnet URL.
            quote_asset: Quote used to build symbol (default USDT = USD).
        """
        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            self.base_url = (
                BINANCE_SPOT_TESTNET_BASE_URL if use_testnet else BINANCE_SPOT_BASE_URL
            )
        self.timeout = timeout
        self.quote_asset = (quote_asset or "USDT").strip().upper()

    def get_prices(self, assets: List[str]) -> Dict[str, Optional[float]]:
        """
        Fetch last prices from GET /api/v3/ticker/price; return asset -> USD price.

        Symbol for each asset is {asset}{quote_asset}. Response is filtered to
        those symbols; asset key is derived by stripping quote from symbol.
        """
        if not assets:
            return {}

        want_assets = {str(a).strip().upper() for a in assets if a and str(a).strip()}
        if not want_assets:
            return {}

        url = f"{self.base_url}/api/v3/ticker/price"
        try:
            resp = requests.get(url, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning("Binance ticker/price request failed: {}", e)
            raise AssetPriceProviderError(f"Binance ticker request failed: {e}") from e
        except (ValueError, TypeError, KeyError) as e:
            logger.warning("Binance ticker/price parse error: {}", e)
            raise AssetPriceProviderError(f"Binance ticker parse error: {e}") from e

        if isinstance(data, dict):
            items = [data]
        elif isinstance(data, list):
            items = data
        else:
            items = []

        out: Dict[str, Optional[float]] = {}
        q = self.quote_asset
        for raw in items:
            try:
                item = BinanceTickerPriceItem.model_validate(raw)
                sym = item.symbol.strip().upper()
                if not sym.endswith(q):
                    continue
                asset = sym[: -len(q)] if len(sym) > len(q) else ""
                if asset not in want_assets:
                    continue
                try:
                    price_f = float(str(item.price))
                    if price_f >= 0:
                        out[asset] = price_f
                    else:
                        logger.debug("Skip negative price for {}: {}", asset, item.price)
                except (TypeError, ValueError):
                    logger.debug("Skip invalid price for {}: {}", asset, item.price)
            except Exception as e:
                logger.debug("Skip invalid ticker item {}: {}", raw, e)
                continue

        return out
