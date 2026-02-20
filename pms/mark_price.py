"""
Mark price provider: interface and implementations (Binance for Phase 2).

Task 12.3.1a: Define interface (e.g. get_mark_prices per symbol); Phase 2 implement
by wrapping Binance REST. Switch via PMS_MARK_PRICE_SOURCE. See docs/pms/PMS_ARCHITECTURE.md §11.
"""

from decimal import Decimal, InvalidOperation
from typing import List, Optional

import requests

from pms.log import logger
from pms.schemas_pydantic import BinanceTickerPriceItem, MarkPricesResult

# Default Binance spot URLs (public endpoints; no auth)
BINANCE_SPOT_BASE_URL = "https://api.binance.com"
BINANCE_SPOT_TESTNET_BASE_URL = "https://testnet.binance.vision"


class MarkPriceProviderError(Exception):
    """Raised when mark price provider fails (e.g. network, API error)."""
    pass


class MarkPriceProvider:
    """
    Interface for fetching mark (or last) prices per symbol.

    Used by PMS when computing unrealized PnL. Implementations are swappable
    via PMS_MARK_PRICE_SOURCE (e.g. binance now; redis/market_data in Phase 4).
    """

    def get_mark_prices(self, symbols: List[str]) -> MarkPricesResult:
        """
        Return current mark/last prices for the requested symbols.

        Missing or failed symbols are omitted from the result; caller may
        treat missing as no price (e.g. zero unrealized PnL for that symbol).

        Args:
            symbols: List of trading symbols (e.g. ["BTCUSDT", "ETHUSDT"]).

        Returns:
            MarkPricesResult with prices dict (symbol -> Decimal). Symbol case
            is normalized to uppercase.
        """
        raise NotImplementedError


class BinanceMarkPriceProvider(MarkPriceProvider):
    """
    Phase 2: Mark price via Binance REST (spot: ticker/price = last price).

    Public endpoint; no API key required. For futures mark price use
    /fapi/v1/premiumIndex in a future implementation.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: float = 10.0,
        use_testnet: bool = True,
    ):
        """
        Args:
            base_url: Binance REST base (e.g. https://testnet.binance.vision).
                      If None, uses testnet or production based on use_testnet.
            timeout: Request timeout in seconds.
            use_testnet: If True and base_url not set, use testnet URL.
        """
        if base_url:
            self.base_url = base_url.rstrip("/")
        else:
            self.base_url = (
                BINANCE_SPOT_TESTNET_BASE_URL if use_testnet else BINANCE_SPOT_BASE_URL
            )
        self.timeout = timeout

    def get_mark_prices(self, symbols: List[str]) -> MarkPricesResult:
        """
        Fetch last prices from GET /api/v3/ticker/price.
        No symbol param = all symbols (one request); then filter to requested.
        """
        if not symbols:
            return MarkPricesResult(prices={})

        want = {s.strip().upper() for s in symbols if s and str(s).strip()}
        if not want:
            return MarkPricesResult(prices={})

        url = f"{self.base_url}/api/v3/ticker/price"
        try:
            resp = requests.get(url, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            logger.warning("Binance ticker/price request failed: {}", e)
            raise MarkPriceProviderError(f"Binance ticker request failed: {e}") from e
        except (ValueError, KeyError) as e:
            logger.warning("Binance ticker/price parse error: {}", e)
            raise MarkPriceProviderError(f"Binance ticker parse error: {e}") from e

        # Response: single dict if ?symbol=X, else list of {symbol, price}
        if isinstance(data, dict):
            items = [data]
        elif isinstance(data, list):
            items = data
        else:
            items = []

        prices = {}
        for raw in items:
            try:
                item = BinanceTickerPriceItem.model_validate(raw)
                sym = item.symbol.strip().upper()
                if sym not in want:
                    continue
                # Coerce to Decimal for MarkPricesResult; skip invalid numbers
                try:
                    prices[sym] = Decimal(str(item.price))
                except (InvalidOperation, TypeError, ValueError):
                    logger.debug("Skip ticker item with invalid price {}: {}", sym, item.price)
                    continue
            except Exception as e:
                logger.debug("Skip invalid ticker item {}: {}", raw, e)
                continue

        return MarkPricesResult(prices=prices)


class FakeMarkPriceProvider(MarkPriceProvider):
    """
    Test double: returns fixed or empty prices. For unit tests.
    """

    def __init__(self, prices: Optional[dict] = None):
        """
        Args:
            prices: Optional dict symbol -> price (float/str/Decimal). If None, returns empty.
        """
        from decimal import Decimal
        self._prices = {}
        if prices:
            for k, v in prices.items():
                self._prices[str(k).strip().upper()] = Decimal(str(v))

    def get_mark_prices(self, symbols: List[str]) -> MarkPricesResult:
        want = {s.strip().upper() for s in symbols if s and str(s).strip()}
        out = {k: v for k, v in self._prices.items() if k in want}
        return MarkPricesResult(prices=out)


def get_mark_price_provider(
    source: str,
    *,
    binance_base_url: Optional[str] = None,
    binance_use_testnet: bool = True,
    timeout: float = 10.0,
) -> MarkPriceProvider:
    """
    Factory: return MarkPriceProvider for the given source.

    Args:
        source: From PMS_MARK_PRICE_SOURCE. Supported: 'binance'. Phase 4: 'redis', 'market_data'.
        binance_base_url: Override for Binance REST base URL (when source=binance).
        binance_use_testnet: Use testnet when binance_base_url not set.
        timeout: Request timeout for HTTP (Binance).

    Returns:
        MarkPriceProvider implementation.

    Raises:
        ValueError: If source is unsupported.
    """
    src = (source or "").strip().lower()
    if src == "binance":
        return BinanceMarkPriceProvider(
            base_url=binance_base_url,
            timeout=timeout,
            use_testnet=binance_use_testnet,
        )
    if src in ("", "none", "fake"):
        return FakeMarkPriceProvider(prices={})
    raise ValueError(f"Unsupported PMS_MARK_PRICE_SOURCE: {source!r}. Use binance, or leave unset.")
