"""
Data collection universe: bases we trade / track first; **USDT-quoted** spot symbols only.

Used as the default list for Phase 4 OHLCV/ticker ingestion and as the base list for the
PMS asset USD price feed (``ASSET_PRICE_FEED_ASSETS`` re-exports ``DATA_COLLECTION_BASE_ASSETS``).

Extend by appending to ``DATA_COLLECTION_BASE_ASSETS``; symbols are always ``{BASE}USDT``.
"""

from __future__ import annotations

QUOTE_ASSET: str = "USDT"

# Thirty bases — keep in sync with trading intent; pairs are DATA_COLLECTION_SYMBOLS.
DATA_COLLECTION_BASE_ASSETS: tuple[str, ...] = (
    "BTC",
    "ETH",
    "BNB",
    "SOL",
    "XRP",
    "DOGE",
    "ADA",
    "AVAX",
    "TRX",
    "DOT",
    "LINK",
    "MATIC",
    "SHIB",
    "LTC",
    "BCH",
    "UNI",
    "ATOM",
    "XLM",
    "XMR",
    "ETC",
    "FIL",
    "APT",
    "HBAR",
    "VET",
    "OP",
    "ARB",
    "INJ",
    "IMX",
    "SAND",
    "MANA",
)


def usdt_spot_symbol(base: str) -> str:
    """Binance spot style: BASE + USDT (e.g. BTC -> BTCUSDT)."""
    return f"{base.strip().upper()}{QUOTE_ASSET}"


DATA_COLLECTION_SYMBOLS: tuple[str, ...] = tuple(
    usdt_spot_symbol(b) for b in DATA_COLLECTION_BASE_ASSETS
)
