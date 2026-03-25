"""
Data collection universe: bases we trade / track first; **USDT-quoted** spot symbols only.

Used as the default list for Phase 4 OHLCV/ticker ingestion and as the base list for the
PMS asset USD price feed (``ASSET_PRICE_FEED_ASSETS`` re-exports ``DATA_COLLECTION_BASE_ASSETS``).

Extend by appending to ``DATA_COLLECTION_BASE_ASSETS``; symbols are always ``{BASE}USDT``.

Many appended names are **liquidity-screen probes**: Binance may not list every ``BASEUSDT``;
ingest/backfill will show which pairs exist. Screen ticker **RENDER** is **RNDR** here (Binance
spot). **M** (ambiguous) omitted.

**Do not drop** bases whose spot history **ends early** (delist, migration, symbol change): keeping
them in this tuple preserves a **fuller cross-section** and avoids **survivorship bias** in research
and completeness reporting; only remove tickers that are **invalid** on the venue (e.g. wrong symbol).
"""

from __future__ import annotations

QUOTE_ASSET: str = "USDT"

# Original 30 + expanded top-cap probe list. PMS price feed uses the same tuple.
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
    "LTC",
    "BCH",
    "UNI",
    "ATOM",
    "XLM",
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
    # --- probes: listed on Binance spot USDT (invalid tickers removed) ---
    "AAVE",
    "ALGO",
    "ASTER",
    "ENA",
    "FET",
    "ICP",
    "JST",
    "JUP",
    "MORPHO",
    "NEAR",
    "NIGHT",
    "ONDO",
    "POL",
    "PUMP",
    "QNT",
    "SKY",
    "SUI",
    "TAO",
    "TON",
    "TRUMP",
    "WIF",
    "WLD",
    "ZEC",
    "ZRO",
    #"PEPE",
    #"BONK",
    #"SHIB",
)


def usdt_spot_symbol(base: str) -> str:
    """Binance spot style: BASE + USDT (e.g. BTC -> BTCUSDT)."""
    return f"{base.strip().upper()}{QUOTE_ASSET}"


DATA_COLLECTION_SYMBOLS: tuple[str, ...] = tuple(
    usdt_spot_symbol(b) for b in DATA_COLLECTION_BASE_ASSETS
)
