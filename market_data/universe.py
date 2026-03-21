"""
Data collection universe: bases we trade / track first; **USDT-quoted** spot symbols only.

Used as the default list for Phase 4 OHLCV/ticker ingestion and as the base list for the
PMS asset USD price feed (``ASSET_PRICE_FEED_ASSETS`` re-exports ``DATA_COLLECTION_BASE_ASSETS``).

Extend by appending to ``DATA_COLLECTION_BASE_ASSETS``; symbols are always ``{BASE}USDT``.

Many appended names are **liquidity-screen probes**: Binance may not list every ``BASEUSDT``;
ingest/backfill will show which pairs exist. Screen ticker **RENDER** is **RNDR** here (Binance
spot). **M** (ambiguous) omitted.
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
    # --- probes: top-cap alts not in the block above, Binance symbol where known ---
    "AAVE",
    "ALGO",
    "ASTER",
    "BDX",
    "BOB",
    "BONK",
    "CC",
    "CHO",
    "CRO",
    "CT",
    "ENA",
    "FET",
    "FLR",
    "FUTBL",
    "HASH",
    "HTX",
    "HYPE",
    "ICP",
    "JST",
    "JTRSY",
    "JUP",
    "KAS",
    "KCS",
    "LEO",
    "MNT",
    "MORPHO",
    "NEAR",
    "NIGHT",
    "OKB",
    "ONDO",
    "OUSC",
    "PEPE",
    "PI",
    "POL",
    "PUMP",
    "QNT",
    "RAIN",
    "RNDR",
    "RIVER",
    "SIREN",
    "SKY",
    "STABLL",
    "SUI",
    "TAO",
    "TON",
    "TRUMP",
    "USTB",
    "VLT",
    "WBT",
    "WIF",
    "WLD",
    "XDC",
    "ZEC",
    "ZRO",
)


def usdt_spot_symbol(base: str) -> str:
    """Binance spot style: BASE + USDT (e.g. BTC -> BTCUSDT)."""
    return f"{base.strip().upper()}{QUOTE_ASSET}"


DATA_COLLECTION_SYMBOLS: tuple[str, ...] = tuple(
    usdt_spot_symbol(b) for b in DATA_COLLECTION_BASE_ASSETS
)
