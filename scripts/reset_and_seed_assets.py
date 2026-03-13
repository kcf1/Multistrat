#!/usr/bin/env python3
"""
Reset the assets table and seed with:
  1. USDT (and optional stables) with fixed 1:1 USD, price_source='fixed'
  2. Top 30 largest market-cap coins with usd_symbol for the price feed to fill

Requires: DATABASE_URL. Destructive: deletes all existing asset rows.

  python scripts/reset_and_seed_assets.py
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
except ImportError:
    pass

from pms.asset_init import (
    PRICE_SOURCE_FIXED,
    init_assets_stables,
    truncate_assets,
    upsert_asset,
)

# Stables: fixed 1:1 USD, price_source='fixed'
STABLES = ("USDT", "USDC", "BUSD", "DAI")

# Top ~30 by market cap (Binance spot pairs as USDT); exclude stables already in STABLES
TOP_COINS_BY_MARKET_CAP = (
    "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "TRX", "DOT",
    "LINK", "MATIC", "SHIB", "LTC", "BCH", "UNI", "ATOM", "XLM", "XMR", "ETC",
    "FIL", "APT", "HBAR", "VET", "OP", "ARB", "INJ", "IMX", "SAND", "MANA",
)


def main() -> int:
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1

    # 1. Reset
    truncate_assets(database_url)
    print("reset_and_seed_assets: truncated assets table")

    # 2. Stables: fixed 1:1, price_source='fixed'
    n_stables = init_assets_stables(
        database_url,
        assets=STABLES,
        price_source=PRICE_SOURCE_FIXED,
    )
    print("reset_and_seed_assets: added %s stable(s) with usd_price=1, price_source=%r" % (n_stables, PRICE_SOURCE_FIXED))

    # 3. Top 30 coins: usd_symbol only (feed will set usd_price)
    quote = "USDT"
    count = 0
    for asset in TOP_COINS_BY_MARKET_CAP:
        if upsert_asset(database_url, asset, usd_symbol=f"{asset}{quote}"):
            count += 1
    print("reset_and_seed_assets: added %s top coins with usd_symbol (feed will set usd_price)" % count)

    return 0


if __name__ == "__main__":
    sys.exit(main())
