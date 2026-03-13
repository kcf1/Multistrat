#!/usr/bin/env python3
"""
Upsert a single asset into the assets table (manual, one by one). No sync from symbols.

Requires: DATABASE_URL.

  python scripts/upsert_asset.py BTC --usd-symbol BTCUSDT
  python scripts/upsert_asset.py USDT --usd-price 1
  python scripts/upsert_asset.py ETH --usd-symbol ETHUSDT --usd-price 3000
"""

import argparse
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

from pms.asset_init import upsert_asset


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upsert one asset (asset, optional usd_symbol, optional usd_price)"
    )
    parser.add_argument("asset", help="Asset code (e.g. BTC, USDT)")
    parser.add_argument("--usd-symbol", default=None, help="Trading symbol for price fetch (e.g. BTCUSDT)")
    parser.add_argument("--usd-price", default=None, help="Fixed USD price (e.g. 1 for stables)")
    args = parser.parse_args()

    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1

    ok = upsert_asset(
        database_url,
        args.asset.strip(),
        usd_symbol=args.usd_symbol.strip() if args.usd_symbol else None,
        usd_price=args.usd_price,
    )
    if ok:
        print("upsert_asset:", args.asset)
    else:
        print("upsert_asset: no change (empty asset?)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
