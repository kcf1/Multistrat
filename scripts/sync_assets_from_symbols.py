#!/usr/bin/env python3
"""
Sync assets table from symbols table: add/update usd_symbol for each base_asset (e.g. BTC → BTCUSDT).

Ensures asset rows exist so the asset price feed can update usd_price. Run after OMS symbol sync
or whenever symbols table has new pairs. Requires: DATABASE_URL.

  python scripts/sync_assets_from_symbols.py [--quote USDT]
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

from pms.asset_init import sync_assets_from_symbols


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sync assets from symbols table (base_asset → asset with usd_symbol)"
    )
    parser.add_argument(
        "--quote",
        default="USDT",
        help="Quote asset to match (default USDT); usd_symbol = base_asset + quote",
    )
    args = parser.parse_args()

    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1

    n = sync_assets_from_symbols(database_url, quote_asset=args.quote)
    print("sync_assets_from_symbols: upserted %s asset(s)" % n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
