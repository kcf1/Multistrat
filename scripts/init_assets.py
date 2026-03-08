#!/usr/bin/env python3
"""
Init assets table: upsert fixed usd_price=1 for major quoting (stable) assets.

Same as PMS startup init; can be run once after migration or standalone.
Requires: DATABASE_URL (e.g. from .env).

  python scripts/init_assets.py
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

from pms.asset_init import init_assets_stables


def main() -> int:
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    if not database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        return 1
    n = init_assets_stables(database_url)
    print("init_assets: upserted %s stable asset(s) with usd_price=1" % n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
