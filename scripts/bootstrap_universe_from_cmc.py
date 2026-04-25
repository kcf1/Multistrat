#!/usr/bin/env python3
"""
One-time bootstrap: fetch CMC top-100 and upsert into market_data universe table.

Requires:
  - MARKET_DATA_DATABASE_URL (or DATABASE_URL)
  - MARKET_DATA_CMC_API_KEY

Usage:
  python scripts/bootstrap_universe_from_cmc.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from market_data.config import load_settings
from market_data.jobs.universe_refresh_top100 import run_universe_refresh_top100


def main() -> int:
    settings = load_settings()
    run_universe_refresh_top100(settings, n=100)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

