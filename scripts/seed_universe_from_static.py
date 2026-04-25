#!/usr/bin/env python3
"""
Seed market_data universe table from static list in market_data/universe.py.

Requires: MARKET_DATA_DATABASE_URL (or DATABASE_URL).

Usage:
  python scripts/seed_universe_from_static.py
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from market_data.config import load_settings
from market_data.jobs.universe_seed_static import run_universe_seed_from_static


def main() -> int:
    settings = load_settings()
    run_universe_seed_from_static(settings)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

