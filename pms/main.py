"""
PMS entrypoint (12.3.8): run the periodic read → derive → calculate → write loop.

Usage:
  python -m pms.main           # loop: refresh every PMS_TICK_INTERVAL_SECONDS (default 10s)
  python -m pms.main --once   # on-request refresh: run one tick and exit
Skips Redis; writes granular positions table only.
"""

import argparse
import logging
import sys

from pms.config import PmsSettings
from pms.loop import run_one_tick, run_pms_loop
from pms.mark_price import get_mark_price_provider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="PMS: read orders → derive positions → write positions table")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one refresh (read → derive → write) and exit; default is loop every tick interval",
    )
    args = parser.parse_args()

    settings = PmsSettings()
    if not settings.database_url:
        logger.error("DATABASE_URL is not set; cannot run PMS")
        sys.exit(1)
    mark_provider = get_mark_price_provider(
        settings.pms_mark_price_source,
        binance_base_url=settings.binance_base_url,
    )

    if args.once:
        logger.info("On-request refresh (one tick); mark source=%s", settings.pms_mark_price_source)
        n = run_one_tick(settings.database_url, mark_provider)
        logger.info("Refreshed %s positions", n)
        return

    logger.info(
        "Starting PMS loop (tick every %s s, mark source=%s); Redis skipped",
        settings.pms_tick_interval_seconds,
        settings.pms_mark_price_source,
    )
    run_pms_loop(
        settings.database_url,
        mark_provider,
        tick_interval_seconds=settings.pms_tick_interval_seconds,
    )


if __name__ == "__main__":
    main()
