"""
PMS entrypoint (12.3.8): run the periodic read → derive → calculate → write loop.

Usage:
  python -m pms.main           # loop: refresh every PMS_TICK_INTERVAL_SECONDS (default 10s)
  python -m pms.main --once   # on-request refresh: run one tick and exit
Skips Redis; writes granular positions table only.
When ASSET_PRICE_FEED_SOURCE is set (e.g. binance), runs asset price feed before each tick.
"""

import argparse
import sys

from pms.asset_init import init_assets_stables
from pms.asset_price_feed import run_asset_price_feed_step
from pms.config import ASSET_PRICE_FEED_ASSETS, ASSET_PRICE_FEED_SOURCE
from pms.asset_price_providers import get_asset_price_provider
from pms.config import PmsSettings
from pms.log import logger
from pms.loop import run_one_tick, run_pms_loop
from pms.mark_price import get_mark_price_provider


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

    # One-time assets init at startup (fixed usd_price for major stables, like symbol sync in OMS)
    try:
        n = init_assets_stables(settings.database_url)
        if n:
            logger.info("Assets init: upserted {} stable(s) with usd_price=1", n)
    except Exception as e:
        logger.warning("Assets init at startup failed (continuing): {}", e)

    mark_provider = get_mark_price_provider(
        settings.pms_mark_price_source,
        binance_base_url=settings.binance_base_url,
    )

    # Asset price feed (optional): update assets.usd_price before each tick
    feed_provider = None
    feed_assets = []
    if ASSET_PRICE_FEED_SOURCE and ASSET_PRICE_FEED_SOURCE.strip():
        feed_provider = get_asset_price_provider(
            ASSET_PRICE_FEED_SOURCE.strip(),
            base_url=settings.binance_price_feed_base_url,
        )
        if feed_provider is not None:
            feed_assets = list(ASSET_PRICE_FEED_ASSETS)

    def run_feed_step() -> None:
        if feed_provider is not None and feed_assets:
            try:
                n = run_asset_price_feed_step(
                    settings.database_url,
                    feed_provider,
                    ASSET_PRICE_FEED_SOURCE.strip(),
                    feed_assets,
                )
                if n:
                    logger.info("Asset price feed updated {} asset(s)", n)
            except Exception as e:
                logger.exception("Asset price feed failed: {}", e)

    if args.once:
        logger.info("On-request refresh (one tick); mark source={}", settings.pms_mark_price_source)
        run_feed_step()
        n = run_one_tick(settings.database_url, mark_provider)
        logger.info("Refreshed {} positions", n)
        return

    logger.info(
        "Starting PMS loop (tick every {} s, mark source={}); Redis skipped",
        settings.pms_tick_interval_seconds,
        settings.pms_mark_price_source,
    )
    run_pms_loop(
        settings.database_url,
        mark_provider,
        tick_interval_seconds=settings.pms_tick_interval_seconds,
        pre_tick_callback=run_feed_step if feed_provider is not None and feed_assets else None,
    )


if __name__ == "__main__":
    main()
