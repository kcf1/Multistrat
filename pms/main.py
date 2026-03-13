"""
PMS entrypoint (12.3.8): run the periodic read → derive → calculate → write loop.

Usage:
  python -m pms.main           # loop: refresh every PMS_TICK_INTERVAL_SECONDS (default 10s)
  python -m pms.main --once   # on-request refresh: run one tick and exit
Skips Redis; writes granular positions table only.
When PMS_ASSET_PRICE_SOURCE is set (e.g. binance), runs asset price feed before each tick.
"""

import argparse
import sys

from pms.asset_init import init_assets_stables
from pms.asset_price_feed import query_assets_for_price_source, run_asset_price_feed_step
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
            logger.info("Assets init: upserted %s stable(s) with usd_price=1", n)
    except Exception as e:
        logger.warning("Assets init at startup failed (continuing): %s", e)

    mark_provider = get_mark_price_provider(
        settings.pms_mark_price_source,
        binance_base_url=settings.binance_base_url,
    )

    # Asset price feed (optional): update assets.usd_price before each tick
    feed_provider = None
    feed_assets = []
    if settings.pms_asset_price_source and settings.pms_asset_price_source.strip():
        feed_provider = get_asset_price_provider(
            settings.pms_asset_price_source.strip(),
            base_url=settings.binance_price_feed_base_url,
        )
        if feed_provider is not None:
            if settings.pms_asset_price_assets and settings.pms_asset_price_assets.strip():
                feed_assets = [a.strip() for a in settings.pms_asset_price_assets.split(",") if a.strip()]
            else:
                try:
                    feed_assets = query_assets_for_price_source(settings.database_url)
                except Exception as e:
                    logger.warning("Asset price feed: query_assets_for_price_source failed (continuing): %s", e)

    def run_feed_step() -> None:
        if feed_provider is not None and feed_assets:
            try:
                n = run_asset_price_feed_step(
                    settings.database_url,
                    feed_provider,
                    settings.pms_asset_price_source.strip(),
                    feed_assets,
                )
                if n:
                    logger.info("Asset price feed updated %s asset(s)", n)
            except Exception as e:
                logger.exception("Asset price feed failed: %s", e)

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
