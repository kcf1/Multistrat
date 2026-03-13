"""
PMS configuration via environment (Pydantic BaseSettings).

Task 12.3.1b: Use Pydantic for config. See docs/pms/PMS_ARCHITECTURE.md §12.
"""

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PmsSettings(BaseSettings):
    """
    PMS configuration from environment.

    Variables: REDIS_URL, DATABASE_URL, PMS_TICK_INTERVAL_SECONDS,
    PMS_MARK_PRICE_SOURCE, PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis connection URL")
    database_url: Optional[str] = Field(default=None, description="Postgres connection URL")
    pms_tick_interval_seconds: float = Field(
        default=10.0,
        ge=0.1,
        description="How often to run read → derive → calculate → write (seconds)",
    )
    pms_mark_price_source: str = Field(
        default="binance",
        description="Mark price provider: 'binance' (Phase 2), 'redis'/'market_data' (Phase 4)",
    )
    pms_rebuild_from_orders_interval_seconds: Optional[float] = Field(
        default=None,
        ge=1.0,
        description="How often to rebuild positions from orders and run repairs (seconds); unset = disabled",
    )
    # Binance mark price (when pms_mark_price_source=binance)
    binance_base_url: Optional[str] = Field(
        default=None,
        description="Binance REST base URL (e.g. https://testnet.binance.vision); default testnet for spot",
    )

    # Asset price feed (docs/pms/ASSET_PRICE_FEED_PLAN.md)
    pms_asset_price_source: str = Field(
        default="",
        description="Asset price feed source: 'binance' or '' to disable",
    )
    pms_asset_price_interval_seconds: float = Field(
        default=60.0,
        ge=1.0,
        description="How often to run asset price feed (seconds)",
    )
    binance_price_feed_base_url: Optional[str] = Field(
        default=None,
        description="Binance REST base URL for asset price feed; default testnet when use_testnet",
    )
    pms_asset_price_assets: Optional[str] = Field(
        default=None,
        description="Comma-separated assets to update from feed; if empty, use DB (usd_symbol set)",
    )
