"""
Market data service configuration (Phase 4 §9.1).

**Micro** settings (symbols, intervals, default REST host) live as **constants** in this
module. **Macro** settings use **service-prefixed** env vars where applicable (see
``.cursor/rules/env-and-config.mdc``).
"""

from __future__ import annotations

from pydantic import AliasChoices, Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from market_data.universe import DATA_COLLECTION_SYMBOLS

# --- Micro (code constants; not .env) ---

OHLCV_INTERVALS: tuple[str, ...] = ("1m",)
OHLCV_SYMBOLS: tuple[str, ...] = DATA_COLLECTION_SYMBOLS

# Used only when ``MARKET_DATA_BINANCE_BASE_URL`` is unset.
DEFAULT_BINANCE_REST_URL: str = "https://api.binance.com"

# Min seconds between REST calls **per provider instance** (shared by all jobs on that instance).
# ``None`` = **unlimited** (default) until venue weight/QPS is documented.
MARKET_DATA_MIN_REQUEST_INTERVAL_SEC: float | None = None

# First-time series with no rows / cursor: fetch history back this many days from "now".
OHLCV_INITIAL_BACKFILL_DAYS: int = 30

# Max klines per HTTP request (Binance cap).
OHLCV_KLINES_CHUNK_LIMIT: int = 1000

# ``correct_window`` re-fetches this many recent bars per series for vendor drift checks.
OHLCV_CORRECT_WINDOW_BARS: int = 48


class MarketDataSettings(BaseSettings):
    """
    Macro env (isolated from OMS):

    - **DATABASE_URL** or **MARKET_DATA_DATABASE_URL** (latter wins if both set).
    - Optional **MARKET_DATA_BINANCE_BASE_URL** for public REST klines (testnet vs mainnet).

    Micro: **OHLCV_SYMBOLS**, **OHLCV_INTERVALS**, **DEFAULT_BINANCE_REST_URL** in this file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        ...,
        validation_alias=AliasChoices("MARKET_DATA_DATABASE_URL", "DATABASE_URL"),
        description="Postgres; prefer MARKET_DATA_DATABASE_URL for this service.",
    )

    market_data_binance_base_url: str | None = Field(
        default=None,
        validation_alias="MARKET_DATA_BINANCE_BASE_URL",
        description="REST base for Binance public endpoints used by market_data.",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def binance_rest_url(self) -> str:
        u = (self.market_data_binance_base_url or "").strip().rstrip("/")
        return u if u else DEFAULT_BINANCE_REST_URL

    @computed_field  # type: ignore[prop-decorator]
    @property
    def symbols(self) -> tuple[str, ...]:
        return OHLCV_SYMBOLS

    @computed_field  # type: ignore[prop-decorator]
    @property
    def intervals(self) -> tuple[str, ...]:
        return OHLCV_INTERVALS


def load_settings() -> MarketDataSettings:
    """Raises ValidationError if no database URL is available from env."""
    return MarketDataSettings()
