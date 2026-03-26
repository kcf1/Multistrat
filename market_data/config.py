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

OHLCV_INTERVALS: tuple[str, ...] = ("1h",)
OHLCV_SYMBOLS: tuple[str, ...] = DATA_COLLECTION_SYMBOLS

# Used only when ``MARKET_DATA_BINANCE_BASE_URL`` is unset.
DEFAULT_BINANCE_REST_URL: str = "https://api.binance.com"

# Used when ``MARKET_DATA_BINANCE_PERPS_BASE_URL`` is unset (basis / open interest / futures data).
# Do **not** fall back to spot ``api.binance.com`` — USD-M futures REST is on ``fapi``.
DEFAULT_BINANCE_PERPS_REST_URL: str = "https://fapi.binance.com"

# Min seconds between REST calls **per provider instance** (shared by all jobs on that instance).
# ``None`` = **unlimited** (default) until venue weight/QPS is documented.
MARKET_DATA_MIN_REQUEST_INTERVAL_SEC: float | None = None

# First-time series with no rows / cursor: fetch history back this many days from "now".
OHLCV_INITIAL_BACKFILL_DAYS: int = 5 * 365  # ~5 calendar years (empty series / cold start)

# Shared cold-start / policy horizon for Binance futures datasets whose history is capped near
# ~30 calendar days (basis, open interest, etc.). We intentionally use **27** days—not the full
# advertised window—as a **buffer** so ``startTime`` stays safely inside the venue-valid range
# (reduces invalid-parameter / retention-edge failures at the oldest boundary).
BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS: int = 27

# Backward-compatible alias (value is 27 with buffer semantics, not a literal 30-day request).
BINANCE_FUTURES_30D_DATASET_BACKFILL_DAYS: int = BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS

# Max klines per HTTP request (Binance cap).
OHLCV_KLINES_CHUNK_LIMIT: int = 1000

# ``skip_existing_when_no_watermark``: :func:`detect_ohlcv_time_gaps` uses this multiple of
# the bar length to classify a hole (same semantics as ``repair_gap``).
OHLCV_SKIP_EXISTING_GAP_MULTIPLE: float = 1.5

# ``correct_window`` re-fetches this many recent bars per series for vendor drift checks.
OHLCV_CORRECT_WINDOW_BARS: int = 48

# Binance klines: retries when HTTP fails or payload fails validation (incomplete rows).
OHLCV_KLINES_FETCH_MAX_ATTEMPTS: int = 5
OHLCV_KLINES_FETCH_RETRY_BASE_SLEEP_SEC: float = 0.75

# Interior ``open_time`` step must be ~one bar length (ratios of interval ms).
OHLCV_KLINES_GRID_MAX_STEP_RATIO: float = 1.51
OHLCV_KLINES_GRID_MIN_STEP_RATIO: float = 0.99

# With explicit start+end: head slack beyond this many intervals is logged (non-fatal ingest).
OHLCV_KLINES_HEAD_MAX_SLACK_INTERVALS: int = 3

# Only run span / tail / head coverage checks when window is at least this many bars wide.
OHLCV_KLINES_SPAN_CHECK_MIN_INTERVALS: int = 10

# Log ``loguru.warning`` when consecutive kline ``open_time`` skips **more than** this many
# implied bar slots (venues omit candles; small gaps stay quiet).
OHLCV_KLINES_WARN_OPEN_TIME_GAP_BARS: int = 5

# ``python -m market_data.main`` scheduler cadence (UTC-aligned after first immediate run).
# Do not put these in ``.env`` — tune here (see ``.cursor/rules/env-and-config.mdc``).
OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS: int = 300
OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS: int = 3600
# ``0`` disables scheduled policy-window gap repair (use ``scripts/backfill_ohlcv.py`` / ``--with-repair``).
OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS: int = 0
# Global provider worker cap (threads) for market data jobs.
GLOBAL_PROVIDER_MAX_WORKERS: int = 6
# Provider-scoped worker pool size for OHLCV ingest orchestration.
# ``1`` preserves sequential behavior; increase cautiously after benchmark validation.
OHLCV_PROVIDER_MAX_WORKERS: int = GLOBAL_PROVIDER_MAX_WORKERS
# Worker pool size for OHLCV correct-window orchestration.
# ``1`` preserves sequential behavior.
OHLCV_CORRECT_WINDOW_MAX_WORKERS: int = GLOBAL_PROVIDER_MAX_WORKERS
# Worker pool size for OHLCV policy-window gap-repair orchestration.
# ``1`` preserves sequential behavior.
OHLCV_REPAIR_GAP_MAX_WORKERS: int = GLOBAL_PROVIDER_MAX_WORKERS
# Guardrail for concurrent in-flight OHLCV fetch calls per provider pool.
# Keep this conservative relative to venue limits; ``1`` effectively serializes fetch calls.
OHLCV_PROVIDER_MAX_IN_FLIGHT_FETCHES: int = 4

# Basis (Binance futures basis endpoint) micro constants.
BASIS_PAIRS: tuple[str, ...] = DATA_COLLECTION_SYMBOLS
BASIS_CONTRACT_TYPES: tuple[str, ...] = ("PERPETUAL",)
BASIS_PERIODS: tuple[str, ...] = ("1h",)
BASIS_INITIAL_BACKFILL_DAYS: int = BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS
BASIS_FETCH_CHUNK_LIMIT: int = 500
BASIS_CORRECT_WINDOW_POINTS: int = 48
BASIS_SCHEDULER_INGEST_INTERVAL_SECONDS: int = 300
BASIS_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS: int = 3600
BASIS_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS: int = 0
BASIS_FETCH_MAX_ATTEMPTS: int = 5
BASIS_FETCH_RETRY_BASE_SLEEP_SEC: float = 0.75

# Open Interest (Binance futures openInterestHist endpoint) micro constants.
OPEN_INTEREST_SYMBOLS: tuple[str, ...] = DATA_COLLECTION_SYMBOLS
OPEN_INTEREST_CONTRACT_TYPES: tuple[str, ...] = ("PERPETUAL",)
OPEN_INTEREST_PERIODS: tuple[str, ...] = ("1h",)
OPEN_INTEREST_INITIAL_BACKFILL_DAYS: int = BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS
OPEN_INTEREST_FETCH_CHUNK_LIMIT: int = 500
OPEN_INTEREST_CORRECT_WINDOW_POINTS: int = 48
OPEN_INTEREST_SCHEDULER_INGEST_INTERVAL_SECONDS: int = 300
OPEN_INTEREST_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS: int = 3600
OPEN_INTEREST_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS: int = 0
OPEN_INTEREST_FETCH_MAX_ATTEMPTS: int = 5
OPEN_INTEREST_FETCH_RETRY_BASE_SLEEP_SEC: float = 0.75

# Taker Buy/Sell Volume (Binance USD-M takerlongshortRatio endpoint) micro constants.
TAKER_BUYSELL_VOLUME_SYMBOLS: tuple[str, ...] = DATA_COLLECTION_SYMBOLS
TAKER_BUYSELL_VOLUME_PERIODS: tuple[str, ...] = ("1h",)
# Binance documents "only the latest 30 days is available" for this dataset family; use the
# same 27-day buffered default as basis/open interest to stay safely within the venue window.
TAKER_BUYSELL_VOLUME_INITIAL_BACKFILL_DAYS: int = BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS
TAKER_BUYSELL_VOLUME_FETCH_CHUNK_LIMIT: int = 500
TAKER_BUYSELL_VOLUME_CORRECT_WINDOW_POINTS: int = 48
TAKER_BUYSELL_VOLUME_SCHEDULER_INGEST_INTERVAL_SECONDS: int = 300
TAKER_BUYSELL_VOLUME_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS: int = 3600
TAKER_BUYSELL_VOLUME_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS: int = 0
TAKER_BUYSELL_VOLUME_FETCH_MAX_ATTEMPTS: int = 5
TAKER_BUYSELL_VOLUME_FETCH_RETRY_BASE_SLEEP_SEC: float = 0.75

# Top Trader Long/Short (Binance USD-M topLongShortPositionRatio endpoint) micro constants.
TOP_TRADER_LONG_SHORT_SYMBOLS: tuple[str, ...] = DATA_COLLECTION_SYMBOLS
# Binance period enum: "5m","15m","30m","1h","2h","4h","6h","12h","1d"
# Start with a subset; expand once the pipeline is stable.
TOP_TRADER_LONG_SHORT_PERIODS: tuple[str, ...] = ("1h",)
# Binance documents "only the latest 30 days is available" for this dataset family; use the
# same 27-day buffered default as basis/open interest to stay safely within the venue window.
TOP_TRADER_LONG_SHORT_INITIAL_BACKFILL_DAYS: int = BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS
TOP_TRADER_LONG_SHORT_FETCH_CHUNK_LIMIT: int = 500
TOP_TRADER_LONG_SHORT_CORRECT_WINDOW_POINTS: int = 48
TOP_TRADER_LONG_SHORT_SCHEDULER_INGEST_INTERVAL_SECONDS: int = 300
TOP_TRADER_LONG_SHORT_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS: int = 3600
TOP_TRADER_LONG_SHORT_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS: int = 0
TOP_TRADER_LONG_SHORT_FETCH_MAX_ATTEMPTS: int = 5
TOP_TRADER_LONG_SHORT_FETCH_RETRY_BASE_SLEEP_SEC: float = 0.75


class MarketDataSettings(BaseSettings):
    """
    Macro env (isolated from OMS):

    - **DATABASE_URL** or **MARKET_DATA_DATABASE_URL** (latter wins if both set).
    - Optional **MARKET_DATA_BINANCE_BASE_URL** for public REST klines (testnet vs mainnet).
    - Optional **MARKET_DATA_BINANCE_PERPS_BASE_URL** for basis/funding futures data.

    Micro: OHLCV_SYMBOLS, OHLCV_INTERVALS, OHLCV_SCHEDULER_* cadence, DEFAULT_BINANCE_REST_URL in this file.
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

    market_data_binance_perps_base_url: str | None = Field(
        default=None,
        validation_alias="MARKET_DATA_BINANCE_PERPS_BASE_URL",
        description="REST base for Binance perps public endpoints used by market_data.",
    )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def binance_rest_url(self) -> str:
        u = (self.market_data_binance_base_url or "").strip().rstrip("/")
        return u if u else DEFAULT_BINANCE_REST_URL

    @computed_field  # type: ignore[prop-decorator]
    @property
    def binance_perps_rest_url(self) -> str:
        u = (self.market_data_binance_perps_base_url or "").strip().rstrip("/")
        return u if u else DEFAULT_BINANCE_PERPS_REST_URL

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
