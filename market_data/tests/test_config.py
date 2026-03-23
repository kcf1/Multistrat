"""Tests for market_data.config.MarketDataSettings."""

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from market_data.config import (
    BASIS_CONTRACT_TYPES,
    BASIS_INITIAL_BACKFILL_DAYS,
    BASIS_PAIRS,
    BASIS_PERIODS,
    BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS,
    BINANCE_FUTURES_30D_DATASET_BACKFILL_DAYS,
    DEFAULT_BINANCE_PERPS_REST_URL,
    DEFAULT_BINANCE_REST_URL,
    OPEN_INTEREST_CONTRACT_TYPES,
    OPEN_INTEREST_INITIAL_BACKFILL_DAYS,
    OPEN_INTEREST_PERIODS,
    OPEN_INTEREST_SYMBOLS,
    OHLCV_INTERVALS,
    OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS,
    OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS,
    MarketDataSettings,
)
from market_data.universe import DATA_COLLECTION_SYMBOLS


@pytest.fixture
def minimal_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/multistrat")


def test_settings_requires_database_url(minimal_env: None) -> None:
    s = MarketDataSettings()
    assert s.database_url.startswith("postgresql://")


def test_settings_database_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("MARKET_DATA_DATABASE_URL", raising=False)

    class NoDotEnv(MarketDataSettings):
        model_config = SettingsConfigDict(env_file=None, extra="ignore")

    with pytest.raises(ValidationError):
        NoDotEnv()


def test_market_data_database_url_wins_over_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://legacy:5432/a")
    monkeypatch.setenv("MARKET_DATA_DATABASE_URL", "postgresql://md:5432/b")
    s = MarketDataSettings()
    assert s.database_url == "postgresql://md:5432/b"


def test_default_symbols_match_universe(minimal_env: None) -> None:
    s = MarketDataSettings()
    assert s.symbols == DATA_COLLECTION_SYMBOLS


def test_intervals_are_module_constants(minimal_env: None) -> None:
    s = MarketDataSettings()
    assert s.intervals == OHLCV_INTERVALS


def test_binance_rest_url_default_when_market_data_base_unset(
    minimal_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MARKET_DATA_BINANCE_BASE_URL", raising=False)

    class NoDotEnv(MarketDataSettings):
        model_config = SettingsConfigDict(env_file=None, extra="ignore")

    s = NoDotEnv()
    assert s.binance_rest_url == DEFAULT_BINANCE_REST_URL


def test_binance_rest_url_from_market_data_binance_base_url(
    minimal_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MARKET_DATA_BINANCE_BASE_URL", "https://testnet.binance.vision/")
    s = MarketDataSettings()
    assert s.binance_rest_url == "https://testnet.binance.vision"


def test_binance_perps_rest_url_default_when_unset(
    minimal_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MARKET_DATA_BINANCE_PERPS_BASE_URL", raising=False)

    class NoDotEnv(MarketDataSettings):
        model_config = SettingsConfigDict(env_file=None, extra="ignore")

    s = NoDotEnv()
    assert s.binance_perps_rest_url == DEFAULT_BINANCE_PERPS_REST_URL


def test_binance_perps_rest_url_from_market_data_binance_perps_base_url(
    minimal_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "MARKET_DATA_BINANCE_PERPS_BASE_URL",
        "https://fapi.binance.com/",
    )
    s = MarketDataSettings()
    assert s.binance_perps_rest_url == "https://fapi.binance.com"


def test_scheduler_cadence_is_module_constants() -> None:
    assert OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS == 300
    assert OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS == 3600
    assert OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS == 0


def test_basis_and_open_interest_series_defaults_align() -> None:
    assert BASIS_PAIRS == DATA_COLLECTION_SYMBOLS
    assert OPEN_INTEREST_SYMBOLS == DATA_COLLECTION_SYMBOLS
    assert BASIS_CONTRACT_TYPES == ("PERPETUAL",)
    assert OPEN_INTEREST_CONTRACT_TYPES == ("PERPETUAL",)
    assert BASIS_PERIODS == ("1h",)
    assert OPEN_INTEREST_PERIODS == ("1h",)
    assert BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS == 27
    assert BINANCE_FUTURES_30D_DATASET_BACKFILL_DAYS == BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS
    assert BASIS_INITIAL_BACKFILL_DAYS == BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS
    assert OPEN_INTEREST_INITIAL_BACKFILL_DAYS == BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS
