"""Tests for Binance kline → OhlcvBar parsing."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from market_data.schemas import OhlcvBar, parse_binance_kline, parse_binance_klines


# Sample shape from Binance klines API (strings as returned by JSON).
_SAMPLE = [
    1499049600000,
    "0.01634790",
    "0.80000000",
    "0.01575800",
    "0.01577100",
    "148976.11427815",
    1499644799999,
    "2434.19055334",
    308,
    "1756.87402397",
    "28.46694368",
    "0",
]


def test_parse_binance_kline_full() -> None:
    bar = parse_binance_kline(list(_SAMPLE), symbol="btcusdt", interval="1m")
    assert bar.symbol == "BTCUSDT"
    assert bar.interval == "1m"
    assert bar.open_time == datetime(2017, 7, 3, 2, 40, 0, tzinfo=timezone.utc)
    assert bar.open == Decimal("0.01634790")
    assert bar.high == Decimal("0.80000000")
    assert bar.low == Decimal("0.01575800")
    assert bar.close == Decimal("0.01577100")
    assert bar.volume == Decimal("148976.11427815")
    assert bar.quote_volume == Decimal("2434.19055334")
    assert bar.trades == 308
    assert bar.close_time == datetime(2017, 7, 9, 23, 59, 59, 999000, tzinfo=timezone.utc)


def test_parse_binance_kline_minimal_seven_elements() -> None:
    minimal = _SAMPLE[:7]
    bar = parse_binance_kline(minimal, symbol="ETHUSDT", interval="5m")
    assert bar.symbol == "ETHUSDT"
    assert bar.quote_volume is None
    assert bar.trades is None
    assert bar.close_time == datetime(2017, 7, 9, 23, 59, 59, 999000, tzinfo=timezone.utc)


def test_parse_binance_kline_too_short() -> None:
    with pytest.raises(ValueError, match="too short"):
        parse_binance_kline([1, "1", "2", "3", "4", "5"], symbol="X", interval="1m")


def test_parse_binance_klines_multiple() -> None:
    rows = [_SAMPLE, _SAMPLE]
    bars = parse_binance_klines(rows, symbol="BTCUSDT", interval="1h")
    assert len(bars) == 2
    assert all(isinstance(b, OhlcvBar) for b in bars)


def test_ohlcv_bar_frozen() -> None:
    from pydantic import ValidationError

    bar = parse_binance_kline(list(_SAMPLE), symbol="BTCUSDT", interval="1m")
    with pytest.raises(ValidationError):
        bar.symbol = "OTHER"  # type: ignore[misc]
