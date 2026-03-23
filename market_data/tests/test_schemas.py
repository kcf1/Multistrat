"""Tests for Binance kline → OhlcvBar parsing."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from market_data.schemas import (
    BasisPoint,
    OhlcvBar,
    parse_binance_basis_row,
    parse_binance_basis_rows,
    parse_binance_kline,
    parse_binance_klines,
)


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


_BASIS_SAMPLE = {
    "indexPrice": "46317.16333333",
    "contractType": "PERPETUAL",
    "basisRate": "0.00295565",
    "futuresPrice": "46454.22",
    "annualizedBasisRate": "1.07731205",
    "basis": "137.05666667",
    "pair": "BTCUSDT",
    "timestamp": 1640995200000,
    "period": "1h",
}


def test_parse_binance_basis_row_full() -> None:
    p = parse_binance_basis_row(_BASIS_SAMPLE)
    assert p.pair == "BTCUSDT"
    assert p.contract_type == "PERPETUAL"
    assert p.period == "1h"
    assert p.basis_rate == Decimal("0.00295565")
    assert p.annualized_basis_rate == Decimal("1.07731205")
    assert p.sample_time == datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_parse_binance_basis_row_with_explicit_context() -> None:
    row = dict(_BASIS_SAMPLE)
    row["pair"] = "IGNORED"
    row["contractType"] = "CURRENT_QUARTER"
    row["period"] = "5m"
    p = parse_binance_basis_row(
        row,
        pair="ethusdt",
        contract_type="perpetual",
        period="1h",
    )
    assert p.pair == "ETHUSDT"
    assert p.contract_type == "PERPETUAL"
    assert p.period == "1h"


def test_parse_binance_basis_rows_multiple() -> None:
    points = parse_binance_basis_rows([_BASIS_SAMPLE, _BASIS_SAMPLE])
    assert len(points) == 2
    assert all(isinstance(p, BasisPoint) for p in points)


def test_basis_point_frozen() -> None:
    from pydantic import ValidationError

    p = parse_binance_basis_row(_BASIS_SAMPLE)
    with pytest.raises(ValidationError):
        p.period = "4h"  # type: ignore[misc]
