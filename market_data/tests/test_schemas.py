"""Tests for Binance kline → OhlcvBar parsing."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from market_data.schemas import (
    BasisPoint,
    OhlcvBar,
    OpenInterestPoint,
    TopTraderLongShortPoint,
    TakerBuySellVolumePoint,
    parse_binance_basis_row,
    parse_binance_basis_rows,
    parse_binance_kline,
    parse_binance_klines,
    parse_binance_open_interest_row,
    parse_binance_open_interest_rows,
    parse_binance_top_trader_long_short_position_ratio_row,
    parse_binance_top_trader_long_short_position_ratio_rows,
    parse_binance_taker_buy_sell_volume_row,
    parse_binance_taker_buy_sell_volume_rows,
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
    assert bar.taker_buy_base_volume == Decimal("1756.87402397")
    assert bar.taker_buy_quote_volume == Decimal("28.46694368")
    assert bar.trades == 308
    assert bar.close_time == datetime(2017, 7, 9, 23, 59, 59, 999000, tzinfo=timezone.utc)


def test_parse_binance_kline_minimal_seven_elements() -> None:
    minimal = _SAMPLE[:7]
    bar = parse_binance_kline(minimal, symbol="ETHUSDT", interval="5m")
    assert bar.symbol == "ETHUSDT"
    assert bar.quote_volume is None
    assert bar.taker_buy_base_volume is None
    assert bar.taker_buy_quote_volume is None
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


def test_ohlcv_bar_rejects_negative_taker_buy_volumes() -> None:
    with pytest.raises(ValueError, match="taker_buy_base_volume must be >= 0 when set"):
        OhlcvBar(
            symbol="BTCUSDT",
            interval="1m",
            open_time=datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1"),
            volume=Decimal("10"),
            taker_buy_base_volume=Decimal("-1"),
        )

    with pytest.raises(ValueError, match="taker_buy_quote_volume must be >= 0 when set"):
        OhlcvBar(
            symbol="BTCUSDT",
            interval="1m",
            open_time=datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1"),
            volume=Decimal("10"),
            taker_buy_quote_volume=Decimal("-1"),
        )


_BASIS_SAMPLE = {
    "indexPrice": "46317.16333333",
    "contractType": "PERPETUAL",
    "basisRate": "0.00295565",
    "futuresPrice": "46454.22",
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


_OPEN_INTEREST_SAMPLE = {
    "symbol": "BTCUSDT",
    "contractType": "PERPETUAL",
    "sumOpenInterest": "12345.6789",
    "sumOpenInterestValue": "987654321.123456",
    "CMCCirculatingSupply": "19500000.0",
    "timestamp": 1640995200000,
    "period": "1h",
}


def test_parse_binance_open_interest_row_full() -> None:
    p = parse_binance_open_interest_row(_OPEN_INTEREST_SAMPLE)
    assert p.symbol == "BTCUSDT"
    assert p.contract_type == "PERPETUAL"
    assert p.period == "1h"
    assert p.sum_open_interest == Decimal("12345.6789")
    assert p.sum_open_interest_value == Decimal("987654321.123456")
    assert p.cmc_circulating_supply == Decimal("19500000.0")
    assert p.sample_time == datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_parse_binance_open_interest_row_with_explicit_context() -> None:
    row = dict(_OPEN_INTEREST_SAMPLE)
    row["symbol"] = "IGNORED"
    row["contractType"] = "CURRENT_QUARTER"
    row["period"] = "5m"
    p = parse_binance_open_interest_row(
        row,
        symbol="ethusdt",
        contract_type="perpetual",
        period="1h",
    )
    assert p.symbol == "ETHUSDT"
    assert p.contract_type == "PERPETUAL"
    assert p.period == "1h"


def test_parse_binance_open_interest_rows_multiple() -> None:
    points = parse_binance_open_interest_rows([_OPEN_INTEREST_SAMPLE, _OPEN_INTEREST_SAMPLE])
    assert len(points) == 2
    assert all(isinstance(p, OpenInterestPoint) for p in points)


def test_open_interest_point_frozen() -> None:
    from pydantic import ValidationError

    p = parse_binance_open_interest_row(_OPEN_INTEREST_SAMPLE)
    with pytest.raises(ValidationError):
        p.period = "4h"  # type: ignore[misc]


_TAKER_BUYSELL_VOLUME_SAMPLE = {
    "buySellRatio": "1.5586",
    "buyVol": "387.3300",
    "sellVol": "248.5030",
    "timestamp": 1640995200000,
    "symbol": "BTCUSDT",
    "period": "1h",
}


def test_parse_binance_taker_buy_sell_volume_row_full() -> None:
    p = parse_binance_taker_buy_sell_volume_row(_TAKER_BUYSELL_VOLUME_SAMPLE)
    assert p.symbol == "BTCUSDT"
    assert p.period == "1h"
    assert p.buy_sell_ratio == Decimal("1.5586")
    assert p.buy_vol == Decimal("387.3300")
    assert p.sell_vol == Decimal("248.5030")
    assert p.sample_time == datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_parse_binance_taker_buy_sell_volume_row_with_explicit_context() -> None:
    row = dict(_TAKER_BUYSELL_VOLUME_SAMPLE)
    row["symbol"] = "IGNORED"
    row["period"] = "5m"
    p = parse_binance_taker_buy_sell_volume_row(
        row,
        symbol="ethusdt",
        period="1h",
    )
    assert p.symbol == "ETHUSDT"
    assert p.period == "1h"


def test_parse_binance_taker_buy_sell_volume_rows_multiple() -> None:
    points = parse_binance_taker_buy_sell_volume_rows(
        [_TAKER_BUYSELL_VOLUME_SAMPLE, _TAKER_BUYSELL_VOLUME_SAMPLE],
    )
    assert len(points) == 2
    assert all(isinstance(p, TakerBuySellVolumePoint) for p in points)


def test_taker_buy_sell_volume_point_frozen() -> None:
    from pydantic import ValidationError

    p = parse_binance_taker_buy_sell_volume_row(_TAKER_BUYSELL_VOLUME_SAMPLE)
    with pytest.raises(ValidationError):
        p.period = "4h"  # type: ignore[misc]


_TOP_TRADER_LONG_SHORT_SAMPLE = {
    "longShortRatio": "1.4342",
    "longAccount": "0.5891",
    "shortAccount": "0.4108",
    "timestamp": 1640995200000,
    "symbol": "BTCUSDT",
    # Intentionally omit `period` here to ensure the parser can use explicit context.
}


def test_parse_binance_top_trader_long_short_position_ratio_row_full() -> None:
    p = parse_binance_top_trader_long_short_position_ratio_row(
        dict(_TOP_TRADER_LONG_SHORT_SAMPLE),
        period="1h",
    )
    assert p.symbol == "BTCUSDT"
    assert p.period == "1h"
    assert p.long_short_ratio == Decimal("1.4342")
    assert p.long_account_ratio == Decimal("0.5891")
    assert p.short_account_ratio == Decimal("0.4108")
    assert p.sample_time == datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_parse_binance_top_trader_long_short_position_ratio_row_with_explicit_context() -> None:
    row = dict(_TOP_TRADER_LONG_SHORT_SAMPLE)
    row["symbol"] = "IGNORED"
    # Keep payload period absent; override symbol via explicit context.
    p = parse_binance_top_trader_long_short_position_ratio_row(
        row,
        symbol="ethusdt",
        period="1h",
    )
    assert p.symbol == "ETHUSDT"
    assert p.period == "1h"


def test_parse_binance_top_trader_long_short_position_ratio_rows_multiple() -> None:
    points = parse_binance_top_trader_long_short_position_ratio_rows(
        [dict(_TOP_TRADER_LONG_SHORT_SAMPLE), dict(_TOP_TRADER_LONG_SHORT_SAMPLE)],
        period="1h",
    )
    assert len(points) == 2
    assert all(isinstance(p, TopTraderLongShortPoint) for p in points)


def test_top_trader_long_short_point_frozen() -> None:
    from pydantic import ValidationError

    p = parse_binance_top_trader_long_short_position_ratio_row(
        _TOP_TRADER_LONG_SHORT_SAMPLE,
        period="1h",
    )
    with pytest.raises(ValidationError):
        p.period = "4h"  # type: ignore[misc]
