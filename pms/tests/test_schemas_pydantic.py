"""
Unit tests for PMS Pydantic schemas (12.3.1b): MarkPricesResult, BinanceTickerPriceItem, DerivedPosition.
"""

from decimal import Decimal

import pytest

from pms.schemas_pydantic import BinanceTickerPriceItem, DerivedPosition, MarkPricesResult


class TestMarkPricesResult:
    def test_coerce_float_to_decimal(self):
        r = MarkPricesResult(prices={"BTCUSDT": 50000.5})
        assert r.prices["BTCUSDT"] == Decimal("50000.5")

    def test_coerce_symbol_uppercase(self):
        r = MarkPricesResult(prices={"btcusdt": "50000"})
        assert "BTCUSDT" in r.prices
        assert r.prices["BTCUSDT"] == Decimal("50000")


class TestBinanceTickerPriceItem:
    def test_parse(self):
        item = BinanceTickerPriceItem(symbol="BTCUSDT", price="97250.25")
        assert item.symbol == "BTCUSDT"
        assert item.price == "97250.25"

    def test_strip_whitespace(self):
        item = BinanceTickerPriceItem(symbol="  BTCUSDT  ", price=" 50000 ")
        assert item.symbol == "BTCUSDT"
        assert item.price == "50000"

    def test_model_validate_from_dict(self):
        item = BinanceTickerPriceItem.model_validate({"symbol": "ETHUSDT", "price": "3500"})
        assert item.symbol == "ETHUSDT"
        assert item.price == "3500"


class TestDerivedPosition:
    """DerivedPosition: usd_price and computed usd_notional = open_qty * usd_price."""

    def test_usd_notional_none_when_usd_price_none(self):
        p = DerivedPosition(broker="", account_id="a", book="b", asset="BTC", open_qty=1.0, position_side="long")
        assert p.usd_price is None
        assert p.usd_notional is None

    def test_usd_notional_positive_long(self):
        p = DerivedPosition(
            broker="", account_id="a", book="b", asset="BTC",
            open_qty=2.5, position_side="long", usd_price=50000.0,
        )
        assert p.usd_price == 50000.0
        assert p.usd_notional == 125000.0

    def test_usd_notional_negative_short(self):
        p = DerivedPosition(
            broker="", account_id="a", book="b", asset="ETH",
            open_qty=-3.0, position_side="short", usd_price=3000.0,
        )
        assert p.usd_price == 3000.0
        assert p.usd_notional == -9000.0

    def test_usd_notional_flat_zero(self):
        p = DerivedPosition(
            broker="", account_id="a", book="b", asset="USDT",
            open_qty=0.0, position_side="flat", usd_price=1.0,
        )
        assert p.usd_notional == 0.0

    def test_usd_notional_after_model_copy_update(self):
        p = DerivedPosition(broker="", account_id="a", book="b", asset="BTC", open_qty=1.0, position_side="long")
        assert p.usd_notional is None
        p2 = p.model_copy(update={"usd_price": 60000.0})
        assert p2.usd_notional == 60000.0
