"""
Unit tests for PMS Pydantic schemas (12.3.1b): MarkPricesResult, BinanceTickerPriceItem.
"""

from decimal import Decimal

import pytest

from pms.schemas_pydantic import BinanceTickerPriceItem, MarkPricesResult


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
