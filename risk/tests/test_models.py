"""Unit tests for risk models (OrderIntent, to_risk_approved_fields)."""

import pytest
from pydantic import ValidationError

from risk.models import OrderIntent


class TestOrderIntent:
    def test_valid_minimal(self):
        o = OrderIntent(broker="binance", symbol="BTCUSDT", side="BUY", quantity=0.001)
        assert o.broker == "binance"
        assert o.symbol == "BTCUSDT"
        assert o.side == "BUY"
        assert o.quantity == 0.001
        assert o.order_type == "MARKET"
        assert o.account_id == ""
        assert o.limit_price is None

    def test_valid_full(self):
        o = OrderIntent(
            broker="binance",
            symbol="BTCUSDT",
            side="SELL",
            quantity=1.5,
            order_id="oid-1",
            account_id="acc1",
            order_type="LIMIT",
            limit_price=50000.0,
            time_in_force="GTC",
            book="ma_cross",
            comment="test",
        )
        assert o.limit_price == 50000.0
        assert o.book == "ma_cross"

    def test_missing_broker(self):
        with pytest.raises(ValidationError):
            OrderIntent(symbol="BTCUSDT", side="BUY", quantity=0.001)

    def test_missing_quantity(self):
        with pytest.raises(ValidationError):
            OrderIntent(broker="binance", symbol="BTCUSDT", side="BUY")

    def test_quantity_positive(self):
        with pytest.raises(ValidationError):
            OrderIntent(broker="binance", symbol="BTCUSDT", side="BUY", quantity=0)
        with pytest.raises(ValidationError):
            OrderIntent(broker="binance", symbol="BTCUSDT", side="BUY", quantity=-0.001)

    def test_to_risk_approved_fields(self):
        o = OrderIntent(
            broker="binance",
            symbol="BTCUSDT",
            side="BUY",
            quantity=0.001,
            order_id="x",
            limit_price=40000.0,
        )
        d = o.to_risk_approved_fields()
        assert d["broker"] == "binance"
        assert d["symbol"] == "BTCUSDT"
        assert d["side"] == "BUY"
        assert d["quantity"] == "0.001"
        assert d["price"] == "40000.0"  # OMS expects 'price' for limit
        assert "order_id" in d
        assert d["order_id"] == "x"
