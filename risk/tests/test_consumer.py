"""Unit tests for risk consumer (parse strategy_orders message, read_one)."""

import pytest

from risk.consumer import (
    OrderIntentParseError,
    parse_strategy_order_message,
    read_one_strategy_order,
)
from risk.schemas import STRATEGY_ORDERS_STREAM


class TestParseStrategyOrderMessage:
    def test_valid_minimal(self):
        fields = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "0.001"}
        o = parse_strategy_order_message(fields)
        assert o.broker == "binance"
        assert o.symbol == "BTCUSDT"
        assert o.side == "BUY"
        assert o.quantity == 0.001

    def test_price_maps_to_limit_price(self):
        fields = {
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": "1",
            "price": "50000",
        }
        o = parse_strategy_order_message(fields)
        assert o.limit_price == 50000.0

    def test_missing_broker(self):
        fields = {"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.001"}
        with pytest.raises(OrderIntentParseError):
            parse_strategy_order_message(fields)

    def test_missing_quantity(self):
        fields = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY"}
        with pytest.raises(OrderIntentParseError):
            parse_strategy_order_message(fields)

    def test_invalid_quantity(self):
        fields = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "x"}
        with pytest.raises(OrderIntentParseError):
            parse_strategy_order_message(fields)

    def test_not_dict(self):
        with pytest.raises(OrderIntentParseError):
            parse_strategy_order_message(None)  # type: ignore[arg-type]


class TestReadOneStrategyOrder:
    def test_empty_stream_returns_none(self):
        import fakeredis
        redis = fakeredis.FakeRedis(decode_responses=True)
        out = read_one_strategy_order(redis, start_id="0", block_ms=0)
        assert out is None

    def test_one_message_returns_parsed_intent(self):
        import fakeredis
        redis = fakeredis.FakeRedis(decode_responses=True)
        redis.xadd(
            STRATEGY_ORDERS_STREAM,
            {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "0.001"},
        )
        out = read_one_strategy_order(redis, start_id="0", block_ms=0)
        assert out is not None
        entry_id, intent = out
        assert entry_id
        assert intent.broker == "binance"
        assert intent.quantity == 0.001
