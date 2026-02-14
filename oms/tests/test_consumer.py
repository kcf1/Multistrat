"""
Unit tests for OMS Redis consumer (task 12.1.6).

Mock Redis client; verify message parsing and error handling.
"""

import pytest

from oms.consumer import (
    RiskApprovedParseError,
    parse_risk_approved_message,
    read_one_risk_approved,
    read_risk_approved,
)


class TestParseRiskApprovedMessage:
    def test_parses_valid_message(self):
        fields = {
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": "0.01",
            "order_type": "MARKET",
            "book": "ma_cross",
            "comment": "test",
        }
        order = parse_risk_approved_message(fields)
        assert order["broker"] == "binance"
        assert order["symbol"] == "BTCUSDT"
        assert order["side"] == "BUY"
        assert order["quantity"] == 0.01
        assert order["order_type"] == "MARKET"
        assert order["book"] == "ma_cross"
        assert order["comment"] == "test"
        assert order.get("limit_price") is None
        assert "order_id" not in order

    def test_parses_with_order_id_and_limit_price(self):
        """risk_approved 'price' is limit price -> stored as limit_price (12.1.12)."""
        fields = {
            "broker": "binance",
            "symbol": "ETHUSDT",
            "side": "SELL",
            "quantity": "0.5",
            "order_type": "LIMIT",
            "price": "3000",
            "order_id": "ord-123",
        }
        order = parse_risk_approved_message(fields)
        assert order["order_id"] == "ord-123"
        assert order["limit_price"] == 3000.0

    def test_missing_broker_raises(self):
        fields = {"symbol": "BTCUSDT", "side": "BUY", "quantity": "0.01"}
        with pytest.raises(RiskApprovedParseError, match="missing broker"):
            parse_risk_approved_message(fields)

    def test_missing_symbol_raises(self):
        fields = {"broker": "binance", "side": "BUY", "quantity": "0.01"}
        with pytest.raises(RiskApprovedParseError, match="missing symbol"):
            parse_risk_approved_message(fields)

    def test_missing_side_raises(self):
        fields = {"broker": "binance", "symbol": "BTCUSDT", "quantity": "0.01"}
        with pytest.raises(RiskApprovedParseError, match="missing side"):
            parse_risk_approved_message(fields)

    def test_missing_quantity_raises(self):
        fields = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY"}
        with pytest.raises(RiskApprovedParseError, match="missing quantity"):
            parse_risk_approved_message(fields)

    def test_invalid_quantity_raises(self):
        fields = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "abc"}
        with pytest.raises(RiskApprovedParseError, match="invalid quantity"):
            parse_risk_approved_message(fields)

    def test_quantity_zero_raises(self):
        fields = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "0"}
        with pytest.raises(RiskApprovedParseError, match="positive"):
            parse_risk_approved_message(fields)

    def test_fields_not_dict_raises(self):
        with pytest.raises(RiskApprovedParseError, match="must be a dict"):
            parse_risk_approved_message(None)  # type: ignore[arg-type]


class TestReadRiskApproved:
    def test_read_one_empty_stream_returns_none(self):
        from fakeredis import FakeRedis
        redis = FakeRedis(decode_responses=True)
        out = read_one_risk_approved(redis, start_id="0")
        assert out is None

    def test_read_one_returns_parsed_message(self):
        from fakeredis import FakeRedis
        from oms.schemas import RISK_APPROVED_STREAM
        redis = FakeRedis(decode_responses=True)
        redis.xadd(RISK_APPROVED_STREAM, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "0.001"})
        out = read_one_risk_approved(redis, start_id="0")
        assert out is not None
        entry_id, order = out
        assert order["broker"] == "binance"
        assert order["symbol"] == "BTCUSDT"
        assert order["quantity"] == 0.001

    def test_read_risk_approved_skips_invalid_message(self):
        from fakeredis import FakeRedis
        from oms.schemas import RISK_APPROVED_STREAM
        redis = FakeRedis(decode_responses=True)
        redis.xadd(RISK_APPROVED_STREAM, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY"})  # no quantity
        messages = read_risk_approved(redis, start_id="0", count=5)
        assert messages == []


class TestConsumerGroup:
    """Consumer group: read_one_risk_approved_cg and ack_risk_approved."""

    def test_read_cg_returns_message_then_none_after_ack(self):
        from fakeredis import FakeRedis
        from oms.consumer import ack_risk_approved, read_one_risk_approved_cg
        from oms.schemas import RISK_APPROVED_STREAM
        redis = FakeRedis(decode_responses=True)
        redis.xadd(RISK_APPROVED_STREAM, {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": "0.001"})
        out = read_one_risk_approved_cg(redis, "oms", "oms-1")
        assert out is not None
        entry_id, order = out
        assert order["symbol"] == "BTCUSDT"
        ack_risk_approved(redis, "oms", entry_id)
        out2 = read_one_risk_approved_cg(redis, "oms", "oms-1")
        assert out2 is None
