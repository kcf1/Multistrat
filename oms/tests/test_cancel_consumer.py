"""
Unit tests for OMS cancel_requested consumer (task 12.1.9f).
"""

import pytest
from fakeredis import FakeRedis

from oms.cancel_consumer import (
    CancelRequestParseError,
    ack_cancel_requested,
    parse_cancel_request_message,
    read_one_cancel_request,
    read_one_cancel_request_cg,
)
from oms.schemas import CANCEL_REQUESTED_STREAM


class TestParseCancelRequestMessage:
    def test_order_id_and_broker(self):
        out = parse_cancel_request_message({"order_id": "ord-1", "broker": "binance"})
        assert out["order_id"] == "ord-1"
        assert out["broker"] == "binance"
        assert out["broker_order_id"] is None
        assert out["symbol"] is None

    def test_broker_order_id_symbol_broker(self):
        out = parse_cancel_request_message({
            "broker_order_id": "12345",
            "symbol": "BTCUSDT",
            "broker": "binance",
        })
        assert out["order_id"] is None
        assert out["broker_order_id"] == "12345"
        assert out["symbol"] == "BTCUSDT"
        assert out["broker"] == "binance"

    def test_missing_broker_raises(self):
        with pytest.raises(CancelRequestParseError, match="missing broker"):
            parse_cancel_request_message({"order_id": "ord-1"})
        with pytest.raises(CancelRequestParseError, match="missing broker"):
            parse_cancel_request_message({"broker_order_id": "1", "symbol": "BTCUSDT"})

    def test_need_order_id_or_broker_order_id_and_symbol(self):
        with pytest.raises(CancelRequestParseError, match="need order_id"):
            parse_cancel_request_message({"broker": "binance"})
        with pytest.raises(CancelRequestParseError, match="need order_id"):
            parse_cancel_request_message({"broker": "binance", "broker_order_id": "1"})
        with pytest.raises(CancelRequestParseError, match="need order_id"):
            parse_cancel_request_message({"broker": "binance", "symbol": "BTCUSDT"})


class TestReadOneCancelRequest:
    @pytest.fixture
    def redis_client(self):
        return FakeRedis(decode_responses=True)

    def test_no_message_returns_none(self, redis_client):
        assert read_one_cancel_request(redis_client, start_id="0") is None

    def test_reads_and_parses_by_order_id(self, redis_client):
        redis_client.xadd(CANCEL_REQUESTED_STREAM, {"order_id": "ord-99", "broker": "binance"})
        out = read_one_cancel_request(redis_client, start_id="0")
        assert out is not None
        entry_id, req = out
        assert entry_id
        assert req["order_id"] == "ord-99"
        assert req["broker"] == "binance"

    def test_reads_by_broker_order_id_and_symbol(self, redis_client):
        redis_client.xadd(CANCEL_REQUESTED_STREAM, {
            "broker_order_id": "999",
            "symbol": "ETHUSDT",
            "broker": "binance",
        })
        out = read_one_cancel_request(redis_client, start_id="0")
        assert out is not None
        _, req = out
        assert req["broker_order_id"] == "999"
        assert req["symbol"] == "ETHUSDT"
        assert req["broker"] == "binance"

    def test_invalid_message_returns_none(self, redis_client):
        redis_client.xadd(CANCEL_REQUESTED_STREAM, {"broker": "binance"})
        out = read_one_cancel_request(redis_client, start_id="0")
        assert out is None


class TestCancelRequestConsumerGroup:
    """Consumer group: read_one_cancel_request_cg and ack_cancel_requested."""

    @pytest.fixture
    def redis_client(self):
        return FakeRedis(decode_responses=True)

    def test_read_cg_and_ack(self, redis_client):
        redis_client.xadd(CANCEL_REQUESTED_STREAM, {"order_id": "ord-cg-1", "broker": "binance"})
        out = read_one_cancel_request_cg(redis_client, "oms", "oms-1")
        assert out is not None
        entry_id, req = out
        assert req["order_id"] == "ord-cg-1"
        ack_cancel_requested(redis_client, "oms", entry_id)
        out2 = read_one_cancel_request_cg(redis_client, "oms", "oms-1")
        assert out2 is None, "After ack, same consumer gets no message"

    def test_second_read_returns_next_message_not_first(self, redis_client):
        redis_client.xadd(CANCEL_REQUESTED_STREAM, {"order_id": "ord-1", "broker": "binance"})
        redis_client.xadd(CANCEL_REQUESTED_STREAM, {"order_id": "ord-2", "broker": "binance"})
        out1 = read_one_cancel_request_cg(redis_client, "oms", "oms-1")
        assert out1 is not None
        eid1, req1 = out1
        assert req1["order_id"] == "ord-1"
        ack_cancel_requested(redis_client, "oms", eid1)
        out2 = read_one_cancel_request_cg(redis_client, "oms", "oms-1")
        assert out2 is not None
        _, req2 = out2
        assert req2["order_id"] == "ord-2", "Second read must return second message (no double-process)"
