"""
Unit tests for OMS Redis producer (task 12.1.8).

Mock Redis client; verify message format.
"""

import pytest

from oms.producer import format_oms_fill_event, produce_oms_fill
from oms.schemas import OMS_FILLS_FIELDS, OMS_FILLS_STREAM


class TestFormatOmsFillEvent:
    def test_required_fields(self):
        evt = format_oms_fill_event(
            event_type="fill",
            order_id="ord-1",
            broker_order_id="b-1",
            symbol="BTCUSDT",
            side="BUY",
            quantity=0.01,
        )
        assert evt["event_type"] == "fill"
        assert evt["order_id"] == "ord-1"
        assert evt["broker_order_id"] == "b-1"
        assert evt["symbol"] == "BTCUSDT"
        assert evt["side"] == "BUY"
        assert evt["quantity"] == 0.01
        assert evt["book"] == ""
        assert evt["comment"] == ""

    def test_reject_with_reason(self):
        evt = format_oms_fill_event(
            event_type="reject",
            order_id="ord-2",
            broker_order_id="",
            symbol="ETHUSDT",
            side="SELL",
            quantity=0,
            reject_reason="Insufficient balance",
            book="manual",
            comment="test",
        )
        assert evt["event_type"] == "reject"
        assert evt["reject_reason"] == "Insufficient balance"
        assert evt["book"] == "manual"
        assert evt["comment"] == "test"


class TestProduceOmsFill:
    def test_produce_writes_to_stream_with_schema_fields(self):
        from fakeredis import FakeRedis
        from oms.streams import read_latest
        redis = FakeRedis(decode_responses=True)
        event = {
            "event_type": "fill",
            "order_id": "ord-1",
            "broker_order_id": "b-1",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.001,
            "price": 50000,
            "fee": 0.1,
            "fee_asset": "BNB",
            "executed_at": "2025-01-01T12:00:00Z",
            "fill_id": "trade-1",
            "reject_reason": "",
            "book": "ma_cross",
            "comment": "test",
        }
        eid = produce_oms_fill(redis, event)
        assert eid
        entries = read_latest(redis, OMS_FILLS_STREAM, count=1)
        assert len(entries) == 1
        _id, fields = entries[0]
        for key in OMS_FILLS_FIELDS:
            assert key in fields, f"missing field {key}"
        assert fields["event_type"] == "fill"
        assert fields["order_id"] == "ord-1"
        assert fields["broker_order_id"] == "b-1"
        assert fields["book"] == "ma_cross"

    def test_produce_fills_missing_fields_with_empty_string(self):
        from fakeredis import FakeRedis
        from oms.streams import read_latest
        redis = FakeRedis(decode_responses=True)
        event = {
            "event_type": "reject",
            "order_id": "ord-2",
            "broker_order_id": "",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": "0",
        }
        produce_oms_fill(redis, event)
        entries = read_latest(redis, OMS_FILLS_STREAM, count=1)
        assert len(entries) == 1
        _id, fields = entries[0]
        assert fields["reject_reason"] == ""
        assert fields["book"] == ""
        assert fields["comment"] == ""
