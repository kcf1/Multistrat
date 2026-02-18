"""
Unit tests for OMS Redis stream and order-key cleanup (task 12.1.9b).

Uses fakeredis to verify trim_oms_streams (XTRIM) and set_order_key_ttl.
"""

import pytest
from fakeredis import FakeRedis

from oms.cleanup import (
    ACCOUNT_KEY_PREFIX,
    DEFAULT_STREAM_MAXLEN,
    ORDER_KEY_PREFIX,
    set_account_key_ttl,
    set_order_key_ttl,
    trim_oms_streams,
)
from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM


@pytest.fixture
def redis_client():
    return FakeRedis(decode_responses=True)


class TestTrimOmsStreams:
    def test_trim_oms_streams_skips_missing_streams(self, redis_client):
        removed = trim_oms_streams(redis_client, maxlen=10)
        assert removed == 0

    def test_trim_oms_streams_trims_risk_approved(self, redis_client):
        for i in range(5):
            redis_client.xadd(RISK_APPROVED_STREAM, {"i": str(i)})
        assert redis_client.xlen(RISK_APPROVED_STREAM) == 5
        removed = trim_oms_streams(redis_client, maxlen=2)
        assert redis_client.xlen(RISK_APPROVED_STREAM) <= 2
        assert removed >= 3

    def test_trim_oms_streams_trims_oms_fills(self, redis_client):
        for i in range(5):
            redis_client.xadd(OMS_FILLS_STREAM, {"event_type": "fill", "i": str(i)})
        assert redis_client.xlen(OMS_FILLS_STREAM) == 5
        removed = trim_oms_streams(redis_client, maxlen=2)
        assert redis_client.xlen(OMS_FILLS_STREAM) <= 2
        assert removed >= 3

    def test_trim_oms_streams_respects_flags(self, redis_client):
        for i in range(3):
            redis_client.xadd(RISK_APPROVED_STREAM, {"i": str(i)})
            redis_client.xadd(OMS_FILLS_STREAM, {"i": str(i)})
        removed = trim_oms_streams(
            redis_client, maxlen=1, risk_approved=True, oms_fills=False
        )
        assert redis_client.xlen(RISK_APPROVED_STREAM) <= 1
        assert redis_client.xlen(OMS_FILLS_STREAM) == 3
        removed2 = trim_oms_streams(
            redis_client, maxlen=1, risk_approved=False, oms_fills=True
        )
        assert redis_client.xlen(OMS_FILLS_STREAM) <= 1

    def test_trim_oms_streams_uses_default_maxlen(self, redis_client):
        for i in range(DEFAULT_STREAM_MAXLEN + 100):
            redis_client.xadd(RISK_APPROVED_STREAM, {"i": str(i)})
        removed = trim_oms_streams(redis_client)
        assert redis_client.xlen(RISK_APPROVED_STREAM) <= DEFAULT_STREAM_MAXLEN
        assert removed >= 100


class TestSetOrderKeyTtl:
    def test_set_order_key_ttl_sets_expire(self, redis_client):
        order_id = "ord-ttl-1"
        key = ORDER_KEY_PREFIX + order_id
        redis_client.hset(key, mapping={"status": "filled", "symbol": "BTCUSDT"})
        assert redis_client.ttl(key) == -1
        ok = set_order_key_ttl(redis_client, order_id, 300)
        assert ok is True
        assert redis_client.ttl(key) == 300

    def test_set_order_key_ttl_returns_false_when_key_missing(self, redis_client):
        ok = set_order_key_ttl(redis_client, "nonexistent", 300)
        assert ok is False

    def test_set_order_key_ttl_skips_when_key_already_has_ttl(self, redis_client):
        """TTL is set only when key has none; periodic sync does not refresh it."""
        order_id = "ord-ttl-2"
        key = ORDER_KEY_PREFIX + order_id
        redis_client.hset(key, mapping={"status": "filled"})
        set_order_key_ttl(redis_client, order_id, 300)
        assert redis_client.ttl(key) == 300
        # Second call with different TTL should not change expiry (returns False, TTL unchanged)
        ok = set_order_key_ttl(redis_client, order_id, 600)
        assert ok is False
        assert redis_client.ttl(key) == 300


class TestSetAccountKeyTtl:
    """Unit tests for set_account_key_ttl (task 12.2.10)."""

    def test_set_account_key_ttl_sets_expire_on_all_keys(self, redis_client):
        broker, account_id = "binance", "default"
        base = f"{ACCOUNT_KEY_PREFIX}{broker}:{account_id}"
        redis_client.hset(base, mapping={"broker": broker, "account_id": account_id})
        redis_client.hset(f"{base}:balances", mapping={"USDT": "1000"})
        redis_client.hset(f"{base}:positions", mapping={"BTCUSDT": "0.1"})
        for key in [base, f"{base}:balances", f"{base}:positions"]:
            assert redis_client.ttl(key) == -1
        n = set_account_key_ttl(redis_client, broker, account_id, 60)
        assert n == 3
        assert redis_client.ttl(base) == 60
        assert redis_client.ttl(f"{base}:balances") == 60
        assert redis_client.ttl(f"{base}:positions") == 60

    def test_set_account_key_ttl_skips_missing_keys(self, redis_client):
        redis_client.hset("account:binance:default", mapping={"broker": "binance"})
        n = set_account_key_ttl(redis_client, "binance", "default", 60)
        assert n == 1
        n2 = set_account_key_ttl(redis_client, "binance", "nonexistent", 60)
        assert n2 == 0

    def test_set_account_key_ttl_skips_keys_already_with_ttl(self, redis_client):
        broker, account_id = "binance", "acc1"
        base = f"{ACCOUNT_KEY_PREFIX}{broker}:{account_id}"
        redis_client.hset(base, mapping={"broker": broker})
        redis_client.expire(base, 120)
        n = set_account_key_ttl(redis_client, broker, account_id, 60)
        assert n == 0
        assert redis_client.ttl(base) == 120
