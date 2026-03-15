"""Unit tests for risk producer (publish to risk_approved)."""

import pytest
from redis import Redis

from risk.models import OrderIntent
from risk.producer import publish_risk_approved
from risk.schemas import RISK_APPROVED_STREAM


@pytest.fixture
def redis_client():
    import fakeredis
    return fakeredis.FakeRedis(decode_responses=True)


def test_publish_risk_approved(redis_client: Redis):
    order = OrderIntent(
        broker="binance",
        symbol="BTCUSDT",
        side="BUY",
        quantity=0.001,
        order_type="MARKET",
    )
    eid = publish_risk_approved(redis_client, order)
    assert eid is not None
    entries = redis_client.xrange(RISK_APPROVED_STREAM)
    assert len(entries) == 1
    _id, fields = entries[0]
    assert fields["broker"] == "binance"
    assert fields["symbol"] == "BTCUSDT"
    assert fields["side"] == "BUY"
    assert fields["quantity"] == "0.001"
    assert fields["order_type"] == "MARKET"
