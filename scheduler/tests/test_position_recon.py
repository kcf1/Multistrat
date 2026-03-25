"""Unit tests for spot position vs wallet reconciliation."""

from __future__ import annotations

from decimal import Decimal

from scheduler.config import POSITION_RECON_QTY_EPSILON
from scheduler.jobs.reconciliation.position_recon import (
    broker_balances_total,
    reconcile_spot_position_balances,
)


def test_reconcile_clean_no_diff_rows() -> None:
    r = reconcile_spot_position_balances(
        "2026-03-26T12:00:00+00:00",
        {"BTC": Decimal("1")},
        {"BTC": Decimal("1")},
        broker="binance",
        account_filter="a1",
        epsilon=POSITION_RECON_QTY_EPSILON,
        broker_free_locked={"BTC": (Decimal("0.9"), Decimal("0.1"))},
    )
    assert r.diff_rows == []
    assert r.summary["field_mismatch_rows"] == 0
    assert r.summary["matched_assets"] == 1


def test_internal_only() -> None:
    r = reconcile_spot_position_balances(
        "2026-03-26T12:00:00+00:00",
        {"BTC": Decimal("1")},
        {},
        broker="binance",
        account_filter=None,
        epsilon=POSITION_RECON_QTY_EPSILON,
    )
    assert r.summary["internal_only_rows"] == 1
    assert r.diff_rows[0]["diff_type"] == "internal_only"


def test_broker_only() -> None:
    r = reconcile_spot_position_balances(
        "2026-03-26T12:00:00+00:00",
        {},
        {"ETH": Decimal("2")},
        broker="binance",
        account_filter=None,
        epsilon=POSITION_RECON_QTY_EPSILON,
        broker_free_locked={"ETH": (Decimal("2"), Decimal("0"))},
    )
    assert r.summary["broker_only_rows"] == 1
    assert r.diff_rows[0]["binance_available"] == "2"


def test_field_mismatch_qty() -> None:
    r = reconcile_spot_position_balances(
        "2026-03-26T12:00:00+00:00",
        {"USDT": Decimal("100")},
        {"USDT": Decimal("99")},
        broker="binance",
        account_filter=None,
        epsilon=POSITION_RECON_QTY_EPSILON,
    )
    assert r.summary["field_mismatch_rows"] == 1
    assert "delta" in r.diff_rows[0]["remarks"]


def test_broker_balances_total_sums_free_locked() -> None:
    resp = {
        "balances": [
            {"asset": "BTC", "free": "1", "locked": "0.5"},
        ]
    }
    t = broker_balances_total(resp)
    assert t["BTC"] == Decimal("1.5")
