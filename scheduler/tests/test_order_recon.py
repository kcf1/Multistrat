"""Unit tests for Binance order reconciliation (Phase 5 §4.6)."""

from __future__ import annotations

from scheduler.jobs.reconciliation.order_recon import reconcile_open_orders


def test_reconcile_matched_and_mismatches() -> None:
    recon_at = "2026-03-26T12:00:00+00:00"
    internal = [
        ("cid-1", "101", "BTCUSDT", "BUY", "sent", "1", "0", "acc1"),
        ("cid-2", "102", "ETHUSDT", "SELL", "pending", "2", "0", "acc1"),
        ("orphan", "", "XRPUSDT", "BUY", "sent", "1", "0", "acc1"),
    ]
    binance = [
        {
            "symbol": "BTCUSDT",
            "orderId": 101,
            "clientOrderId": "cid-1",
            "status": "NEW",
            "origQty": "1",
            "executedQty": "0",
        },
        {
            "symbol": "ETHUSDT",
            "orderId": 102,
            "clientOrderId": "cid-2",
            "status": "NEW",
            "origQty": "2",
            "executedQty": "0",
        },
        {
            "symbol": "DOGEUSDT",
            "orderId": 999,
            "clientOrderId": "external-manual",
            "status": "NEW",
            "origQty": "100",
            "executedQty": "0",
        },
    ]
    r = reconcile_open_orders(recon_at, internal, binance, broker="binance", account_filter=None)
    assert r.summary["internal_open_rows"] == 3
    assert r.summary["broker_open_rows"] == 3
    assert r.summary["matched_pairs"] == 2
    assert r.summary["internal_only_rows"] == 1
    assert r.summary["broker_only_rows"] == 1
    assert r.summary["status_mismatch_rows"] == 0

    types = {d["diff_type"] for d in r.diff_rows}
    assert "internal_only" in types
    assert "broker_only" in types


def test_reconcile_status_mismatch() -> None:
    recon_at = "2026-03-26T12:00:00+00:00"
    internal = [
        ("cid-x", "55", "BTCUSDT", "BUY", "filled", "1", "1", "acc1"),
    ]
    binance = [
        {
            "symbol": "BTCUSDT",
            "orderId": 55,
            "clientOrderId": "cid-x",
            "status": "NEW",
            "origQty": "1",
            "executedQty": "0",
        },
    ]
    r = reconcile_open_orders(recon_at, internal, binance, broker="binance", account_filter=None)
    assert any(d["diff_type"] == "status_mismatch" for d in r.diff_rows)
    # Still matched same broker row → not broker_only
    assert r.summary["matched_pairs"] == 1
