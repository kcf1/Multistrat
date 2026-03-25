"""Unit tests for Binance trade-window order reconciliation (Phase 5 §4.6)."""

from __future__ import annotations

from scheduler.jobs.reconciliation.order_recon import DIFF_HEADER, reconcile_trade_window


def _db_row(
    *,
    internal_id: str,
    broker_order_id: str,
    symbol: str = "BTCUSDT",
    side: str = "BUY",
    order_type: str = "LIMIT",
    quantity: str = "1",
    limit_price: str = "100",
    price: str = "100",
    executed_qty: str = "1",
    status: str = "filled",
    account_id: str = "acc1",
) -> dict:
    return {
        "internal_id": internal_id,
        "broker_order_id": broker_order_id,
        "account_id": account_id,
        "symbol": symbol,
        "side": side,
        "order_type": order_type,
        "quantity": quantity,
        "limit_price": limit_price,
        "price": price,
        "time_in_force": "GTC",
        "status": status,
        "executed_qty": executed_qty,
        "created_at": None,
        "updated_at": None,
        "binance_transact_time": None,
    }


def _trade(
    *,
    trade_id: int,
    order_id: int,
    symbol: str = "BTCUSDT",
    price: str = "100",
    qty: str = "1",
    quote_qty: str = "100",
    is_buyer: bool = True,
    is_maker: bool = False,
    time_ms: int = 3_000_000,
) -> dict:
    return {
        "id": trade_id,
        "orderId": order_id,
        "symbol": symbol,
        "price": price,
        "qty": qty,
        "quoteQty": quote_qty,
        "commission": "0",
        "commissionAsset": "USDT",
        "time": time_ms,
        "isBuyer": is_buyer,
        "isMaker": is_maker,
    }


def _windows() -> tuple[str, str]:
    return "2026-03-26T10:00:00+00:00", "2026-03-26T12:00:00+00:00"


def test_reconcile_trade_window_clean_match_no_diff_rows() -> None:
    recon_at = "2026-03-26T12:00:00+00:00"
    w0, w1 = _windows()
    db = [_db_row(internal_id="cid-1", broker_order_id="101")]
    broker = [_trade(trade_id=5001, order_id=101, qty="1", price="100")]
    r = reconcile_trade_window(
        recon_at, db, broker, broker="binance", account_filter=None, window_start_utc_iso=w0, window_end_utc_iso=w1
    )
    assert r.summary["matched_ids"] == 1
    assert r.summary["internal_only_rows"] == 0
    assert r.summary["broker_only_rows"] == 0
    assert r.summary["field_mismatch_rows"] == 0
    assert r.diff_rows == []


def test_internal_only_no_trades_for_order() -> None:
    recon_at = "2026-03-26T12:00:00+00:00"
    w0, w1 = _windows()
    db = [_db_row(internal_id="orphan", broker_order_id="999")]
    broker = [_trade(trade_id=1, order_id=200)]
    r = reconcile_trade_window(
        recon_at, db, broker, broker="binance", account_filter=None, window_start_utc_iso=w0, window_end_utc_iso=w1
    )
    assert r.summary["internal_only_rows"] == 1
    assert r.diff_rows[0]["diff_type"] == "internal_only"
    assert r.diff_rows[0]["db_symbol"] == "BTCUSDT"


def test_broker_only_trade_unmatched_order() -> None:
    recon_at = "2026-03-26T12:00:00+00:00"
    w0, w1 = _windows()
    db = [_db_row(internal_id="cid-1", broker_order_id="101")]
    broker = [
        _trade(trade_id=1, order_id=101, qty="1", price="100"),
        _trade(trade_id=2, order_id=9999, qty="0.1", price="50"),
    ]
    r = reconcile_trade_window(
        recon_at, db, broker, broker="binance", account_filter=None, window_start_utc_iso=w0, window_end_utc_iso=w1
    )
    only = [row for row in r.diff_rows if row["diff_type"] == "broker_only"]
    assert len(only) == 1
    assert only[0]["binance_trade_id"] == "2"


def test_field_mismatch_on_qty_aggregate() -> None:
    recon_at = "2026-03-26T12:00:00+00:00"
    w0, w1 = _windows()
    db = [_db_row(internal_id="cid-1", broker_order_id="101", executed_qty="2")]
    broker = [
        _trade(trade_id=1, order_id=101, qty="1", price="100"),
        _trade(trade_id=2, order_id=101, qty="0.5", price="200", time_ms=3_000_001),
    ]
    r = reconcile_trade_window(
        recon_at, db, broker, broker="binance", account_filter=None, window_start_utc_iso=w0, window_end_utc_iso=w1
    )
    assert r.summary["field_mismatch_rows"] == 1
    row = r.diff_rows[0]
    assert row["diff_type"] == "field_mismatch"
    assert "executed_qty" in row["remarks"]


def test_diff_header_trailing_diff_type_remarks() -> None:
    recon_at = "2026-03-26T12:00:00+00:00"
    w0, w1 = _windows()
    db = [_db_row(internal_id="i", broker_order_id="1")]
    broker = []
    r = reconcile_trade_window(
        recon_at, db, broker, broker="binance", account_filter=None, window_start_utc_iso=w0, window_end_utc_iso=w1
    )
    row = r.diff_rows[0]
    assert row["diff_type"] == "internal_only"
    assert DIFF_HEADER[-2:] == ("diff_type", "remarks")
    assert set(DIFF_HEADER) == set(row.keys())
