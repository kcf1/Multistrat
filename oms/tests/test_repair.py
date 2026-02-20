"""
Unit tests for OMS order repairs (Binance payload recovery).
"""

import pytest

from oms.repair import (
    _binance_status_to_oms,
    _extract_from_binance_payload,
    repair_binance_cumulative_quote_qty_from_payload,
    repair_binance_price_from_payload,
    repair_binance_status_from_payload,
    repair_binance_time_in_force_from_payload,
    run_all_repairs,
)


class TestExtractFromBinancePayload:
    def test_extracts_price_from_avg_price(self):
        payload = {"binance": {"avgPrice": "50100.5", "orderId": "123"}}
        out = _extract_from_binance_payload(payload)
        assert out["price"] == 50100.5

    def test_extracts_price_from_fills_when_avg_missing(self):
        payload = {
            "binance": {
                "avgPrice": "",
                "fills": [{"price": "50200", "qty": "0.001"}],
            }
        }
        out = _extract_from_binance_payload(payload)
        assert out["price"] == 50200.0

    def test_extracts_time_in_force(self):
        payload = {"binance": {"timeInForce": "GTC", "orderId": "123"}}
        out = _extract_from_binance_payload(payload)
        assert out["time_in_force"] == "GTC"

    def test_extracts_cumulative_quote_qty(self):
        payload = {"binance": {"cumulativeQuoteQty": "5010.25", "orderId": "123"}}
        out = _extract_from_binance_payload(payload)
        assert out["binance_cumulative_quote_qty"] == 5010.25

    def test_extracts_status(self):
        payload = {"binance": {"status": "FILLED", "orderId": "123"}}
        out = _extract_from_binance_payload(payload)
        assert out["status"] == "filled"
        assert _extract_from_binance_payload({"binance": {"status": "NEW"}})["status"] == "sent"
        assert _extract_from_binance_payload({"binance": {"status": "CANCELED"}})["status"] == "cancelled"

    def test_empty_payload_returns_empty(self):
        assert _extract_from_binance_payload({}) == {}
        assert _extract_from_binance_payload(None) == {}


def _mock_pg_connect(select_rows=None):
    """Mock pg_connect that returns SELECT rows and tracks UPDATEs."""
    updates = []
    if select_rows is None:
        select_rows = []

    class MockCursor:
        def execute(self, sql, params=None):
            self._last_sql = sql
            self._last_params = params or ()
            if sql.strip().upper().startswith("SELECT"):
                self._rows = select_rows
            elif sql.strip().upper().startswith("UPDATE"):
                self._rowcount = 1
                updates.append((sql, params))

        def fetchall(self):
            return getattr(self, "_rows", [])

        @property
        def rowcount(self):
            return getattr(self, "_rowcount", 0)

    class MockConn:
        def cursor(self):
            return MockCursor()

        def commit(self):
            pass

    def connect():
        return MockConn()

    return connect, updates


class TestRepairFunctions:
    def test_repair_price_updates_when_flawed(self):
        rows = [
            (
                "ord-1",
                {"binance": {"avgPrice": "50000", "orderId": "1"}},
            ),
        ]
        connect, updates = _mock_pg_connect(select_rows=rows)
        n = repair_binance_price_from_payload(connect)
        assert n == 1
        assert len(updates) == 1
        sql, params = updates[0]
        assert "UPDATE orders SET price" in sql
        assert params[0] == 50000.0

    def test_repair_price_skips_when_no_valid_price_in_payload(self):
        rows = [("ord-1", {"binance": {"orderId": "1"}})]  # no avgPrice
        connect, updates = _mock_pg_connect(select_rows=rows)
        n = repair_binance_price_from_payload(connect)
        assert n == 0
        assert len(updates) == 0

    def test_repair_time_in_force_updates(self):
        rows = [("ord-1", {"binance": {"timeInForce": "IOC", "orderId": "1"}})]
        connect, updates = _mock_pg_connect(select_rows=rows)
        n = repair_binance_time_in_force_from_payload(connect)
        assert n == 1
        assert len(updates) == 1
        sql, params = updates[0]
        assert "time_in_force" in sql
        assert params[0] == "IOC"

    def test_repair_cumulative_quote_qty_updates(self):
        rows = [
            (
                "ord-1",
                {"binance": {"cumulativeQuoteQty": "1000.5", "orderId": "1"}},
            ),
        ]
        connect, updates = _mock_pg_connect(select_rows=rows)
        n = repair_binance_cumulative_quote_qty_from_payload(connect)
        assert n == 1
        assert len(updates) == 1
        sql, params = updates[0]
        assert "binance_cumulative_quote_qty" in sql
        assert params[0] == 1000.5

    def test_repair_status_fallback_when_empty(self):
        """Only update when DB status is NULL/empty; then set from payload."""
        rows = [
            ("ord-1", None, {"binance": {"status": "FILLED", "orderId": "1"}}),
        ]
        connect, updates = _mock_pg_connect(select_rows=rows)
        n = repair_binance_status_from_payload(connect)
        assert n == 1
        assert len(updates) == 1
        assert updates[0][1][0] == "filled"

    def test_repair_status_unchanged_when_payload_has_no_status(self):
        """When payload has no status we skip (remain unchanged)."""
        rows = [
            ("ord-1", None, {"binance": {"orderId": "1"}}),  # no status in payload
        ]
        connect, updates = _mock_pg_connect(select_rows=rows)
        n = repair_binance_status_from_payload(connect)
        assert n == 0
        assert len(updates) == 0

    def test_binance_status_to_oms_mapping(self):
        assert _binance_status_to_oms("FILLED") == "filled"
        assert _binance_status_to_oms("NEW") == "sent"
        assert _binance_status_to_oms("CANCELED") == "cancelled"
        assert _binance_status_to_oms("REJECTED") == "rejected"
        assert _binance_status_to_oms("EXPIRED") == "expired"
        assert _binance_status_to_oms("PARTIALLY_FILLED") == "partially_filled"
        assert _binance_status_to_oms("unknown") is None
        assert _binance_status_to_oms("") is None

    def test_run_all_repairs_calls_each(self):
        connect, _ = _mock_pg_connect(select_rows=[])  # no rows to repair
        total = run_all_repairs(connect)
        assert total == 0
