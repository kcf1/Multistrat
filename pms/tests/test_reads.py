"""
Unit tests for PMS Postgres reads and position derivation (12.3.3).
Quantity uses executed_quantity (from orders.executed_qty).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from pms.reads import (
    POSITION_ORDER_STATUSES,
    derive_positions_from_orders,
    query_orders_for_positions,
)
from pms.schemas_pydantic import DerivedPosition, OrderRow


class TestDerivePositionsFromOrders:
    """Test position derivation; uses executed_quantity."""

    def test_empty_returns_empty(self):
        assert derive_positions_from_orders([]) == []

    def test_single_buy(self):
        rows = [
            {"account_id": "acc1", "book": "b1", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        assert pos[0].account_id == "acc1"
        assert pos[0].book == "b1"
        assert pos[0].symbol == "BTCUSDT"
        assert pos[0].open_qty == 1.0
        assert pos[0].position_side == "long"
        assert pos[0].entry_avg == 50000.0

    def test_buy_then_sell_flat(self):
        rows = [
            {"account_id": "a", "book": "", "symbol": "ETHUSDT", "side": "BUY", "executed_quantity": 2.0, "price": 3000.0},
            {"account_id": "a", "book": "", "symbol": "ETHUSDT", "side": "SELL", "executed_quantity": 2.0, "price": 3100.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        assert pos[0].open_qty == 0.0
        assert pos[0].position_side == "flat"
        assert pos[0].entry_avg is None

    def test_partial_sell_leaves_long(self):
        rows = [
            {"account_id": "a", "book": "b", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 3.0, "price": 40000.0},
            {"account_id": "a", "book": "b", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 41000.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        assert pos[0].open_qty == 2.0
        assert pos[0].position_side == "long"
        # FIFO: remaining 2 from first buy at 40000
        assert pos[0].entry_avg == 40000.0

    def test_short_position(self):
        rows = [
            {"account_id": "a", "book": "", "symbol": "XRPUSDT", "side": "SELL", "executed_quantity": 10.0, "price": 0.5},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        assert pos[0].open_qty == -10.0
        assert pos[0].position_side == "short"
        assert pos[0].entry_avg == 0.5

    def test_accepts_executed_qty_key(self):
        """When row has executed_qty (DB name), derivation uses it as executed_quantity."""
        rows = [
            {"account_id": "a", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_qty": 0.5, "price": 60000.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        assert pos[0].open_qty == 0.5

    def test_accepts_order_row_model(self):
        row = OrderRow(
            account_id="a", book="b", symbol="BTCUSDT", side="BUY",
            executed_quantity=1.25, price=55000.0,
        )
        pos = derive_positions_from_orders([row])
        assert len(pos) == 1
        assert pos[0].open_qty == 1.25
        assert pos[0].entry_avg == 55000.0

    def test_multiple_groups(self):
        rows = [
            {"account_id": "a1", "book": "b1", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
            {"account_id": "a1", "book": "b1", "symbol": "ETHUSDT", "side": "BUY", "executed_quantity": 2.0, "price": 3000.0},
            {"account_id": "a2", "book": "b1", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 0.5, "price": 51000.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 3
        by_key = {(p.account_id, p.symbol): p for p in pos}
        assert by_key[("a1", "BTCUSDT")].open_qty == 1.0
        assert by_key[("a1", "ETHUSDT")].open_qty == 2.0
        assert by_key[("a2", "BTCUSDT")].open_qty == 0.5

    def test_two_buys_at_one_then_one_sell_at_two(self):
        """2 fake buys at $1, then 1 fake sell at $2 → net long 1, entry_avg $1 (FIFO)."""
        rows = [
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 1.0},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 1.0},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 2.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        p = pos[0]
        assert p.account_id == "acc"
        assert p.symbol == "BTCUSDT"
        assert p.open_qty == 1.0
        assert p.position_side == "long"
        assert p.entry_avg == 1.0  # FIFO: remaining unit from first buy at $1

    def test_two_buys_then_three_sells_including_two_at_half(self):
        """2 buys at $1, 1 sell at $2, then 2 sells at $0.5 → net short 1 at entry_avg $0.5."""
        rows = [
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 1.0},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 1.0},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 2.0},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 0.5},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 0.5},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        p = pos[0]
        assert p.account_id == "acc"
        assert p.symbol == "BTCUSDT"
        assert p.open_qty == -1.0
        assert p.position_side == "short"
        assert p.entry_avg == 0.5  # short opened at $0.5 (both sells at 0.5 → avg 0.5)


class TestQueryOrdersForPositions:
    """Test query_orders_for_positions uses executed_qty and exposes executed_quantity."""

    def test_filter_status_and_order_by_created_at(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [
            ("account_id",), ("book",), ("symbol",), ("side",),
            ("executed_qty",), ("price",), ("created_at",),
        ]
        mock_cur.fetchall.return_value = [
            ("acc1", "b1", "BTCUSDT", "BUY", Decimal("1.0"), Decimal("50000"), None),
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        with patch("psycopg2.connect", return_value=mock_conn):
            rows = query_orders_for_positions("postgres://localhost/db")
        assert mock_cur.execute.called
        call_args = mock_cur.execute.call_args[0][0]
        assert "partially_filled" in call_args or "%s" in call_args
        assert "executed_qty" in call_args
        assert "ORDER BY created_at" in call_args
        assert len(rows) == 1
        assert rows[0]["executed_quantity"] == 1.0
        assert "executed_qty" not in rows[0]

    def test_callable_connect(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("account_id",), ("book",), ("symbol",), ("side",), ("executed_qty",), ("price",), ("created_at",)]
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        rows = query_orders_for_positions(lambda: mock_conn)
        assert rows == []


class TestPositionOrderStatuses:
    def test_includes_partially_filled_and_filled(self):
        assert "partially_filled" in POSITION_ORDER_STATUSES
        assert "filled" in POSITION_ORDER_STATUSES
