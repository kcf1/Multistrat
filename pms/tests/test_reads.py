"""
Unit tests for PMS Postgres reads and position derivation (12.3.3).
Quantity uses executed_quantity (from orders.executed_qty).
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from pms.reads import (
    POSITION_ORDER_STATUSES,
    derive_asset_positions_from_orders,
    derive_positions_from_orders,
    derive_positions_from_orders_and_balance_changes,
    query_assets_usd_config,
    query_balance_changes_net_by_account_book_asset,
    query_orders_for_positions,
    query_symbol_map,
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
        assert pos[0].asset == "BTCUSDT"
        assert pos[0].open_qty == 1.0
        assert pos[0].position_side == "long"

    def test_buy_then_sell_flat(self):
        rows = [
            {"account_id": "a", "book": "", "symbol": "ETHUSDT", "side": "BUY", "executed_quantity": 2.0, "price": 3000.0},
            {"account_id": "a", "book": "", "symbol": "ETHUSDT", "side": "SELL", "executed_quantity": 2.0, "price": 3100.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        assert pos[0].open_qty == 0.0
        assert pos[0].position_side == "flat"

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

    def test_short_position(self):
        rows = [
            {"account_id": "a", "book": "", "symbol": "XRPUSDT", "side": "SELL", "executed_quantity": 10.0, "price": 0.5},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        assert pos[0].open_qty == -10.0
        assert pos[0].position_side == "short"

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

    def test_multiple_groups(self):
        rows = [
            {"account_id": "a1", "book": "b1", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
            {"account_id": "a1", "book": "b1", "symbol": "ETHUSDT", "side": "BUY", "executed_quantity": 2.0, "price": 3000.0},
            {"account_id": "a2", "book": "b1", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 0.5, "price": 51000.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 3
        by_key = {(p.account_id, p.asset): p for p in pos}
        assert by_key[("a1", "BTCUSDT")].open_qty == 1.0
        assert by_key[("a1", "ETHUSDT")].open_qty == 2.0
        assert by_key[("a2", "BTCUSDT")].open_qty == 0.5

    def test_two_buys_at_one_then_one_sell_at_two(self):
        """2 fake buys at $1, then 1 fake sell at $2 → net long 1 (FIFO)."""
        rows = [
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 1.0},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 1.0},
            {"account_id": "acc", "book": "", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 2.0},
        ]
        pos = derive_positions_from_orders(rows)
        assert len(pos) == 1
        p = pos[0]
        assert p.account_id == "acc"
        assert p.asset == "BTCUSDT"
        assert p.open_qty == 1.0
        assert p.position_side == "long"
        # FIFO: remaining unit from first buy at $1

    def test_two_buys_then_three_sells_including_two_at_half(self):
        """2 buys at $1, 1 sell at $2, then 2 sells at $0.5 → net short 1."""
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
        assert p.asset == "BTCUSDT"
        assert p.open_qty == -1.0
        assert p.position_side == "short"


class TestQuerySymbolMap:
    """Test query_symbol_map returns symbol -> (base_asset, quote_asset) from symbols table."""

    def test_returns_dict_keyed_by_uppercase_symbol(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("symbol",), ("base_asset",), ("quote_asset",)]
        mock_cur.fetchall.return_value = [
            ("BTCUSDT", "BTC", "USDT"),
            ("ethusdt", "ETH", "USDT"),
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        result = query_symbol_map(lambda: mock_conn)
        assert result == {
            "BTCUSDT": ("BTC", "USDT"),
            "ETHUSDT": ("ETH", "USDT"),
        }

    def test_skips_rows_with_empty_symbol_or_assets(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("symbol",), ("base_asset",), ("quote_asset",)]
        mock_cur.fetchall.return_value = [
            ("BTCUSDT", "BTC", "USDT"),
            ("", "X", "Y"),
            ("ETHUSDT", "", "USDT"),
            ("XRPUSDT", "XRP", "USDT"),
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        result = query_symbol_map(lambda: mock_conn)
        assert result == {
            "BTCUSDT": ("BTC", "USDT"),
            "XRPUSDT": ("XRP", "USDT"),
        }

    def test_empty_table_returns_empty_dict(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("symbol",), ("base_asset",), ("quote_asset",)]
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        result = query_symbol_map(lambda: mock_conn)
        assert result == {}


class TestDeriveAssetPositionsFromOrders:
    """Test order → base/quote legs at asset grain; uses symbol map."""

    def test_buy_btcusdt_produces_base_and_quote_legs(self):
        symbol_map = {"BTCUSDT": ("BTC", "USDT")}
        rows = [
            {
                "account_id": "acc1", "book": "b1", "symbol": "BTCUSDT", "side": "BUY",
                "executed_quantity": 1.0, "price": 50000.0,
            },
        ]
        pos = derive_asset_positions_from_orders(rows, symbol_map)
        assert len(pos) == 2
        by_asset = {p["asset"]: p for p in pos}
        assert by_asset["BTC"]["account_id"] == "acc1"
        assert by_asset["BTC"]["book"] == "b1"
        assert by_asset["BTC"]["open_qty"] == 1.0
        assert by_asset["BTC"]["position_side"] == "long"
        assert by_asset["BTC"]["entry_avg"] == 50000.0
        assert by_asset["USDT"]["open_qty"] == -50000.0
        assert by_asset["USDT"]["position_side"] == "short"
        assert by_asset["USDT"]["entry_avg"] == 1.0

    def test_sell_ethusdt_produces_negative_base_positive_quote(self):
        symbol_map = {"ETHUSDT": ("ETH", "USDT")}
        rows = [
            {
                "account_id": "a", "book": "", "symbol": "ETHUSDT", "side": "SELL",
                "executed_quantity": 2.0, "price": 3000.0,
            },
        ]
        pos = derive_asset_positions_from_orders(rows, symbol_map)
        by_asset = {p["asset"]: p for p in pos}
        assert by_asset["ETH"]["open_qty"] == -2.0
        assert by_asset["ETH"]["position_side"] == "short"
        assert by_asset["ETH"]["entry_avg"] == 3000.0
        assert by_asset["USDT"]["open_qty"] == 6000.0  # 2 * 3000
        assert by_asset["USDT"]["position_side"] == "long"

    def test_uses_binance_cumulative_quote_qty_when_present(self):
        symbol_map = {"BTCUSDT": ("BTC", "USDT")}
        rows = [
            {
                "account_id": "a", "book": "b", "symbol": "BTCUSDT", "side": "BUY",
                "executed_quantity": 0.1, "price": 50000.0,
                "binance_cumulative_quote_qty": 4999.5,  # broker value
            },
        ]
        pos = derive_asset_positions_from_orders(rows, symbol_map)
        by_asset = {p["asset"]: p for p in pos}
        assert by_asset["USDT"]["open_qty"] == -4999.5

    def test_unknown_symbol_skipped(self):
        symbol_map = {"ETHUSDT": ("ETH", "USDT")}
        rows = [
            {"account_id": "a", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
            {"account_id": "a", "book": "", "symbol": "ETHUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 3000.0},
        ]
        pos = derive_asset_positions_from_orders(rows, symbol_map)
        # Only ETHUSDT contributes; BTCUSDT skipped
        assets = {p["asset"] for p in pos}
        assert assets == {"ETH", "USDT"}
        assert len(pos) == 2

    @patch("pms.reads.logger")
    def test_unknown_symbol_logs_warning(self, mock_logger):
        """E.1: When symbol is not in symbol_map, skip legs and log warning with symbol and order id."""
        symbol_map = {"ETHUSDT": ("ETH", "USDT")}
        rows = [
            {"internal_id": "ord-unknown-1", "symbol": "UNKNOWNPAIR", "side": "BUY", "executed_quantity": 1.0, "price": 1.0},
        ]
        pos = derive_asset_positions_from_orders(rows, symbol_map)
        assert pos == []
        mock_logger.warning.assert_called_once()
        args = mock_logger.warning.call_args[0]
        assert args[0] == "symbol {!r} not in symbols, skipping legs for order {!r}"
        assert args[1] == "UNKNOWNPAIR"
        assert args[2] == "ord-unknown-1"

    def test_mixed_orders_same_asset_fifo(self):
        symbol_map = {"BTCUSDT": ("BTC", "USDT")}
        rows = [
            {"account_id": "a", "book": "b", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 2.0, "price": 40000.0},
            {"account_id": "a", "book": "b", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 41000.0},
        ]
        pos = derive_asset_positions_from_orders(rows, symbol_map)
        by_asset = {p["asset"]: p for p in pos}
        assert by_asset["BTC"]["open_qty"] == 1.0
        assert by_asset["BTC"]["position_side"] == "long"
        assert by_asset["BTC"]["entry_avg"] == 40000.0  # FIFO: remaining from first buy
        # Buy 2 @ 40k: quote -80k; sell 1 @ 41k: quote +41k; net USDT = -39000
        assert by_asset["USDT"]["open_qty"] == -39000.0

    def test_empty_orders_returns_empty(self):
        assert derive_asset_positions_from_orders([], {"BTCUSDT": ("BTC", "USDT")}) == []

    def test_flat_position_omitted_or_flat_side(self):
        symbol_map = {"BTCUSDT": ("BTC", "USDT")}
        rows = [
            {"account_id": "a", "book": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
            {"account_id": "a", "book": "", "symbol": "BTCUSDT", "side": "SELL", "executed_quantity": 1.0, "price": 51000.0},
        ]
        pos = derive_asset_positions_from_orders(rows, symbol_map)
        by_asset = {p["asset"]: p for p in pos}
        assert by_asset["BTC"]["open_qty"] == 0.0
        assert by_asset["BTC"]["position_side"] == "flat"
        assert by_asset["USDT"]["open_qty"] == 1000.0  # -50k + 51k


class TestQueryBalanceChangesNetByAccountBookAsset:
    """Test balance_changes aggregation by (broker, account_id, book, asset); no join to accounts."""

    def test_one_deposit_returns_positive_net(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("broker",), ("account_id",), ("book",), ("asset",), ("net_delta",)]
        mock_cur.fetchall.return_value = [
            ("", "acc1", "default", "USDT", Decimal("100")),
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        result = query_balance_changes_net_by_account_book_asset(lambda: mock_conn)
        assert result == {("", "acc1", "default", "USDT"): Decimal("100")}

    def test_deposit_and_withdrawal_returns_net(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("broker",), ("account_id",), ("book",), ("asset",), ("net_delta",)]
        # Simulate GROUP BY result: one row per (broker, account_id, book, asset) with SUM(delta)
        mock_cur.fetchall.return_value = [
            ("binance", "acc1", "default", "USDT", Decimal("50")),   # e.g. +100 deposit, -50 withdrawal
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        result = query_balance_changes_net_by_account_book_asset(lambda: mock_conn)
        assert result[("binance", "acc1", "default", "USDT")] == Decimal("50")

    def test_filters_by_account_ids_when_provided(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("broker",), ("account_id",), ("book",), ("asset",), ("net_delta",)]
        mock_cur.fetchall.return_value = [("", "a1", "default", "BTC", Decimal("0.1"))]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        query_balance_changes_net_by_account_book_asset(lambda: mock_conn, account_ids=["a1", "a2"])
        assert mock_cur.execute.called
        call_args = mock_cur.execute.call_args[0]
        assert "account_id = ANY(%s)" in call_args[0] or "ANY(%s)" in call_args[0]
        assert call_args[1] == (["a1", "a2"],)

    def test_excludes_snapshot_change_type(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("broker",), ("account_id",), ("book",), ("asset",), ("net_delta",)]
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        query_balance_changes_net_by_account_book_asset(lambda: mock_conn)
        sql = mock_cur.execute.call_args[0][0]
        assert "deposit" in sql and "withdrawal" in sql and "transfer" in sql

    def test_empty_table_returns_empty_dict(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [("broker",), ("account_id",), ("book",), ("asset",), ("net_delta",)]
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        result = query_balance_changes_net_by_account_book_asset(lambda: mock_conn)
        assert result == {}


class TestDerivePositionsFromOrdersAndBalanceChanges:
    """Test combined derivation: orders (base/quote legs) + balance_changes."""

    @patch("pms.reads.query_balance_changes_net_by_account_book_asset")
    @patch("pms.reads.query_symbol_map")
    @patch("pms.reads.query_orders_for_positions")
    def test_one_buy_btcusdt_at_50k_produces_btc_and_usdt_legs(self, mock_orders, mock_symbol_map, mock_bc):
        """Canonical example: BUY 1 BTCUSDT at 50k → two positions (BTC +1, USDT −50k)."""
        mock_orders.return_value = [
            {"account_id": "a", "book": "b", "broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
        ]
        mock_symbol_map.return_value = {"BTCUSDT": ("BTC", "USDT")}
        mock_bc.return_value = {}

        pos = derive_positions_from_orders_and_balance_changes("postgres://x")
        assert len(pos) == 2
        by_asset = {p.asset: p for p in pos}
        assert by_asset["BTC"].broker == "binance"
        assert by_asset["BTC"].open_qty == 1.0
        assert by_asset["BTC"].position_side == "long"
        assert by_asset["USDT"].open_qty == -50000.0
        assert by_asset["USDT"].position_side == "short"

    @patch("pms.reads.query_balance_changes_net_by_account_book_asset")
    @patch("pms.reads.query_symbol_map")
    @patch("pms.reads.query_orders_for_positions")
    def test_orders_only_returns_asset_positions(self, mock_orders, mock_symbol_map, mock_bc):
        mock_orders.return_value = [
            {"account_id": "a", "book": "b", "broker": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
        ]
        mock_symbol_map.return_value = {"BTCUSDT": ("BTC", "USDT")}
        mock_bc.return_value = {}

        pos = derive_positions_from_orders_and_balance_changes("postgres://x")
        assert len(pos) == 2  # BTC and USDT
        by_asset = {p.asset: p for p in pos}
        assert by_asset["BTC"].open_qty == 1.0
        assert by_asset["USDT"].open_qty == -50000.0
        assert all(p.asset for p in pos)
        assert all(len(p.asset) > 0 for p in pos)

    @patch("pms.reads.query_balance_changes_net_by_account_book_asset")
    @patch("pms.reads.query_symbol_map")
    @patch("pms.reads.query_orders_for_positions")
    def test_balance_changes_only_returns_deposit_positions(self, mock_orders, mock_symbol_map, mock_bc):
        mock_orders.return_value = []
        mock_symbol_map.return_value = {}
        mock_bc.return_value = {("", "acc1", "default", "USDT"): Decimal("1000")}

        pos = derive_positions_from_orders_and_balance_changes("postgres://x")
        assert len(pos) == 1
        assert pos[0].broker == ""
        assert pos[0].account_id == "acc1"
        assert pos[0].book == "default"
        assert pos[0].asset == "USDT"
        assert pos[0].open_qty == 1000.0
        assert pos[0].position_side == "long"

    @patch("pms.reads.query_balance_changes_net_by_account_book_asset")
    @patch("pms.reads.query_symbol_map")
    @patch("pms.reads.query_orders_for_positions")
    def test_orders_plus_deposit_same_asset_adds_net(self, mock_orders, mock_symbol_map, mock_bc):
        mock_orders.return_value = [
            {"account_id": "a", "book": "default", "broker": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 0.5, "price": 40000.0},
        ]
        mock_symbol_map.return_value = {"BTCUSDT": ("BTC", "USDT")}
        # Deposit 10k USDT on same account/book
        mock_bc.return_value = {("", "a", "default", "USDT"): Decimal("10000")}

        pos = derive_positions_from_orders_and_balance_changes("postgres://x")
        by_asset = {p.asset: p for p in pos}
        # Order: BTC +0.5, USDT -20000. Deposit: USDT +10000. Net USDT = -20000 + 10000 = -10000
        assert by_asset["USDT"].open_qty == -10000.0
        assert by_asset["BTC"].open_qty == 0.5

    @patch("pms.reads.query_balance_changes_net_by_account_book_asset")
    @patch("pms.reads.query_symbol_map")
    @patch("pms.reads.query_orders_for_positions")
    def test_combined_derivation_skips_orders_with_unknown_symbol(self, mock_orders, mock_symbol_map, mock_bc):
        """Orders whose symbol is not in symbol_map do not contribute; only known symbols and balance_changes do."""
        mock_orders.return_value = [
            {"account_id": "a", "book": "b", "broker": "", "symbol": "BTCUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 50000.0},
            {"account_id": "a", "book": "b", "broker": "", "symbol": "ETHUSDT", "side": "BUY", "executed_quantity": 1.0, "price": 3000.0},
        ]
        mock_symbol_map.return_value = {"ETHUSDT": ("ETH", "USDT")}  # BTCUSDT unknown
        mock_bc.return_value = {}

        pos = derive_positions_from_orders_and_balance_changes("postgres://x")
        # Only ETHUSDT contributes → ETH +1, USDT -3000
        assert len(pos) == 2
        by_asset = {p.asset: p for p in pos}
        assert by_asset["ETH"].open_qty == 1.0
        assert by_asset["USDT"].open_qty == -3000.0
        assert "BTC" not in by_asset


class TestQueryOrdersForPositions:
    """Test query_orders_for_positions uses executed_qty and exposes executed_quantity."""

    def test_filter_status_and_order_by_created_at(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.description = [
            ("account_id",), ("book",), ("broker",), ("symbol",), ("side",),
            ("executed_qty",), ("price",), ("created_at",),
        ]
        mock_cur.fetchall.return_value = [
            ("acc1", "b1", "binance", "BTCUSDT", "BUY", Decimal("1.0"), Decimal("50000"), None),
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        with patch("psycopg2.connect", return_value=mock_conn):
            rows = query_orders_for_positions("postgres://localhost/db")
        assert mock_cur.execute.called
        call_args = mock_cur.execute.call_args[0][0]
        assert "partially_filled" in call_args or "%s" in call_args
        assert "executed_qty" in call_args
        assert "broker" in call_args
        assert "ORDER BY created_at" in call_args
        assert len(rows) == 1
        assert rows[0]["executed_quantity"] == 1.0
        assert rows[0]["broker"] == "binance"
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


class TestQueryAssetsUsdConfig:
    """Test query_assets_usd_config from assets table."""

    def test_returns_empty_when_no_rows(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        out = query_assets_usd_config(lambda: mock_conn)
        assert out == {}

    def test_returns_usd_price_and_usd_symbol_per_asset(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            ("USDT", None, Decimal("1")),
            ("BTC", "BTCUSDT", None),
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        out = query_assets_usd_config(lambda: mock_conn)
        assert out["USDT"] == {"usd_price": Decimal("1"), "usd_symbol": None}
        assert out["BTC"]["usd_price"] is None
        assert out["BTC"]["usd_symbol"] == "BTCUSDT"


class TestPositionOrderStatuses:
    def test_includes_partially_filled_and_filled(self):
        assert "partially_filled" in POSITION_ORDER_STATUSES
        assert "filled" in POSITION_ORDER_STATUSES
