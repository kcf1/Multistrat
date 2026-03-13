"""
Tests for asset_init: upsert_asset, init_assets_stables, sync_assets_from_symbols.
"""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from pms.asset_init import (
    PRICE_SOURCE_FIXED,
    init_assets_stables,
    sync_assets_from_symbols,
    truncate_assets,
    upsert_asset,
)


class TestTruncateAssets:
    def test_deletes_all_rows(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        truncate_assets(lambda: mock_conn)
        mock_cur.execute.assert_called_once()
        assert "DELETE FROM assets" in mock_cur.execute.call_args[0][0]
        assert mock_conn.commit.called


class TestUpsertAsset:
    def test_returns_false_for_empty_asset(self):
        assert upsert_asset("postgres://localhost/db", "") is False
        assert upsert_asset("postgres://localhost/db", "  ") is False

    def test_inserts_asset_with_optional_fields(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 1
        mock_conn.cursor.return_value = mock_cur
        ok = upsert_asset(lambda: mock_conn, "BTC", usd_symbol="BTCUSDT")
        assert ok is True
        call = mock_cur.execute.call_args[0]
        assert "INSERT INTO assets" in call[0]
        assert call[1][0] == "BTC"
        assert call[1][1] == "BTCUSDT"
        assert call[1][2] is None
        assert mock_conn.commit.called

    def test_upserts_with_usd_price(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 1
        mock_conn.cursor.return_value = mock_cur
        ok = upsert_asset(lambda: mock_conn, "USDT", usd_price=1)
        assert ok is True
        call = mock_cur.execute.call_args[0]
        assert call[1][2] == Decimal("1")

    def test_upserts_with_price_source_fixed(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 1
        mock_conn.cursor.return_value = mock_cur
        ok = upsert_asset(lambda: mock_conn, "USDT", usd_price=1, price_source=PRICE_SOURCE_FIXED)
        assert ok is True
        call = mock_cur.execute.call_args[0]
        assert call[1][4] == "fixed"
        assert "price_source" in call[0]


class TestSyncAssetsFromSymbols:
    def test_empty_when_no_symbols(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = []
        n = sync_assets_from_symbols(lambda: mock_conn, quote_asset="USDT")
        assert n == 0
        assert "symbols" in mock_cur.execute.call_args[0][0].lower()
        assert mock_cur.execute.call_count == 1

    def test_upserts_one_asset_with_usd_symbol(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [("BTC",)]
        n = sync_assets_from_symbols(lambda: mock_conn, quote_asset="USDT")
        assert n == 1
        calls = mock_cur.execute.call_args_list
        assert len(calls) >= 2
        insert_call = [c for c in calls if "INSERT INTO assets" in c[0][0]][0]
        assert "usd_symbol" in insert_call[0][0]
        assert "ON CONFLICT" in insert_call[0][0]
        assert insert_call[0][1][0] == "BTC"
        assert insert_call[0][1][1] == "BTCUSDT"

    def test_upserts_multiple_bases(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [("BTC",), ("ETH",), ("BNB",)]
        n = sync_assets_from_symbols(lambda: mock_conn, quote_asset="USDT")
        assert n == 3
        assert mock_conn.commit.called

    def test_custom_quote_asset(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [("BTC",)]
        n = sync_assets_from_symbols(lambda: mock_conn, quote_asset="BUSD")
        assert n == 1
        insert_call = [c for c in mock_cur.execute.call_args_list if "INSERT INTO assets" in c[0][0]][0]
        assert insert_call[0][1][1] == "BTCBUSD"
