"""
Tests for asset price feed: query_assets_for_price_source, update_asset_prices, run_asset_price_feed_step.

See docs/pms/ASSET_PRICE_FEED_PLAN.md §3.3, §3.6 step 14.
"""

from unittest.mock import MagicMock

import pytest

from pms.asset_price_feed import (
    query_assets_for_price_source,
    run_asset_price_feed_step,
    update_asset_prices,
)
from pms.asset_price_providers.interface import AssetPriceProvider


class TestQueryAssetsForPriceSource:
    def test_returns_assets_with_usd_symbol(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = [("BTC",), ("ETH",), ("USDT",)]
        out = query_assets_for_price_source(lambda: mock_conn)
        assert out == ["BTC", "ETH", "USDT"]
        assert "usd_symbol" in mock_cur.execute.call_args[0][0].lower()

    def test_returns_empty_when_no_rows(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_cur.fetchall.return_value = []
        out = query_assets_for_price_source(lambda: mock_conn)
        assert out == []


class TestUpdateAssetPrices:
    def test_empty_prices_returns_zero(self):
        assert update_asset_prices("postgres://localhost/db", "binance", {}) == 0

    def test_calls_update_per_asset_and_commits(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 1
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()
        n = update_asset_prices(
            lambda: mock_conn,
            "binance",
            {"BTC": 50000.0, "ETH": 3000.0},
        )
        assert mock_cur.execute.call_count == 2
        assert mock_conn.commit.called
        assert n == 2
        calls = mock_cur.execute.call_args_list
        sql = calls[0][0][0]
        assert "UPDATE assets" in sql
        assert "usd_price" in sql and "price_source" in sql
        assert calls[0][0][1][0] == 50000.0
        assert calls[0][0][1][2] == "binance"
        assert calls[0][0][1][3] == "BTC"

    def test_skips_negative_price(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 0
        mock_conn.cursor.return_value = mock_cur
        n = update_asset_prices(lambda: mock_conn, "binance", {"BTC": -1.0})
        assert mock_cur.execute.call_count == 0
        assert n == 0

    def test_skips_empty_asset_key(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 1
        mock_conn.cursor.return_value = mock_cur
        n = update_asset_prices(lambda: mock_conn, "binance", {"": 50000.0, "BTC": 50000.0})
        assert mock_cur.execute.call_count == 1
        assert n == 1


class TestRunAssetPriceFeedStep:
    def test_empty_assets_returns_zero(self):
        class FakeProvider(AssetPriceProvider):
            def get_prices(self, assets):
                return {}
        n = run_asset_price_feed_step("postgres://localhost/db", FakeProvider(), "binance", [])
        assert n == 0

    def test_calls_provider_then_updates_db(self):
        class FakeProvider(AssetPriceProvider):
            def get_prices(self, assets):
                return {"BTC": 51000.0, "ETH": None}
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.rowcount = 1
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()
        n = run_asset_price_feed_step(
            lambda: mock_conn,
            FakeProvider(),
            "binance",
            ["BTC", "ETH"],
        )
        assert n == 1
        assert mock_cur.execute.call_count == 1
        assert mock_cur.execute.call_args[0][1][3] == "BTC"
        assert mock_cur.execute.call_args[0][1][0] == 51000.0
