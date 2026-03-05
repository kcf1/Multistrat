"""
Unit tests for symbol sync (task 12.2.15).

Mock HTTP (requests) and Postgres to verify fetch_binance_exchange_info
and sync_symbols_to_postgres / sync_symbols_from_binance.
"""

import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from oms.symbol_sync import (
    fetch_binance_exchange_info,
    sync_symbols_to_postgres,
    sync_symbols_from_binance,
    _find_filter,
)


class TestFindFilter:
    def test_returns_first_matching_filter(self):
        filters = [
            {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
            {"filterType": "LOT_SIZE", "stepSize": "0.001"},
        ]
        assert _find_filter(filters, "PRICE_FILTER") == {"filterType": "PRICE_FILTER", "tickSize": "0.01"}
        assert _find_filter(filters, "LOT_SIZE") == {"filterType": "LOT_SIZE", "stepSize": "0.001"}

    def test_returns_none_when_empty(self):
        assert _find_filter([], "PRICE_FILTER") is None

    def test_returns_none_when_no_match(self):
        assert _find_filter([{"filterType": "OTHER"}], "PRICE_FILTER") is None


class TestFetchBinanceExchangeInfo:
    @patch("oms.symbol_sync.requests.get")
    def test_parses_symbols_and_filters(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.00001", "minQty": "0.00001"},
                    ],
                },
                {
                    "symbol": "ETHUSDT",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                    "filters": [],
                },
            ],
        }
        result = fetch_binance_exchange_info("https://api.binance.com", timeout=10)
        assert len(result) == 2
        btc = next(r for r in result if r["symbol"] == "BTCUSDT")
        assert btc["base_asset"] == "BTC"
        assert btc["quote_asset"] == "USDT"
        assert btc["product_type"] == "spot"
        assert btc["tick_size"] == Decimal("0.01")
        assert btc["lot_size"] == Decimal("0.00001")
        assert btc["min_qty"] == Decimal("0.00001")
        eth = next(r for r in result if r["symbol"] == "ETHUSDT")
        assert eth["base_asset"] == "ETH"
        assert eth["quote_asset"] == "USDT"
        assert eth["tick_size"] is None
        assert eth["lot_size"] is None
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "api.binance.com" in call_args[0][0]
        assert "/api/v3/exchangeInfo" in call_args[0][0]
        assert call_args[1].get("timeout") == 10

    @patch("oms.symbol_sync.requests.get")
    def test_raises_on_http_error(self, mock_get):
        mock_get.return_value.raise_for_status.side_effect = Exception("404")
        with pytest.raises(Exception, match="404"):
            fetch_binance_exchange_info("https://api.binance.com")


class TestSyncSymbolsToPostgres:
    def test_executes_upsert_with_correct_columns(self):
        execute_calls = []
        cur = MagicMock()
        cur.execute = lambda sql, params=None: execute_calls.append((sql, params or {}))
        conn = MagicMock()
        conn.cursor.return_value = cur

        def connect():
            return conn

        symbols = [
            {
                "symbol": "BTCUSDT",
                "base_asset": "BTC",
                "quote_asset": "USDT",
                "tick_size": Decimal("0.01"),
                "lot_size": Decimal("0.00001"),
                "min_qty": Decimal("0.00001"),
                "product_type": "spot",
            },
        ]
        count = sync_symbols_to_postgres(connect, symbols, broker="binance")
        assert count == 1
        assert len(execute_calls) == 1
        sql, params = execute_calls[0]
        assert "INSERT INTO symbols" in sql
        assert "ON CONFLICT (symbol) DO UPDATE" in sql
        assert params.get("symbol") == "BTCUSDT"
        assert params.get("base_asset") == "BTC"
        assert params.get("quote_asset") == "USDT"
        assert params.get("broker") == "binance"
        assert params.get("tick_size") == Decimal("0.01")

    def test_returns_zero_when_empty_list(self):
        execute_calls = []
        cur = MagicMock()
        cur.execute = lambda sql, params=None: execute_calls.append((sql, params))
        conn = MagicMock()
        conn.cursor.return_value = cur

        def connect():
            return conn

        count = sync_symbols_to_postgres(connect, [], broker="binance")
        assert count == 0
        assert len(execute_calls) == 0

    def test_skips_rows_without_symbol_or_assets(self):
        execute_calls = []
        cur = MagicMock()
        cur.execute = lambda sql, params=None: execute_calls.append((sql, params))
        conn = MagicMock()
        conn.cursor.return_value = cur

        def connect():
            return conn

        symbols = [
            {"symbol": "BTCUSDT", "base_asset": "BTC", "quote_asset": "USDT"},
            {"symbol": "", "base_asset": "X", "quote_asset": "Y"},
            {"symbol": "ETHBTC", "base_asset": "ETH", "quote_asset": "BTC"},
        ]
        count = sync_symbols_to_postgres(connect, symbols, broker="binance")
        assert count == 2
        assert execute_calls[0][1]["symbol"] == "BTCUSDT"
        assert execute_calls[1][1]["symbol"] == "ETHBTC"


class TestSyncSymbolsFromBinance:
    @patch("oms.symbol_sync.requests.get")
    def test_fetches_then_upserts(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "filters": []},
            ],
        }
        execute_calls = []
        cur = MagicMock()
        cur.execute = lambda sql, params=None: execute_calls.append((sql, params))
        conn = MagicMock()
        conn.cursor.return_value = cur

        def connect():
            return conn

        count = sync_symbols_from_binance("https://testnet.binance.vision", connect, broker="binance")
        assert count == 1
        mock_get.assert_called_once()
        assert "exchangeInfo" in mock_get.call_args[0][0]
        assert len(execute_calls) == 1
        assert execute_calls[0][1]["symbol"] == "BTCUSDT"
        assert execute_calls[0][1]["base_asset"] == "BTC"
        assert execute_calls[0][1]["quote_asset"] == "USDT"
