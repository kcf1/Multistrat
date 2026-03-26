"""
Unit tests for asset price providers (AssetPriceProvider, BinanceAssetPriceProvider).

See docs/pms/ASSET_PRICE_FEED_PLAN.md §3.2 step 5.
"""

from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta, timezone

import pytest

from pms.asset_price_providers import (
    AssetPriceProvider,
    AssetPriceProviderError,
    BinanceAssetPriceProvider,
    OhlcvDbAssetPriceProvider,
    get_asset_price_provider,
)


class TestAssetPriceProvider:
    """Interface contract: get_prices(assets) -> Dict[str, Optional[float]]."""

    def test_interface_requires_get_prices(self):
        class Concrete(AssetPriceProvider):
            def get_prices(self, assets):
                return {}

        p = Concrete()
        assert p.get_prices(["BTC"]) == {}

    def test_empty_assets_returns_empty(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, json=MagicMock(return_value=[]))
            out = provider.get_prices([])
        assert out == {}


class TestBinanceAssetPriceProvider:
    """Binance provider: symbol = asset+USDT, parse ticker/price response."""

    def test_empty_assets_returns_empty_dict(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        out = provider.get_prices([])
        assert out == {}

    def test_whitespace_assets_normalized(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value=[
                    {"symbol": "BTCUSDT", "price": "50000.5"},
                ]),
            )
            out = provider.get_prices(["  btc  "])
        assert out == {"BTC": 50000.5}

    def test_returns_asset_to_price_for_requested_assets(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value=[
                    {"symbol": "BTCUSDT", "price": "50000.25"},
                    {"symbol": "ETHUSDT", "price": "3000.5"},
                    {"symbol": "BNBUSDT", "price": "400"},
                ]),
            )
            out = provider.get_prices(["BTC", "ETH", "BNB"])
        assert out["BTC"] == 50000.25
        assert out["ETH"] == 3000.5
        assert out["BNB"] == 400
        assert len(out) == 3

    def test_filters_to_requested_assets_only(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value=[
                    {"symbol": "BTCUSDT", "price": "50000"},
                    {"symbol": "ETHUSDT", "price": "3000"},
                    {"symbol": "DOGEUSDT", "price": "0.1"},
                ]),
            )
            out = provider.get_prices(["BTC"])
        assert out == {"BTC": 50000.0}

    def test_skips_symbols_not_ending_with_quote(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision", quote_asset="USDT")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value=[
                    {"symbol": "BTCBUSD", "price": "50000"},
                    {"symbol": "BTCUSDT", "price": "50001"},
                ]),
            )
            out = provider.get_prices(["BTC"])
        assert out == {"BTC": 50001.0}

    def test_custom_quote_asset(self):
        provider = BinanceAssetPriceProvider(
            base_url="https://testnet.binance.vision",
            quote_asset="BUSD",
        )
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value=[
                    {"symbol": "BTCBUSD", "price": "49900"},
                ]),
            )
            out = provider.get_prices(["BTC"])
        assert out == {"BTC": 49900.0}

    def test_network_error_raises(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            import requests
            mock_get.side_effect = requests.exceptions.RequestException("timeout")
            with pytest.raises(AssetPriceProviderError) as exc_info:
                provider.get_prices(["BTC"])
            assert "Binance ticker" in str(exc_info.value)

    def test_invalid_json_raises(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, json=MagicMock(side_effect=ValueError("bad json")))
            with pytest.raises(AssetPriceProviderError) as exc_info:
                provider.get_prices(["BTC"])
            assert "parse" in str(exc_info.value).lower()

    def test_negative_price_skipped(self):
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value=[
                    {"symbol": "BTCUSDT", "price": "-1"},
                ]),
            )
            out = provider.get_prices(["BTC"])
        assert "BTC" not in out or out.get("BTC") is None

    def test_single_dict_response_supported(self):
        """Binance can return single dict when ?symbol=X is used."""
        provider = BinanceAssetPriceProvider(base_url="https://testnet.binance.vision")
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=200,
                json=MagicMock(return_value={"symbol": "ETHUSDT", "price": "3500"}),
            )
            out = provider.get_prices(["ETH"])
        assert out == {"ETH": 3500.0}


class TestGetAssetPriceProvider:
    """Registry: get_asset_price_provider(name, **kwargs)."""

    def test_binance_returns_binance_provider(self):
        p = get_asset_price_provider("binance")
        assert p is not None
        assert isinstance(p, BinanceAssetPriceProvider)

    def test_binance_with_base_url(self):
        p = get_asset_price_provider("binance", base_url="https://api.binance.com")
        assert p is not None
        assert p.base_url == "https://api.binance.com"

    def test_empty_or_unknown_returns_none(self):
        assert get_asset_price_provider("") is None
        assert get_asset_price_provider("none") is None
        assert get_asset_price_provider("  ") is None
        assert get_asset_price_provider("coingecko") is None

    def test_custom_timeout_and_quote(self):
        p = get_asset_price_provider(
            "binance",
            timeout=5.0,
            quote_asset="BUSD",
        )
        assert p is not None
        assert p.timeout == 5.0
        assert p.quote_asset == "BUSD"

    def test_ohlcv_db_returns_provider(self):
        p = get_asset_price_provider("ohlcv_db", pg_connect="postgres://localhost/db")
        assert p is not None
        assert isinstance(p, OhlcvDbAssetPriceProvider)

    def test_ohlcv_db_without_pg_connect_returns_none(self):
        p = get_asset_price_provider("ohlcv_db")
        assert p is None


class TestOhlcvDbAssetPriceProvider:
    def _mock_conn_with_rows(self, assets_rows, ohlcv_rows):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.fetchall.side_effect = [assets_rows, ohlcv_rows]
        return mock_conn

    def test_returns_fresh_close_prices(self):
        now = datetime.now(timezone.utc)
        mock_conn = self._mock_conn_with_rows(
            [("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")],
            [
                ("BTCUSDT", now - timedelta(hours=10), 50123.5, now - timedelta(minutes=5)),
                ("ETHUSDT", now - timedelta(hours=8), 3123.0, now - timedelta(minutes=10)),
            ],
        )
        p = OhlcvDbAssetPriceProvider(lambda: mock_conn, interval="1h", max_staleness_seconds=7200)
        out = p.get_prices(["BTC", "ETH"])
        assert out == {"BTC": 50123.5, "ETH": 3123.0}

    def test_excludes_stale_rows(self):
        now = datetime.now(timezone.utc)
        mock_conn = self._mock_conn_with_rows(
            [("BTC", "BTCUSDT")],
            [("BTCUSDT", now - timedelta(minutes=5), 50123.5, now - timedelta(hours=5))],
        )
        p = OhlcvDbAssetPriceProvider(lambda: mock_conn, interval="1h", max_staleness_seconds=3600)
        out = p.get_prices(["BTC"])
        assert out == {}

    def test_skips_assets_without_usd_symbol_or_missing_ohlcv(self):
        now = datetime.now(timezone.utc)
        mock_conn = self._mock_conn_with_rows(
            [("BTC", "BTCUSDT")],
            [("BTCUSDT", now - timedelta(minutes=2), 50000.0, now - timedelta(minutes=2))],
        )
        p = OhlcvDbAssetPriceProvider(lambda: mock_conn, interval="1h", max_staleness_seconds=7200)
        out = p.get_prices(["BTC", "DOGE"])
        assert out == {"BTC": 50000.0}
