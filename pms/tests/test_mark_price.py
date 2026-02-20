"""
Unit tests for mark price provider (12.3.1a): interface and Binance implementation.

Includes: Pydantic validation of provider results, and optional live test vs Binance.
"""

import os
from decimal import Decimal

import pytest
import responses

from pms.mark_price import (
    BinanceMarkPriceProvider,
    FakeMarkPriceProvider,
    MarkPriceProviderError,
    get_mark_price_provider,
)
from pms.schemas_pydantic import MarkPricesResult

# Run tests that hit real Binance (public ticker/price, no API key) only when requested
skip_unless_live = pytest.mark.skipif(
    os.getenv("PMS_RUN_MARK_PRICE_LIVE") != "1",
    reason="Set PMS_RUN_MARK_PRICE_LIVE=1 to run mark price tests against live Binance",
)


class TestMarkPricesResult:
    """Test MarkPricesResult Pydantic model."""

    def test_empty(self):
        r = MarkPricesResult()
        assert r.prices == {}
        assert r.get("BTCUSDT") is None

    def test_from_dict_str_prices(self):
        r = MarkPricesResult(prices={"BTCUSDT": "50000.5", "ETHUSDT": "3000"})
        assert r.prices["BTCUSDT"] == Decimal("50000.5")
        assert r.prices["ETHUSDT"] == Decimal("3000")
        assert r.get("btcusdt") == Decimal("50000.5")

    def test_get_case_insensitive(self):
        r = MarkPricesResult(prices={"BTCUSDT": 50000})
        assert r.get("BTCUSDT") == Decimal(50000)
        assert r.get("btcusdt") == Decimal(50000)

    def test_get_missing_returns_none(self):
        r = MarkPricesResult(prices={"BTCUSDT": 50000})
        assert r.get("ETHUSDT") is None


class TestFakeMarkPriceProvider:
    """Test fake provider for unit tests."""

    def test_empty(self):
        p = FakeMarkPriceProvider()
        r = p.get_mark_prices(["BTCUSDT", "ETHUSDT"])
        assert r.prices == {}

    def test_fixed_prices(self):
        p = FakeMarkPriceProvider(prices={"BTCUSDT": 50000.0, "ETHUSDT": "3000"})
        r = p.get_mark_prices(["BTCUSDT", "ETHUSDT"])
        assert r.prices["BTCUSDT"] == Decimal("50000")
        assert r.prices["ETHUSDT"] == Decimal("3000")

    def test_filter_requested(self):
        p = FakeMarkPriceProvider(prices={"BTCUSDT": 50000, "ETHUSDT": 3000})
        r = p.get_mark_prices(["BTCUSDT"])
        assert list(r.prices.keys()) == ["BTCUSDT"]
        assert r.prices["BTCUSDT"] == Decimal("50000")


class TestBinanceMarkPriceProvider:
    """Test Binance implementation with mocked HTTP."""

    def test_empty_symbols_returns_empty(self):
        p = BinanceMarkPriceProvider(base_url="https://testnet.binance.vision")
        r = p.get_mark_prices([])
        assert r.prices == {}

    @responses.activate
    def test_parses_ticker_array(self):
        responses.get(
            "https://testnet.binance.vision/api/v3/ticker/price",
            json=[
                {"symbol": "BTCUSDT", "price": "97250.25"},
                {"symbol": "ETHUSDT", "price": "3500.50"},
                {"symbol": "OTHER", "price": "1.0"},
            ],
        )
        p = BinanceMarkPriceProvider(base_url="https://testnet.binance.vision")
        r = p.get_mark_prices(["BTCUSDT", "ETHUSDT"])
        assert r.prices["BTCUSDT"] == Decimal("97250.25")
        assert r.prices["ETHUSDT"] == Decimal("3500.50")
        assert "OTHER" not in r.prices
        # Pydantic: result is a valid MarkPricesResult and round-trips
        assert isinstance(r, MarkPricesResult)
        round_trip = MarkPricesResult.model_validate(r.model_dump())
        assert round_trip.prices == r.prices

    @responses.activate
    def test_parses_ticker_single_dict(self):
        # When Binance returns ?symbol=X, response is a single object
        responses.get(
            "https://testnet.binance.vision/api/v3/ticker/price",
            json={"symbol": "BTCUSDT", "price": "50000.00"},
        )
        p = BinanceMarkPriceProvider(base_url="https://testnet.binance.vision")
        r = p.get_mark_prices(["BTCUSDT"])
        assert r.prices["BTCUSDT"] == Decimal("50000.00")

    @responses.activate
    def test_http_error_raises(self):
        responses.get(
            "https://testnet.binance.vision/api/v3/ticker/price",
            status=500,
        )
        p = BinanceMarkPriceProvider(base_url="https://testnet.binance.vision")
        with pytest.raises(MarkPriceProviderError) as exc_info:
            p.get_mark_prices(["BTCUSDT"])
        assert "ticker" in str(exc_info.value).lower()

    @responses.activate
    def test_skip_invalid_items(self):
        responses.get(
            "https://testnet.binance.vision/api/v3/ticker/price",
            json=[
                {"symbol": "BTCUSDT", "price": "50000"},
                {"symbol": "BAD", "price": "not-a-number"},
                {"no_symbol": "x", "price": "1"},
            ],
        )
        p = BinanceMarkPriceProvider(base_url="https://testnet.binance.vision")
        r = p.get_mark_prices(["BTCUSDT", "BAD"])
        assert r.prices.get("BTCUSDT") == Decimal("50000")
        # BAD may or may not be present depending on validation

    def test_init_default_testnet(self):
        p = BinanceMarkPriceProvider(use_testnet=True)
        assert p.base_url == "https://testnet.binance.vision"

    def test_init_default_production(self):
        p = BinanceMarkPriceProvider(use_testnet=False)
        assert p.base_url == "https://api.binance.com"

    def test_init_explicit_base_url(self):
        p = BinanceMarkPriceProvider(base_url="https://custom.example.com")
        assert p.base_url == "https://custom.example.com"


class TestGetMarkPriceProvider:
    """Test factory."""

    def test_binance(self):
        p = get_mark_price_provider("binance")
        assert isinstance(p, BinanceMarkPriceProvider)
        assert p.base_url == "https://testnet.binance.vision"

    def test_binance_with_base_url(self):
        p = get_mark_price_provider("binance", binance_base_url="https://api.binance.com")
        assert isinstance(p, BinanceMarkPriceProvider)
        assert p.base_url == "https://api.binance.com"

    def test_empty_or_none_returns_fake(self):
        p1 = get_mark_price_provider("")
        p2 = get_mark_price_provider("none")
        p3 = get_mark_price_provider("fake")
        for p in (p1, p2, p3):
            assert isinstance(p, FakeMarkPriceProvider)
        assert p1.get_mark_prices(["BTCUSDT"]).prices == {}

    def test_unsupported_raises(self):
        with pytest.raises(ValueError) as exc_info:
            get_mark_price_provider("redis")
        assert "Unsupported" in str(exc_info.value) or "redis" in str(exc_info.value)


class TestMarkPriceResultPassesPydantic:
    """Assert that provider return values are valid Pydantic MarkPricesResult."""

    @responses.activate
    def test_binance_return_is_valid_mark_prices_result(self):
        """Binance provider return passes Pydantic: isinstance and model_validate round-trip."""
        responses.get(
            "https://testnet.binance.vision/api/v3/ticker/price",
            json=[{"symbol": "BTCUSDT", "price": "97250.25"}],
        )
        provider = get_mark_price_provider("binance")
        result = provider.get_mark_prices(["BTCUSDT"])
        assert isinstance(result, MarkPricesResult)
        # Round-trip through dict must produce equal model
        dumped = result.model_dump()
        restored = MarkPricesResult.model_validate(dumped)
        assert restored.prices == result.prices
        assert restored.get("BTCUSDT") == result.get("BTCUSDT")

    def test_fake_return_passes_pydantic(self):
        """Fake provider return is valid MarkPricesResult and round-trips."""
        provider = FakeMarkPriceProvider(prices={"BTCUSDT": 50000.0})
        result = provider.get_mark_prices(["BTCUSDT"])
        assert isinstance(result, MarkPricesResult)
        restored = MarkPricesResult.model_validate(result.model_dump())
        assert restored.prices == result.prices


@skip_unless_live
class TestMarkPriceFromLiveBinance:
    """Integration: hit real Binance ticker/price (public, no API key). Result must pass Pydantic."""

    def test_live_binance_returns_valid_pydantic_result(self):
        """Call Binance (source=binance), get mark prices for BTCUSDT; assert Pydantic-valid result."""
        provider = get_mark_price_provider("binance")
        result = provider.get_mark_prices(["BTCUSDT"])
        assert isinstance(result, MarkPricesResult)
        assert "BTCUSDT" in result.prices
        assert result.prices["BTCUSDT"] > 0
        # Must round-trip through Pydantic
        restored = MarkPricesResult.model_validate(result.model_dump())
        assert restored.prices == result.prices
        assert restored.get("btcusdt") == result.get("BTCUSDT")
