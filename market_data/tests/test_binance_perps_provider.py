"""Tests for BinancePerpsMarketDataProvider (mocked HTTP)."""

from unittest.mock import MagicMock, patch

import pytest

from market_data.providers.binance_perps import BinancePerpsMarketDataProvider
from market_data.rate_limit import ProviderRateLimiter


_BASIS_ROW = {
    "indexPrice": "46317.16333333",
    "contractType": "PERPETUAL",
    "basisRate": "0.00295565",
    "futuresPrice": "46454.22",
    "basis": "137.05666667",
    "pair": "BTCUSDT",
    "timestamp": 1640995200000,
    "period": "1h",
}

_OPEN_INTEREST_ROW = {
    "symbol": "BTCUSDT",
    "contractType": "PERPETUAL",
    "sumOpenInterest": "12345.6789",
    "sumOpenInterestValue": "987654321.123456",
    "CMCCirculatingSupply": "19500000.0",
    "timestamp": 1640995200000,
    "period": "1h",
}


def _mock_response(payload: list) -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.json.return_value = payload
    r.raise_for_status = MagicMock()
    return r


def test_fetch_basis_parses_and_calls_get() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response([_BASIS_ROW])
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        rate_limiter=ProviderRateLimiter(None),
        fetch_max_attempts=1,
    )
    # Same as start → provider bumps end by one period so endTime > startTime (Binance -1102).
    fixed_now = 1640995200000
    with patch("market_data.providers.binance_perps._utc_now_ms", return_value=fixed_now):
        rows = prov.fetch_basis(
            "btcusdt",
            "perpetual",
            "1h",
            start_time_ms=1640995200000,
            limit=100,
        )
    assert len(rows) == 1
    assert rows[0].pair == "BTCUSDT"
    assert rows[0].contract_type == "PERPETUAL"
    session.get.assert_called_once()
    url = session.get.call_args[0][0]
    assert "pair=BTCUSDT" in url
    assert "contractType=PERPETUAL" in url
    assert "period=1h" in url
    assert "startTime=1640995200000" in url
    assert "endTime=1640998800000" in url  # +1h
    assert "limit=100" in url


def test_fetch_basis_includes_end_time() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response([])
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        fetch_max_attempts=1,
    )
    # Aligned 1h window so URL reflects floor-aligned start/end (Binance grid).
    prov.fetch_basis(
        "BTCUSDT",
        "PERPETUAL",
        "1h",
        start_time_ms=3_600_000,
        end_time_ms=7_200_000,
        limit=10,
    )
    url = session.get.call_args[0][0]
    assert "startTime=3600000" in url
    assert "endTime=7200000" in url


def test_limit_out_of_range() -> None:
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=MagicMock(),
        fetch_max_attempts=1,
    )
    with pytest.raises(ValueError, match="500"):
        prov.fetch_basis("BTCUSDT", "PERPETUAL", "1h", start_time_ms=1, limit=0)
    with pytest.raises(ValueError, match="500"):
        prov.fetch_basis("BTCUSDT", "PERPETUAL", "1h", start_time_ms=1, limit=501)


def test_acquire_before_http() -> None:
    order: list[str] = []
    limiter = MagicMock()

    def _acquire() -> None:
        order.append("acquire")

    limiter.acquire.side_effect = _acquire

    session = MagicMock()

    def get(*args, **kwargs):
        order.append("get")
        return _mock_response([_BASIS_ROW])

    session.get.side_effect = get
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        rate_limiter=limiter,
        fetch_max_attempts=1,
    )
    with patch("market_data.providers.binance_perps._utc_now_ms", return_value=7_200_000):
        prov.fetch_basis("BTCUSDT", "PERPETUAL", "1h", start_time_ms=1)
    assert order == ["acquire", "get"]


def test_http_400_no_retry_returns_empty() -> None:
    session = MagicMock()
    resp = MagicMock()
    resp.status_code = 400
    resp.text = '{"code":-1121,"msg":"Invalid symbol."}'
    session.get.return_value = resp
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        fetch_max_attempts=5,
        fetch_retry_base_sleep_sec=0.0,
    )
    with patch("market_data.providers.binance_perps.time.sleep") as sl:
        with patch("market_data.providers.binance_perps._utc_now_ms", return_value=7_200_000):
            rows = prov.fetch_basis("BADUSDT", "PERPETUAL", "1h", start_time_ms=1)
    sl.assert_not_called()
    session.get.assert_called_once()
    assert rows == []
    assert len(prov.fetch_give_ups) == 1
    assert "400" in prov.fetch_give_ups[0]
    url = session.get.call_args[0][0]
    assert "endTime=" in url


def test_fetch_retries_after_invalid_payload_then_succeeds() -> None:
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(["not-an-object"]),
        _mock_response([_BASIS_ROW]),
    ]
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        rate_limiter=ProviderRateLimiter(None),
        fetch_max_attempts=3,
        fetch_retry_base_sleep_sec=0.0,
    )
    with patch("market_data.providers.binance_perps.time.sleep"):
        with patch("market_data.providers.binance_perps._utc_now_ms", return_value=7_200_000):
            rows = prov.fetch_basis("BTCUSDT", "PERPETUAL", "1h", start_time_ms=1)
    assert len(rows) == 1
    assert session.get.call_count == 2


def test_fetch_open_interest_parses_and_calls_get() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response([_OPEN_INTEREST_ROW])
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        rate_limiter=ProviderRateLimiter(None),
        open_interest_fetch_max_attempts=1,
    )
    rows = prov.fetch_open_interest_hist(
        "btcusdt",
        "perpetual",
        "1h",
        start_time_ms=1640995200000,
        limit=100,
    )
    assert len(rows) == 1
    assert rows[0].symbol == "BTCUSDT"
    assert rows[0].contract_type == "PERPETUAL"
    session.get.assert_called_once()
    url = session.get.call_args[0][0]
    assert "symbol=BTCUSDT" in url
    assert "contractType=PERPETUAL" in url
    assert "period=1h" in url
    assert "startTime=1640995200000" in url
    assert "limit=100" in url


def test_fetch_open_interest_includes_end_time() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response([])
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        open_interest_fetch_max_attempts=1,
    )
    prov.fetch_open_interest_hist(
        "BTCUSDT",
        "PERPETUAL",
        "1h",
        start_time_ms=100,
        end_time_ms=200,
        limit=10,
    )
    url = session.get.call_args[0][0]
    assert "endTime=200" in url


def test_open_interest_limit_out_of_range() -> None:
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=MagicMock(),
        open_interest_fetch_max_attempts=1,
    )
    with pytest.raises(ValueError, match="500"):
        prov.fetch_open_interest_hist("BTCUSDT", "PERPETUAL", "1h", start_time_ms=1, limit=0)
    with pytest.raises(ValueError, match="500"):
        prov.fetch_open_interest_hist("BTCUSDT", "PERPETUAL", "1h", start_time_ms=1, limit=501)


def test_open_interest_http_400_no_retry_returns_empty() -> None:
    session = MagicMock()
    resp = MagicMock()
    resp.status_code = 400
    resp.text = '{"code":-1121,"msg":"Invalid symbol."}'
    session.get.return_value = resp
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        open_interest_fetch_max_attempts=5,
        open_interest_fetch_retry_base_sleep_sec=0.0,
    )
    with patch("market_data.providers.binance_perps.time.sleep") as sl:
        rows = prov.fetch_open_interest_hist("BADUSDT", "PERPETUAL", "1h", start_time_ms=1)
    sl.assert_not_called()
    session.get.assert_called_once()
    assert rows == []
    assert len(prov.fetch_give_ups) == 1
    assert "400" in prov.fetch_give_ups[0]


def test_fetch_open_interest_retries_after_invalid_payload_then_succeeds() -> None:
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(["not-an-object"]),
        _mock_response([_OPEN_INTEREST_ROW]),
    ]
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        rate_limiter=ProviderRateLimiter(None),
        open_interest_fetch_max_attempts=3,
        open_interest_fetch_retry_base_sleep_sec=0.0,
    )
    with patch("market_data.providers.binance_perps.time.sleep"):
        rows = prov.fetch_open_interest_hist("BTCUSDT", "PERPETUAL", "1h", start_time_ms=1)
    assert len(rows) == 1
    assert session.get.call_count == 2


def test_open_interest_invalid_start_time_http_400_returns_empty() -> None:
    """Invalid startTime is a client error: one GET, no retries, no follow-up probes."""
    session = MagicMock()
    resp = MagicMock()
    resp.status_code = 400
    resp.text = "{\"msg\":\"parameter 'startTime' is invalid.\",\"code\":-1130}"
    session.get.return_value = resp
    prov = BinancePerpsMarketDataProvider(
        "https://fapi.binance.com",
        session=session,
        rate_limiter=ProviderRateLimiter(None),
        open_interest_fetch_max_attempts=5,
        open_interest_fetch_retry_base_sleep_sec=0.0,
    )
    with patch("market_data.providers.binance_perps.time.sleep") as sl:
        rows = prov.fetch_open_interest_hist(
            "TRUMPUSDT",
            "PERPETUAL",
            "1h",
            start_time_ms=1,
            end_time_ms=2_000_000,
            limit=10,
        )
    sl.assert_not_called()
    session.get.assert_called_once()
    assert rows == []
    assert len(prov.fetch_give_ups) == 1
    assert "400" in prov.fetch_give_ups[0]
    url = session.get.call_args[0][0]
    assert "startTime=1" in url
