"""Tests for BinanceSpotKlinesProvider (mocked HTTP)."""

from unittest.mock import MagicMock, patch

import pytest

from market_data.providers.binance_spot import BinanceSpotKlinesProvider
from market_data.rate_limit import ProviderRateLimiter


# Minimal Binance-style row (7 fields through close time; parser accepts)
_KLINE_ROW = [
    1499049600000,
    "0.01634790",
    "0.80000000",
    "0.01575800",
    "0.01577100",
    "148976.11427815",
    1499644799999,
]


def _mock_response(payload: list) -> MagicMock:
    r = MagicMock()
    r.json.return_value = payload
    r.raise_for_status = MagicMock()
    return r


def test_fetch_klines_parses_and_calls_get() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response([_KLINE_ROW])
    prov = BinanceSpotKlinesProvider(
        "https://api.binance.com",
        session=session,
        rate_limiter=ProviderRateLimiter(None),
        fetch_max_attempts=1,
    )
    bars = prov.fetch_klines(
        "btcusdt",
        "1m",
        start_time_ms=1499049600000,
        limit=500,
    )
    assert len(bars) == 1
    assert bars[0].symbol == "BTCUSDT"
    assert bars[0].interval == "1m"
    session.get.assert_called_once()
    url = session.get.call_args[0][0]
    assert "symbol=BTCUSDT" in url
    assert "interval=1m" in url
    assert "startTime=1499049600000" in url
    assert "limit=500" in url


def test_fetch_klines_includes_end_time() -> None:
    session = MagicMock()
    session.get.return_value = _mock_response([])
    prov = BinanceSpotKlinesProvider(
        "https://api.binance.com",
        session=session,
        fetch_max_attempts=1,
    )
    prov.fetch_klines(
        "ETHUSDT",
        "5m",
        start_time_ms=100,
        end_time_ms=200,
        limit=10,
    )
    url = session.get.call_args[0][0]
    assert "endTime=200" in url


def test_acquire_before_http() -> None:
    order: list[str] = []
    limiter = MagicMock()

    def _acquire() -> None:
        order.append("acquire")

    limiter.acquire.side_effect = _acquire

    session = MagicMock()

    def get(*args, **kwargs):
        order.append("get")
        return _mock_response([_KLINE_ROW])

    session.get.side_effect = get
    prov = BinanceSpotKlinesProvider(
        "https://api.binance.com",
        session=session,
        rate_limiter=limiter,
        fetch_max_attempts=1,
    )
    prov.fetch_klines("BTCUSDT", "1m", start_time_ms=1)
    assert order == ["acquire", "get"]


def test_limit_out_of_range() -> None:
    prov = BinanceSpotKlinesProvider(
        "https://api.binance.com",
        session=MagicMock(),
        fetch_max_attempts=1,
    )
    with pytest.raises(ValueError, match="1000"):
        prov.fetch_klines("BTCUSDT", "1m", start_time_ms=1, limit=1001)
    with pytest.raises(ValueError, match="1000"):
        prov.fetch_klines("BTCUSDT", "1m", start_time_ms=1, limit=0)


def test_http_error_propagates() -> None:
    session = MagicMock()
    resp = MagicMock()
    resp.raise_for_status.side_effect = RuntimeError("429")
    session.get.return_value = resp
    prov = BinanceSpotKlinesProvider(
        "https://api.binance.com",
        session=session,
        fetch_max_attempts=1,
    )
    with pytest.raises(RuntimeError, match="failed after") as ei:
        prov.fetch_klines("BTCUSDT", "1m", start_time_ms=1)
    assert ei.value.__cause__ is not None and "429" in str(ei.value.__cause__)


def test_fetch_retries_after_invalid_payload_then_succeeds() -> None:
    good = [_KLINE_ROW]
    session = MagicMock()
    session.get.side_effect = [
        _mock_response(["not-a-list-row"]),
        _mock_response(good),
    ]
    prov = BinanceSpotKlinesProvider(
        "https://api.binance.com",
        session=session,
        rate_limiter=ProviderRateLimiter(None),
        fetch_max_attempts=3,
        fetch_retry_base_sleep_sec=0.0,
    )
    with patch("market_data.providers.binance_spot.time.sleep"):
        bars = prov.fetch_klines("BTCUSDT", "1m", start_time_ms=1)
    assert len(bars) == 1
    assert session.get.call_count == 2
