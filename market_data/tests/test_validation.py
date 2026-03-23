"""Tests for klines payload validation."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from market_data.schemas import OhlcvBar, parse_binance_kline
from market_data.validation import (
    process_binance_basis_payload,
    process_binance_klines_payload,
    scan_bar_series_grid_gaps,
    theoretical_max_bars_in_window,
)


def _row(
    open_ms: int,
    o: str = "1",
    h: str = "2",
    l: str = "0.5",
    c: str = "1",
    v: str = "10",
    close_ms: int | None = None,
) -> list:
    ct = close_ms if close_ms is not None else open_ms + 59_999
    return [open_ms, o, h, l, c, v, ct]


def test_process_payload_rejects_non_list() -> None:
    with pytest.raises(ValueError, match="JSON array"):
        process_binance_klines_payload({}, symbol="BTCUSDT", interval="1m")


def test_process_payload_row_not_list() -> None:
    with pytest.raises(ValueError, match="row\\[0\\]"):
        process_binance_klines_payload([123], symbol="BTCUSDT", interval="1m")


def test_process_payload_empty_ok() -> None:
    assert process_binance_klines_payload([], symbol="BTCUSDT", interval="1m") == []


def test_process_payload_duplicate_open_time() -> None:
    r = _row(1000, "1", "2", "0.5", "1", "10", 2000)
    with pytest.raises(ValueError, match="duplicate open_time"):
        process_binance_klines_payload([r, r], symbol="BTCUSDT", interval="1m")


def test_process_payload_non_monotonic() -> None:
    a = _row(1000, "1", "2", "0.5", "1", "10", 2000)
    b = _row(70_000, "1", "2", "0.5", "1", "10", 80_000)
    with pytest.raises(ValueError, match="non-increasing"):
        process_binance_klines_payload([b, a], symbol="BTCUSDT", interval="1m")


def test_ohlcv_bar_model_rejects_bad_spread() -> None:
    from pydantic import ValidationError

    ot = datetime(2020, 1, 1, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="high must be >= low"):
        OhlcvBar(
            symbol="BTCUSDT",
            interval="1m",
            open_time=ot,
            open=Decimal("1"),
            high=Decimal("0.5"),
            low=Decimal("2"),
            close=Decimal("1"),
            volume=Decimal("1"),
        )


def test_parse_rejects_empty_ohlc_field() -> None:
    r = _row(1000, "", "2", "0.5", "1", "10", close_ms=2000)
    with pytest.raises(ValueError, match="empty kline field"):
        parse_binance_kline(r, symbol="BTCUSDT", interval="1m")


def test_large_time_gap_allowed_in_payload() -> None:
    """Venues skip candles for illiquid periods; multi-interval jumps must not fail ingest."""
    a = _row(0)
    b = _row(180_000)
    with patch("market_data.validation.logger.warning") as w:
        bars = process_binance_klines_payload([a, b], symbol="BTCUSDT", interval="1m")
    w.assert_not_called()
    assert len(bars) == 2


def test_gap_beyond_buffer_logs_warning() -> None:
    """More than ``OHLCV_KLINES_WARN_OPEN_TIME_GAP_BARS`` implied missing bars → warning."""
    a = _row(0)
    b = _row(420_000)  # 7 min on 1m → implied 6 missing
    with patch("market_data.validation.logger.warning") as w:
        bars = process_binance_klines_payload([a, b], symbol="BTCUSDT", interval="1m")
    assert len(bars) == 2
    w.assert_called_once()


def test_interior_overlap_rejected() -> None:
    a = _row(0)
    b = _row(30_000)
    with pytest.raises(ValueError, match="overlap|open_time"):
        process_binance_klines_payload([a, b], symbol="BTCUSDT", interval="1m")


def test_tail_shortfall_with_explicit_window_warns_only() -> None:
    """Short last page with room left to ``end_ms`` is allowed (delisted / venue gaps)."""
    raw = [_row(0), _row(60_000)]
    with patch("market_data.validation.logger.warning") as w:
        bars = process_binance_klines_payload(
            raw,
            symbol="BTCUSDT",
            interval="1m",
            start_time_ms=0,
            end_time_ms=600_000,
            request_limit=1000,
        )
    assert len(bars) == 2
    assert w.called


def test_span_checks_skipped_for_small_window() -> None:
    raw = [_row(0), _row(60_000)]
    bars = process_binance_klines_payload(
        raw,
        symbol="BTCUSDT",
        interval="1m",
        start_time_ms=0,
        end_time_ms=500_000,
        request_limit=1000,
    )
    assert len(bars) == 2


def test_head_slack_warns_only() -> None:
    """Large gap from ``startTime`` to first open is allowed (late-listed symbols)."""
    raw = [_row(250_000)]
    with patch("market_data.validation.logger.warning") as w:
        bars = process_binance_klines_payload(
            raw,
            symbol="BTCUSDT",
            interval="1m",
            start_time_ms=0,
            end_time_ms=600_000,
            request_limit=1000,
        )
    assert len(bars) == 1
    assert w.called


def test_theoretical_max_bars_in_window() -> None:
    assert theoretical_max_bars_in_window(0, 600_000, 60_000, 1000) == 12
    assert theoretical_max_bars_in_window(0, 600_000, 60_000, 5) == 5


def test_scan_bar_series_grid_gaps_clean() -> None:
    ot0 = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    ot1 = datetime(2020, 1, 1, 0, 1, tzinfo=timezone.utc)
    bars = [
        OhlcvBar(
            symbol="BTCUSDT",
            interval="1m",
            open_time=ot0,
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1"),
            volume=Decimal("10"),
        ),
        OhlcvBar(
            symbol="BTCUSDT",
            interval="1m",
            open_time=ot1,
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1"),
            volume=Decimal("10"),
        ),
    ]
    assert scan_bar_series_grid_gaps(bars, "1m") == []


def test_scan_bar_series_grid_gaps_detects() -> None:
    """Notes only when implied missing bars exceed the configured buffer (default 5)."""
    bars = [
        OhlcvBar(
            symbol="BTCUSDT",
            interval="1m",
            open_time=datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc),
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1"),
            volume=Decimal("10"),
        ),
        OhlcvBar(
            symbol="BTCUSDT",
            interval="1m",
            open_time=datetime(2020, 1, 1, 0, 7, tzinfo=timezone.utc),
            open=Decimal("1"),
            high=Decimal("2"),
            low=Decimal("0.5"),
            close=Decimal("1"),
            volume=Decimal("10"),
        ),
    ]
    g = scan_bar_series_grid_gaps(bars, "1m")
    assert g and "gap" in g[0].lower()


def _basis_row(ts: int, *, pair: str = "BTCUSDT") -> dict:
    return {
        "indexPrice": "46317.16333333",
        "contractType": "PERPETUAL",
        "basisRate": "0.00295565",
        "futuresPrice": "46454.22",
        "basis": "137.05666667",
        "pair": pair,
        "timestamp": ts,
        "period": "1h",
    }


def test_process_basis_payload_rejects_non_list() -> None:
    with pytest.raises(ValueError, match="JSON array"):
        process_binance_basis_payload(
            {},
            pair="BTCUSDT",
            contract_type="PERPETUAL",
            period="1h",
        )


def test_process_basis_payload_row_not_object() -> None:
    with pytest.raises(ValueError, match=r"row\[0\]"):
        process_binance_basis_payload(
            [123],
            pair="BTCUSDT",
            contract_type="PERPETUAL",
            period="1h",
        )


def test_process_basis_payload_empty_ok() -> None:
    out = process_binance_basis_payload(
        [],
        pair="BTCUSDT",
        contract_type="PERPETUAL",
        period="1h",
    )
    assert out == []


def test_process_basis_payload_duplicate_sample_time() -> None:
    r = _basis_row(1_640_995_200_000)
    with pytest.raises(ValueError, match="duplicate sample_time"):
        process_binance_basis_payload(
            [r, r],
            pair="BTCUSDT",
            contract_type="PERPETUAL",
            period="1h",
        )


def test_process_basis_payload_non_monotonic() -> None:
    a = _basis_row(1_640_995_200_000)
    b = _basis_row(1_640_995_100_000)
    with pytest.raises(ValueError, match="non-increasing"):
        process_binance_basis_payload(
            [a, b],
            pair="BTCUSDT",
            contract_type="PERPETUAL",
            period="1h",
        )
