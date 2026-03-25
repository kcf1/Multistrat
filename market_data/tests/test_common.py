"""Tests for shared job helpers in ``market_data.jobs.common``."""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

from market_data.jobs.common import (
    floor_align_ms_to_interval,
    iter_open_interest_batches_forward,
    iter_taker_buy_sell_volume_batches_forward,
    iter_top_trader_long_short_batches_forward,
)
from market_data.schemas import OpenInterestPoint, TakerBuySellVolumePoint, TopTraderLongShortPoint


def _oi_at(ms: int) -> OpenInterestPoint:
    return OpenInterestPoint(
        symbol="BTCUSDT",
        contract_type="PERPETUAL",
        period="1h",
        sample_time=datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc),
        sum_open_interest=Decimal("1"),
        sum_open_interest_value=Decimal("2"),
        cmc_circulating_supply=Decimal("3"),
    )


def test_iter_open_interest_batches_pages_tail_then_head_like_binance() -> None:
    """
    openInterestHist returns the *latest* ``limit`` rows in [start,end], so two windows
    of 500 + 149 hours require two HTTP calls with a lowered ``endTime`` on the second.
    Iterator must yield the older 149 rows before the newer 500 (chronological ingest).
    """
    pd_ms = 3_600_000
    lo = 1_700_000_000_000
    hi = lo + 649 * pd_ms
    # Newest 500 hours in window: hours index 149..648 from lo
    tail = [_oi_at(lo + (149 + i) * pd_ms) for i in range(500)]
    # Older 149 hours: index 0..148
    head = [_oi_at(lo + i * pd_ms) for i in range(149)]
    second_end = lo + 148 * pd_ms

    prov = MagicMock()

    def fetch(
        _s: str,
        _ct: str,
        _pd: str,
        *,
        start_time_ms: int,
        end_time_ms: int,
        limit: int,
    ) -> list[OpenInterestPoint]:
        assert start_time_ms == lo
        assert limit == 500
        if end_time_ms == hi:
            return tail
        if end_time_ms == second_end:
            return head
        raise AssertionError(f"unexpected end_time_ms={end_time_ms}")

    prov.fetch_open_interest_hist.side_effect = fetch

    batches = list(
        iter_open_interest_batches_forward(
            prov,
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            start_ms=lo,
            end_ms=hi,
            chunk_limit=500,
        )
    )
    assert len(batches) == 2
    assert batches[0] == head
    assert batches[1] == tail
    assert sum(len(b) for b in batches) == 649


def test_floor_align_ms_to_interval_1h() -> None:
    pd_ms = 3_600_000
    raw = 1_234_567_890
    assert floor_align_ms_to_interval(raw, "1h") == (raw // pd_ms) * pd_ms


def test_floor_align_ms_to_interval_already_aligned() -> None:
    ms = 10 * 3_600_000
    assert floor_align_ms_to_interval(ms, "1h") == ms


def _taker_point(ms: int) -> TakerBuySellVolumePoint:
    return TakerBuySellVolumePoint(
        symbol="BTCUSDT",
        period="1h",
        sample_time=datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc),
        buy_sell_ratio=Decimal("1.0"),
        buy_vol=Decimal("1.0"),
        sell_vol=Decimal("1.0"),
    )


def _top_trader_long_short_point(ms: int) -> TopTraderLongShortPoint:
    return TopTraderLongShortPoint(
        symbol="BTCUSDT",
        period="1h",
        sample_time=datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc),
        long_short_ratio=Decimal("1.0"),
        long_account_ratio=Decimal("1.0"),
        short_account_ratio=Decimal("0.0"),
    )


def test_iter_taker_buy_sell_volume_pages_tail_then_head_like_binance() -> None:
    """
    takerlongshortRatio returns the *latest* ``limit`` rows in [start,end].

    So two windows of 500 + 149 buckets require two HTTP calls with a lowered ``endTime``
    on the second. Iterator must yield the older 149 rows before the newer 500
    (chronological ingest).
    """
    pd_ms = 3_600_000
    lo = 1_700_000_000_000
    hi = lo + 649 * pd_ms

    # Newest 500 buckets in window: hours index 149..648 from lo
    tail = [_taker_point(lo + (149 + i) * pd_ms) for i in range(500)]
    # Older 149 buckets: index 0..148
    head = [_taker_point(lo + i * pd_ms) for i in range(149)]
    second_end = lo + 148 * pd_ms

    prov = MagicMock()

    def fetch(
        _s: str,
        _p: str,
        *,
        start_time_ms: int,
        end_time_ms: int,
        limit: int,
    ) -> list[TakerBuySellVolumePoint]:
        assert start_time_ms == lo
        assert limit == 500
        if end_time_ms == hi:
            return tail
        if end_time_ms == second_end:
            return head
        raise AssertionError(f"unexpected end_time_ms={end_time_ms}")

    prov.fetch_taker_buy_sell_volume.side_effect = fetch

    batches = list(
        iter_taker_buy_sell_volume_batches_forward(
            prov,
            "BTCUSDT",
            "1h",
            start_ms=lo,
            end_ms=hi,
            chunk_limit=500,
        )
    )
    assert len(batches) == 2
    assert batches[0] == head
    assert batches[1] == tail
    assert sum(len(b) for b in batches) == 649


def test_iter_top_trader_long_short_pages_tail_then_head_like_binance() -> None:
    """
    topLongShortPositionRatio returns the *latest* ``limit`` rows in [start,end].

    So two windows of 500 + 149 buckets require two HTTP calls with a lowered ``endTime``
    on the second. Iterator must yield the older 149 rows before the newer 500
    (chronological ingest).
    """
    pd_ms = 3_600_000
    lo = 1_700_000_000_000
    hi = lo + 649 * pd_ms

    tail = [_top_trader_long_short_point(lo + (149 + i) * pd_ms) for i in range(500)]
    head = [_top_trader_long_short_point(lo + i * pd_ms) for i in range(149)]
    second_end = lo + 148 * pd_ms

    prov = MagicMock()

    def fetch(
        _s: str,
        _p: str,
        *,
        start_time_ms: int,
        end_time_ms: int,
        limit: int,
    ) -> list[TopTraderLongShortPoint]:
        assert start_time_ms == lo
        assert limit == 500
        if end_time_ms == hi:
            return tail
        if end_time_ms == second_end:
            return head
        raise AssertionError(f"unexpected end_time_ms={end_time_ms}")

    prov.fetch_top_trader_long_short_position_ratio.side_effect = fetch

    batches = list(
        iter_top_trader_long_short_batches_forward(
            prov,
            "BTCUSDT",
            "1h",
            start_ms=lo,
            end_ms=hi,
            chunk_limit=500,
        )
    )

    assert len(batches) == 2
    assert batches[0] == head
    assert batches[1] == tail
    assert sum(len(b) for b in batches) == 649


