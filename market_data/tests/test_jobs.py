"""Tests for market_data jobs (§9.5)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from market_data.config import (
    OPEN_INTEREST_CONTRACT_TYPES,
    OPEN_INTEREST_PERIODS,
    OPEN_INTEREST_SYMBOLS,
    TAKER_BUYSELL_VOLUME_PERIODS,
    TAKER_BUYSELL_VOLUME_SYMBOLS,
)
from market_data.jobs.common import (
    expected_ohlcv_slots,
    iter_kline_batches_forward,
    open_time_plus_interval_ms,
)
from market_data.jobs.correct_window import _log_drifts, run_correct_window_series
from market_data.jobs.correct_window_basis_rate import (
    _log_basis_drifts,
    run_correct_window_basis_series,
)
from market_data.jobs.correct_window_open_interest import (
    CorrectOpenInterestWindowResult,
    _log_open_interest_drifts,
    run_correct_window_open_interest,
    run_correct_window_open_interest_series,
)
from market_data.jobs.ingest_basis_rate import ingest_basis_series
from market_data.jobs.ingest_open_interest import (
    IngestOpenInterestSeriesResult,
    ingest_open_interest_series,
    run_ingest_open_interest,
)
from market_data.jobs.ingest_taker_buy_sell_volume import (
    IngestTakerBuySellVolumeSeriesResult,
    ingest_taker_buy_sell_volume_series,
    resolve_taker_buy_sell_volume_ingest_start_ms,
    run_ingest_taker_buy_sell_volume,
)
from market_data.jobs.ingest_ohlcv import ingest_ohlcv_series
from market_data.jobs.repair_gap_open_interest import (
    detect_open_interest_time_gaps,
    run_repair_open_interest_gap,
)
from market_data.jobs.repair_gap_taker_buy_sell_volume import (
    detect_taker_buy_sell_volume_time_gaps,
    run_repair_taker_buy_sell_volume_gap,
)
from market_data.jobs.repair_gap_basis_rate import (
    detect_basis_time_gaps,
    run_repair_basis_gap,
)
from market_data.jobs.repair_gap import detect_ohlcv_time_gaps, run_repair_gap
from market_data.schemas import BasisPoint, OhlcvBar, OpenInterestPoint
from market_data.jobs.correct_window_taker_buy_sell_volume import (
    CorrectTakerBuySellVolumeWindowResult,
    run_correct_window_taker_buy_sell_volume_series,
    run_correct_window_taker_buy_sell_volume,
)
from market_data.schemas import TakerBuySellVolumePoint


def _bar(
    open_ms: int,
    *,
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    close: Decimal = Decimal("1"),
) -> OhlcvBar:
    ot = datetime.fromtimestamp(open_ms / 1000.0, tz=timezone.utc)
    return OhlcvBar(
        symbol=symbol,
        interval=interval,
        open_time=ot,
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("0.5"),
        close=close,
        volume=Decimal("10"),
    )


def _basis_point(sample_ms: int, *, pair: str = "BTCUSDT") -> BasisPoint:
    st = datetime.fromtimestamp(sample_ms / 1000.0, tz=timezone.utc)
    return BasisPoint(
        pair=pair,
        contract_type="PERPETUAL",
        period="1h",
        sample_time=st,
        basis=Decimal("100"),
        basis_rate=Decimal("0.001"),
        futures_price=Decimal("50100"),
        index_price=Decimal("50000"),
    )


def _open_interest_point(sample_ms: int, *, symbol: str = "BTCUSDT") -> OpenInterestPoint:
    st = datetime.fromtimestamp(sample_ms / 1000.0, tz=timezone.utc)
    return OpenInterestPoint(
        symbol=symbol,
        contract_type="PERPETUAL",
        period="1h",
        sample_time=st,
        sum_open_interest=Decimal("12345.6789"),
        sum_open_interest_value=Decimal("987654321.123456"),
        cmc_circulating_supply=Decimal("19500000.0"),
    )


def _taker_buy_sell_volume_point(sample_ms: int, *, symbol: str = "BTCUSDT") -> TakerBuySellVolumePoint:
    st = datetime.fromtimestamp(sample_ms / 1000.0, tz=timezone.utc)
    return TakerBuySellVolumePoint(
        symbol=symbol,
        period="1h",
        sample_time=st,
        buy_sell_ratio=Decimal("1.5586"),
        buy_vol=Decimal("387.3300"),
        sell_vol=Decimal("248.5030"),
    )


def test_resolve_ingest_start_backfill_when_empty() -> None:
    conn = MagicMock()
    now_ms = 1_000_000_000_000
    with (
        patch("market_data.jobs.ingest_ohlcv.get_ingestion_cursor", return_value=None),
        patch("market_data.jobs.ingest_ohlcv.max_open_time_ohlcv", return_value=None),
    ):
        from market_data.jobs.ingest_ohlcv import resolve_ingest_start_ms

        start = resolve_ingest_start_ms(
            conn,
            "BTCUSDT",
            "1m",
            now_ms=now_ms,
            backfill_days=7,
        )
    assert start == now_ms - 7 * 86_400_000


def test_resolve_ingest_start_after_max_db() -> None:
    conn = MagicMock()
    ref = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    with (
        patch("market_data.jobs.ingest_ohlcv.get_ingestion_cursor", return_value=None),
        patch("market_data.jobs.ingest_ohlcv.max_open_time_ohlcv", return_value=ref),
    ):
        from market_data.jobs.ingest_ohlcv import resolve_ingest_start_ms

        start = resolve_ingest_start_ms(
            conn,
            "BTCUSDT",
            "1m",
            now_ms=2_000_000_000_000,
            backfill_days=30,
        )
    assert start == open_time_plus_interval_ms(ref, 60_000)


def test_resolve_ingest_start_ignores_watermark_when_disabled() -> None:
    conn = MagicMock()
    now_ms = 1_000_000_000_000
    ref = datetime(2024, 1, 1, tzinfo=timezone.utc)
    with (
        patch("market_data.jobs.ingest_ohlcv.get_ingestion_cursor", return_value=ref),
        patch("market_data.jobs.ingest_ohlcv.max_open_time_ohlcv", return_value=ref),
    ):
        from market_data.jobs.ingest_ohlcv import resolve_ingest_start_ms

        start = resolve_ingest_start_ms(
            conn,
            "BTCUSDT",
            "1m",
            now_ms=now_ms,
            backfill_days=7,
            use_watermark=False,
        )
    assert start == now_ms - 7 * 86_400_000


def test_resolve_no_watermark_skip_existing_same_as_horizon() -> None:
    """``skip_existing`` does not change :func:`resolve_ingest_start_ms` (gap logic is in ingest)."""
    conn = MagicMock()
    now_ms = 2_000_000_000_000
    ref = datetime(2020, 1, 1, 12, 0, tzinfo=timezone.utc)
    horizon = now_ms - 7 * 86_400_000
    with patch("market_data.jobs.ingest_ohlcv.max_open_time_ohlcv", return_value=ref):
        from market_data.jobs.ingest_ohlcv import resolve_ingest_start_ms

        with_skip = resolve_ingest_start_ms(
            conn,
            "BTCUSDT",
            "1m",
            now_ms=now_ms,
            backfill_days=7,
            use_watermark=False,
            skip_existing_when_no_watermark=True,
        )
        no_skip = resolve_ingest_start_ms(
            conn,
            "BTCUSDT",
            "1m",
            now_ms=now_ms,
            backfill_days=7,
            use_watermark=False,
            skip_existing_when_no_watermark=False,
        )
    assert with_skip == horizon == no_skip


def test_ingest_skip_existing_gap_mode_calls_detect_and_segments() -> None:
    end_ms = 2_000_000_000_000
    backfill_days = 1
    horizon_ms = end_ms - backfill_days * 86_400_000
    h_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    g1 = h_dt + timedelta(minutes=10)
    m_tail = datetime.fromtimestamp(end_ms / 1000.0 - 300, tz=timezone.utc)

    seg_calls: list[tuple[int, int]] = []

    def fake_seg(_conn, _prov, _sym, _iv, *, start_ms, end_ms, chunk_limit, chunk_progress):
        seg_calls.append((start_ms, end_ms))
        return (1, 1)

    conn = MagicMock()
    prov = MagicMock()

    with (
        patch(
            "market_data.jobs.ingest_ohlcv.detect_ohlcv_time_gaps",
            return_value=[(h_dt, g1)],
        ) as det,
        patch(
            "market_data.jobs.ingest_ohlcv._ingest_forward_segment",
            side_effect=fake_seg,
        ),
        patch("market_data.jobs.ingest_ohlcv.max_open_time_ohlcv", return_value=m_tail),
    ):
        from market_data.jobs.ingest_ohlcv import ingest_ohlcv_series

        r = ingest_ohlcv_series(
            conn,
            prov,
            "BTCUSDT",
            "1m",
            now_ms=end_ms,
            backfill_days=backfill_days,
            use_watermark=False,
            skip_existing_when_no_watermark=True,
        )

    det.assert_called_once()
    assert len(seg_calls) == 2
    assert r.bars_upserted == 2
    assert r.chunks == 2


def test_expected_ohlcv_slots() -> None:
    assert expected_ohlcv_slots(0, 600_000, 60_000) == 11


def test_iter_kline_batches_forward_pages() -> None:
    calls: list[tuple[int, int | None]] = []

    class P:
        def fetch_klines(self, symbol, interval, *, start_time_ms, end_time_ms=None, limit=1000):
            calls.append((start_time_ms, end_time_ms))
            if start_time_ms == 1000:
                return [_bar(1000), _bar(61_000)]
            if start_time_ms == 121_000:
                return [_bar(121_000)]
            return []

    p = P()
    batches = list(
        iter_kline_batches_forward(
            p,
            "BTCUSDT",
            "1m",
            start_ms=1000,
            end_ms=200_000,
            chunk_limit=1000,
        )
    )
    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1
    assert calls[0][0] == 1000
    assert calls[1][0] == 121_000


def test_ingest_ohlcv_series_commits_per_chunk() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    b1 = [_bar(60_000), _bar(120_000)]
    b2 = [_bar(180_000)]

    class P:
        def __init__(self) -> None:
            self.n = 0

        def fetch_klines(self, symbol, interval, *, start_time_ms, end_time_ms=None, limit=1000):
            self.n += 1
            if self.n == 1:
                return b1
            if self.n == 2:
                return b2
            return []

    with (
        patch("market_data.jobs.ingest_ohlcv.resolve_ingest_start_ms", return_value=60_000),
        patch("market_data.jobs.ingest_ohlcv.upsert_ohlcv_bars") as uo,
        patch("market_data.jobs.ingest_ohlcv.upsert_ingestion_cursor") as uc,
    ):
        r = ingest_ohlcv_series(
            conn,
            P(),
            "BTCUSDT",
            "1m",
            now_ms=400_000,
            chunk_limit=1000,
        )

    assert r.bars_upserted == 3
    assert r.chunks == 2
    assert r.fetch_give_ups == ()
    assert uo.call_count == 2
    assert uc.call_count == 2
    assert conn.commit.call_count == 2


def test_run_repair_gap_noop_on_bad_range() -> None:
    conn = MagicMock()
    p = MagicMock()
    assert run_repair_gap(conn, p, "BTCUSDT", "1m", start_time_ms=500, end_time_ms=500) == 0
    p.fetch_klines.assert_not_called()


def test_detect_ohlcv_time_gaps_interior() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = t0.replace(minute=1)
    t3 = t0.replace(minute=3)
    cur.fetchall.return_value = [(t0,), (t1,), (t3,)]
    conn.cursor.return_value = cur

    rs = datetime(2023, 12, 31, 23, 59, tzinfo=timezone.utc)
    re_ = datetime(2024, 1, 1, 0, 10, tzinfo=timezone.utc)
    gaps = detect_ohlcv_time_gaps(conn, "BTCUSDT", "1m", rs, re_)
    assert len(gaps) >= 1
    # missing minute 2 between t1 and t3
    assert any(g[0].minute == 2 or g[1].minute == 2 for g in gaps)


def test_log_drifts_warns_on_mismatch() -> None:
    ot = datetime(2020, 1, 1, tzinfo=timezone.utc)
    bar = _bar(int(ot.timestamp() * 1000), close=Decimal("2"))
    existing = {ot: (Decimal("1"), Decimal("2"), Decimal("0.5"), Decimal("1"))}
    with patch("market_data.jobs.correct_window.logger.debug") as w:
        n = _log_drifts(existing, [bar])
    assert n == 1
    assert w.called


def test_run_correct_window_series_upserts() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    bars = [_bar(1_700_000_000_000)]

    class P:
        def fetch_klines(self, *a, **k):
            return bars

    with (
        patch("market_data.jobs.correct_window.chunk_fetch_forward", return_value=bars),
        patch("market_data.jobs.correct_window.fetch_ohlc_by_open_times", return_value={}),
        patch("market_data.jobs.correct_window.upsert_ohlcv_bars") as uo,
    ):
        r = run_correct_window_series(
            conn,
            P(),
            "BTCUSDT",
            "1m",
            lookback_bars=10,
            now_ms=1_700_000_060_000,
        )
    assert r.bars_fetched == 1
    assert r.drift_rows == 0
    uo.assert_called_once_with(conn, bars)
    conn.commit.assert_called_once()


def test_resolve_basis_ingest_start_backfill_when_empty() -> None:
    conn = MagicMock()
    now_ms = 1_000_000_000_000
    with (
        patch("market_data.jobs.ingest_basis_rate.get_basis_cursor", return_value=None),
        patch("market_data.jobs.ingest_basis_rate.max_sample_time_basis", return_value=None),
    ):
        from market_data.jobs.ingest_basis_rate import resolve_basis_ingest_start_ms

        start = resolve_basis_ingest_start_ms(
            conn,
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            now_ms=now_ms,
            backfill_days=7,
        )
    assert start == now_ms - 7 * 86_400_000


def test_ingest_basis_series_commits_per_chunk() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    p1 = [_basis_point(60_000), _basis_point(3_660_000)]
    p2 = [_basis_point(7_260_000)]

    class P:
        def __init__(self) -> None:
            self.n = 0

        def fetch_basis(self, *args, **kwargs):
            self.n += 1
            if self.n == 1:
                return p1
            if self.n == 2:
                return p2
            return []

    with (
        patch("market_data.jobs.ingest_basis_rate.resolve_basis_ingest_start_ms", return_value=60_000),
        patch("market_data.jobs.ingest_basis_rate.upsert_basis_points") as up,
        patch("market_data.jobs.ingest_basis_rate.upsert_basis_cursor") as uc,
    ):
        r = ingest_basis_series(
            conn,
            P(),
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            now_ms=10_000_000,
            chunk_limit=500,
        )
    assert r.rows_upserted == 3
    assert r.chunks == 2
    assert up.call_count == 2
    assert uc.call_count == 2
    assert conn.commit.call_count == 2


def test_run_repair_basis_gap_noop_on_bad_range() -> None:
    conn = MagicMock()
    p = MagicMock()
    assert run_repair_basis_gap(conn, p, "BTCUSDT", "PERPETUAL", "1h", start_time_ms=500, end_time_ms=500) == 0
    p.fetch_basis.assert_not_called()


def test_detect_basis_time_gaps_interior() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
    t3 = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)
    cur.fetchall.return_value = [(t0,), (t1,), (t3,)]
    conn.cursor.return_value = cur
    rs = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    re_ = datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc)
    gaps = detect_basis_time_gaps(conn, "BTCUSDT", "PERPETUAL", "1h", rs, re_)
    assert any(g[0].hour == 2 or g[1].hour == 2 for g in gaps)


def test_log_basis_drifts_warns_on_mismatch() -> None:
    st = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    row = _basis_point(int(st.timestamp() * 1000))
    existing = {st: (Decimal("99"), Decimal("0.001"), Decimal("50100"), Decimal("50000"))}
    with patch("market_data.jobs.correct_window_basis_rate.logger.debug") as w:
        n = _log_basis_drifts(existing, [row])
    assert n == 1
    assert w.called


def test_run_correct_window_basis_series_upserts() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    rows = [_basis_point(1_700_000_000_000)]

    class P:
        def fetch_basis(self, *a, **k):
            return rows

    with (
        patch("market_data.jobs.correct_window_basis_rate.chunk_fetch_basis_forward", return_value=rows),
        patch("market_data.jobs.correct_window_basis_rate.fetch_basis_rates_by_sample_times", return_value={}),
        patch("market_data.jobs.correct_window_basis_rate.upsert_basis_points") as up,
    ):
        r = run_correct_window_basis_series(
            conn,
            P(),
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            lookback_points=10,
            now_ms=1_700_000_060_000,
        )
    assert r.rows_fetched == 1
    assert r.drift_rows == 0
    up.assert_called_once_with(conn, rows)
    conn.commit.assert_called_once()


def test_resolve_open_interest_ingest_start_backfill_when_empty() -> None:
    conn = MagicMock()
    now_ms = 1_000_000_000_000
    with (
        patch("market_data.jobs.ingest_open_interest.get_open_interest_cursor", return_value=None),
        patch("market_data.jobs.ingest_open_interest.max_sample_time_open_interest", return_value=None),
    ):
        from market_data.jobs.ingest_open_interest import resolve_open_interest_ingest_start_ms

        start = resolve_open_interest_ingest_start_ms(
            conn,
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            now_ms=now_ms,
            backfill_days=7,
        )
    raw_horizon = now_ms - 7 * 86_400_000
    pd_ms = 3_600_000
    assert start == (raw_horizon // pd_ms) * pd_ms


def test_resolve_taker_buy_sell_volume_ingest_start_backfill_when_empty() -> None:
    conn = MagicMock()
    now_ms = 1_000_000_000_000
    with (
        patch(
            "market_data.jobs.ingest_taker_buy_sell_volume.get_taker_buy_sell_volume_cursor",
            return_value=None,
        ),
        patch(
            "market_data.jobs.ingest_taker_buy_sell_volume.max_sample_time_taker_buy_sell_volume",
            return_value=None,
        ),
    ):
        from market_data.jobs.ingest_taker_buy_sell_volume import (
            resolve_taker_buy_sell_volume_ingest_start_ms,
        )

        start = resolve_taker_buy_sell_volume_ingest_start_ms(
            conn,
            "BTCUSDT",
            "1h",
            now_ms=now_ms,
            backfill_days=7,
        )

    raw_horizon = now_ms - 7 * 86_400_000
    pd_ms = 3_600_000
    assert start == (raw_horizon // pd_ms) * pd_ms


def test_ingest_taker_buy_sell_volume_series_commits_per_chunk() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    p_tail = [_taker_buy_sell_volume_point(7_260_000)]
    p_head = [_taker_buy_sell_volume_point(60_000), _taker_buy_sell_volume_point(3_660_000)]

    class P:
        def fetch_taker_buy_sell_volume(self, *args, **kwargs):
            end_time_ms = kwargs["end_time_ms"]
            # iterator pages backward by lowering endTime; first returns the tail (newest),
            # then returns the head (older window chunk).
            if end_time_ms == 10_000_000:
                return p_tail
            if end_time_ms == 3_660_000:
                return p_head
            return []

    with (
        patch(
            "market_data.jobs.ingest_taker_buy_sell_volume.resolve_taker_buy_sell_volume_ingest_start_ms",
            return_value=60_000,
        ),
        patch("market_data.jobs.ingest_taker_buy_sell_volume.upsert_taker_buy_sell_volume_points") as up,
        patch(
            "market_data.jobs.ingest_taker_buy_sell_volume.upsert_taker_buy_sell_volume_cursor"
        ) as uc,
    ):
        r = ingest_taker_buy_sell_volume_series(
            conn,
            P(),
            "BTCUSDT",
            "1h",
            now_ms=10_000_000,
            chunk_limit=500,
        )

    assert r.rows_upserted == 3
    assert r.chunks == 2
    assert up.call_count == 2
    assert uc.call_count == 2
    assert conn.commit.call_count == 2


def test_ingest_taker_buy_sell_volume_skip_existing_gap_mode_calls_detect_and_segments() -> None:
    end_ms = 2_000_000_000_000
    backfill_days = 1
    horizon_ms = end_ms - backfill_days * 86_400_000
    horizon_dt = datetime.fromtimestamp(horizon_ms / 1000.0, tz=timezone.utc)
    g1 = horizon_dt + timedelta(hours=2)

    pd_ms = 3_600_000  # period=1h in this test
    # choose last stored sample time so tail_start = m_tail + 1h is < end_ms
    m_tail = datetime.fromtimestamp((end_ms - pd_ms - 300_000) / 1000.0, tz=timezone.utc)

    seg_calls: list[tuple[int, int]] = []

    def fake_seg(_conn, _prov, _sym, _period, *, start_ms, end_ms, chunk_limit, chunk_progress):
        seg_calls.append((start_ms, end_ms))
        return (1, 1)

    conn = MagicMock()
    prov = MagicMock()

    with (
        patch(
            "market_data.jobs.ingest_taker_buy_sell_volume.detect_taker_buy_sell_volume_time_gaps",
            return_value=[(horizon_dt, g1)],
        ),
        patch(
            "market_data.jobs.ingest_taker_buy_sell_volume._ingest_taker_buy_sell_volume_forward_segment",
            side_effect=fake_seg,
        ),
        patch(
            "market_data.jobs.ingest_taker_buy_sell_volume.max_sample_time_taker_buy_sell_volume",
            return_value=m_tail,
        ),
    ):
        r = ingest_taker_buy_sell_volume_series(
            conn,
            prov,
            "BTCUSDT",
            "1h",
            now_ms=end_ms,
            backfill_days=backfill_days,
            use_watermark=False,
            skip_existing_when_no_watermark=True,
            chunk_limit=500,
        )

    assert r.rows_upserted == 2
    assert r.chunks == 2
    assert len(seg_calls) == 2


def test_ingest_open_interest_series_commits_per_chunk() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    # openInterestHist returns newest segment first; older page uses lower endTime.
    p_tail = [_open_interest_point(7_260_000)]
    p_head = [_open_interest_point(60_000), _open_interest_point(3_660_000)]

    class P:
        def fetch_open_interest_hist(self, *args, **kwargs):
            end_time_ms = kwargs["end_time_ms"]
            if end_time_ms == 10_000_000:
                return p_tail
            if end_time_ms == 3_660_000:
                return p_head
            return []

    with (
        patch(
            "market_data.jobs.ingest_open_interest.resolve_open_interest_ingest_start_ms",
            return_value=60_000,
        ),
        patch("market_data.jobs.ingest_open_interest.upsert_open_interest_points") as up,
        patch("market_data.jobs.ingest_open_interest.upsert_open_interest_cursor") as uc,
    ):
        r = ingest_open_interest_series(
            conn,
            P(),
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            now_ms=10_000_000,
            chunk_limit=500,
        )
    assert r.rows_upserted == 3
    assert r.chunks == 2
    assert up.call_count == 2
    assert uc.call_count == 2
    assert conn.commit.call_count == 2


def test_run_repair_open_interest_gap_noop_on_bad_range() -> None:
    conn = MagicMock()
    p = MagicMock()
    assert (
        run_repair_open_interest_gap(
            conn,
            p,
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            start_time_ms=500,
            end_time_ms=500,
        )
        == 0
    )
    p.fetch_open_interest_hist.assert_not_called()


def test_run_repair_taker_buy_sell_volume_gap_noop_on_bad_range() -> None:
    conn = MagicMock()
    p = MagicMock()
    assert (
        run_repair_taker_buy_sell_volume_gap(
            conn,
            p,
            "BTCUSDT",
            "1h",
            start_time_ms=500,
            end_time_ms=500,
        )
        == 0
    )
    p.fetch_taker_buy_sell_volume.assert_not_called()


def test_detect_open_interest_time_gaps_interior() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
    t3 = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)
    cur.fetchall.return_value = [(t0,), (t1,), (t3,)]
    conn.cursor.return_value = cur
    rs = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    re_ = datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc)
    gaps = detect_open_interest_time_gaps(conn, "BTCUSDT", "PERPETUAL", "1h", rs, re_)
    assert any(g[0].hour == 2 or g[1].hour == 2 for g in gaps)


def test_detect_taker_buy_sell_volume_time_gaps_interior() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    t0 = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
    t3 = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)
    cur.fetchall.return_value = [(t0,), (t1,), (t3,)]
    conn.cursor.return_value = cur
    rs = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    re_ = datetime(2024, 1, 1, 6, 0, tzinfo=timezone.utc)
    gaps = detect_taker_buy_sell_volume_time_gaps(conn, "BTCUSDT", "1h", rs, re_)
    assert any(g[0].hour == 2 or g[1].hour == 2 for g in gaps)


def test_log_open_interest_drifts_warns_on_mismatch() -> None:
    st = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    row = _open_interest_point(int(st.timestamp() * 1000))
    existing = {st: (Decimal("1"), Decimal("2"), Decimal("3"))}
    with patch("market_data.jobs.correct_window_open_interest.logger.debug") as w:
        n = _log_open_interest_drifts(existing, [row])
    assert n == 1
    assert w.called


def test_run_correct_window_open_interest_series_upserts() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    rows = [_open_interest_point(1_700_000_000_000)]

    class P:
        def fetch_open_interest_hist(self, *a, **k):
            return rows

    with (
        patch("market_data.jobs.correct_window_open_interest.chunk_fetch_open_interest_forward", return_value=rows),
        patch("market_data.jobs.correct_window_open_interest.fetch_open_interest_by_sample_times", return_value={}),
        patch("market_data.jobs.correct_window_open_interest.upsert_open_interest_points") as up,
    ):
        r = run_correct_window_open_interest_series(
            conn,
            P(),
            "BTCUSDT",
            "PERPETUAL",
            "1h",
            lookback_points=10,
            now_ms=1_700_000_060_000,
        )
    assert r.rows_fetched == 1
    assert r.drift_rows == 0
    up.assert_called_once_with(conn, rows)
    conn.commit.assert_called_once()


def test_run_correct_window_taker_buy_sell_volume_series_upserts() -> None:
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    rows = [_taker_buy_sell_volume_point(1_700_000_000_000)]

    class P:
        def fetch_taker_buy_sell_volume(self, *a, **k):
            return rows

    with (
        patch(
            "market_data.jobs.correct_window_taker_buy_sell_volume.chunk_fetch_taker_buy_sell_volume_forward",
            return_value=rows,
        ),
        patch(
            "market_data.jobs.correct_window_taker_buy_sell_volume.fetch_taker_buy_sell_volume_by_sample_times",
            return_value={},
        ),
        patch(
            "market_data.jobs.correct_window_taker_buy_sell_volume.upsert_taker_buy_sell_volume_points"
        ) as up,
    ):
        r = run_correct_window_taker_buy_sell_volume_series(
            conn,
            P(),
            "BTCUSDT",
            "1h",
            lookback_points=10,
            now_ms=1_700_000_060_000,
        )

    assert r.rows_fetched == 1
    assert r.drift_rows == 0
    up.assert_called_once_with(conn, rows)
    conn.commit.assert_called_once()


def test_run_ingest_taker_buy_sell_volume_calls_series_once_per_config_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_ingest(conn, prov, symbol, period, **kwargs):
        calls.append((symbol, period))
        return IngestTakerBuySellVolumeSeriesResult(symbol, period, 0, 0, ())

    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.ingest_taker_buy_sell_volume_series",
        fake_ingest,
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.psycopg2.connect",
        lambda _url: MagicMock(close=MagicMock()),
    )
    settings = SimpleNamespace(database_url="postgresql://test")
    run_ingest_taker_buy_sell_volume(settings, provider=MagicMock())

    expected_n = len(TAKER_BUYSELL_VOLUME_SYMBOLS) * len(TAKER_BUYSELL_VOLUME_PERIODS)
    assert len(calls) == expected_n


def test_run_correct_window_taker_buy_sell_volume_calls_series_once_per_config_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str]] = []

    def fake_series(conn, prov, symbol, period, **kwargs):
        calls.append((symbol, period))
        return CorrectTakerBuySellVolumeWindowResult(symbol, period, 0, 0)

    monkeypatch.setattr(
        "market_data.jobs.correct_window_taker_buy_sell_volume.run_correct_window_taker_buy_sell_volume_series",
        fake_series,
    )
    monkeypatch.setattr(
        "market_data.jobs.correct_window_taker_buy_sell_volume.psycopg2.connect",
        lambda _url: MagicMock(close=MagicMock()),
    )
    settings = SimpleNamespace(database_url="postgresql://test")
    run_correct_window_taker_buy_sell_volume(settings, provider=MagicMock())

    expected_n = len(TAKER_BUYSELL_VOLUME_SYMBOLS) * len(TAKER_BUYSELL_VOLUME_PERIODS)
    assert len(calls) == expected_n


def test_run_ingest_open_interest_calls_series_once_per_config_key(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str]] = []

    def fake_ingest(conn, prov, symbol, contract_type, period, **kwargs):
        calls.append((symbol, contract_type, period))
        return IngestOpenInterestSeriesResult(symbol, contract_type, period, 0, 0, ())

    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.ingest_open_interest_series",
        fake_ingest,
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.psycopg2.connect",
        lambda _url: MagicMock(close=MagicMock()),
    )
    settings = SimpleNamespace(database_url="postgresql://test")
    run_ingest_open_interest(settings, provider=MagicMock())
    expected_n = len(OPEN_INTEREST_SYMBOLS) * len(OPEN_INTEREST_CONTRACT_TYPES) * len(OPEN_INTEREST_PERIODS)
    assert len(calls) == expected_n


def test_run_correct_window_open_interest_calls_series_once_per_config_key(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str, str]] = []

    def fake_series(conn, prov, symbol, contract_type, period, **kwargs):
        calls.append((symbol, contract_type, period))
        return CorrectOpenInterestWindowResult(symbol, contract_type, period, 0, 0)

    monkeypatch.setattr(
        "market_data.jobs.correct_window_open_interest.run_correct_window_open_interest_series",
        fake_series,
    )
    monkeypatch.setattr(
        "market_data.jobs.correct_window_open_interest.psycopg2.connect",
        lambda _url: MagicMock(close=MagicMock()),
    )
    settings = SimpleNamespace(database_url="postgresql://test")
    run_correct_window_open_interest(settings, provider=MagicMock())
    expected_n = len(OPEN_INTEREST_SYMBOLS) * len(OPEN_INTEREST_CONTRACT_TYPES) * len(OPEN_INTEREST_PERIODS)
    assert len(calls) == expected_n
