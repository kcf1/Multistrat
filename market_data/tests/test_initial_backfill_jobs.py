"""Tests for per-dataset ``run_backfill_new_symbols_*`` helpers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from market_data.jobs.ingest_basis_rate import run_backfill_new_symbols_basis_rate
from market_data.jobs.ingest_ohlcv import IngestSeriesResult, run_backfill_new_symbols_ohlcv
from market_data.jobs.ingest_open_interest import (
    IngestOpenInterestSeriesResult,
    run_backfill_new_symbols_open_interest,
)
from market_data.jobs.ingest_taker_buy_sell_volume import (
    IngestTakerBuySellVolumeSeriesResult,
    run_backfill_new_symbols_taker_buy_sell_volume,
)
from market_data.jobs.ingest_top_trader_long_short import (
    IngestTopTraderLongShortSeriesResult,
    run_backfill_new_symbols_top_trader_long_short,
)


class _DummyConn:
    def __init__(self) -> None:
        self.committed = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


@pytest.fixture
def md_settings() -> SimpleNamespace:
    return SimpleNamespace(database_url="postgresql://u:p@localhost:5432/md", intervals=("1h",))


def test_run_backfill_new_symbols_ohlcv_skips_already_done(
    md_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "market_data.jobs.ingest_ohlcv.resolve_runtime_symbols",
        lambda _s: ("AAAUSDT", "BBBUSDT"),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_ohlcv.query_ohlcv_symbols_initial_backfill_complete",
        lambda _conn, **kw: {"AAAUSDT"},
    )
    ingest_calls: list[tuple[str, str]] = []

    def _ingest(conn, prov, sym, iv, **kw):
        ingest_calls.append((sym, iv))
        return IngestSeriesResult(sym, iv, 1, 1)

    monkeypatch.setattr("market_data.jobs.ingest_ohlcv.ingest_ohlcv_series", _ingest)
    monkeypatch.setattr(
        "market_data.jobs.ingest_ohlcv.build_binance_spot_provider", lambda _s: MagicMock()
    )
    marks: list[tuple[str, str]] = []

    def _mark(conn, sym, iv):
        marks.append((sym, iv))

    monkeypatch.setattr("market_data.jobs.ingest_ohlcv.mark_ohlcv_initial_backfill_done", _mark)
    dummy = _DummyConn()
    monkeypatch.setattr("market_data.jobs.ingest_ohlcv.psycopg2.connect", lambda _url: dummy)
    monkeypatch.setattr("market_data.jobs.ingest_ohlcv.configure_for_market_data", lambda _c: None)

    n = run_backfill_new_symbols_ohlcv(md_settings)  # type: ignore[arg-type]
    assert n == 1
    assert ingest_calls == [("BBBUSDT", "1h")]
    assert marks == [("BBBUSDT", "1h")]
    assert dummy.committed is True


def test_run_backfill_new_symbols_ohlcv_continues_after_symbol_failure(
    md_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "market_data.jobs.ingest_ohlcv.resolve_runtime_symbols",
        lambda _s: ("BADUSDT", "GOODUSDT"),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_ohlcv.query_ohlcv_symbols_initial_backfill_complete",
        lambda _conn, **kw: set(),
    )

    def _ingest(conn, prov, sym, iv, **kw):
        if sym == "BADUSDT":
            raise RuntimeError("boom")
        return IngestSeriesResult(sym, iv, 1, 1)

    monkeypatch.setattr("market_data.jobs.ingest_ohlcv.ingest_ohlcv_series", _ingest)
    monkeypatch.setattr(
        "market_data.jobs.ingest_ohlcv.build_binance_spot_provider", lambda _s: MagicMock()
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_ohlcv.mark_ohlcv_initial_backfill_done", lambda *a, **k: None
    )
    monkeypatch.setattr("market_data.jobs.ingest_ohlcv.psycopg2.connect", lambda _url: _DummyConn())
    monkeypatch.setattr("market_data.jobs.ingest_ohlcv.configure_for_market_data", lambda _c: None)

    n = run_backfill_new_symbols_ohlcv(md_settings)  # type: ignore[arg-type]
    assert n == 1


def test_run_backfill_new_symbols_basis_rate_skips_done(
    md_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "market_data.jobs.ingest_basis_rate.resolve_runtime_symbols",
        lambda _s: ("AAAUSDT", "BBBUSDT"),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_basis_rate.query_basis_pairs_initial_backfill_complete",
        lambda _conn, **kw: {"AAAUSDT"},
    )
    ingest_calls: list[str] = []

    def _ingest(conn, prov, pair, ct, pd, **kw):
        ingest_calls.append(pair)
        return MagicMock(rows_upserted=1, chunks=1, pair=pair, contract_type=ct, period=pd)

    monkeypatch.setattr("market_data.jobs.ingest_basis_rate.ingest_basis_series", _ingest)
    monkeypatch.setattr(
        "market_data.jobs.ingest_basis_rate.build_binance_perps_provider", lambda _s: MagicMock()
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_basis_rate.mark_basis_initial_backfill_done", lambda *a, **k: None
    )
    monkeypatch.setattr("market_data.jobs.ingest_basis_rate.psycopg2.connect", lambda _url: _DummyConn())
    monkeypatch.setattr(
        "market_data.jobs.ingest_basis_rate.configure_for_market_data", lambda _c: None
    )

    n = run_backfill_new_symbols_basis_rate(md_settings)  # type: ignore[arg-type]
    assert n == 1
    assert ingest_calls == ["BBBUSDT"]


def test_run_backfill_new_symbols_open_interest_skips_done(
    md_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.resolve_runtime_symbols",
        lambda _s: ("AAAUSDT", "BBBUSDT"),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.query_open_interest_symbols_initial_backfill_complete",
        lambda _conn, **kw: {"AAAUSDT"},
    )
    ingest_calls: list[str] = []

    def _ingest(conn, prov, sym, ct, pd, **kw):
        ingest_calls.append(sym)
        return IngestOpenInterestSeriesResult(sym, ct, pd, 1, 1)

    monkeypatch.setattr("market_data.jobs.ingest_open_interest.ingest_open_interest_series", _ingest)
    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.build_binance_perps_provider",
        lambda _s: MagicMock(),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.mark_open_interest_initial_backfill_done",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.psycopg2.connect", lambda _url: _DummyConn()
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_open_interest.configure_for_market_data", lambda _c: None
    )

    n = run_backfill_new_symbols_open_interest(md_settings)  # type: ignore[arg-type]
    assert n == 1
    assert ingest_calls == ["BBBUSDT"]


def test_run_backfill_new_symbols_taker_skips_done(
    md_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.resolve_runtime_symbols",
        lambda _s: ("AAAUSDT", "BBBUSDT"),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.query_taker_buy_sell_volume_symbols_initial_backfill_complete",
        lambda _conn, **kw: {"AAAUSDT"},
    )
    ingest_calls: list[str] = []

    def _ingest(conn, prov, sym, period, **kw):
        ingest_calls.append(sym)
        return IngestTakerBuySellVolumeSeriesResult(sym, period, 1, 1)

    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.ingest_taker_buy_sell_volume_series", _ingest
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.build_binance_perps_provider",
        lambda _s: MagicMock(),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.mark_taker_buy_sell_volume_initial_backfill_done",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.psycopg2.connect", lambda _url: _DummyConn()
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_taker_buy_sell_volume.configure_for_market_data", lambda _c: None
    )

    n = run_backfill_new_symbols_taker_buy_sell_volume(md_settings)  # type: ignore[arg-type]
    assert n == 1
    assert ingest_calls == ["BBBUSDT"]


def test_run_backfill_new_symbols_top_trader_skips_done(
    md_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "market_data.jobs.ingest_top_trader_long_short.resolve_runtime_symbols",
        lambda _s: ("AAAUSDT", "BBBUSDT"),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_top_trader_long_short.query_top_trader_long_short_symbols_initial_backfill_complete",
        lambda _conn, **kw: {"AAAUSDT"},
    )
    ingest_calls: list[str] = []

    def _ingest(conn, prov, sym, period, **kw):
        ingest_calls.append(sym)
        return IngestTopTraderLongShortSeriesResult(sym, period, 1, 1)

    monkeypatch.setattr(
        "market_data.jobs.ingest_top_trader_long_short.ingest_top_trader_long_short_series", _ingest
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_top_trader_long_short.build_binance_perps_provider",
        lambda _s: MagicMock(),
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_top_trader_long_short.mark_top_trader_long_short_initial_backfill_done",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_top_trader_long_short.psycopg2.connect", lambda _url: _DummyConn()
    )
    monkeypatch.setattr(
        "market_data.jobs.ingest_top_trader_long_short.configure_for_market_data", lambda _c: None
    )

    n = run_backfill_new_symbols_top_trader_long_short(md_settings)  # type: ignore[arg-type]
    assert n == 1
    assert ingest_calls == ["BBBUSDT"]
