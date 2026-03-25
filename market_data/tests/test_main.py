"""Tests for market_data.main entrypoint."""

import sys
from types import SimpleNamespace

import pytest

from market_data.main import (
    _due,
    _next_periodic_deadline_after,
    _sleep_deadline,
    main,
    run_scheduler_loop,
)


@pytest.fixture
def mock_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    s = SimpleNamespace(database_url="postgresql://u:p@localhost:5432/md")
    monkeypatch.setattr("market_data.main.load_settings", lambda: s)
    return s


def test_once_invokes_ingest_and_correct(mock_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "market_data.main.run_ingest_ohlcv",
        lambda _s: calls.append("ingest") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_correct_window",
        lambda _s: calls.append("correct") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_repair_gaps_policy_window_all_series",
        lambda _s: calls.append("repair") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_ingest_basis_rate",
        lambda _s: calls.append("basis_ingest") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_correct_window_basis_rate",
        lambda _s: calls.append("basis_correct") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_repair_basis_gaps_policy_window_all_series",
        lambda _s: calls.append("basis_repair") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_ingest_open_interest",
        lambda _s: calls.append("oi_ingest") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_correct_window_open_interest",
        lambda _s: calls.append("oi_correct") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_repair_open_interest_gaps_policy_window_all_series",
        lambda _s: calls.append("oi_repair") or [],
    )
    monkeypatch.setattr(sys, "argv", ["market_data.main", "--once"])
    main()
    assert calls == ["ingest", "basis_ingest", "oi_ingest", "correct", "basis_correct", "oi_correct"]


def test_once_with_repair(mock_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        "market_data.main.run_ingest_ohlcv",
        lambda _s: calls.append("ingest") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_correct_window",
        lambda _s: calls.append("correct") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_repair_gaps_policy_window_all_series",
        lambda _s: calls.append("repair") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_ingest_basis_rate",
        lambda _s: calls.append("basis_ingest") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_correct_window_basis_rate",
        lambda _s: calls.append("basis_correct") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_repair_basis_gaps_policy_window_all_series",
        lambda _s: calls.append("basis_repair") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_ingest_open_interest",
        lambda _s: calls.append("oi_ingest") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_correct_window_open_interest",
        lambda _s: calls.append("oi_correct") or [],
    )
    monkeypatch.setattr(
        "market_data.main.run_repair_open_interest_gaps_policy_window_all_series",
        lambda _s: calls.append("oi_repair") or [],
    )
    monkeypatch.setattr(sys, "argv", ["market_data.main", "--once", "--with-repair"])
    main()
    assert calls == ["ingest", "basis_ingest", "oi_ingest", "correct", "repair", "basis_correct", "basis_repair", "oi_correct", "oi_repair"]


def test_scheduler_loop_respects_immediate_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []
    monkeypatch.setattr("market_data.main._run_ingest_step", lambda: called.append("i"))
    monkeypatch.setattr("market_data.main._run_correct_step", lambda: called.append("c"))
    monkeypatch.setattr("market_data.main._run_repair_step", lambda: called.append("r"))
    monkeypatch.setattr("market_data.main._run_basis_ingest_step", lambda: called.append("bi"))
    monkeypatch.setattr("market_data.main._run_basis_correct_step", lambda: called.append("bc"))
    monkeypatch.setattr("market_data.main._run_basis_repair_step", lambda: called.append("br"))

    import threading

    stop = threading.Event()
    stop.set()
    run_scheduler_loop(
        ingest_interval_seconds=300,
        correct_interval_seconds=3600,
        repair_interval_seconds=0,
        basis_ingest_interval_seconds=300,
        basis_correct_interval_seconds=3600,
        basis_repair_interval_seconds=0,
        open_interest_ingest_interval_seconds=300,
        open_interest_correct_interval_seconds=3600,
        open_interest_repair_interval_seconds=0,
        stop_event=stop,
    )
    assert called == []


def test_scheduler_loop_runs_ingest_once(monkeypatch: pytest.MonkeyPatch) -> None:
    import threading

    stop = threading.Event()
    monkeypatch.setattr(
        "market_data.main._run_ingest_step",
        stop.set,
    )
    monkeypatch.setattr("market_data.main._run_correct_step", lambda: None)
    monkeypatch.setattr("market_data.main._run_repair_step", lambda: None)
    monkeypatch.setattr("market_data.main._run_basis_ingest_step", lambda: None)
    monkeypatch.setattr("market_data.main._run_basis_correct_step", lambda: None)
    monkeypatch.setattr("market_data.main._run_basis_repair_step", lambda: None)
    monkeypatch.setattr("market_data.main._run_open_interest_ingest_step", lambda: None)
    monkeypatch.setattr("market_data.main._run_open_interest_correct_step", lambda: None)
    monkeypatch.setattr("market_data.main._run_open_interest_repair_step", lambda: None)

    run_scheduler_loop(
        ingest_interval_seconds=300,
        correct_interval_seconds=3600,
        repair_interval_seconds=0,
        basis_ingest_interval_seconds=300,
        basis_correct_interval_seconds=3600,
        basis_repair_interval_seconds=0,
        open_interest_ingest_interval_seconds=300,
        open_interest_correct_interval_seconds=3600,
        open_interest_repair_interval_seconds=0,
        stop_event=stop,
    )
    assert stop.is_set()


def test_next_periodic_deadline_after() -> None:
    assert _next_periodic_deadline_after(1000.0, 300) == 1200.0
    assert _next_periodic_deadline_after(1200.0, 300) == 1500.0
    assert _next_periodic_deadline_after(2000.0, 300) == 2100.0


def test_due_and_sleep_deadline() -> None:
    assert _due(100.0, None) is True
    assert _due(100.0, 50.0) is True
    assert _due(100.0, 150.0) is False
    assert _sleep_deadline(None) == 0.0
    assert _sleep_deadline(500.0) == 500.0


def test_open_interest_ingest_step_calls_run_ingest(mock_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[object] = []
    monkeypatch.setattr("market_data.main.load_settings", lambda: mock_settings)
    monkeypatch.setattr(
        "market_data.main.run_ingest_open_interest",
        lambda s: called.append(s) or [],
    )
    from market_data.main import _run_open_interest_ingest_step

    _run_open_interest_ingest_step()
    assert len(called) == 1
    assert called[0] is mock_settings


def test_open_interest_repair_step_calls_policy_repair(mock_settings: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[object] = []
    monkeypatch.setattr("market_data.main.load_settings", lambda: mock_settings)
    monkeypatch.setattr(
        "market_data.main.run_repair_open_interest_gaps_policy_window_all_series",
        lambda s: called.append(s) or [],
    )
    from market_data.main import _run_open_interest_repair_step

    _run_open_interest_repair_step()
    assert len(called) == 1
    assert called[0] is mock_settings
