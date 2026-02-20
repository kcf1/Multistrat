"""
Integration tests for PMS loop (12.3.8): read → derive → calculate → write.

Mock data; verify loop runs and writes results. Redis skipped.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from pms.loop import enrich_positions_with_mark_prices, run_one_tick, run_pms_loop
from pms.mark_price import FakeMarkPriceProvider
from pms.schemas_pydantic import DerivedPosition


class TestEnrichPositionsWithMarkPrices:
    def test_empty_returns_empty(self):
        from pms.mark_price import MarkPriceProvider
        class Noop(MarkPriceProvider):
            def get_mark_prices(self, symbols):
                return type('R', (), {'prices': {}})()
        assert enrich_positions_with_mark_prices([], Noop()) == []

    def test_sets_mark_notional_unrealized(self):
        positions = [
            DerivedPosition(
                account_id="a", book="", symbol="BTCUSDT",
                open_qty=1.0, position_side="long", entry_avg=50000.0,
                mark_price=None, notional=None, unrealized_pnl=0.0,
            ),
        ]
        provider = FakeMarkPriceProvider({"BTCUSDT": 51000})
        out = enrich_positions_with_mark_prices(positions, provider)
        assert len(out) == 1
        assert out[0].mark_price == 51000.0
        assert out[0].notional == 51000.0
        assert out[0].unrealized_pnl == 1000.0  # (51000 - 50000) * 1

    def test_flat_position_keeps_zero_unrealized(self):
        positions = [
            DerivedPosition(
                account_id="a", book="", symbol="ETHUSDT",
                open_qty=0.0, position_side="flat", entry_avg=None,
                mark_price=None, notional=None, unrealized_pnl=0.0,
            ),
        ]
        provider = FakeMarkPriceProvider({"ETHUSDT": 3000})
        out = enrich_positions_with_mark_prices(positions, provider)
        assert len(out) == 1
        assert out[0].mark_price == 3000.0
        assert out[0].notional is None
        assert out[0].unrealized_pnl == 0.0


class TestRunOneTick:
    """Integration: one tick with mocked Postgres and real derivation + mark + write."""

    @patch("pms.loop.write_pms_positions")
    @patch("pms.loop.query_orders_for_positions")
    def test_loop_runs_and_writes_enriched_positions(
        self, mock_query, mock_write
    ):
        mock_query.return_value = [
            {
                "account_id": "acc",
                "book": "",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "executed_quantity": 1.0,
                "price": 50000.0,
            },
        ]
        mock_write.return_value = 1

        mock_conn = MagicMock()
        mark_provider = FakeMarkPriceProvider({"BTCUSDT": 51000})

        n = run_one_tick(lambda: mock_conn, mark_provider)

        assert mock_query.called
        assert mock_write.called
        assert n == 1
        (pg_connect_arg, positions_arg) = mock_write.call_args[0]
        assert callable(pg_connect_arg)
        assert len(positions_arg) == 1
        p = positions_arg[0]
        assert p.account_id == "acc"
        assert p.symbol == "BTCUSDT"
        assert p.open_qty == 1.0
        assert p.position_side == "long"
        assert p.entry_avg == 50000.0
        assert p.mark_price == 51000.0
        assert p.notional == 51000.0
        assert p.unrealized_pnl == 1000.0

    @patch("pms.loop.write_pms_positions")
    @patch("pms.loop.query_orders_for_positions")
    def test_run_pms_loop_stops_on_stop_event(self, mock_query, mock_write):
        mock_query.return_value = []
        mock_write.return_value = 0
        stop = threading.Event()
        stop.set()  # stop immediately after first tick
        run_pms_loop(
            lambda: MagicMock(),
            FakeMarkPriceProvider(),
            tick_interval_seconds=0.01,
            stop_event=stop,
        )
        assert mock_query.call_count >= 1
        assert mock_write.call_count >= 1
