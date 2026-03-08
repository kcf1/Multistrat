"""
Integration tests for PMS loop (12.3.8): read → derive → calculate → write.

Mock data; verify loop runs and writes results. Redis skipped.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from pms.loop import (
    enrich_positions_with_mark_prices,
    enrich_positions_with_usd_from_assets,
    run_one_tick,
    run_pms_loop,
)
from pms.mark_price import FakeMarkPriceProvider
from pms.schemas_pydantic import DerivedPosition


class TestEnrichPositionsWithMarkPrices:
    def test_empty_returns_empty(self):
        from pms.mark_price import MarkPriceProvider
        class Noop(MarkPriceProvider):
            def get_mark_prices(self, symbols):
                return type('R', (), {'prices': {}})()
        assert enrich_positions_with_mark_prices([], Noop()) == []

    def test_sets_usd_value_from_provider(self):
        positions = [
            DerivedPosition(
                broker="",
                account_id="a", book="", asset="BTCUSDT",
                open_qty=1.0, position_side="long", usd_value=None,
            ),
        ]
        provider = FakeMarkPriceProvider({"BTCUSDT": 51000})
        out = enrich_positions_with_mark_prices(positions, provider)
        assert len(out) == 1
        assert out[0].usd_value == 51000.0

    def test_flat_position_gets_usd_value(self):
        positions = [
            DerivedPosition(
                broker="",
                account_id="a", book="", asset="ETHUSDT",
                open_qty=0.0, position_side="flat", usd_value=None,
            ),
        ]
        provider = FakeMarkPriceProvider({"ETHUSDT": 3000})
        out = enrich_positions_with_mark_prices(positions, provider)
        assert len(out) == 1
        assert out[0].usd_value == 3000.0


class TestEnrichPositionsWithUsdFromAssets:
    """Stables-first USD enrichment: assets with usd_price get usd_value; others stay None."""

    @patch("pms.loop.query_assets_usd_config")
    def test_sets_usd_value_for_stables_only(self, mock_assets_config):
        from decimal import Decimal
        mock_assets_config.return_value = {
            "USDT": {"usd_price": Decimal("1"), "usd_symbol": None},
            "BTC": {"usd_price": None, "usd_symbol": "BTCUSDT"},
        }
        positions = [
            DerivedPosition(
                broker="",
                account_id="a", book="", asset="USDT",
                open_qty=10000.0, position_side="long", usd_value=None,
            ),
            DerivedPosition(
                broker="",
                account_id="a", book="", asset="BTC",
                open_qty=0.5, position_side="long", usd_value=None,
            ),
        ]
        out = enrich_positions_with_usd_from_assets(lambda: None, positions)
        assert len(out) == 2
        by_asset = {p.asset: p for p in out}
        assert by_asset["USDT"].usd_value == 1.0
        assert by_asset["BTC"].usd_value is None

    @patch("pms.loop.query_assets_usd_config")
    def test_empty_positions_returns_empty(self, mock_assets_config):
        mock_assets_config.return_value = {}
        assert enrich_positions_with_usd_from_assets(lambda: None, []) == []


class TestRunOneTick:
    """Integration: one tick with mocked Postgres and stables-first USD from assets."""

    @patch("pms.loop.write_pms_positions")
    @patch("pms.loop.query_assets_usd_config")
    @patch("pms.loop.derive_positions_from_orders_and_balance_changes")
    def test_loop_runs_and_writes_enriched_positions(
        self, mock_derive, mock_assets_config, mock_write
    ):
        from decimal import Decimal
        mock_derive.return_value = [
            DerivedPosition(
                broker="",
                account_id="acc",
                book="",
                asset="USDT",
                open_qty=50000.0,
                position_side="long",
            ),
        ]
        mock_assets_config.return_value = {
            "USDT": {"usd_price": Decimal("1"), "usd_symbol": None},
        }
        mock_write.return_value = 1

        mock_conn = MagicMock()
        mark_provider = FakeMarkPriceProvider()

        n = run_one_tick(lambda: mock_conn, mark_provider)

        assert mock_derive.called
        assert mock_assets_config.called
        assert mock_write.called
        assert n == 1
        (pg_connect_arg, positions_arg) = mock_write.call_args[0]
        assert callable(pg_connect_arg)
        assert len(positions_arg) == 1
        p = positions_arg[0]
        assert p.account_id == "acc"
        assert p.asset == "USDT"
        assert p.open_qty == 50000.0
        assert p.position_side == "long"
        assert p.usd_value == 1.0

    @patch("pms.loop.write_pms_positions")
    @patch("pms.loop.derive_positions_from_orders_and_balance_changes")
    def test_run_pms_loop_stops_on_stop_event(self, mock_derive, mock_write):
        mock_derive.return_value = []
        mock_write.return_value = 0
        stop = threading.Event()
        stop.set()  # stop immediately after first tick
        run_pms_loop(
            lambda: MagicMock(),
            FakeMarkPriceProvider(),
            tick_interval_seconds=0.01,
            stop_event=stop,
        )
        assert mock_derive.call_count >= 1
        assert mock_write.call_count >= 1
