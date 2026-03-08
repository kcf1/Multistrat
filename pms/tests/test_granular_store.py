"""
Unit tests for PMS granular store writes (12.3.4).
"""

from unittest.mock import MagicMock

import pytest

from pms.granular_store import write_pms_positions
from pms.schemas_pydantic import DerivedPosition


class TestWritePmsPositions:
    def test_empty_positions_returns_zero(self):
        assert write_pms_positions("postgres://localhost/db", []) == 0

    def test_upserts_one_position(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        positions = [
            DerivedPosition(
                broker="",
                account_id="acc1", book="b1", asset="BTCUSDT",
                open_qty=1.0, position_side="long", usd_value=50000.0,
            ),
        ]
        n = write_pms_positions(lambda: mock_conn, positions)
        assert n == 1
        assert mock_cur.execute.called
        sql = mock_cur.execute.call_args[0][0]
        assert "broker" in sql and "ON CONFLICT" in sql
        call_args = mock_cur.execute.call_args[0][1]
        assert call_args["broker"] == ""
        assert call_args["account_id"] == "acc1"
        assert call_args["book"] == "b1"
        assert call_args["asset"] == "BTCUSDT"
        assert float(call_args["open_qty"]) == 1.0
        assert call_args["position_side"] == "long"
        assert call_args["usd_value"] == 50000.0
        assert mock_conn.commit.called

    def test_upserts_position_with_usd_value(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        positions = [
            DerivedPosition(
                broker="",
                account_id="a", book="b", asset="BTCUSDT",
                open_qty=1.0, position_side="long", usd_value=51000.0,
            ),
        ]
        n = write_pms_positions(lambda: mock_conn, positions)
        assert n == 1
        call_args = mock_cur.execute.call_args[0][1]
        assert call_args["usd_value"] == 51000.0

    def test_upserts_multiple_and_commits_once(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        positions = [
            DerivedPosition(broker="", account_id="a", book="", asset="BTCUSDT", open_qty=1.0, position_side="long", usd_value=50000.0),
            DerivedPosition(broker="", account_id="a", book="", asset="ETHUSDT", open_qty=-2.0, position_side="short", usd_value=3000.0),
        ]
        n = write_pms_positions(lambda: mock_conn, positions)
        assert n == 2
        assert mock_cur.execute.call_count == 2
        assert mock_conn.commit.call_count == 1
        # Store writes broker and asset columns
        assert mock_cur.execute.call_args_list[0][0][1]["broker"] == ""
        assert mock_cur.execute.call_args_list[0][0][1]["asset"] == "BTCUSDT"


class TestWritePmsPositionsAssetGrain:
    """Assert DB params use asset column for asset-grain positions (e.g. BTC, USDT)."""

    def test_upserts_asset_grain_positions_btc_and_usdt(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        positions = [
            DerivedPosition(
                broker="",
                account_id="acc1", book="default", asset="BTC",
                open_qty=0.5, position_side="long", usd_value=50000.0,
            ),
            DerivedPosition(
                broker="",
                account_id="acc1", book="default", asset="USDT",
                open_qty=-25000.0, position_side="short", usd_value=1.0,
            ),
        ]
        n = write_pms_positions(lambda: mock_conn, positions)
        assert n == 2
        calls = mock_cur.execute.call_args_list
        assert len(calls) == 2
        params0 = calls[0][0][1]
        params1 = calls[1][0][1]
        assert params0["asset"] == "BTC"
        assert params0["open_qty"] == 0.5
        assert params1["asset"] == "USDT"
        assert params1["open_qty"] == -25000.0
