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
                account_id="acc1", book="b1", symbol="BTCUSDT",
                open_qty=1.0, position_side="long", entry_avg=50000.0,
                notional=50000.0, unrealized_pnl=100.0,
            ),
        ]
        n = write_pms_positions(lambda: mock_conn, positions)
        assert n == 1
        assert mock_cur.execute.called
        call_args = mock_cur.execute.call_args[0][1]
        assert call_args["account_id"] == "acc1"
        assert call_args["book"] == "b1"
        assert call_args["symbol"] == "BTCUSDT"
        assert float(call_args["open_qty"]) == 1.0
        assert call_args["position_side"] == "long"
        assert call_args["notional"] == 50000.0
        assert mock_conn.commit.called

    def test_upserts_position_with_mark_price(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        positions = [
            DerivedPosition(
                account_id="a", book="b", symbol="BTCUSDT",
                open_qty=1.0, position_side="long", entry_avg=50000.0,
                mark_price=51000.0, notional=51000.0, unrealized_pnl=1000.0,
            ),
        ]
        n = write_pms_positions(lambda: mock_conn, positions)
        assert n == 1
        call_args = mock_cur.execute.call_args[0][1]
        assert call_args["mark_price"] == 51000.0

    def test_upserts_multiple_and_commits_once(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        mock_conn.close = MagicMock()

        positions = [
            DerivedPosition(account_id="a", book="", symbol="BTCUSDT", open_qty=1.0, position_side="long", entry_avg=50000.0, notional=50000.0),
            DerivedPosition(account_id="a", book="", symbol="ETHUSDT", open_qty=-2.0, position_side="short", entry_avg=3000.0, notional=-6000.0),
        ]
        n = write_pms_positions(lambda: mock_conn, positions)
        assert n == 2
        assert mock_cur.execute.call_count == 2
        assert mock_conn.commit.call_count == 1
