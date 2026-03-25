"""Unit tests for hourly position snapshot reports (Phase 5 §4.5 / §5.5.3)."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from scheduler.jobs.reports.position_snapshot_hourly import (
    run_position_snapshot_hourly,
    utc_hour_start,
)


def test_utc_hour_start_truncate() -> None:
    dt = datetime(2026, 3, 26, 14, 35, 17, 123456, tzinfo=timezone.utc)
    assert utc_hour_start(dt) == datetime(2026, 3, 26, 14, 0, 0, 0, tzinfo=timezone.utc)


@patch("scheduler.jobs.reports.position_snapshot_hourly.psycopg2.connect")
def test_run_position_snapshot_hourly_writes_four_csvs(mock_connect: MagicMock, tmp_path: Path) -> None:
    rows_asset = [("BTC", Decimal("10"), Decimal("10"), 1, Decimal("0"))]
    rows_broker = [("binance", Decimal("10"), Decimal("10"), 1, Decimal("0"))]
    rows_book = [("binance", "bk", Decimal("10"), Decimal("10"), 1, Decimal("0"))]
    rows_gran = [
        (
            1,
            "binance",
            "acc1",
            "bk",
            "BTC",
            Decimal("0.1"),
            "long",
            Decimal("100000"),
            Decimal("10000"),
            Decimal("0"),
            datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ),
    ]

    mock_cur = MagicMock()
    mock_cur.fetchall.side_effect = [rows_asset, rows_broker, rows_book, rows_gran]
    mock_ctx = MagicMock()
    mock_ctx.__enter__.return_value = mock_cur
    mock_ctx.__exit__.return_value = None
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_ctx
    mock_connect.return_value = mock_conn

    snap = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    out = run_position_snapshot_hourly(
        "postgresql://u:p@localhost/db",
        snapshot_at=snap,
        output_dir=tmp_path,
    )

    assert set(out.keys()) == {"by_asset", "by_broker", "by_book", "granular"}
    ts = "20260601T1200Z"
    expected_names = {
        "by_asset": f"position_by_asset_{ts}.csv",
        "by_broker": f"position_by_broker_{ts}.csv",
        "by_book": f"position_by_book_{ts}.csv",
        "granular": f"position_granular_{ts}.csv",
    }
    for kind, (path, n) in out.items():
        assert path.name == expected_names[kind]
        assert path.is_file()
        assert n >= 1

    mock_cur.execute.assert_called()
    assert mock_cur.execute.call_count == 4
    mock_conn.close.assert_called_once()

    with (tmp_path / expected_names["by_asset"]).open(encoding="utf-8") as f:
        r = list(csv.reader(f))
    assert r[0][0] == "snapshot_at"
    assert r[0][1] == "asset"
    assert r[1][1] == "BTC"

    with (tmp_path / expected_names["granular"]).open(encoding="utf-8") as f:
        r = list(csv.reader(f))
    assert r[0][3] == "account_id"
    assert r[1][5] == "BTC"
