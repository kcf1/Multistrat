"""
Position snapshots from PMS ``positions``: four CSVs per run (**by asset**, **by broker**, **by book**, **granular**).

Reads **only** Postgres; outputs under ``scheduler/reports_out/`` (gitignored). See Phase 5 §4.5.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from loguru import logger

from pgconn import SCHEMA_PMS, configure_for_scheduler
from scheduler.config import load_scheduler_settings, scheduler_reports_csv_dir
from scheduler.types import JobContext

FILENAME_TS_FMT = "%Y%m%dT%H%MZ"

# Shared roll-up metrics (per group key). ``positions`` has no ``realized_pnl`` (dropped in schema);
# fourth column kept as ``0`` for stable CSV shape (legacy header total_realized_pnl).
# Qualified: scheduler ``search_path`` is scheduler, public — PMS home is ``pms``.
_SQL_SUFFIX = f"""
  COALESCE(SUM(usd_notional), 0),
  COALESCE(SUM(ABS(usd_notional)), 0),
  COALESCE(COUNT(*) FILTER (WHERE open_qty <> 0), 0)::integer,
  0::numeric
FROM {SCHEMA_PMS}.positions
"""

SELECT_BY_ASSET_SQL = (
    "SELECT asset, " + _SQL_SUFFIX + "GROUP BY asset ORDER BY asset;"
)

SELECT_BY_BROKER_SQL = (
    "SELECT broker, " + _SQL_SUFFIX + "GROUP BY broker ORDER BY broker;"
)

SELECT_BY_BOOK_SQL = (
    "SELECT broker, book, " + _SQL_SUFFIX + "GROUP BY broker, book ORDER BY broker, book;"
)

SELECT_GRANULAR_SQL = f"""
SELECT
  id,
  broker,
  account_id,
  book,
  asset,
  open_qty,
  position_side,
  usd_price,
  usd_notional,
  updated_at
FROM {SCHEMA_PMS}.positions
ORDER BY broker, account_id, book, asset;
"""

AGG_HEADER = (
    "snapshot_at",
    "total_usd_notional",
    "gross_usd_exposure",
    "open_position_rows",
    "total_realized_pnl",
)

HEADER_BY_ASSET = ("snapshot_at", "asset") + AGG_HEADER[1:]
HEADER_BY_BROKER = ("snapshot_at", "broker") + AGG_HEADER[1:]
HEADER_BY_BOOK = ("snapshot_at", "broker", "book") + AGG_HEADER[1:]

HEADER_GRANULAR = (
    "snapshot_at",
    "id",
    "broker",
    "account_id",
    "book",
    "asset",
    "open_qty",
    "position_side",
    "usd_price",
    "usd_notional",
    "updated_at",
)


def utc_hour_start(dt: datetime | None = None) -> datetime:
    """Floor to the start of the current UTC hour (tz-aware UTC)."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)


def _fmt_decimal(v: Decimal | float | int) -> str:
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


def _write_agg_csv(
    path: Path,
    snap_iso: str,
    header: tuple[str, ...],
    key_prefix: list[str],
    rows: list[tuple],
) -> int:
    """Write aggregate rows; each DB row is ``key cols + 4 metrics``."""
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            keys = list(row[: len(key_prefix)])
            tun, gexp, oprows, rpnl = row[len(key_prefix) :]
            w.writerow(
                [
                    snap_iso,
                    *keys,
                    _fmt_decimal(tun),
                    _fmt_decimal(gexp),
                    oprows,
                    _fmt_decimal(rpnl),
                ]
            )
    return len(rows)


def _write_granular_csv(path: Path, snap_iso: str, rows: list[tuple]) -> int:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(HEADER_GRANULAR)
        for row in rows:
            (
                pid,
                broker,
                account_id,
                book,
                asset,
                open_qty,
                position_side,
                usd_price,
                usd_notional,
                updated_at,
            ) = row
            updated_s = updated_at.isoformat() if updated_at is not None else ""
            w.writerow(
                [
                    snap_iso,
                    pid,
                    broker,
                    account_id,
                    book,
                    asset,
                    _fmt_decimal(open_qty),
                    position_side,
                    _fmt_decimal(usd_price) if usd_price is not None else "",
                    _fmt_decimal(usd_notional) if usd_notional is not None else "",
                    updated_s,
                ]
            )
    return len(rows)


def run_position_snapshot_hourly(
    database_url: str,
    *,
    snapshot_at: datetime | None = None,
    output_dir: Path | None = None,
) -> dict[str, tuple[Path, int]]:
    """
    Run four exports for the same UTC hour (same timestamp in filenames).

    Files (each overwrites if the job re-runs the same hour):

    - ``position_by_asset_{ts}Z.csv``
    - ``position_by_broker_{ts}Z.csv``
    - ``position_by_book_{ts}Z.csv``
    - ``position_granular_{ts}Z.csv``

    Returns map ``kind -> (path, data_row_count)``.
    """
    snap = utc_hour_start(snapshot_at)
    ts = snap.strftime(FILENAME_TS_FMT)
    out_dir = output_dir if output_dir is not None else scheduler_reports_csv_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    snap_iso = snap.isoformat()

    conn = psycopg2.connect(database_url)
    configure_for_scheduler(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(SELECT_BY_ASSET_SQL)
            rows_asset = cur.fetchall()
            cur.execute(SELECT_BY_BROKER_SQL)
            rows_broker = cur.fetchall()
            cur.execute(SELECT_BY_BOOK_SQL)
            rows_book = cur.fetchall()
            cur.execute(SELECT_GRANULAR_SQL)
            rows_gran = cur.fetchall()
    finally:
        conn.close()

    path_asset = out_dir / f"position_by_asset_{ts}.csv"
    path_broker = out_dir / f"position_by_broker_{ts}.csv"
    path_book = out_dir / f"position_by_book_{ts}.csv"
    path_gran = out_dir / f"position_granular_{ts}.csv"

    na = _write_agg_csv(path_asset, snap_iso, HEADER_BY_ASSET, ["asset"], rows_asset)
    nb = _write_agg_csv(path_broker, snap_iso, HEADER_BY_BROKER, ["broker"], rows_broker)
    nk = _write_agg_csv(path_book, snap_iso, HEADER_BY_BOOK, ["broker", "book"], rows_book)
    ng = _write_granular_csv(path_gran, snap_iso, rows_gran)

    out = {
        "by_asset": (path_asset, na),
        "by_broker": (path_broker, nb),
        "by_book": (path_book, nk),
        "granular": (path_gran, ng),
    }
    logger.info(
        "position_snapshot_hourly: snapshot_at={} rows by_asset={} by_broker={} by_book={} granular={}",
        snap_iso,
        na,
        nb,
        nk,
        ng,
    )
    logger.info(
        "position_snapshot_hourly csv: by_asset={} by_broker={} by_book={} granular={}",
        path_asset,
        path_broker,
        path_book,
        path_gran,
    )
    return out


class PositionSnapshotHourlyJob:
    """Scheduler ``Job``; reads ``DATABASE_URL``, writes four CSVs under ``reports_out/``."""

    __slots__ = ("_job_id",)

    def __init__(self, job_id: str = "position_snapshot_hourly") -> None:
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    def run(self, ctx: JobContext) -> None:
        settings = load_scheduler_settings()
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is required for position_snapshot_hourly")
        run_position_snapshot_hourly(settings.database_url.strip())
