"""
Binance **spot** open-order reconciliation vs Postgres ``orders`` — **CSV reports only**.

Compares internally tracked non-terminal orders (pending / sent / partially_filled) with
``GET /api/v3/openOrders``. Emits ``order_recon_summary_*.csv`` and ``order_recon_diff_*.csv``
under ``scheduler/reports_out/``. Position reconciliation is **not** implemented here.

Phase 5 §4.6 (order rec only).
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from loguru import logger

from oms.brokers.binance.api_client import BinanceAPIClient, BinanceAPIError

from scheduler.config import (
    ORDER_RECON_ACCOUNT_ID,
    ORDER_RECON_BROKER,
    ORDER_RECON_INTERNAL_OPEN_STATUSES,
    load_scheduler_settings,
    scheduler_reports_csv_dir,
)
from scheduler.jobs.reports.position_snapshot_hourly import utc_hour_start
from scheduler.types import JobContext

FILENAME_TS_FMT = "%Y%m%dT%H%MZ"

SUMMARY_HEADER = (
    "recon_at",
    "broker",
    "account_id_filter",
    "internal_open_rows",
    "broker_open_rows",
    "matched_pairs",
    "internal_only_rows",
    "broker_only_rows",
    "status_mismatch_rows",
)

DIFF_HEADER = (
    "recon_at",
    "diff_type",
    "internal_id",
    "broker_order_id",
    "symbol",
    "account_id",
    "db_status",
    "binance_status",
    "db_quantity",
    "db_executed_qty",
    "binance_orig_qty",
    "binance_executed_qty",
    "client_order_id_binance",
    "note",
)


def _fmt_dec(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


def _status_compatible(db_status: str, binance_status: str) -> bool:
    """Rough alignment: Binance NEW vs our pending/sent; PARTIALLY_FILLED vs partially_filled."""
    d = (db_status or "").lower().strip()
    b = (binance_status or "").upper().strip()
    if b in ("NEW",):
        return d in ("pending", "sent")
    if b == "PARTIALLY_FILLED":
        return d == "partially_filled"
    # Other open states (rare); treat as compatible if exact lowered match
    return d == b.lower().replace("_", "")


def _find_binance_row(
    internal_id: str,
    broker_order_id: str,
    binance_by_cid: dict[str, dict[str, Any]],
    binance_by_oid: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    cid = (internal_id or "").strip()
    if cid and cid in binance_by_cid:
        return binance_by_cid[cid]
    oid = str(broker_order_id or "").strip()
    if oid and oid in binance_by_oid:
        return binance_by_oid[oid]
    return None


def _index_binance_open(orders: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_cid: dict[str, dict[str, Any]] = {}
    by_oid: dict[str, dict[str, Any]] = {}
    for o in orders:
        cid = str(o.get("clientOrderId") or "").strip()
        if cid:
            by_cid[cid] = o
        oid = str(o.get("orderId", ""))
        if oid:
            by_oid[oid] = o
    return by_cid, by_oid


@dataclass
class OrderReconResult:
    summary: dict[str, Any]
    diff_rows: list[dict[str, Any]] = field(default_factory=list)


def reconcile_open_orders(
    recon_at_iso: str,
    internal_rows: list[tuple[Any, ...]],
    binance_orders: list[dict[str, Any]],
    *,
    broker: str,
    account_filter: str | None,
) -> OrderReconResult:
    """
    ``internal_rows``: tuples
    ``(internal_id, broker_order_id, symbol, side, status, quantity, executed_qty, account_id)``.
    """
    by_cid, by_oid = _index_binance_open(binance_orders)
    matched_binance_keys: set[tuple[str, str]] = set()
    diff_rows: list[dict[str, Any]] = []

    for row in internal_rows:
        internal_id, broker_order_id, symbol, side, status, quantity, executed_qty, account_id = row
        sid = str(internal_id or "").strip()
        b = _find_binance_row(sid, str(broker_order_id or ""), by_cid, by_oid)
        if b is None:
            diff_rows.append(
                {
                    "recon_at": recon_at_iso,
                    "diff_type": "internal_only",
                    "internal_id": sid,
                    "broker_order_id": str(broker_order_id or ""),
                    "symbol": str(symbol or ""),
                    "account_id": str(account_id or ""),
                    "db_status": str(status or ""),
                    "binance_status": "",
                    "db_quantity": _fmt_dec(quantity),
                    "db_executed_qty": _fmt_dec(executed_qty),
                    "binance_orig_qty": "",
                    "binance_executed_qty": "",
                    "client_order_id_binance": "",
                    "note": "open in DB, not in Binance openOrders",
                }
            )
            continue
        matched_binance_keys.add((str(b.get("symbol", "")), str(b.get("orderId", ""))))
        b_st = str(b.get("status", ""))
        if not _status_compatible(str(status or ""), b_st):
            diff_rows.append(
                {
                    "recon_at": recon_at_iso,
                    "diff_type": "status_mismatch",
                    "internal_id": sid,
                    "broker_order_id": str(broker_order_id or ""),
                    "symbol": str(symbol or ""),
                    "account_id": str(account_id or ""),
                    "db_status": str(status or ""),
                    "binance_status": b_st,
                    "db_quantity": _fmt_dec(quantity),
                    "db_executed_qty": _fmt_dec(executed_qty),
                    "binance_orig_qty": str(b.get("origQty", "")),
                    "binance_executed_qty": str(b.get("executedQty", "")),
                    "client_order_id_binance": str(b.get("clientOrderId", "")),
                    "note": "matched id but status mapping differs",
                }
            )

    for b in binance_orders:
        key = (str(b.get("symbol", "")), str(b.get("orderId", "")))
        if key in matched_binance_keys:
            continue
        diff_rows.append(
            {
                "recon_at": recon_at_iso,
                "diff_type": "broker_only",
                "internal_id": "",
                "broker_order_id": str(b.get("orderId", "")),
                "symbol": str(b.get("symbol", "")),
                "account_id": "",
                "db_status": "",
                "binance_status": str(b.get("status", "")),
                "db_quantity": "",
                "db_executed_qty": "",
                "binance_orig_qty": str(b.get("origQty", "")),
                "binance_executed_qty": str(b.get("executedQty", "")),
                "client_order_id_binance": str(b.get("clientOrderId", "")),
                "note": "open on Binance, not matched to DB open row (clientOrderId / orderId)",
            }
        )

    matched_pairs = len(matched_binance_keys)
    summary = {
        "recon_at": recon_at_iso,
        "broker": broker,
        "account_id_filter": account_filter or "",
        "internal_open_rows": len(internal_rows),
        "broker_open_rows": len(binance_orders),
        "matched_pairs": matched_pairs,
        "internal_only_rows": sum(1 for r in diff_rows if r["diff_type"] == "internal_only"),
        "broker_only_rows": sum(1 for r in diff_rows if r["diff_type"] == "broker_only"),
        "status_mismatch_rows": sum(1 for r in diff_rows if r["diff_type"] == "status_mismatch"),
    }
    return OrderReconResult(summary=summary, diff_rows=diff_rows)


def _load_internal_open_orders(cur: Any, broker: str, account_id: str | None) -> list[tuple[Any, ...]]:
    st = ORDER_RECON_INTERNAL_OPEN_STATUSES
    cur.execute(
        """
        SELECT internal_id, broker_order_id, symbol, side, status, quantity, executed_qty, account_id
        FROM orders
        WHERE broker = %s
          AND status IN %s
          AND (%s IS NULL OR account_id = %s)
        ORDER BY account_id, symbol, internal_id
        """,
        (broker, tuple(st), account_id, account_id),
    )
    return list(cur.fetchall())


def _write_summary_csv(path: Path, summary: dict[str, Any]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(SUMMARY_HEADER))
        w.writeheader()
        w.writerow({k: summary.get(k, "") for k in SUMMARY_HEADER})


def _write_diff_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(DIFF_HEADER))
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in DIFF_HEADER})


def _binance_client_from_env() -> BinanceAPIClient:
    key = (os.environ.get("BINANCE_API_KEY") or "").strip()
    secret = (os.environ.get("BINANCE_API_SECRET") or "").strip()
    base = (os.environ.get("BINANCE_BASE_URL") or "").strip()
    testnet_raw = (os.environ.get("BINANCE_TESTNET") or "").strip().lower()
    testnet = testnet_raw in ("1", "true", "yes")
    if not base:
        base = "https://testnet.binance.vision" if testnet else "https://api.binance.com"
    if not key or not secret:
        raise RuntimeError("BINANCE_API_KEY and BINANCE_API_SECRET are required for order reconciliation")
    return BinanceAPIClient(api_key=key, api_secret=secret, base_url=base, testnet=testnet)


def run_order_reconciliation_binance(
    database_url: str,
    *,
    account_id: str | None = None,
    output_dir: Path | None = None,
    client: BinanceAPIClient | None = None,
    snapshot_at: datetime | None = None,
) -> dict[str, tuple[Path, int]]:
    """
    Run reconciliation and write two CSVs (summary + diff). Safe to re-run (overwrites same-hour files).

    Returns ``{"summary": (path, 1), "diff": (path, n)}`` where ``n`` = diff row count.
    """
    snap = utc_hour_start(snapshot_at)
    ts = snap.strftime(FILENAME_TS_FMT)
    recon_at = datetime.now(timezone.utc).isoformat()
    out_dir = output_dir if output_dir is not None else scheduler_reports_csv_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    broker = ORDER_RECON_BROKER
    acct = account_id if account_id is not None else ORDER_RECON_ACCOUNT_ID

    conn = psycopg2.connect(database_url)
    try:
        with conn.cursor() as cur:
            internal = _load_internal_open_orders(cur, broker, acct)
    finally:
        conn.close()

    api = client or _binance_client_from_env()
    try:
        binance_open = api.get_open_orders()
    except BinanceAPIError as e:
        logger.exception("order_recon: Binance get_open_orders failed: {}", e)
        raise

    result = reconcile_open_orders(recon_at, internal, binance_open, broker=broker, account_filter=acct)

    path_sum = out_dir / f"order_recon_summary_{ts}.csv"
    path_diff = out_dir / f"order_recon_diff_{ts}.csv"
    _write_summary_csv(path_sum, result.summary)
    _write_diff_csv(path_diff, result.diff_rows)

    logger.info(
        "order_recon: recon_at={} summary={} diff_rows={} paths summary={} diff={}",
        recon_at,
        result.summary,
        len(result.diff_rows),
        path_sum,
        path_diff,
    )
    return {"summary": (path_sum, 1), "diff": (path_diff, len(result.diff_rows))}


class OrderReconciliationBinanceJob:
    """Scheduler job: ``DATABASE_URL`` + Binance keys from env; CSV under ``reports_out/``."""

    __slots__ = ("_job_id",)

    def __init__(self, job_id: str = "order_reconciliation_binance") -> None:
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    def run(self, ctx: JobContext) -> None:
        settings = load_scheduler_settings()
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is required for order reconciliation")
        run_order_reconciliation_binance(settings.database_url.strip())
