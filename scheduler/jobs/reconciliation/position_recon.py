"""
Binance **spot** position-style reconciliation vs PMS ``positions`` — **CSV reports only**.

Why there is **no broker “position id”** (walk-through)
--------------------------------------------------------
- **Orders** recon matches on stable ids: ``orderId`` / ``clientOrderId``.
- **Spot** does not expose futures-style position ids. The exchange truth is **wallet balances**
  per **asset** (``GET /api/v3/account`` → ``balances[]``: ``free``, ``locked``).
- Internal **PMS** stores rows at grain ``(broker, account_id, book, asset)`` with ``open_qty``.
  Books are **ours**; the broker only knows wallet totals.

Recon therefore uses a **natural key** ``(broker, account_id, asset)`` (not an exchange id):

1. **Load internal:** For each asset, ``SUM(open_qty)`` across **books** for the scoped broker
   (and optional ``account_id``). That is “modeled net position qty” in that asset for the account.
2. **Load broker:** Parse Binance balances → **total** = available + locked per asset (spot).
3. **Compare:** For each asset in the **union** of internal and broker sets (skipping both-side dust):

   - **Matched / no diff row:** abs(internal − broker_total) <= epsilon.
   - **internal_only:** modeled qty non-trivial, broker total dust / missing (e.g. internal-only asset).
   - **broker_only:** wallet has asset, modeled sum is dust / missing (wallet deposit, unsynced book, etc.).
   - **field_mismatch:** both non-trivial but quantities disagree (``remarks`` explains delta).

``account_id`` filter is **recommended** when one API key maps to one logical broker account; if
``None``, all ``positions`` rows for the broker are aggregated (fine for a single account_id in DB).

Phase 5 §4.6 / 5.6.2 (position rec).
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
import psycopg2.extras
from loguru import logger

from pgconn import SCHEMA_PMS, configure_for_scheduler
from oms.brokers.binance.api_client import BinanceAPIClient, BinanceAPIError

from scheduler.config import (
    POSITION_RECON_ACCOUNT_ID,
    POSITION_RECON_BROKER,
    POSITION_RECON_QTY_EPSILON,
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
    "assets_compared",
    "matched_assets",
    "internal_only_rows",
    "broker_only_rows",
    "field_mismatch_rows",
)

DIFF_HEADER = (
    "recon_at",
    "broker",
    "account_id_filter",
    "db_asset",
    "db_net_open_qty",
    "binance_asset",
    "binance_available",
    "binance_locked",
    "binance_total",
    "diff_type",
    "remarks",
)


def _fmt_dec(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


def _to_dec(x: Any) -> Decimal:
    if x is None or x == "":
        return Decimal(0)
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _dust(q: Decimal, eps: Decimal) -> bool:
    return abs(q) <= eps


def broker_balances_total(account_resp: dict[str, Any]) -> dict[str, Decimal]:
    """asset (as returned by Binance, normalized upper) -> available + locked."""
    out: dict[str, Decimal] = {}
    for bal in account_resp.get("balances") or []:
        asset = str(bal.get("asset") or "").strip().upper()
        if not asset:
            continue
        free = _to_dec(bal.get("free"))
        locked = _to_dec(bal.get("locked"))
        total = free + locked
        if asset in out:
            out[asset] = out[asset] + total
        else:
            out[asset] = total
    return out


@dataclass
class PositionReconResult:
    summary: dict[str, Any]
    diff_rows: list[dict[str, Any]] = field(default_factory=list)


def reconcile_spot_position_balances(
    recon_at_iso: str,
    internal_by_asset: dict[str, Decimal],
    broker_by_asset: dict[str, Decimal],
    *,
    broker: str,
    account_filter: str | None,
    epsilon: Decimal,
    broker_free_locked: dict[str, tuple[Decimal, Decimal]] | None = None,
) -> PositionReconResult:
    """Compare aggregated internal open_qty per asset vs Binance wallet totals."""
    assets = set(internal_by_asset.keys()) | set(broker_by_asset.keys())
    diff_rows: list[dict[str, Any]] = []
    matched = 0
    compared = 0

    for asset in sorted(assets):
        i_qty = internal_by_asset.get(asset, Decimal(0))
        b_qty = broker_by_asset.get(asset, Decimal(0))
        i_dust = _dust(i_qty, epsilon)
        b_dust = _dust(b_qty, epsilon)

        if i_dust and b_dust:
            continue

        compared += 1
        fl = broker_free_locked.get(asset) if broker_free_locked else None
        base = {
            "recon_at": recon_at_iso,
            "broker": broker,
            "account_id_filter": account_filter or "",
            "db_asset": asset,
            "db_net_open_qty": _fmt_dec(i_qty) if not i_dust else "",
            "binance_asset": asset,
            "binance_available": _fmt_dec(fl[0]) if fl else "",
            "binance_locked": _fmt_dec(fl[1]) if fl else "",
            "binance_total": _fmt_dec(b_qty) if not b_dust else "",
            "diff_type": "",
            "remarks": "",
        }

        if not i_dust and b_dust:
            row = {**base, "diff_type": "internal_only", "remarks": "modeled open_qty non-zero; broker total is dust or asset absent"}
            diff_rows.append(row)
            continue
        if i_dust and not b_dust:
            row = {**base, "diff_type": "broker_only", "remarks": "wallet balance non-zero; modeled net is dust or asset absent"}
            diff_rows.append(row)
            continue
        if not _dust(i_qty - b_qty, epsilon):
            row = {
                **base,
                "db_net_open_qty": _fmt_dec(i_qty),
                "binance_total": _fmt_dec(b_qty),
                "diff_type": "field_mismatch",
                "remarks": f"qty delta (db_net − broker_total) = {_fmt_dec(i_qty - b_qty)}",
            }
            diff_rows.append(row)
        else:
            matched += 1

    summary = {
        "recon_at": recon_at_iso,
        "broker": broker,
        "account_id_filter": account_filter or "",
        "assets_compared": compared,
        "matched_assets": matched,
        "internal_only_rows": sum(1 for r in diff_rows if r["diff_type"] == "internal_only"),
        "broker_only_rows": sum(1 for r in diff_rows if r["diff_type"] == "broker_only"),
        "field_mismatch_rows": sum(1 for r in diff_rows if r["diff_type"] == "field_mismatch"),
    }
    return PositionReconResult(summary=summary, diff_rows=diff_rows)


def _load_internal_net_by_asset(
    cur: Any,
    broker_name: str,
    account_id: str | None,
) -> dict[str, Decimal]:
    cur.execute(
        f"""
        SELECT UPPER(TRIM(asset)) AS asset, COALESCE(SUM(open_qty), 0) AS net_qty
        FROM {SCHEMA_PMS}.positions
        WHERE broker = %s
          AND (%s IS NULL OR account_id = %s)
        GROUP BY UPPER(TRIM(asset))
        """,
        (broker_name, account_id, account_id),
    )
    out: dict[str, Decimal] = {}
    for row in cur.fetchall():
        a = str(row["asset"] or "").strip()
        if a:
            out[a] = _to_dec(row["net_qty"])
    return out


def _broker_free_locked_map(account_resp: dict[str, Any]) -> dict[str, tuple[Decimal, Decimal]]:
    m: dict[str, tuple[Decimal, Decimal]] = {}
    for bal in account_resp.get("balances") or []:
        asset = str(bal.get("asset") or "").strip().upper()
        if not asset:
            continue
        free = _to_dec(bal.get("free"))
        locked = _to_dec(bal.get("locked"))
        if asset in m:
            p = m[asset]
            m[asset] = (p[0] + free, p[1] + locked)
        else:
            m[asset] = (free, locked)
    return m


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
        raise RuntimeError("BINANCE_API_KEY and BINANCE_API_SECRET are required for position reconciliation")
    return BinanceAPIClient(api_key=key, api_secret=secret, base_url=base, testnet=testnet)


def run_position_reconciliation_binance(
    database_url: str,
    *,
    account_id: str | None = None,
    output_dir: Path | None = None,
    client: BinanceAPIClient | None = None,
    snapshot_at: datetime | None = None,
) -> dict[str, tuple[Path, int]]:
    snap = utc_hour_start(snapshot_at)
    ts = snap.strftime(FILENAME_TS_FMT)
    recon_at = datetime.now(timezone.utc).isoformat()
    out_dir = output_dir if output_dir is not None else scheduler_reports_csv_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    broker = POSITION_RECON_BROKER
    acct = account_id if account_id is not None else POSITION_RECON_ACCOUNT_ID
    eps = POSITION_RECON_QTY_EPSILON

    conn = psycopg2.connect(database_url)
    configure_for_scheduler(conn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            internal = _load_internal_net_by_asset(cur, broker, acct)
    finally:
        conn.close()

    api = client or _binance_client_from_env()
    try:
        account_resp = api.get_account()
    except BinanceAPIError as e:
        logger.exception("position_recon: Binance get_account failed: {}", e)
        raise

    broker_totals = broker_balances_total(account_resp)
    fl_map = _broker_free_locked_map(account_resp)

    result = reconcile_spot_position_balances(
        recon_at,
        internal,
        broker_totals,
        broker=broker,
        account_filter=acct,
        epsilon=eps,
        broker_free_locked=fl_map,
    )

    path_sum = out_dir / f"position_recon_summary_{ts}.csv"
    path_diff = out_dir / f"position_recon_diff_{ts}.csv"
    _write_summary_csv(path_sum, result.summary)
    _write_diff_csv(path_diff, result.diff_rows)

    logger.info(
        "position_recon: recon_at={} summary={} diff_rows={}",
        recon_at,
        result.summary,
        len(result.diff_rows),
    )
    return {"summary": (path_sum, 1), "diff": (path_diff, len(result.diff_rows))}


class PositionReconciliationBinanceJob:
    """Hourly job: PMS ``positions`` vs Binance spot wallet; CSV under ``reports_out/``."""

    __slots__ = ("_job_id",)

    def __init__(self, job_id: str = "position_reconciliation_binance") -> None:
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        return self._job_id

    def run(self, ctx: JobContext) -> None:
        settings = load_scheduler_settings()
        if not settings.database_url:
            raise RuntimeError("DATABASE_URL is required for position reconciliation")
        run_position_reconciliation_binance(settings.database_url.strip())
