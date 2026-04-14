"""
Binance **spot** order/trade reconciliation vs Postgres ``orders`` — **CSV reports only**.

**Workflow (hourly scheduler job):** In the last **``ORDER_RECON_TRADE_LOOKBACK_HOURS``** (default 2h, UTC), load up to
**N** recent **filled** ``orders`` rows (by ``updated_at`` / ``created_at``), fetch **``myTrades``**
per involved symbol over the **same** ``[start_ms, end_ms]`` window, merge and take the latest **N**
trades by trade time. Match DB orders to broker **trades** by ``(symbol, orderId)``; multiple trades
per order are **aggregated** (sum qty, VWAP price) for comparison to ``executed_qty`` / ``price``.
Diff rows use a trade-oriented Binance column set plus **``diff_type``** / **``remarks``**.

``diff_type``: ``internal_only`` | ``broker_only`` | ``field_mismatch``.

Phase 5 §4.6 (order rec only).
"""

from __future__ import annotations

import csv
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from loguru import logger

from pgconn import SCHEMA_OMS, configure_for_scheduler

from oms.brokers.binance.api_client import BinanceAPIClient, BinanceAPIError

from scheduler.config import (
    ORDER_RECON_ACCOUNT_ID,
    ORDER_RECON_BROKER,
    ORDER_RECON_DB_FILLED_STATUSES,
    ORDER_RECON_TRADE_LOOKBACK_HOURS,
    ORDER_RECON_TRADE_TAIL_LIMIT,
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
    "window_start_utc",
    "window_end_utc",
    "internal_scope_rows",
    "broker_trade_rows",
    "matched_ids",
    "internal_only_rows",
    "broker_only_rows",
    "field_mismatch_rows",
)

# Order (DB) + trade (Binance ``myTrades``) shape + ``diff_type`` / ``remarks``.
DIFF_HEADER = (
    "recon_at",
    "internal_id",
    "broker_order_id",
    "account_id",
    "db_symbol",
    "db_side",
    "db_order_type",
    "db_quantity",
    "db_limit_price",
    "db_price",
    "db_executed_qty",
    "db_status",
    "db_updated_at",
    "binance_trade_id",
    "binance_order_id",
    "binance_symbol",
    "binance_trade_price",
    "binance_trade_qty",
    "binance_quote_qty",
    "binance_commission",
    "binance_commission_asset",
    "binance_trade_time_ms",
    "binance_is_buyer",
    "binance_is_maker",
    "diff_type",
    "remarks",
)


def _fmt_dec(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return format(v, "f")
    return str(v)


def _fmt_ts(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc).isoformat()
    return str(v)


def _dec_eq(a: Any, b: Any) -> bool:
    if a is None or a == "":
        a = None
    if b is None or b == "":
        b = None
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        da, db_ = Decimal(str(a)), Decimal(str(b))
    except Exception:
        return str(a).strip() == str(b).strip()
    return da == db_


def _order_agg_key(db: dict[str, Any]) -> tuple[str, str]:
    return ((db.get("symbol") or "").upper().strip(), str(db.get("broker_order_id") or "").strip())


def _trade_side(is_buyer: Any) -> str:
    return "BUY" if bool(is_buyer) else "SELL"


def _aggregate_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Sum qty, VWAP price, combined quote commission from Binance myTrades list."""
    if not trades:
        return {}
    trades = sorted(trades, key=lambda t: int(t.get("time") or t.get("T") or 0))
    sum_q = Decimal(0)
    sum_pxq = Decimal(0)
    sum_quote = Decimal(0)
    sum_comm = Decimal(0)
    comm_asset = ""
    for t in trades:
        q = Decimal(str(t.get("qty") or "0"))
        p = Decimal(str(t.get("price") or "0"))
        sum_q += q
        sum_pxq += p * q
        if t.get("quoteQty") is not None:
            sum_quote += Decimal(str(t.get("quoteQty")))
        if t.get("commission") is not None:
            sum_comm += Decimal(str(t.get("commission")))
            comm_asset = str(t.get("commissionAsset") or comm_asset)
    vwap = (sum_pxq / sum_q) if sum_q != 0 else Decimal(0)
    return {
        "ids": [int(t.get("id")) for t in trades],
        "order_id": str(trades[0].get("orderId", "")),
        "symbol": str(trades[0].get("symbol", "")),
        "qty_sum": sum_q,
        "vwap": vwap,
        "quote_sum": sum_quote,
        "commission_sum": sum_comm,
        "commission_asset": comm_asset,
        "time_ms": max(int(t.get("time") or t.get("T") or 0) for t in trades),
        "is_buyer": trades[0].get("isBuyer"),
        "is_maker": trades[0].get("isMaker"),
    }


def _trade_row_base(recon_at: str) -> dict[str, Any]:
    return {k: "" for k in DIFF_HEADER if k not in ("diff_type", "remarks")} | {
        "recon_at": recon_at,
        "diff_type": "",
        "remarks": "",
    }


def _fill_db_side(row: dict[str, Any], db: dict[str, Any]) -> None:
    row["internal_id"] = str(db.get("internal_id") or "")
    row["broker_order_id"] = str(db.get("broker_order_id") or "")
    row["account_id"] = str(db.get("account_id") or "")
    row["db_symbol"] = str(db.get("symbol") or "")
    row["db_side"] = str(db.get("side") or "")
    row["db_order_type"] = str(db.get("order_type") or "")
    row["db_quantity"] = _fmt_dec(db.get("quantity"))
    row["db_limit_price"] = _fmt_dec(db.get("limit_price"))
    row["db_price"] = _fmt_dec(db.get("price"))
    row["db_executed_qty"] = _fmt_dec(db.get("executed_qty"))
    row["db_status"] = str(db.get("status") or "")
    row["db_updated_at"] = _fmt_ts(db.get("updated_at"))


def _fill_binance_single_trade(row: dict[str, Any], t: dict[str, Any]) -> None:
    row["binance_trade_id"] = str(t.get("id", ""))
    row["binance_order_id"] = str(t.get("orderId", ""))
    row["binance_symbol"] = str(t.get("symbol", ""))
    row["binance_trade_price"] = str(t.get("price", ""))
    row["binance_trade_qty"] = str(t.get("qty", ""))
    row["binance_quote_qty"] = str(t.get("quoteQty", ""))
    row["binance_commission"] = str(t.get("commission", ""))
    row["binance_commission_asset"] = str(t.get("commissionAsset", ""))
    row["binance_trade_time_ms"] = str(t.get("time", ""))
    row["binance_is_buyer"] = str(t.get("isBuyer", ""))
    row["binance_is_maker"] = str(t.get("isMaker", ""))


def _fill_binance_aggregate(row: dict[str, Any], agg: dict[str, Any]) -> None:
    row["binance_trade_id"] = ",".join(str(i) for i in agg.get("ids", []))
    row["binance_order_id"] = str(agg.get("order_id", ""))
    row["binance_symbol"] = str(agg.get("symbol", ""))
    row["binance_trade_price"] = format(agg["vwap"], "f") if isinstance(agg.get("vwap"), Decimal) else ""
    row["binance_trade_qty"] = format(agg["qty_sum"], "f") if isinstance(agg.get("qty_sum"), Decimal) else ""
    row["binance_quote_qty"] = format(agg["quote_sum"], "f") if agg.get("quote_sum") else ""
    row["binance_commission"] = format(agg["commission_sum"], "f") if agg.get("commission_sum") else ""
    row["binance_commission_asset"] = str(agg.get("commission_asset", ""))
    row["binance_trade_time_ms"] = str(agg.get("time_ms", ""))
    row["binance_is_buyer"] = str(agg.get("is_buyer", ""))
    row["binance_is_maker"] = str(agg.get("is_maker", ""))


def _matched_field_issues_trades(db: dict[str, Any], trades: list[dict[str, Any]]) -> list[str]:
    issues: list[str] = []
    agg = _aggregate_trades(trades)
    if (db.get("symbol") or "").upper() != (agg.get("symbol") or "").upper():
        issues.append("symbol")
    db_side = (db.get("side") or "").upper()
    br_side = _trade_side(agg.get("is_buyer"))
    if db_side != br_side:
        issues.append("side")
    if not _dec_eq(db.get("executed_qty"), agg.get("qty_sum")):
        issues.append("executed_qty")
    if not _dec_eq(db.get("price"), agg.get("vwap")):
        issues.append("avg_price")
    ds = (db.get("status") or "").lower().strip()
    if ds != "filled":
        issues.append("status")
    return issues


def fetch_my_trades_symbol_window(
    client: BinanceAPIClient,
    symbol: str,
    start_time_ms: int,
    end_time_ms: int,
) -> list[dict[str, Any]]:
    """All trades for ``symbol`` in [start, end], paginating past 1000 if needed."""
    out: list[dict[str, Any]] = []
    from_id: int | None = None
    while True:
        batch = client.get_my_trades(
            symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=1000,
            from_id=from_id,
        )
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 1000:
            break
        last_id = max(int(t.get("id", 0)) for t in batch)
        nxt = last_id + 1
        if nxt == from_id:
            break
        from_id = nxt
    return out


def fetch_broker_trades_merged(
    client: BinanceAPIClient,
    symbols: set[str],
    *,
    start_time_ms: int,
    end_time_ms: int,
    global_cap: int,
) -> list[dict[str, Any]]:
    if not symbols:
        return []
    merged: list[dict[str, Any]] = []
    for sym in sorted(symbols):
        merged.extend(fetch_my_trades_symbol_window(client, sym, start_time_ms, end_time_ms))
    merged.sort(key=lambda t: int(t.get("time") or t.get("T") or 0), reverse=True)
    return merged[:global_cap]


@dataclass
class OrderReconResult:
    summary: dict[str, Any]
    diff_rows: list[dict[str, Any]] = field(default_factory=list)


def reconcile_trade_window(
    recon_at_iso: str,
    db_rows: list[dict[str, Any]],
    broker_trades: list[dict[str, Any]],
    *,
    broker: str,
    account_filter: str | None,
    window_start_utc_iso: str,
    window_end_utc_iso: str,
) -> OrderReconResult:
    trades_by_order: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for t in broker_trades:
        sym = str(t.get("symbol") or "").upper()
        oid = str(t.get("orderId") or "").strip()
        if sym and oid:
            trades_by_order[(sym, oid)].append(t)

    matched_trade_ids: set[int] = set()
    diff_rows: list[dict[str, Any]] = []
    matched_ids = 0

    for db in db_rows:
        key = _order_agg_key(db)
        trades = trades_by_order.get(key, [])
        if not trades:
            row = _trade_row_base(recon_at_iso)
            _fill_db_side(row, db)
            row["diff_type"] = "internal_only"
            row["remarks"] = "filled order in DB window, no myTrades for (symbol, orderId) in scope"
            diff_rows.append(row)
            continue
        matched_ids += 1
        for t in trades:
            tid = t.get("id")
            if tid is not None:
                matched_trade_ids.add(int(tid))
        issues = _matched_field_issues_trades(db, trades)
        if issues:
            row = _trade_row_base(recon_at_iso)
            _fill_db_side(row, db)
            _fill_binance_aggregate(row, _aggregate_trades(trades))
            row["diff_type"] = "field_mismatch"
            row["remarks"] = "; ".join(sorted(set(issues)))
            diff_rows.append(row)

    for t in broker_trades:
        raw_id = t.get("id")
        if raw_id is None:
            continue
        tid = int(raw_id)
        if tid in matched_trade_ids:
            continue
        row = _trade_row_base(recon_at_iso)
        _fill_binance_single_trade(row, t)
        row["diff_type"] = "broker_only"
        row["remarks"] = "trade in broker window, order not in DB scoped filled rows"
        diff_rows.append(row)

    summary = {
        "recon_at": recon_at_iso,
        "broker": broker,
        "account_id_filter": account_filter or "",
        "window_start_utc": window_start_utc_iso,
        "window_end_utc": window_end_utc_iso,
        "internal_scope_rows": len(db_rows),
        "broker_trade_rows": len(broker_trades),
        "matched_ids": matched_ids,
        "internal_only_rows": sum(1 for r in diff_rows if r["diff_type"] == "internal_only"),
        "broker_only_rows": sum(1 for r in diff_rows if r["diff_type"] == "broker_only"),
        "field_mismatch_rows": sum(1 for r in diff_rows if r["diff_type"] == "field_mismatch"),
    }
    return OrderReconResult(summary=summary, diff_rows=diff_rows)


def _load_internal_filled_in_window(
    cur: Any,
    broker: str,
    account_id: str | None,
    window_start: datetime,
    limit: int,
    statuses: tuple[str, ...],
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT internal_id, broker_order_id, account_id, symbol, side, order_type,
               quantity, limit_price, price, time_in_force, status, executed_qty,
               created_at, updated_at, binance_transact_time
        FROM {SCHEMA_OMS}.orders
        WHERE broker = %s
          AND status IN %s
          AND COALESCE(updated_at, created_at) >= %s
          AND (%s IS NULL OR account_id = %s)
        ORDER BY COALESCE(updated_at, created_at) DESC NULLS LAST
        LIMIT %s
        """,
        (broker, tuple(statuses), window_start, account_id, account_id, limit),
    )
    return [dict(r) for r in cur.fetchall()]


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
    Run trade-window reconciliation and write two CSVs (summary + diff). Safe to re-run (overwrites
    same-hour files).

    Returns ``{"summary": (path, 1), "diff": (path, n)}`` where ``n`` = diff row count.
    """
    snap = utc_hour_start(snapshot_at)
    ts = snap.strftime(FILENAME_TS_FMT)
    recon_at = datetime.now(timezone.utc).isoformat()
    out_dir = output_dir if output_dir is not None else scheduler_reports_csv_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    broker = ORDER_RECON_BROKER
    acct = account_id if account_id is not None else ORDER_RECON_ACCOUNT_ID
    n = ORDER_RECON_TRADE_TAIL_LIMIT
    st = ORDER_RECON_DB_FILLED_STATUSES
    hours = ORDER_RECON_TRADE_LOOKBACK_HOURS

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=hours)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    window_start_iso = start_dt.isoformat()
    window_end_iso = end_dt.isoformat()

    conn = psycopg2.connect(database_url)
    configure_for_scheduler(conn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            db_rows = _load_internal_filled_in_window(cur, broker, acct, start_dt, n, st)
    finally:
        conn.close()

    api = client or _binance_client_from_env()
    symbols = {str(r.get("symbol") or "").strip().upper() for r in db_rows if str(r.get("symbol") or "").strip()}
    try:
        broker_trades = fetch_broker_trades_merged(
            api,
            symbols,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            global_cap=n,
        )
    except BinanceAPIError as e:
        logger.exception("order_recon: Binance myTrades failed: {}", e)
        raise

    result = reconcile_trade_window(
        recon_at,
        db_rows,
        broker_trades,
        broker=broker,
        account_filter=acct,
        window_start_utc_iso=window_start_iso,
        window_end_utc_iso=window_end_iso,
    )

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
    """Hourly scheduler job: ``DATABASE_URL`` + Binance keys from env; CSV under ``reports_out/``."""

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
