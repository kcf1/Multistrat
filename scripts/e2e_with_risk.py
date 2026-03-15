#!/usr/bin/env python3
"""
E2E test with Risk in path: inject to strategy_orders → Risk → risk_approved → OMS → fill → orders table.

Times each event and each module; checks downstream data integrity.

Assumes: redis, risk, oms (and optionally postgres) running (e.g. docker compose up -d redis risk oms).
Requires: REDIS_URL. Optional: DATABASE_URL for orders table check.

Usage:
    python scripts/e2e_with_risk.py [--timeout 120] [--no-postgres] [--verbose]
"""

import argparse
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
except ImportError:
    pass

if not os.getenv("LOG_OMS"):
    import logging
    logging.disable(logging.CRITICAL)
    try:
        from loguru import logger
        logger.disable("oms")
    except Exception:
        pass

if not os.getenv("LOG_RISK"):
    import logging
    logging.disable(logging.CRITICAL)


def _env(key: str, default: str = "") -> str:
    v = (os.getenv(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


def _read_latest(redis, stream: str, count: int = 50) -> List[Tuple[str, Dict[str, str]]]:
    """Last `count` entries from stream (newest first)."""
    raw = redis.xrevrange(stream, max="+", min="-", count=count)
    out = []
    for eid, flds in raw:
        eid_str = eid.decode() if isinstance(eid, bytes) else eid
        decoded = {}
        for k, v in (flds or {}).items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            decoded[key] = val
        out.append((eid_str, decoded))
    return out


# Fields we assert for data integrity (same across inject, risk_approved, orders table, oms_fills where applicable)
INTEGRITY_KEYS = ("broker", "symbol", "side", "quantity", "order_type", "book", "comment", "order_id")


def _normalize_for_compare(d: Dict[str, Any], quantity_key: str = "quantity") -> Dict[str, Any]:
    """Normalize dict for comparison: stringify numbers where needed."""
    out = {}
    for k in INTEGRITY_KEYS:
        if k not in d:
            continue
        v = d[k]
        if v is None:
            continue
        if k == quantity_key and v is not None:
            try:
                out[k] = str(float(v))
            except (TypeError, ValueError):
                out[k] = str(v)
        else:
            out[k] = str(v).strip()
    return out


def run_e2e_with_risk(
    redis_url: str,
    database_url: str = "",
    timeout: float = 120,
    verbose: bool = True,
    check_postgres: bool = True,
) -> Tuple[bool, Dict[str, Optional[float]]]:
    from redis import Redis
    from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
    from oms.streams import add_message
    from oms.storage.redis_order_store import RedisOrderStore
    from risk.schemas import STRATEGY_ORDERS_STREAM

    redis = Redis.from_url(redis_url, decode_responses=True)
    store = RedisOrderStore(redis)
    poll_interval = 0.05
    deadline = time.monotonic() + timeout

    order_id = f"e2e-risk-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    injected = {
        "order_id": order_id,
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "quantity": 0.0001,
        "order_type": "MARKET",
        "book": "e2e_with_risk",
        "comment": "E2E with risk timing and integrity",
    }

    def log(msg: str) -> None:
        if verbose:
            print(msg)

    # --- Timings (monotonic seconds) ---
    t_inject: Optional[float] = None
    t_risk_approved: Optional[float] = None
    t_oms_staged: Optional[float] = None
    t_oms_fill: Optional[float] = None
    t_orders_table: Optional[float] = None

    # --- Captured payloads for integrity ---
    risk_approved_msg: Optional[Dict[str, str]] = None
    oms_fill_msg: Optional[Dict[str, str]] = None
    pg_row: Optional[Dict[str, Any]] = None

    # 1. Inject to strategy_orders
    log("1. Injecting order to strategy_orders (Risk in path)...")
    t_inject = time.monotonic()
    add_message(redis, STRATEGY_ORDERS_STREAM, injected)
    log(f"   order_id={order_id}")

    # 2. Poll for risk_approved (Risk forwarded), then OMS store, oms_fills, Postgres
    log("2. Polling for events (risk_approved → OMS store → oms_fills → orders table)...")
    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        now = time.monotonic()

        # risk_approved: message may be consumed by OMS quickly; poll frequently
        if t_risk_approved is None:
            risk_entries = _read_latest(redis, RISK_APPROVED_STREAM, count=100)
            for _eid, flds in risk_entries:
                if flds.get("order_id") == order_id or flds.get("order_id") == injected.get("order_id"):
                    t_risk_approved = now
                    risk_approved_msg = dict(flds)
                    break

        # OMS order store (order staged by OMS)
        if t_oms_staged is None:
            order = store.get_order(order_id)
            if order:
                t_oms_staged = now

        # oms_fills (fill event)
        if t_oms_fill is None:
            fill_entries = _read_latest(redis, OMS_FILLS_STREAM, count=50)
            for _eid, flds in fill_entries:
                if flds.get("order_id") == order_id and flds.get("event_type") == "fill":
                    t_oms_fill = now
                    oms_fill_msg = dict(flds)
                    break

        # Postgres orders table
        if check_postgres and database_url and t_orders_table is None:
            try:
                import psycopg2
                conn = psycopg2.connect(database_url)
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT internal_id, broker, symbol, side, quantity, order_type, book, comment FROM orders WHERE internal_id = %s",
                        (order_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        t_orders_table = now
                        pg_row = {
                            "order_id": row[0],
                            "broker": row[1],
                            "symbol": row[2],
                            "side": row[3],
                            "quantity": row[4],
                            "order_type": row[5],
                            "book": row[6],
                            "comment": row[7] or "",
                        }
                finally:
                    conn.close()
            except Exception:
                pass

        # Done when we have fill and (if postgres) orders row
        if t_oms_fill is not None:
            if not check_postgres or not database_url or t_orders_table is not None:
                break
    # End poll loop

    if t_oms_fill is None:
        log("   FAIL: oms_fills (fill) not seen within timeout")
        return False, {}
    if check_postgres and database_url and t_orders_table is None:
        log("   FAIL: orders table row not seen within timeout")
        return False, {}

    log("   OK: all events observed")

    # Build timings dict (ms)
    timings: Dict[str, Optional[float]] = {}
    if t_inject is not None:
        if t_risk_approved is not None:
            timings["inject_to_risk_approved_ms"] = (t_risk_approved - t_inject) * 1000
        else:
            timings["inject_to_risk_approved_ms"] = None
        if t_oms_staged is not None:
            timings["inject_to_oms_staged_ms"] = (t_oms_staged - t_inject) * 1000
        timings["inject_to_oms_fill_ms"] = (t_oms_fill - t_inject) * 1000
        if t_orders_table is not None:
            timings["inject_to_orders_table_ms"] = (t_orders_table - t_inject) * 1000
        else:
            timings["inject_to_orders_table_ms"] = None
    if t_risk_approved is not None and t_inject is not None:
        timings["risk_ms"] = (t_risk_approved - t_inject) * 1000
    else:
        timings["risk_ms"] = None
    t_after_risk = t_risk_approved if t_risk_approved is not None else t_oms_staged
    if t_after_risk is not None:
        timings["oms_ms"] = (t_oms_fill - t_after_risk) * 1000
    else:
        timings["oms_ms"] = None
    if t_orders_table is not None and t_oms_fill is not None:
        timings["sync_ms"] = (t_orders_table - t_oms_fill) * 1000
    else:
        timings["sync_ms"] = None

    # --- Report timings (ms) ---
    log("")
    log("=== TIMING (milliseconds) ===")
    log("  Event (from inject):")
    if t_inject is not None:
        if t_risk_approved is not None:
            log(f"    inject → risk_approved:    {timings['inject_to_risk_approved_ms']:.1f} ms  (Risk)")
        else:
            log("    inject → risk_approved:    (not observed; OMS may have consumed before poll)")
        if t_oms_staged is not None:
            log(f"    inject → OMS staged:       {timings['inject_to_oms_staged_ms']:.1f} ms")
        log(f"    inject → oms_fills (fill):  {timings['inject_to_oms_fill_ms']:.1f} ms")
        if t_orders_table is not None:
            log(f"    inject → orders table:     {timings['inject_to_orders_table_ms']:.1f} ms  (end-to-end)")
    log("  Per-module (segment):")
    if timings.get("risk_ms") is not None:
        log(f"    Risk (inject → risk_approved):     {timings['risk_ms']:.1f} ms")
    if timings.get("oms_ms") is not None:
        log(f"    OMS (risk_approved → oms_fill):   {timings['oms_ms']:.1f} ms")
    if timings.get("sync_ms") is not None:
        log(f"    Sync (oms_fill → orders table):   {timings['sync_ms']:.1f} ms")
    log("  (Note: polling interval 50ms adds uncertainty)")

    # --- Data integrity ---
    log("")
    log("=== DOWNSTREAM DATA INTEGRITY ===")
    injected_norm = _normalize_for_compare(injected)
    all_ok = True

    risk_ok = True
    if risk_approved_msg:
        risk_norm = _normalize_for_compare(risk_approved_msg)
        for k in ("broker", "symbol", "side", "quantity", "order_type", "book", "comment", "order_id"):
            exp = injected_norm.get(k)
            got = risk_norm.get(k)
            if exp is not None and got != exp:
                log(f"  [FAIL] risk_approved.{k}: expected {exp!r}, got {got!r}")
                all_ok = False
                risk_ok = False
        if risk_ok:
            log("  risk_approved: OK (matches injected)")
    else:
        log("  risk_approved: (message not captured; may have been consumed by OMS before poll)")

    oms_fill_ok = True
    if oms_fill_msg:
        if oms_fill_msg.get("order_id") != order_id:
            log(f"  [FAIL] oms_fills.order_id: expected {order_id!r}, got {oms_fill_msg.get('order_id')!r}")
            all_ok = False
            oms_fill_ok = False
        if oms_fill_msg.get("event_type") != "fill":
            log(f"  [FAIL] oms_fills.event_type: expected fill, got {oms_fill_msg.get('event_type')!r}")
            all_ok = False
            oms_fill_ok = False
        if oms_fill_msg.get("symbol") != injected.get("symbol"):
            log(f"  [FAIL] oms_fills.symbol: expected {injected.get('symbol')!r}, got {oms_fill_msg.get('symbol')!r}")
            all_ok = False
            oms_fill_ok = False
        if oms_fill_msg.get("side") != injected.get("side"):
            log(f"  [FAIL] oms_fills.side: expected {injected.get('side')!r}, got {oms_fill_msg.get('side')!r}")
            all_ok = False
            oms_fill_ok = False
        if oms_fill_ok:
            log("  oms_fills: OK (order_id, event_type, symbol, side match)")

    pg_ok = True
    if pg_row:
        for k in ("broker", "symbol", "side", "order_type", "book", "comment"):
            exp = injected_norm.get(k)
            got = str(pg_row.get(k, "")).strip() if pg_row.get(k) is not None else ""
            if exp is not None and got != exp:
                log(f"  [FAIL] orders.{k}: expected {exp!r}, got {got!r}")
                all_ok = False
                pg_ok = False
        # quantity: compare to injected
        exp_qty = injected_norm.get("quantity")
        got_qty = pg_row.get("quantity")
        if exp_qty is not None and got_qty is not None:
            try:
                if abs(float(got_qty) - float(exp_qty)) > 1e-9:
                    log(f"  [FAIL] orders.quantity: expected {exp_qty!r}, got {got_qty!r}")
                    all_ok = False
                    pg_ok = False
            except (TypeError, ValueError):
                if str(got_qty).strip() != exp_qty:
                    all_ok = False
                    pg_ok = False
        if pg_ok:
            log("  orders table: OK (broker, symbol, side, quantity, order_type, book, comment match)")
    elif check_postgres and database_url:
        log("  orders table: (row not captured)")

    if not all_ok:
        log("  At least one integrity check failed.")
        return False, timings
    log("  All downstream checks passed.")
    return True, timings


def _stats(values: List[float]) -> Dict[str, float]:
    """Min, max, avg, std (ms)."""
    n = len(values)
    if n == 0:
        return {}
    avg = sum(values) / n
    variance = sum((x - avg) ** 2 for x in values) / n
    return {
        "min": min(values),
        "max": max(values),
        "avg": avg,
        "std": variance ** 0.5,
        "n": n,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="E2E with Risk: strategy_orders → Risk → risk_approved → OMS → orders table; timing and integrity."
    )
    parser.add_argument("--timeout", type=float, default=120, help="Timeout seconds (default 120)")
    parser.add_argument("--no-postgres", action="store_true", help="Skip Postgres orders table check")
    parser.add_argument("--runs", type=int, default=1, help="Number of E2E runs for stats (default 1)")
    parser.add_argument("--verbose", action="store_true", default=True, help="Verbose output (default True)")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    args = parser.parse_args()

    if args.quiet:
        args.verbose = False

    redis_url = _env("REDIS_URL")
    if not redis_url:
        print("REDIS_URL not set", file=sys.stderr)
        return 1
    database_url = _env("DATABASE_URL")

    runs = max(1, args.runs)
    check_pg = not args.no_postgres and bool(database_url)
    verbose = args.verbose and runs == 1  # Only verbose for single run

    all_timings: Dict[str, List[float]] = {}
    successes = 0
    failures = 0

    for run in range(runs):
        if runs > 1:
            print(f"Run {run + 1}/{runs}...", flush=True)
        ok, timings = run_e2e_with_risk(
            redis_url,
            database_url=database_url or "",
            timeout=args.timeout,
            verbose=verbose,
            check_postgres=check_pg,
        )
        if ok:
            successes += 1
            for k, v in timings.items():
                if v is not None:
                    all_timings.setdefault(k, []).append(v)
        else:
            failures += 1
        if runs > 1 and run < runs - 1:
            time.sleep(0.5)  # Brief pause between runs

    if runs > 1:
        print("")
        print("=" * 60)
        print("STATS (milliseconds)")
        print("=" * 60)
        print(f"Runs: {successes} passed, {failures} failed (total {runs})")
        print("")
        labels = {
            "inject_to_risk_approved_ms": "inject → risk_approved",
            "inject_to_oms_staged_ms": "inject → OMS staged",
            "inject_to_oms_fill_ms": "inject → oms_fills (fill)",
            "inject_to_orders_table_ms": "inject → orders table (e2e)",
            "risk_ms": "Risk (segment)",
            "oms_ms": "OMS (segment)",
            "sync_ms": "Sync (segment)",
        }
        for key in ["inject_to_risk_approved_ms", "inject_to_oms_staged_ms", "inject_to_oms_fill_ms",
                    "inject_to_orders_table_ms", "risk_ms", "oms_ms", "sync_ms"]:
            vals = all_timings.get(key, [])
            if not vals:
                continue
            s = _stats(vals)
            label = labels.get(key, key)
            print(f"  {label}:")
            print(f"    min={s['min']:.1f}  max={s['max']:.1f}  avg={s['avg']:.1f}  std={s['std']:.1f}  n={s['n']}")
        print("=" * 60)

    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
