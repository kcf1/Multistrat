#!/usr/bin/env python3
"""
E2E verification for 12.4.4: inject multiple test orders (broker binance) → OMS → Binance testnet
→ fill → OMS publishes oms_fills; OMS syncs orders/accounts to Postgres → PMS reads orders,
derives positions, writes positions table. Covers 3 symbols and 3 conditions: long only,
short only, long < short (building net-short position). Then verify all downstreams and
remind to check pgAdmin and RedisInsight.

Assumes services are already deployed (docker compose up -d oms pms postgres redis).
Requires: REDIS_URL, DATABASE_URL. Optional: BINANCE_* for testnet.

Usage:
    python scripts/e2e_12_4_4.py [--no-account] [--no-pms] [--timeout 180]
"""

import argparse
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional, Tuple

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


def _env(key: str, default: str = "") -> str:
    v = (os.getenv(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


def _read_latest_fills(redis, stream: str, count: int = 80):
    raw = redis.xrevrange(stream, max="+", min="-", count=count)
    out = []
    for eid, flds in raw:
        eid_str = eid.decode() if isinstance(eid, bytes) else eid
        decoded = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v for k, v in (flds or {}).items()}
        out.append((eid_str, decoded))
    return out


TERMINAL_STATUSES = {"filled", "canceled", "rejected", "expired"}
E2E_BOOK = "e2e_12_4_4"

# 3 symbols, 3 conditions:
# - Long only: symbol with only BUY(s) → net long
# - Short only: symbol with only SELL(s) → net short
# - Long < short: symbol with BUY then more SELL → net short (building short position)
E2E_SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT")
# Order specs: (symbol, side, quantity, condition_label). Lot sizes with 1 decimal.
E2E_ORDERS: List[Tuple[str, str, float, str]] = [
    (E2E_SYMBOLS[0], "BUY", 0.1, "long_only"),
    (E2E_SYMBOLS[1], "SELL", 0.1, "short_only"),
    (E2E_SYMBOLS[2], "BUY", 0.1, "long_lt_short_buy"),
    (E2E_SYMBOLS[2], "SELL", 0.2, "long_lt_short_sell"),
]

# Expected downstream results (PMS derives from orders via FIFO; position_side from open_qty sign):
# - oms_fills: 4 entries (one per order, event_type fill).
# - Redis order store: 4 orders, each status filled.
# - Postgres orders: 4 rows, status filled, executed_qty set.
# - Postgres positions (book=e2e_12_4_4):
#   - BTCUSDT: open_qty ≈ +0.1, position_side long  (long only).
#   - ETHUSDT: open_qty ≈ -0.1, position_side short (short only).
#   - BNBUSDT: open_qty ≈ -0.1, position_side short (BUY 0.1 then SELL 0.2 → net -0.1).
E2E_EXPECTED_OPEN_QTY = {
    E2E_SYMBOLS[0]: (0.1, "long"),   # long only
    E2E_SYMBOLS[1]: (-0.1, "short"),  # short only
    E2E_SYMBOLS[2]: (-0.1, "short"),  # long < short
}
TOLERANCE = 1e-6  # for float comparison


def run_e2e(
    redis_url: str,
    database_url: str,
    check_account: bool = True,
    check_pms: bool = True,
    timeout: float = 180,
    verbose: bool = True,
) -> bool:
    from redis import Redis
    from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
    from oms.streams import add_message
    from oms.storage.redis_order_store import RedisOrderStore
    from oms.storage.redis_account_store import RedisAccountStore

    redis = Redis.from_url(redis_url, decode_responses=True)
    store = RedisOrderStore(redis)
    account_store = RedisAccountStore(redis) if check_account else None

    def log(msg: str) -> None:
        if verbose:
            print(msg)

    start = time.monotonic()
    deadline = start + timeout
    poll_interval = 0.2
    base_id = f"e2e1244-{int(time.time() * 1000)}"
    order_ids: List[str] = []

    # Build and inject all orders
    log("1. Injecting orders to risk_approved (3 symbols, 3 conditions: long only, short only, long < short)...")
    for i, (symbol, side, qty, label) in enumerate(E2E_ORDERS):
        order_id = f"{base_id}-{i}-{uuid.uuid4().hex[:6]}"
        order_ids.append(order_id)
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": symbol,
            "side": side,
            "quantity": qty,
            "order_type": "MARKET",
            "book": E2E_BOOK,
            "comment": f"E2E 12.4.4 {label}",
        }
        add_message(redis, RISK_APPROVED_STREAM, risk_order)
        log(f"   {order_id}  {symbol} {side} {qty}  ({label})")
    log(f"   Injected {len(order_ids)} orders.")

    # Step 2: Wait for all orders — fill in oms_fills and Redis order store
    log("2. Waiting for OMS (all orders → oms_fills, order store)...")
    filled_ids: set = set()
    while time.monotonic() < deadline:
        time.sleep(poll_interval)
        entries = _read_latest_fills(redis, OMS_FILLS_STREAM, count=80)
        for oid in order_ids:
            if oid in filled_ids:
                continue
            fill_entry = next((e for e in entries if e[1].get("order_id") == oid), None)
            if fill_entry:
                filled_ids.add(oid)
                continue
            order = store.get_order(oid)
            if order and order.get("status") in TERMINAL_STATUSES:
                filled_ids.add(oid)
        if len(filled_ids) == len(order_ids):
            break
    if len(filled_ids) != len(order_ids):
        missing = set(order_ids) - filled_ids
        log(f"   FAIL: {len(missing)} order(s) not filled/terminal within timeout: {missing}")
        return False
    log(f"   OK: all {len(order_ids)} orders filled/terminal")

    # Step 3: Postgres orders — all synced
    log("3. Waiting for all orders in Postgres (OMS sync)...")
    pg_account_id: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            import psycopg2
            conn = psycopg2.connect(database_url)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT internal_id, account_id, status FROM orders WHERE internal_id = ANY(%s)",
                    (order_ids,),
                )
                rows = cur.fetchall()
                synced = [r[0] for r in rows if r[2] in ("filled", "partially_filled", *TERMINAL_STATUSES)]
                if pg_account_id is None and rows:
                    pg_account_id = rows[0][1]
                if len(synced) == len(order_ids):
                    break
            finally:
                conn.close()
        except Exception as e:
            if verbose:
                log(f"   Postgres: {e}")
        time.sleep(poll_interval)
    else:
        log("   FAIL: not all orders in Postgres within timeout")
        return False
    log(f"   OK: all {len(order_ids)} orders in Postgres (account_id={pg_account_id})")

    # Step 4: Account flow (optional)
    if check_account and account_store:
        log("4. Checking account flow (Redis account store)...")
        broker, account_id = "binance", (pg_account_id or "default")
        acc_ok = False
        while time.monotonic() < deadline:
            acc = account_store.get_account(broker, account_id)
            balances = account_store.get_balances(broker, account_id) if acc else []
            if acc and len(balances) >= 1:
                acc_ok = True
                break
            time.sleep(1)
        if not acc_ok:
            log("   WARN: no account/balances in Redis (OMS refresh may be slow)")
        else:
            log("   OK: account/balances in Redis")

    # Step 5: PMS — positions table: expect 3 symbols with expected open_qty and position_side (see E2E_EXPECTED_OPEN_QTY)
    if check_pms:
        log("5. Waiting for PMS to write positions (Postgres positions table)...")
        pms_deadline = min(deadline, time.monotonic() + 40)
        found: List[Tuple[Any, ...]] = []
        while time.monotonic() < pms_deadline:
            found = []
            try:
                import psycopg2
                conn = psycopg2.connect(database_url)
                try:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT account_id, book, symbol, open_qty, position_side, unrealized_pnl
                        FROM positions
                        WHERE book = %s AND symbol = ANY(%s)
                        ORDER BY symbol
                        """,
                        (E2E_BOOK, list(E2E_SYMBOLS)),
                    )
                    rows = cur.fetchall()
                    for r in rows:
                        symbol, open_qty = r[2], float(r[3] or 0)
                        found.append((r[0], r[1], symbol, open_qty, r[4], float(r[5] or 0)))
                finally:
                    conn.close()
                by_symbol = {s: (oq, ps) for (_, _, s, oq, ps, _) in found}
                if len(by_symbol) < 3:
                    time.sleep(2)
                    continue
                # Assert expected open_qty (within tolerance) and position_side for each symbol
                all_ok = True
                for sym, (exp_qty, exp_side) in E2E_EXPECTED_OPEN_QTY.items():
                    oq, ps = by_symbol.get(sym, (0, ""))
                    if abs(oq - exp_qty) > TOLERANCE or (exp_side != "flat" and ps != exp_side):
                        all_ok = False
                        break
                if all_ok:
                    for (acc, book, sym, oq, ps, upnl) in found:
                        exp = E2E_EXPECTED_OPEN_QTY.get(sym, (None, ""))
                        log(f"   position: {sym} open_qty={oq} (expected {exp[0]}) side={ps} unrealized_pnl={upnl}")
                    log("   OK: PMS positions match expected (long only, short only, long < short)")
                    break
            except Exception as e:
                if verbose:
                    log(f"   positions query: {e}")
            time.sleep(2)
        else:
            log("   FAIL: positions did not match expected open_qty/position_side within timeout")
            if found:
                for (acc, book, sym, oq, ps, upnl) in found:
                    exp = E2E_EXPECTED_OPEN_QTY.get(sym, (None, "?"))
                    log(f"   seen: {sym} open_qty={oq} (expected {exp[0]}) side={ps}")
            return False

    elapsed = time.monotonic() - start
    log("")
    log("=" * 60)
    log("E2E 12.4.4 PASSED")
    log(f"Orders: {len(order_ids)} (3 symbols, 3 conditions: long only, short only, long < short)")
    log(f"Total time: {elapsed:.1f}s")
    log("")
    log("Verify manually in pgAdmin and RedisInsight:")
    log("  pgAdmin: orders (internal_id, status), accounts, balances, positions")
    log("  RedisInsight: risk_approved, oms_fills, order store, account store")
    log("=" * 60)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="E2E 12.4.4: inject multiple orders (3 symbols, 3 conditions), verify OMS + PMS downstreams")
    parser.add_argument("--no-account", action="store_true", help="Skip account-flow check")
    parser.add_argument("--no-pms", action="store_true", help="Skip PMS positions table check")
    parser.add_argument("--timeout", type=float, default=180, help="Total timeout seconds (default: 180)")
    parser.add_argument("--quiet", action="store_true", help="Less output")
    args = parser.parse_args()

    redis_url = _env("REDIS_URL")
    database_url = _env("DATABASE_URL")
    if not redis_url:
        print("REDIS_URL not set", file=sys.stderr)
        return 1
    if not database_url:
        print("DATABASE_URL not set (required for orders sync and PMS positions check)", file=sys.stderr)
        return 1

    ok = run_e2e(
        redis_url=redis_url,
        database_url=database_url,
        check_account=not args.no_account,
        check_pms=not args.no_pms,
        timeout=args.timeout,
        verbose=not args.quiet,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
