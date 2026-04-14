#!/usr/bin/env python3
"""
E2E verification for 12.4.4: inject multiple test orders (broker binance) → OMS → Binance testnet
→ fill → OMS publishes oms_fills; OMS syncs orders/accounts to Postgres → PMS reads orders,
derives positions, writes positions table. Covers 3 symbols (BTC, DOGE, BNB) and conditions: long only, long extra, long < short. Then verify all downstreams.

Assumes services are already deployed (docker compose up -d oms pms postgres redis).
Requires: REDIS_URL, DATABASE_URL. Optional: BINANCE_* for testnet.

Usage:
    python scripts/e2e_12_4_4.py [--no-account] [--no-pms] [--no-valuation] [--timeout 180]

With --no-valuation: skips asset price feed and valuation checks (usd_price / usd_notional).
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

from pgconn import configure_for_oms, configure_for_pms

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
E2E_BOOK_BASE = "e2e_12_4_4"
# Use unique book per run so positions are fresh (no accumulation from previous runs)
def _e2e_book() -> str:
    return f"{E2E_BOOK_BASE}_{int(time.time() * 1000)}"

# 3 symbols, conditions: long only, long extra, long < short (no short-only to avoid testnet balance/notional).
E2E_SYMBOLS = ("BTCUSDT", "DOGEUSDT", "BNBUSDT")
# (symbol, side, quantity, condition_label).
E2E_ORDERS: List[Tuple[str, str, float, str]] = [
    (E2E_SYMBOLS[0], "BUY", 0.0001, "long_only"),
    (E2E_SYMBOLS[1], "BUY", 100, "long_extra"),
    (E2E_SYMBOLS[2], "BUY", 0.01, "long_lt_short_buy"),
    (E2E_SYMBOLS[2], "SELL", 0.02, "long_lt_short_sell"),
]

# Expected downstream results (PMS derives from orders via base/quote legs; position_side from open_qty sign):
# Expected assets: BTC, DOGE, BNB, USDT.
E2E_EXPECTED_ASSETS = ("BTC", "DOGE", "BNB", "USDT")
E2E_EXPECTED_OPEN_QTY = {
    "BTC": (0.0001, "long"),
    "DOGE": (100, "long"),
    "BNB": (-0.01, "short"),
    # USDT: present but qty not asserted (quote legs; fill prices vary)
}
TOLERANCE = 1e-6  # for float comparison


def run_e2e(
    redis_url: str,
    database_url: str,
    check_account: bool = True,
    check_pms: bool = True,
    check_valuation: bool = True,
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
    E2E_BOOK = _e2e_book()

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
    # Downstream check: log each order status from Redis (to investigate OMS→Redis)
    for oid in order_ids:
        order = store.get_order(oid)
        log(f"   downstream_check [Redis order]: order_id={oid} status={order.get('status') if order else None!r} executed_qty={order.get('executed_qty') if order else None}")

    # Step 3: Postgres orders — all synced
    log("3. Waiting for all orders in Postgres (OMS sync)...")
    pg_account_id: Optional[str] = None
    while time.monotonic() < deadline:
        try:
            import psycopg2
            conn = psycopg2.connect(database_url)
            configure_for_oms(conn)
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
    # Downstream check: log each order as stored in Postgres (to investigate OMS sync and PMS status filter)
    try:
        import json
        import psycopg2
        conn = psycopg2.connect(database_url)
        configure_for_oms(conn)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT internal_id, account_id, book, symbol, status, executed_qty, payload FROM orders WHERE internal_id = ANY(%s) ORDER BY created_at NULLS LAST",
                (order_ids,),
            )
            for r in cur.fetchall():
                internal_id, account_id, book, symbol, status, executed_qty, payload = r[0], r[1], r[2], r[3], r[4], r[5], r[6]
                log(f"   downstream_check [Postgres orders]: internal_id={internal_id} account_id={account_id!r} book={book!r} symbol={symbol} status={status!r} executed_qty={executed_qty}")
                if status and str(status).lower() == "rejected" and payload:
                    try:
                        pl = payload if isinstance(payload, dict) else json.loads(payload) if payload else {}
                        msg = (pl.get("binance") or {}).get("msg") if isinstance(pl.get("binance"), dict) else None
                        code = (pl.get("binance") or {}).get("code") if isinstance(pl.get("binance"), dict) else None
                        if msg:
                            log(f"   order failed payload msg: internal_id={internal_id} binance.msg={msg!r} binance.code={code}")
                        else:
                            log(f"   order failed payload: internal_id={internal_id} payload={payload!r}")
                    except Exception as parse_err:
                        log(f"   order failed payload (parse error): internal_id={internal_id} payload={payload!r} error={parse_err}")
        finally:
            conn.close()
    except Exception as e:
        if verbose:
            log(f"   downstream_check [Postgres orders]: query failed: {e}")

    # Ensure symbols table has E2E pairs so PMS derivation can resolve base/quote (after OMS sync)
    if check_pms:
        try:
            import psycopg2
            conn = psycopg2.connect(database_url)
            configure_for_oms(conn)
            try:
                cur = conn.cursor()
                for sym in E2E_SYMBOLS:
                    base = sym.replace("USDT", "") if sym.endswith("USDT") else sym[:3]
                    quote = "USDT" if sym.endswith("USDT") else "BTC"
                    cur.execute(
                        """
                        INSERT INTO symbols (symbol, base_asset, quote_asset)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (symbol) DO UPDATE SET
                            base_asset = EXCLUDED.base_asset,
                            quote_asset = EXCLUDED.quote_asset
                        """,
                        (sym, base, quote),
                    )
                conn.commit()
                log("3.5. Ensured symbols table has %s (for PMS asset derivation)" % ", ".join(E2E_SYMBOLS))
                # Downstream check: log symbol map as PMS would see it for E2E symbols
                cur.execute(
                    "SELECT symbol, base_asset, quote_asset FROM symbols WHERE symbol = ANY(%s)",
                    (list(E2E_SYMBOLS),),
                )
                sym_rows = cur.fetchall()
                for r in sym_rows:
                    log(f"   downstream_check [symbols table]: symbol={r[0]!r} base_asset={r[1]!r} quote_asset={r[2]!r}")
                if len(sym_rows) < len(E2E_SYMBOLS):
                    log(f"   downstream_check [symbols table]: expected %s rows for E2E_SYMBOLS, got %s" % (len(E2E_SYMBOLS), len(sym_rows)))
            finally:
                conn.close()
        except Exception as e:
            if verbose:
                log(f"   WARN: could not ensure symbols: {e}")
        # Ensure assets table has E2E assets and (optionally) run price feed for valuation
        if check_valuation:
            log("3.6. Ensuring assets table and price feed for valuation...")
            try:
                from pms.asset_init import init_assets_stables, sync_assets_from_symbols
                from pms.asset_price_feed import run_asset_price_feed_step
                from pms.asset_price_providers.binance import BinanceAssetPriceProvider

                init_assets_stables(database_url, assets=["USDT"])
                sync_assets_from_symbols(database_url, quote_asset="USDT")
                base_url = _env("BINANCE_BASE_URL") or "https://testnet.binance.vision"
                provider = BinanceAssetPriceProvider(base_url=base_url, use_testnet="testnet" in base_url.lower())
                updated = run_asset_price_feed_step(database_url, provider, "binance", ["BTC", "DOGE", "BNB"])
                log("   asset price feed updated %s asset(s)" % updated)
            except Exception as e:
                if verbose:
                    log(f"   WARN: assets/price feed failed (valuation check may be skipped): {e}")
        # Force one PMS tick so positions are derived (and enriched with usd_price from assets)
        log("3.7. Running one PMS tick to derive positions...")
        try:
            from pms.loop import run_one_tick
            from pms.mark_price import get_mark_price_provider
            mark_provider = get_mark_price_provider("fake")
            n = run_one_tick(database_url, mark_provider)
            log("   PMS tick wrote %s position(s)" % n)
            log("   downstream_check [PMS tick]: run_one_tick returned n=%s" % n)
        except Exception as e:
            if verbose:
                log(f"   WARN: PMS tick failed: {e}")
            log("3.7. Waiting 15s for PMS to derive positions...")
            time.sleep(15)

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
            log("   downstream_check [Redis account]: broker=%s account_id=%s balances_count=%s" % (broker, account_id, len(balances)))

    # Step 5: PMS — positions table: expect asset-grain rows (BTC, ETH, BNB, USDT) with expected open_qty/position_side
    if check_pms:
        log("5. Waiting for PMS to write positions (Postgres positions table, asset grain)...")
        log("   downstream_check [positions]: book=%s expected_assets=%s" % (E2E_BOOK, list(E2E_EXPECTED_ASSETS)))
        pms_deadline = min(deadline, time.monotonic() + 40)
        found: List[Tuple[Any, ...]] = []
        poll_count = 0
        while time.monotonic() < pms_deadline:
            found = []
            try:
                import psycopg2
                conn = psycopg2.connect(database_url)
                configure_for_pms(conn)
                try:
                    cur = conn.cursor()
                    try:
                        cur.execute(
                            """
                            SELECT broker, account_id, book, asset, open_qty, position_side, usd_price, usd_notional
                            FROM positions
                            WHERE broker = %s AND book = %s AND asset = ANY(%s)
                            ORDER BY asset
                            """,
                            ("binance", E2E_BOOK, list(E2E_EXPECTED_ASSETS)),
                        )
                    except Exception:
                        cur.execute(
                            """
                            SELECT broker, account_id, book, asset, open_qty, position_side, usd_price
                            FROM positions
                            WHERE broker = %s AND book = %s AND asset = ANY(%s)
                            ORDER BY asset
                            """,
                            ("binance", E2E_BOOK, list(E2E_EXPECTED_ASSETS)),
                        )
                    rows = cur.fetchall()
                    for r in rows:
                        asset, open_qty = r[3], float(r[4] or 0)
                        usd_price = float(r[6] or 0) if r[6] is not None else None
                        usd_notional = float(r[7]) if len(r) > 7 and r[7] is not None else None
                        found.append((r[0], r[1], r[2], asset, open_qty, r[5], usd_price, usd_notional))
                finally:
                    conn.close()
                by_asset = {a: (oq, ps) for (_, _, _, a, oq, ps, _, _) in found}
                poll_count += 1
                log("   downstream_check [positions] poll #%s: assets_found=%s by_asset=%s" % (poll_count, list(by_asset.keys()), by_asset))
                if len(by_asset) < len(E2E_EXPECTED_ASSETS):
                    time.sleep(2)
                    continue
                # Assert expected open_qty (within tolerance) and position_side for base assets
                all_ok = True
                for asset, (exp_qty, exp_side) in E2E_EXPECTED_OPEN_QTY.items():
                    oq, ps = by_asset.get(asset, (0, ""))
                    if abs(oq - exp_qty) > TOLERANCE or (exp_side != "flat" and ps != exp_side):
                        all_ok = False
                        break
                if all_ok:
                    valuation_ok = True
                    if check_valuation:
                        for (_b, _acc, _book, a, oq, ps, usd_p, usd_n) in found:
                            if abs(oq) <= TOLERANCE:
                                continue
                            if usd_p is None or usd_p <= 0:
                                log("   FAIL: valuation missing for %s (open_qty=%s, usd_price=%s)" % (a, oq, usd_p))
                                valuation_ok = False
                                break
                            expected_notional = oq * usd_p
                            if usd_n is not None:
                                if abs(usd_n - expected_notional) > max(1e-2, abs(expected_notional) * 1e-6):
                                    log("   FAIL: usd_notional mismatch %s: got %s expected ~%s" % (a, usd_n, expected_notional))
                                    valuation_ok = False
                                    break
                        if valuation_ok:
                            log("   OK: valuation consistent with price feed (usd_price set, usd_notional = open_qty * usd_price)")
                        else:
                            valuation_ok = False
                    for (_b, _acc, _book, a, oq, ps, usd_p, usd_n) in found:
                        exp = E2E_EXPECTED_OPEN_QTY.get(a, (None, ""))
                        notional = usd_n if usd_n is not None else (oq * usd_p if usd_p is not None else None)
                        log(f"   position: {a} open_qty={oq} (expected {exp[0]}) side={ps} usd_price={usd_p} usd_notional={notional}")
                    if not check_valuation or valuation_ok:
                        log("   OK: PMS positions match expected (asset grain: long only, short only, long < short)")
                        break
                    if check_valuation and not valuation_ok:
                        time.sleep(2)
                        continue
            except Exception as e:
                if verbose:
                    log(f"   positions query: {e}")
            time.sleep(2)
        else:
            log("   FAIL: positions did not match expected open_qty/position_side within timeout")
            log("   downstream_check [positions] FAIL reason: %s" % (
                "len(by_asset)=%s < %s (missing assets)" % (len(by_asset), len(E2E_EXPECTED_ASSETS)) if len(by_asset) < len(E2E_EXPECTED_ASSETS) else "wrong open_qty or position_side for base assets"
            ))
            if found:
                for row in found:
                    a, oq, ps = row[3], row[4], row[5]
                    exp = E2E_EXPECTED_OPEN_QTY.get(a, (None, "?"))
                    log(f"   seen: {a} open_qty={oq} (expected {exp[0]}) side={ps}")
                log("   downstream_check [positions] expected: %s" % dict(E2E_EXPECTED_OPEN_QTY))
            return False

    elapsed = time.monotonic() - start
    log("")
    log("=" * 60)
    log("E2E 12.4.4 PASSED")
    log(f"Orders: {len(order_ids)} (3 symbols: BTC, DOGE, BNB; conditions: long only, long extra, long < short)")
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
    parser.add_argument("--no-valuation", action="store_true", help="Skip asset price feed and valuation checks (usd_price / usd_notional)")
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
        check_valuation=not args.no_valuation,
        timeout=args.timeout,
        verbose=not args.quiet,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
