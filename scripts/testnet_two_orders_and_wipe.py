#!/usr/bin/env python3
"""
Testnet: place two orders (BTC, ETH) via injection, sync account to Postgres,
then wipe ETH position and verify UPSERT/delete logic.

Requires: REDIS_URL, BINANCE_API_KEY, BINANCE_API_SECRET. Optional: DATABASE_URL.
OMS must be running (e.g. python -m oms.main) with testnet config so it consumes
risk_approved and places orders. Account snapshot is applied from REST and
sync_accounts_to_postgres is called from this script (OMS may not run account
listeners yet).

Usage:
    python scripts/testnet_two_orders_and_wipe.py [--no-wipe]
    --no-wipe: only run phase 1 (two orders + sync), skip ETH sell and delete check
"""

import argparse
import os
import sys
import time
import uuid
from pathlib import Path

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


TERMINAL_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}
POLL_INTERVAL = 0.1
DEADLINE_SECONDS = 90


def _read_latest_fills(redis, stream: str, count: int = 50):
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


def wait_for_fill(redis, store, order_id: str) -> bool:
    """Poll Redis order store and oms_fills until order is terminal or seen in fills. Returns True if filled/terminal."""
    from oms.schemas import OMS_FILLS_STREAM
    deadline = time.monotonic() + DEADLINE_SECONDS
    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL)
        order = store.get_order(order_id)
        if order and order.get("status") in TERMINAL_STATUSES:
            return True
        entries = _read_latest_fills(redis, OMS_FILLS_STREAM, count=30)
        if any(e[1].get("order_id") == order_id for e in entries):
            return True
    return False


def inject_market_order(redis, symbol: str, side: str, quantity: float, order_id: str) -> None:
    from oms.schemas import RISK_APPROVED_STREAM
    from oms.streams import add_message
    risk_order = {
        "order_id": order_id,
        "broker": "binance",
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "order_type": "MARKET",
        "book": "testnet_two_orders_wipe",
        "comment": f"testnet {symbol} {side}",
    }
    add_message(redis, RISK_APPROVED_STREAM, risk_order)


def apply_snapshot_to_redis(account_store, snapshot: dict, non_zero_only: bool = False) -> None:
    """Apply account snapshot to Redis. If non_zero_only, only include balances with available+locked > 0 (simulates outboundAccountPosition omitting zeros)."""
    broker = snapshot.get("broker", "binance")
    account_id = snapshot.get("account_id", "default")
    balances = snapshot.get("balances", [])
    positions = snapshot.get("positions", [])
    updated_at = snapshot.get("updated_at")
    payload = snapshot.get("payload")
    if non_zero_only:
        filtered = []
        for b in balances:
            av = float(b.get("available") or 0)
            lo = float(b.get("locked") or 0)
            if av + lo > 1e-9:
                filtered.append(b)
        balances = filtered
    account_store.apply_account_position(
        broker=broker,
        account_id=account_id,
        balances=balances,
        positions=positions,
        updated_at=updated_at,
        payload=payload,
    )


def get_pg_balance_assets(pg_connect, broker: str = "binance", account_id: str = "default") -> list:
    """Return list of asset strings for the account from Postgres balances (join accounts)."""
    import psycopg2
    conn = psycopg2.connect(pg_connect)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT b.asset FROM balances b
            JOIN accounts a ON a.id = b.account_id
            WHERE a.broker = %s AND a.account_id = %s
            ORDER BY b.asset
            """,
            (broker, account_id),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Testnet: two orders (BTC, ETH), sync, then wipe ETH and verify delete")
    parser.add_argument("--no-wipe", action="store_true", help="Skip phase 2 (ETH sell and delete check)")
    args = parser.parse_args()

    REDIS_URL = _env("REDIS_URL")
    DATABASE_URL = _env("DATABASE_URL")
    BINANCE_API_KEY = _env("BINANCE_API_KEY")
    BINANCE_API_SECRET = _env("BINANCE_API_SECRET")
    if not REDIS_URL:
        print("REDIS_URL not set", file=sys.stderr)
        return 1
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        print("BINANCE_API_KEY and BINANCE_API_SECRET required", file=sys.stderr)
        return 1

    from redis import Redis
    from oms.storage.redis_order_store import RedisOrderStore
    from oms.storage.redis_account_store import RedisAccountStore
    from oms.account_sync import sync_accounts_to_postgres
    from oms.brokers.binance.api_client import BinanceAPIClient

    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    store = RedisOrderStore(redis)
    account_store = RedisAccountStore(redis)
    base_url = _env("BINANCE_BASE_URL") or "https://testnet.binance.vision"
    client = BinanceAPIClient(
        api_key=BINANCE_API_KEY,
        api_secret=BINANCE_API_SECRET,
        base_url=base_url,
        testnet="testnet" in base_url.lower(),
    )
    account_id = "default"

    print("Phase 1: Place BTC and ETH BUY orders, sync, verify balances")
    print("-" * 60)

    # 1. BTC BUY
    order_id_btc = f"wipe-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    inject_market_order(redis, "BTCUSDT", "BUY", 0.0001, order_id_btc)
    print(f"  Injected BTC BUY order_id={order_id_btc}")
    if not wait_for_fill(redis, store, order_id_btc):
        print("  [FAIL] BTC order did not fill in time", file=sys.stderr)
        return 1
    print("  BTC order filled.")

    time.sleep(0.5)

    # 2. ETH BUY
    order_id_eth = f"wipe-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    inject_market_order(redis, "ETHUSDT", "BUY", 0.001, order_id_eth)
    print(f"  Injected ETH BUY order_id={order_id_eth}")
    if not wait_for_fill(redis, store, order_id_eth):
        print("  [FAIL] ETH order did not fill in time", file=sys.stderr)
        return 1
    print("  ETH order filled.")

    # 3. Fetch snapshot and apply to Redis (full snapshot)
    print("  Fetching account snapshot from Binance REST...")
    snapshot = client.get_account_snapshot(account_id=account_id)
    apply_snapshot_to_redis(account_store, snapshot, non_zero_only=False)
    print("  Applied full snapshot to Redis.")

    # 4. Sync to Postgres
    if DATABASE_URL:
        n = sync_accounts_to_postgres(redis, account_store, DATABASE_URL, ttl_after_sync_seconds=None)
        print(f"  Synced {n} account(s) to Postgres.")
    else:
        print("  DATABASE_URL not set, skipping Postgres sync.")

    # 5. Verify Redis has BTC and ETH
    balances = account_store.get_balances("binance", account_id)
    assets = [b.get("asset") for b in balances if b.get("asset")]
    if "BTC" not in assets or "ETH" not in assets:
        print(f"  [FAIL] Redis balances missing BTC or ETH: assets={assets}", file=sys.stderr)
        return 1
    print(f"  Redis balances OK: BTC and ETH present (assets: {sorted(assets)[:10]}...)")

    # 6. Verify Postgres has BTC and ETH
    if DATABASE_URL:
        pg_assets = get_pg_balance_assets(DATABASE_URL, "binance", account_id)
        if "BTC" not in pg_assets or "ETH" not in pg_assets:
            print(f"  [FAIL] Postgres balances missing BTC or ETH: assets={pg_assets}", file=sys.stderr)
            return 1
        print(f"  Postgres balances OK: BTC and ETH present (assets: {sorted(pg_assets)[:10]}...)")

    if args.no_wipe:
        print()
        print("Phase 2 skipped (--no-wipe).")
        return 0

    print()
    print("Phase 2: Get ETH position, sell it, verify position change and delete")
    print("-" * 60)

    # 7. Get previous ETH position (from exchange snapshot)
    print("  Fetching current account snapshot for ETH position...")
    snapshot_before_sell = client.get_account_snapshot(account_id=account_id)
    eth_before = 0.0
    for b in snapshot_before_sell.get("balances", []):
        if (b.get("asset") or "").strip() == "ETH":
            eth_before = float(b.get("available") or 0) + float(b.get("locked") or 0)
            break
    if eth_before <= 0:
        print("  [FAIL] No ETH position found before sell (expected > 0 from Phase 1)", file=sys.stderr)
        return 1
    # Sell exactly the position we have (round to 8 decimals for Binance)
    sell_qty = round(eth_before, 8)
    print(f"  ETH position before sell: {eth_before} (will sell {sell_qty})")

    # 8. ETH SELL (full position)
    order_id_sell = f"wipe-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
    inject_market_order(redis, "ETHUSDT", "SELL", sell_qty, order_id_sell)
    print(f"  Injected ETH SELL order_id={order_id_sell} quantity={sell_qty}")
    if not wait_for_fill(redis, store, order_id_sell):
        print("  [FAIL] ETH SELL did not fill in time", file=sys.stderr)
        return 1
    print("  ETH SELL filled.")

    time.sleep(0.5)

    # 9. Get ETH position after sell and verify position change
    print("  Fetching account snapshot after sell...")
    snapshot2 = client.get_account_snapshot(account_id=account_id)
    eth_after = 0.0
    for b in snapshot2.get("balances", []):
        if (b.get("asset") or "").strip() == "ETH":
            eth_after = float(b.get("available") or 0) + float(b.get("locked") or 0)
            break
    position_change = eth_before - eth_after
    tolerance = 1e-6
    if abs(position_change - sell_qty) > tolerance:
        print(
            f"  [FAIL] Position change mismatch: eth_before={eth_before} eth_after={eth_after} "
            f"change={position_change} sold={sell_qty}",
            file=sys.stderr,
        )
        return 1
    print(f"  Position change OK: before={eth_before} after={eth_after} change={position_change} (sold={sell_qty})")

    # 10. Apply non-zero-only snapshot to Redis (simulate outboundAccountPosition omitting zero)
    print("  Applying non-zero-only snapshot to Redis...")
    apply_snapshot_to_redis(account_store, snapshot2, non_zero_only=True)
    print("  Applied (ETH omitted if zero).")

    # 11. Sync to Postgres again
    if DATABASE_URL:
        n = sync_accounts_to_postgres(redis, account_store, DATABASE_URL, ttl_after_sync_seconds=None)
        print(f"  Synced {n} account(s) to Postgres.")
    else:
        print("  DATABASE_URL not set, skipping Postgres sync.")

    # 12. Verify Redis: ETH gone or zero
    balances2 = account_store.get_balances("binance", account_id)
    assets2 = [b.get("asset") for b in balances2 if b.get("asset")]
    if "ETH" in assets2:
        eth_bal = next(b for b in balances2 if b.get("asset") == "ETH")
        av = float(eth_bal.get("available") or 0)
        lo = float(eth_bal.get("locked") or 0)
        if av + lo > 1e-9:
            print(f"  [FAIL] Redis still has non-zero ETH: {eth_bal}", file=sys.stderr)
            return 1
        print("  Redis: ETH present but zero.")
    else:
        print("  Redis: ETH not in balances (zero position removed).")

    # 13. Verify Postgres: no ETH row for this account (delete logic)
    if DATABASE_URL:
        pg_assets2 = get_pg_balance_assets(DATABASE_URL, "binance", account_id)
        if "ETH" in pg_assets2:
            print(f"  [FAIL] Postgres still has ETH balance row (delete logic should remove it): assets={pg_assets2}", file=sys.stderr)
            return 1
        print("  Postgres: ETH balance row removed (UPSERT/delete logic OK).")

    print()
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
