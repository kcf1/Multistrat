#!/usr/bin/env python3
"""
Inject multiple test orders: 3 symbols, 3 scenarios (long only, short only, long < short).

Connects to Redis (REDIS_URL), XADDs orders to risk_approved. Use after OMS is running.

Scenarios:
  long_only     — 3 BUY orders (one per symbol)
  short_only    — 3 SELL orders (one per symbol)
  long_lt_short — 1 BUY (small) + 2 SELLs (larger qty) so net short across symbols

Usage:
  python scripts/inject_bulk_orders.py [--symbols A,B,C] [--qty 0.0001] [--dry-run]
"""

import argparse
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
except ImportError:
    pass


def _env(key: str, default: str = "") -> str:
    v = (os.getenv(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
BASE_QTY = 0.0001


def _order_id(prefix: str) -> str:
    return f"{prefix}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"


def build_scenarios(symbols: List[str], qty: float) -> List[Tuple[str, List[dict]]]:
    """Return list of (scenario_name, list of risk_approved payloads)."""
    a, b, c = symbols[0], symbols[1], symbols[2]

    scenarios = []

    # 1. Long only: 3 BUYs (one per symbol)
    scenarios.append((
        "long_only",
        [
            {"order_id": _order_id("long"), "broker": "binance", "symbol": a, "side": "BUY", "quantity": qty, "order_type": "MARKET", "book": "bulk_long_only", "comment": "bulk long only"},
            {"order_id": _order_id("long"), "broker": "binance", "symbol": b, "side": "BUY", "quantity": qty, "order_type": "MARKET", "book": "bulk_long_only", "comment": "bulk long only"},
            {"order_id": _order_id("long"), "broker": "binance", "symbol": c, "side": "BUY", "quantity": qty, "order_type": "MARKET", "book": "bulk_long_only", "comment": "bulk long only"},
        ],
    ))

    # 2. Short only: 3 SELLs (one per symbol)
    scenarios.append((
        "short_only",
        [
            {"order_id": _order_id("short"), "broker": "binance", "symbol": a, "side": "SELL", "quantity": qty, "order_type": "MARKET", "book": "bulk_short_only", "comment": "bulk short only"},
            {"order_id": _order_id("short"), "broker": "binance", "symbol": b, "side": "SELL", "quantity": qty, "order_type": "MARKET", "book": "bulk_short_only", "comment": "bulk short only"},
            {"order_id": _order_id("short"), "broker": "binance", "symbol": c, "side": "SELL", "quantity": qty, "order_type": "MARKET", "book": "bulk_short_only", "comment": "bulk short only"},
        ],
    ))

    # 3. Long < short: 1 BUY (small) + 2 SELLs (larger) so net short
    qty_small = qty
    qty_large = qty * 2
    scenarios.append((
        "long_lt_short",
        [
            {"order_id": _order_id("lt"), "broker": "binance", "symbol": a, "side": "BUY", "quantity": qty_small, "order_type": "MARKET", "book": "bulk_long_lt_short", "comment": "bulk long<short"},
            {"order_id": _order_id("lt"), "broker": "binance", "symbol": b, "side": "SELL", "quantity": qty_large, "order_type": "MARKET", "book": "bulk_long_lt_short", "comment": "bulk long<short"},
            {"order_id": _order_id("lt"), "broker": "binance", "symbol": c, "side": "SELL", "quantity": qty_large, "order_type": "MARKET", "book": "bulk_long_lt_short", "comment": "bulk long<short"},
        ],
    ))

    return scenarios


def inject_all(redis_url: str, scenarios: list[tuple[str, list[dict]]], delay_between_orders: float = 0.5) -> list[tuple[str, str, str]]:
    """Inject all orders. Returns list of (scenario, order_id, entry_id)."""
    from redis import Redis
    from oms.schemas import RISK_APPROVED_STREAM
    from oms.streams import add_message

    redis = Redis.from_url(redis_url, decode_responses=True)
    result = []
    for scenario_name, orders in scenarios:
        for payload in orders:
            entry_id = add_message(redis, RISK_APPROVED_STREAM, payload)
            result.append((scenario_name, payload["order_id"], entry_id))
            if delay_between_orders > 0:
                time.sleep(delay_between_orders)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Inject bulk orders: 3 symbols, long_only / short_only / long_lt_short")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS), help=f"Comma-separated symbols (default: {','.join(DEFAULT_SYMBOLS)})")
    parser.add_argument("--qty", type=float, default=BASE_QTY, help=f"Base quantity (default: {BASE_QTY})")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds between each order (default: 0.5)")
    parser.add_argument("--dry-run", action="store_true", help="Print orders only, do not inject")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    if len(symbols) < 3:
        print("Need at least 3 symbols (e.g. BTCUSDT,ETHUSDT,BNBUSDT)", file=sys.stderr)
        return 1

    scenarios = build_scenarios(symbols[:3], args.qty)

    if args.dry_run:
        print("Dry run — orders to inject:")
        for name, orders in scenarios:
            print(f"\n  [{name}]")
            for o in orders:
                print(f"    {o['side']} {o['quantity']} {o['symbol']}  order_id={o['order_id']} book={o['book']}")
        return 0

    redis_url = _env("REDIS_URL")
    if not redis_url:
        print("REDIS_URL not set", file=sys.stderr)
        return 1

    results = inject_all(redis_url, scenarios, delay_between_orders=args.delay)
    print(f"Injected {len(results)} orders:")
    for scenario, order_id, entry_id in results:
        print(f"  {scenario}  order_id={order_id}  entry_id={entry_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
