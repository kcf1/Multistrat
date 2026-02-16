#!/usr/bin/env python3
"""
Bulk order injection test: inject multiple orders at once, then measure timing
for each order individually to test bulk processing performance.

Requires: REDIS_URL. Optional: DATABASE_URL for Postgres check.
OMS must be running separately (e.g. docker compose up -d oms) with testnet config.

Usage:
    python scripts/bulk_order_test.py [--market] [--count N] [--runs M]
    
Options:
    --market: inject MARKET orders (fill immediately); default: LIMIT (unfillable) + cancel
    --count N: number of orders to inject per run (default: 10)
    --runs M: number of test runs to average (default: 3)
    --verbose: show detailed timing for each order

Examples:
    python scripts/bulk_order_test.py --market --count 20 --runs 3
    python scripts/bulk_order_test.py --count 10 --runs 5 --verbose
"""

import argparse
import os
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path
from statistics import mean, stdev

# Repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env", override=True)
except ImportError:
    pass

# Quiet loguru when running as script
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


def _read_latest_fills(redis, stream: str, count: int = 50):
    """Return last `count` entries from stream (newest first via XREVRANGE)."""
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


TERMINAL_STATUSES = {"filled", "canceled", "rejected", "expired"}
POLL_INTERVAL = 0.05  # 50ms polling for faster detection


def inject_and_track_order(order_id: str, market: bool, redis, store, verbose: bool = False) -> dict:
    """
    Inject a single order and track its timing until completion.
    Returns dict with timing metrics for this order.
    """
    metrics = {
        "order_id": order_id,
        "inject_time": None,
        "detected_time": None,
        "fill_time": None,
        "total_time": None,
        "success": False,
    }
    
    inject_start = time.monotonic()
    
    if market:
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "MARKET",
            "book": "bulk_order_test",
            "comment": f"bulk test market order {order_id}",
        }
    else:
        try:
            import requests
            ticker = requests.get(
                f"{_env('BINANCE_BASE_URL') or 'https://testnet.binance.vision'}/api/v3/ticker/price",
                params={"symbol": "BTCUSDT"},
                timeout=10,
            )
            ticker.raise_for_status()
            last_price = float(ticker.json()["price"])
            price = round(last_price * 0.95, 2)
        except Exception as e:
            if verbose:
                print(f"  Order {order_id}: Could not get ticker: {e}", file=sys.stderr)
            return metrics
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "LIMIT",
            "price": price,
            "time_in_force": "GTC",
            "book": "bulk_order_test",
            "comment": f"bulk test limit order {order_id}",
        }
    
    from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
    from oms.streams import add_message
    
    # Inject order
    add_message(redis, RISK_APPROVED_STREAM, risk_order)
    inject_time = time.monotonic()
    metrics["inject_time"] = inject_time - inject_start
    
    # Wait for order to be detected and processed
    deadline = time.monotonic() + 60
    detected = False
    fill_seen = False
    
    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL)
        
        # Check if order appears in Redis store (OMS started processing)
        order = store.get_order(order_id)
        if order and not detected:
            detected = True
            metrics["detected_time"] = time.monotonic() - inject_time
        
        if market:
            # Check for fill
            entries = _read_latest_fills(redis, OMS_FILLS_STREAM, count=100)
            fill_entry = next((e for e in entries if e[1].get("order_id") == order_id), None)
            if fill_entry:
                fill_seen = True
                metrics["fill_time"] = time.monotonic() - inject_time
                metrics["success"] = True
                break
            if order and order.get("status") in TERMINAL_STATUSES:
                fill_seen = True
                metrics["fill_time"] = time.monotonic() - inject_time
                metrics["success"] = True
                break
        else:
            # For LIMIT orders, check if placed
            if order and order.get("status") in ("sent", "pending", *TERMINAL_STATUSES):
                metrics["fill_time"] = time.monotonic() - inject_time
                metrics["success"] = True
                break
    
    if metrics["success"]:
        metrics["total_time"] = metrics["fill_time"]
    else:
        metrics["total_time"] = time.monotonic() - inject_time
    
    return metrics


def run_bulk_test(market: bool, count: int, redis, store, verbose: bool = False) -> list:
    """
    Inject multiple orders at once, then track timing for each.
    Returns list of metrics dicts, one per order.
    """
    # Generate order IDs
    order_ids = [f"bulk-{int(time.time() * 1000)}-{i}-{uuid.uuid4().hex[:8]}" for i in range(count)]
    
    # Inject all orders as quickly as possible
    inject_start = time.monotonic()
    if verbose:
        print(f"  Injecting {count} orders...")
    
    for order_id in order_ids:
        if market:
            risk_order = {
                "order_id": order_id,
                "broker": "binance",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.0001,
                "order_type": "MARKET",
                "book": "bulk_order_test",
                "comment": f"bulk test market order {order_id}",
            }
        else:
            try:
                import requests
                ticker = requests.get(
                    f"{_env('BINANCE_BASE_URL') or 'https://testnet.binance.vision'}/api/v3/ticker/price",
                    params={"symbol": "BTCUSDT"},
                    timeout=10,
                )
                ticker.raise_for_status()
                last_price = float(ticker.json()["price"])
                price = round(last_price * 0.95, 2)
            except Exception as e:
                if verbose:
                    print(f"  Could not get ticker: {e}", file=sys.stderr)
                return []
            risk_order = {
                "order_id": order_id,
                "broker": "binance",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.0001,
                "order_type": "LIMIT",
                "price": price,
                "time_in_force": "GTC",
                "book": "bulk_order_test",
                "comment": f"bulk test limit order {order_id}",
            }
        
        from oms.schemas import RISK_APPROVED_STREAM
        from oms.streams import add_message
        add_message(redis, RISK_APPROVED_STREAM, risk_order)
    
    inject_end = time.monotonic()
    inject_duration = inject_end - inject_start
    
    if verbose:
        print(f"  All {count} orders injected in {inject_duration*1000:.1f}ms")
        print(f"  Tracking completion for each order...")
    
    # Track each order's completion
    all_metrics = []
    deadline = time.monotonic() + 120  # Extended deadline for bulk processing
    
    from oms.schemas import OMS_FILLS_STREAM
    
    while time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL)
        
        # Check all orders
        for order_id in order_ids:
            if any(m["order_id"] == order_id and m["success"] for m in all_metrics):
                continue  # Already completed
            
            metrics = {
                "order_id": order_id,
                "inject_time": None,
                "detected_time": None,
                "fill_time": None,
                "total_time": None,
                "success": False,
            }
            
            # Check if order appears in Redis store
            order = store.get_order(order_id)
            if order:
                if metrics["detected_time"] is None:
                    # Estimate detection time (when order first appeared)
                    # Since we poll, estimate it appeared halfway through polling interval
                    metrics["detected_time"] = time.monotonic() - inject_end - (POLL_INTERVAL / 2)
            
            if market:
                # Check for fill
                entries = _read_latest_fills(redis, OMS_FILLS_STREAM, count=200)
                fill_entry = next((e for e in entries if e[1].get("order_id") == order_id), None)
                if fill_entry:
                    metrics["fill_time"] = time.monotonic() - inject_end
                    metrics["success"] = True
                    metrics["total_time"] = metrics["fill_time"]
                    all_metrics.append(metrics)
                    continue
                if order and order.get("status") in TERMINAL_STATUSES:
                    metrics["fill_time"] = time.monotonic() - inject_end
                    metrics["success"] = True
                    metrics["total_time"] = metrics["fill_time"]
                    all_metrics.append(metrics)
                    continue
            else:
                # For LIMIT orders, check if placed
                if order and order.get("status") in ("sent", "pending", *TERMINAL_STATUSES):
                    metrics["fill_time"] = time.monotonic() - inject_end
                    metrics["success"] = True
                    metrics["total_time"] = metrics["fill_time"]
                    all_metrics.append(metrics)
                    continue
        
        # Check if all orders completed
        if len(all_metrics) >= count:
            break
    
    # Fill in inject_time for all (same for all orders since injected together)
    for m in all_metrics:
        m["inject_time"] = inject_duration / count  # Average per order
    
    # Sort by order_id for consistent output
    all_metrics.sort(key=lambda x: x["order_id"])
    
    return all_metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Bulk order injection test: inject multiple orders and measure timing")
    parser.add_argument("--market", action="store_true", help="Inject MARKET orders (fill immediately)")
    parser.add_argument("--count", type=int, default=10, help="Number of orders to inject per run (default: 10)")
    parser.add_argument("--runs", type=int, default=3, help="Number of test runs to average (default: 3)")
    parser.add_argument("--verbose", action="store_true", help="Show detailed timing for each order")
    args = parser.parse_args()
    
    REDIS_URL = _env("REDIS_URL")
    DATABASE_URL = _env("DATABASE_URL")
    if not REDIS_URL:
        print("REDIS_URL not set", file=sys.stderr)
        return 1
    
    from redis import Redis
    from oms.storage.redis_order_store import RedisOrderStore
    
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    store = RedisOrderStore(redis)
    
    order_type = "MARKET" if args.market else "LIMIT"
    print(f"Running {args.runs} test run(s) with {args.count} {order_type} orders per run...")
    if args.verbose:
        print("(Verbose mode: showing timing for each order)")
    print()
    
    all_runs_metrics = []
    
    for run_num in range(1, args.runs + 1):
        if args.verbose:
            print(f"--- Run {run_num}/{args.runs} ---")
        
        metrics = run_bulk_test(args.market, args.count, redis, store, verbose=args.verbose)
        
        if not metrics:
            print(f"Run {run_num}: Failed to inject orders", file=sys.stderr)
            continue
        
        success_count = sum(1 for m in metrics if m["success"])
        if success_count < args.count:
            print(f"Run {run_num}: Only {success_count}/{args.count} orders completed", file=sys.stderr)
        
        all_runs_metrics.append(metrics)
        
        if args.verbose:
            print(f"  Completed: {success_count}/{args.count} orders")
            for m in metrics[:5]:  # Show first 5
                if m["success"]:
                    print(f"    {m['order_id'][-12:]}: {m['total_time']*1000:.1f}ms total")
            if len(metrics) > 5:
                print(f"    ... and {len(metrics) - 5} more")
            print()
    
    if not all_runs_metrics:
        print("No successful runs", file=sys.stderr)
        return 1
    
    # Aggregate metrics across all runs
    print("=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)
    print(f"Total runs: {args.runs}")
    print(f"Orders per run: {args.count}")
    print(f"Total orders: {args.runs * args.count}")
    
    # Flatten all metrics
    all_metrics_flat = [m for run_metrics in all_runs_metrics for m in run_metrics if m["success"]]
    success_count = len(all_metrics_flat)
    print(f"Successful orders: {success_count}")
    print()
    
    if not all_metrics_flat:
        print("No successful orders to analyze", file=sys.stderr)
        return 1
    
    # Calculate statistics
    total_times = [m["total_time"] for m in all_metrics_flat]
    detected_times = [m["detected_time"] for m in all_metrics_flat if m["detected_time"] is not None]
    fill_times = [m["fill_time"] for m in all_metrics_flat if m["fill_time"] is not None]
    
    print("Per-Order Timing (milliseconds):")
    print(f"  Total time (inject to completion):")
    print(f"    Avg: {mean(total_times)*1000:.1f}ms  Min: {min(total_times)*1000:.1f}ms  Max: {max(total_times)*1000:.1f}ms")
    if len(total_times) > 1:
        print(f"    Std Dev: {stdev(total_times)*1000:.1f}ms")
    
    if detected_times:
        print(f"  Detection time (inject to OMS start):")
        print(f"    Avg: {mean(detected_times)*1000:.1f}ms  Min: {min(detected_times)*1000:.1f}ms  Max: {max(detected_times)*1000:.1f}ms")
        if len(detected_times) > 1:
            print(f"    Std Dev: {stdev(detected_times)*1000:.1f}ms")
    
    if fill_times:
        print(f"  Processing time (inject to fill/place):")
        print(f"    Avg: {mean(fill_times)*1000:.1f}ms  Min: {min(fill_times)*1000:.1f}ms  Max: {max(fill_times)*1000:.1f}ms")
        if len(fill_times) > 1:
            print(f"    Std Dev: {stdev(fill_times)*1000:.1f}ms")
    
    # Calculate throughput
    if args.runs > 1:
        avg_total_time = mean(total_times)
        throughput = args.count / avg_total_time if avg_total_time > 0 else 0
        print()
        print(f"Throughput: {throughput:.2f} orders/second (average)")
    
    print("=" * 70)
    
    return 0 if success_count == args.runs * args.count else 1


if __name__ == "__main__":
    sys.exit(main())
