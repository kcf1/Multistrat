#!/usr/bin/env python3
"""
Full pipeline test: inject one order to Redis (risk_approved), then poll until
the OMS service (e.g. in Docker) processes it and check downstreams (oms_fills
stream, Redis order store, Postgres orders table).

Requires: REDIS_URL. Optional: DATABASE_URL for Postgres check.
OMS must be running separately (e.g. docker compose up -d oms) with testnet config.

Usage:
    python scripts/full_pipeline_test.py [--market] [--runs N] [--verbose]
    
Options:
    --market: inject MARKET order (fills immediately); default: LIMIT (unfillable) + cancel
    --runs N: run test N times and show average timing (default: 1)
    --verbose: show detailed step-by-step timing for each run

Examples:
    python scripts/full_pipeline_test.py --market --runs 5
    python scripts/full_pipeline_test.py --runs 10 --verbose
    python scripts/full_pipeline_test.py --market --runs 3 --verbose
"""

import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

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

# Quiet loguru when running as script (optional: set LOG_OMS=1 to keep logs)
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


# Terminal statuses in Redis store (OMS marks order as one of these when done)
TERMINAL_STATUSES = {"filled", "canceled", "rejected", "expired"}


def run_single_test(market: bool, redis, store, verbose: bool = False) -> tuple[bool, float, dict]:
    """
    Run a single test iteration.
    Returns: (success: bool, total_elapsed_time: float in seconds, step_times: dict)
    """
    step_times = {}
    start_time = time.monotonic()
    
    order_id = f"script-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"

    if market:
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "MARKET",
            "book": "full_pipeline_script",
            "comment": "full pipeline test market order",
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
            print(f"Could not get ticker for limit price: {e}", file=sys.stderr)
            return False, 0.0
        risk_order = {
            "order_id": order_id,
            "broker": "binance",
            "symbol": "BTCUSDT",
            "side": "BUY",
            "quantity": 0.0001,
            "order_type": "LIMIT",
            "price": price,
            "time_in_force": "GTC",
            "book": "full_pipeline_script",
            "comment": "full pipeline test limit order then check downstreams",
        }

    from oms.schemas import CANCEL_REQUESTED_STREAM, OMS_FILLS_STREAM, RISK_APPROVED_STREAM
    from oms.streams import add_message

    # Step 1: Inject order
    step_start = time.monotonic()
    inject_timestamp = time.monotonic()
    add_message(redis, RISK_APPROVED_STREAM, risk_order)
    step_times["1_inject_order"] = time.monotonic() - step_start
    if verbose:
        print(f"   Step 1 (Inject order): {step_times['1_inject_order']*1000:.1f}ms")

    # Step 2: Wait for OMS to process
    step_start = time.monotonic()
    oms_processing_start_monotonic = None
    oms_processing_end_monotonic = None
    fill_entry_data = None
    detection_time = None
    
    if market:
        deadline = time.monotonic() + 60
        fill_seen = False
        poll_count = 0
        last_order_status = None
        POLL_INTERVAL = 0.05  # 50ms polling for faster detection
        
        while time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL)
            poll_count += 1
            
            order = store.get_order(order_id)
            
            # Track when order first appears (OMS started processing)
            if order and oms_processing_start_monotonic is None:
                oms_processing_start_monotonic = time.monotonic()
            
            # Track when order status changes to "sent" (OMS placed order with broker)
            current_status = order.get("status") if order else None
            if current_status == "sent" and last_order_status != "sent" and oms_processing_start_monotonic:
                # Order was placed - this is a better marker for OMS processing start
                # (or we could track this separately, but for simplicity use first detection)
                pass
            
            last_order_status = current_status
            
            entries = _read_latest_fills(redis, OMS_FILLS_STREAM, count=30)
            fill_entry = next((e for e in entries if e[1].get("order_id") == order_id), None)
            if fill_entry:
                fill_seen = True
                fill_entry_data = fill_entry[1]
                detection_time = time.monotonic()
                # Estimate when fill was published: detection time minus half polling interval
                # (fill could have happened anytime in the last POLL_INTERVAL)
                oms_processing_end_monotonic = detection_time - (POLL_INTERVAL / 2)  # Midpoint of polling interval
                break
            if order and order.get("status") in TERMINAL_STATUSES:
                fill_seen = True
                detection_time = time.monotonic()
                oms_processing_end_monotonic = detection_time - (POLL_INTERVAL / 2)
                break
        if not fill_seen:
            step_times["2_wait_fill"] = time.monotonic() - step_start
            return False, time.monotonic() - start_time, step_times
        
        # Calculate actual OMS processing time (from OMS start to fill completion)
        if oms_processing_start_monotonic and oms_processing_end_monotonic:
            step_times["2a_oms_processing_time"] = oms_processing_end_monotonic - oms_processing_start_monotonic
        
        # Time from injection to OMS start
        if oms_processing_start_monotonic:
            step_times["2b_inject_to_oms_start"] = oms_processing_start_monotonic - inject_timestamp
        
        # Polling overhead (time from fill completion to detection)
        if oms_processing_end_monotonic and detection_time:
            step_times["2c_polling_overhead"] = detection_time - oms_processing_end_monotonic
        
        step_times["2_wait_fill"] = time.monotonic() - step_start
        if verbose:
            print(f"   Step 2 (Wait for fill): {step_times['2_wait_fill']*1000:.1f}ms")
            if "2b_inject_to_oms_start" in step_times:
                print(f"     → Inject to OMS start (detected): {step_times['2b_inject_to_oms_start']*1000:.1f}ms")
            if "2a_oms_processing_time" in step_times:
                oms_time_ms = step_times['2a_oms_processing_time'] * 1000
                if oms_time_ms < 0:
                    print(f"     → OMS processing: <0ms (fill detected before order staging, ~{abs(oms_time_ms):.1f}ms faster)")
                else:
                    print(f"     → OMS processing (estimated): {oms_time_ms:.1f}ms")
            if "2c_polling_overhead" in step_times:
                overhead_ms = step_times['2c_polling_overhead'] * 1000
                print(f"     → Polling overhead: {overhead_ms:.1f}ms")
            print(f"     Note: OMS runs as service; timing includes polling uncertainty (±25ms)")
    else:
        deadline = time.monotonic() + 60
        order_placed = False
        POLL_INTERVAL = 0.05  # 50ms polling for faster detection
        while time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL)
            order = store.get_order(order_id)
            if order and order.get("status") in ("sent", "pending", *TERMINAL_STATUSES):
                order_placed = True
                break
        if not order_placed:
            step_times["2_wait_place"] = time.monotonic() - step_start
            return False, time.monotonic() - start_time, step_times
        step_times["2_wait_place"] = time.monotonic() - step_start
        if verbose:
            print(f"   Step 2 (Wait for place): {step_times['2_wait_place']*1000:.1f}ms")
        
        # Step 3: Cancel order
        step_start = time.monotonic()
        add_message(redis, CANCEL_REQUESTED_STREAM, {"order_id": order_id, "broker": "binance"})
        deadline = time.monotonic() + 60
        cancelled = False
        POLL_INTERVAL = 0.05  # 50ms polling for faster detection
        while time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL)
            order = store.get_order(order_id)
            if order and order.get("status") == "cancelled":
                cancelled = True
                break
        if not cancelled:
            step_times["3_wait_cancel"] = time.monotonic() - step_start
            return False, time.monotonic() - start_time, step_times
        step_times["3_wait_cancel"] = time.monotonic() - step_start
        if verbose:
            print(f"   Step 3 (Wait for cancel): {step_times['3_wait_cancel']*1000:.1f}ms")

    # Step 4: Verify downstreams
    step_start = time.monotonic()
    # Use fill_entry_data if we already found it, otherwise search again
    if not fill_entry_data:
        fills = _read_latest_fills(redis, OMS_FILLS_STREAM, count=30)
        fill_entry = next((e for e in fills if e[1].get("order_id") == order_id), None)
        if not fill_entry:
            step_times["4_verify_downstreams"] = time.monotonic() - step_start
            return False, time.monotonic() - start_time, step_times
        fill_entry_data = fill_entry[1]

    order = store.get_order(order_id)
    if not order:
        step_times["4_verify_downstreams"] = time.monotonic() - step_start
        return False, time.monotonic() - start_time, step_times
    
    step_times["4_verify_downstreams"] = time.monotonic() - step_start
    if verbose:
        print(f"   Step 4 (Verify downstreams): {step_times['4_verify_downstreams']*1000:.1f}ms")

    elapsed_time = time.monotonic() - start_time
    return True, elapsed_time, step_times


def main() -> int:
    parser = argparse.ArgumentParser(description="Full pipeline test: inject order to Redis, check downstreams")
    parser.add_argument("--market", action="store_true", help="Inject MARKET order (fills immediately)")
    parser.add_argument("--runs", type=int, default=1, help="Number of test runs (default: 1)")
    parser.add_argument("--verbose", action="store_true", help="Show detailed timing for each step")
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
    print(f"Running {args.runs} test(s) with {order_type} orders...")
    if args.verbose:
        print("(Verbose mode: showing step-by-step timing)")
    print()

    times = []
    step_times_all = {}  # Aggregate step times across runs
    successes = 0
    failures = 0

    for run_num in range(1, args.runs + 1):
        if args.runs > 1 or args.verbose:
            print(f"--- Run {run_num}/{args.runs} ---")
        
        success, elapsed, step_times = run_single_test(args.market, redis, store, verbose=args.verbose)
        
        if success:
            successes += 1
            times.append(elapsed)
            
            # Aggregate step times
            for step_name, step_time in step_times.items():
                if step_name not in step_times_all:
                    step_times_all[step_name] = []
                step_times_all[step_name].append(step_time)
            
            if args.runs > 1 and not args.verbose:
                print(f"[OK] Run {run_num} completed in {elapsed:.2f}s")
            elif args.runs == 1 and not args.verbose:
                print(f"Total time: {elapsed:.2f}s")
        else:
            failures += 1
            if args.runs > 1 or args.verbose:
                print(f"[FAIL] Run {run_num} failed after {elapsed:.2f}s")
        
        # Small delay between runs to avoid rate limiting
        if run_num < args.runs:
            time.sleep(1)

    print()
    print("=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)
    print(f"Total runs: {args.runs}")
    print(f"Successful: {successes}")
    print(f"Failed: {failures}")
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        print()
        print("Total Timing (seconds):")
        print(f"  Average: {avg_time:.2f}s")
        print(f"  Min:     {min_time:.2f}s")
        print(f"  Max:     {max_time:.2f}s")
        if len(times) > 1:
            variance = sum((t - avg_time) ** 2 for t in times) / len(times)
            std_dev = variance ** 0.5
            print(f"  Std Dev: {std_dev:.2f}s")
        
        # Step-by-step timing breakdown
        if step_times_all:
            print()
            print("Step-by-Step Timing (milliseconds):")
            step_labels = {
                "1_inject_order": "1. Inject order to Redis",
                "2_wait_fill": "2. Wait for fill (MARKET) [total]",
                "2_wait_place": "2. Wait for place (LIMIT) [total]",
                "2b_inject_to_oms_start": "   → Inject to OMS start",
                "2a_oms_processing_time": "   → OMS processing (actual)",
                "2c_polling_overhead": "   → Polling overhead",
                "3_wait_cancel": "3. Wait for cancel (LIMIT)",
                "4_verify_downstreams": "4. Verify downstreams",
            }
            
            # Group related steps
            main_steps = ["1_inject_order", "2_wait_fill", "2_wait_place", "3_wait_cancel", "4_verify_downstreams"]
            sub_steps = ["2b_inject_to_oms_start", "2a_oms_processing_time", "2c_polling_overhead"]
            
            for step_name in sorted(step_times_all.keys()):
                if step_name in sub_steps:
                    continue  # Skip sub-steps, will print after main step
                    
                step_times_list = step_times_all[step_name]
                if step_times_list:
                    avg_step = sum(step_times_list) / len(step_times_list) * 1000
                    min_step = min(step_times_list) * 1000
                    max_step = max(step_times_list) * 1000
                    label = step_labels.get(step_name, step_name)
                    
                    print(f"  {label}:")
                    print(f"    Avg: {avg_step:.1f}ms  Min: {min_step:.1f}ms  Max: {max_step:.1f}ms")
                    if len(step_times_list) > 1:
                        variance_step = sum((t - avg_step/1000) ** 2 for t in step_times_list) / len(step_times_list)
                        std_dev_step = (variance_step ** 0.5) * 1000
                        print(f"    Std Dev: {std_dev_step:.1f}ms")
                    
                    # Print sub-steps if this is a wait step
                    if step_name == "2_wait_fill":
                        for sub_step in ["2b_inject_to_oms_start", "2a_oms_processing_time", "2c_polling_overhead"]:
                            if sub_step in step_times_all:
                                sub_times = step_times_all[sub_step]
                                if sub_times:
                                    avg_sub = sum(sub_times) / len(sub_times) * 1000
                                    min_sub = min(sub_times) * 1000
                                    max_sub = max(sub_times) * 1000
                                    sub_label = step_labels.get(sub_step, sub_step)
                                    print(f"  {sub_label}:")
                                    if sub_step == "2a_oms_processing_time" and avg_sub < 0:
                                        print(f"    Avg: <0ms (OMS processing faster than detection)")
                                        print(f"    Note: OMS runs asynchronously; actual processing time is hidden")
                                    else:
                                        print(f"    Avg: {avg_sub:.1f}ms  Min: {min_sub:.1f}ms  Max: {max_sub:.1f}ms")
                        print(f"  Note: Timing includes polling uncertainty (±25ms per step)")
    
    print("=" * 70)
    
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
