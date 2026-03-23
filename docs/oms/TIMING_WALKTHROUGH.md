# E2E Test Timing Walkthrough

How the "wait for fill" step timing is sampled and calculated.

## Timeline Example (MARKET Order)

Let's trace through a concrete example with timestamps:

```
Time    | Event                                    | What Script Sees
--------|------------------------------------------|------------------
T=0.0s  | Script: inject_timestamp = time.monotonic()
        | Script: XADD order to risk_approved      |
        |                                          |
T=0.0s  | Script: step_start = time.monotonic()   | Start Step 2 timer
        | Script: sleep(0.5)                       | Wait 500ms
        |                                          |
T=0.1s  | OMS (service): Reads risk_approved      | [INVISIBLE - OMS runs separately]
        | OMS: Stages order in Redis               |
        | OMS: Places order with Binance           |
        | Binance: Executes MARKET order           |
        | OMS: Receives fill via websocket         |
        | OMS: Updates Redis store (status=filled) |
        | OMS: Publishes to oms_fills stream       |
        |                                          |
T=0.5s  | Script: Wake up from sleep              | Poll #1
        | Script: store.get_order(order_id)        |
        |   → Returns order (status="filled")      | ✓ Order exists!
        | Script: oms_processing_start_monotonic    |
        |   = time.monotonic() = 0.5s              | [SET] First detection
        |                                          |
        | Script: _read_latest_fills()             |
        |   → Finds fill entry                     | ✓ Fill found!
        | Script: detection_time = time.monotonic()|
        |   = 0.5s                                 | [SET] Fill detected
        | Script: oms_processing_end_monotonic =   |
        |   detection_time - 0.25 = 0.25s          | [ESTIMATE] Fill happened ~0.25s ago
        | Script: break (exit loop)                |
        |                                          |
T=0.5s  | Script: Calculate timings              |
```

## Timing Calculations

### 1. Total Wait Time
```python
step_times["2_wait_fill"] = time.monotonic() - step_start
# = 0.5s - 0.0s = 0.5s
```
**What it measures:** Total time from starting Step 2 to detecting the fill.

### 2. Inject to OMS Start (Detected)
```python
step_times["2b_inject_to_oms_start"] = oms_processing_start_monotonic - inject_timestamp
# = 0.5s - 0.0s = 0.5s
```
**What it measures:** Time from injection to when we **first detect** the order in Redis store.

⚠️ Limitation:** OMS actually started processing earlier (could be 0-500ms before we detected it, due to polling).

### 3. OMS Processing Time (Estimated)
```python
step_times["2a_oms_processing_time"] = oms_processing_end_monotonic - oms_processing_start_monotonic
# = 0.25s - 0.5s = -0.25s ❌ (NEGATIVE!)
```
**What it measures:** Estimated time between OMS starting and fill completing.

⚠️ Problem:** This can be negative because:
- We detect the fill at T=0.5s
- We estimate fill happened at T=0.25s (detection - 0.25s)
- We detect order staging at T=0.5s
- So: 0.25s - 0.5s = -0.25s

**Why negative?** The order was already filled by the time we checked! OMS processed it faster than our 0.5s polling interval.

### 4. Polling Overhead
```python
step_times["2c_polling_overhead"] = detection_time - oms_processing_end_monotonic
# = 0.5s - 0.25s = 0.25s
```
**What it measures:** Time between fill completion (estimated) and detection.

What it represents:** Half the polling interval (0.5s / 2 = 0.25s), because fill could have happened anywhere in the last 0.5s.

## The Problem: Polling-Based Detection

### What Actually Happens (Reality)
```
T=0.0s  → Order injected
T=0.1s  → OMS reads, stages, places, fills (all happens in ~100ms)
T=0.1s  → Fill published to oms_fills
T=0.5s  → Script wakes up and detects fill
```

### What Script Measures (Perception)
```
T=0.0s  → Order injected
T=0.5s  → Script detects order exists (OMS started "at" 0.5s)
T=0.5s  → Script detects fill (fill happened "at" 0.25s estimated)
```

**Gap:** We miss the actual processing window (0.0s - 0.1s) because we're sleeping!

## Why This Happens

1. **Polling interval:** `time.sleep(0.5)` means we check every 500ms
2. **Fast processing:** OMS completes in ~100-200ms (faster than polling)
3. **Race condition:** Fill can be detected before order staging is detected
4. **Service isolation:** OMS runs separately; we can't instrument it directly

## What We Can Actually Measure Accurately

✅ **Total end-to-end time** (injection → detection)
- This is accurate: `step_times["2_wait_fill"]`
- Useful for: Performance monitoring, regression detection

✅ **Polling overhead**
- This is accurate: ~250ms (half the polling interval)
- Represents: Time we "wasted" sleeping while fill was already done

❌ **Actual OMS processing time**
- This is **not accurate** with current approach
- Can be negative or wildly off
- Would need: Instrumentation inside OMS service

## Better Approach (If You Want Accurate OMS Timing)

### Option 1: Reduce Polling Interval
```python
time.sleep(0.05)  # Check every 50ms instead of 500ms
```
- More accurate detection
- But still has ±25ms uncertainty
- More CPU usage

### Option 2: Use Redis Stream Entry IDs
Redis stream entry IDs contain timestamps:
```python
entry_id = "1771257802113-0"  # Format: timestamp_ms-sequence
timestamp_ms = int(entry_id.split('-')[0])
fill_timestamp = timestamp_ms / 1000.0
```
- More accurate fill timestamp
- But still need to detect when OMS started

### Option 3: Instrument OMS Service
Add logging/metrics inside OMS:
```python
# In OMS service
logger.info("order_staged", order_id=order_id, timestamp=time.time())
logger.info("order_placed", order_id=order_id, timestamp=time.time())
logger.info("order_filled", order_id=order_id, timestamp=time.time())
```
- Most accurate
- Requires code changes in OMS

## Current Implementation Summary

The script uses **polling-based detection** with these assumptions:

1. **Order staging detected:** When `store.get_order()` first returns non-None
   - Actual staging happened 0-500ms earlier (polling uncertainty)

2. **Fill completion estimated:** `detection_time - 0.25s`
   - Assumes fill happened at midpoint of last polling interval
   - Actual fill happened 0-500ms earlier (polling uncertainty)

3. **OMS processing time:** `fill_estimated - staging_detected`
   - Can be negative if fill detected before staging
   - Not accurate for fast processing (<500ms)

4. **Polling overhead:** `detection_time - fill_estimated`
   - Always ~250ms (half polling interval)
   - Accurate representation of detection delay

## Conclusion

The "wait for fill" timing shows:
- ✅ **Total latency** (accurate)
- ✅ **Polling overhead** (accurate)
- ⚠️ **OMS processing time** (approximate, can be negative)

The actual OMS processing is "invisible" because it happens faster than our polling interval and in a separate service. The breakdown is useful for understanding where time is spent, but with the caveat that OMS processing time is an estimate with ±250ms uncertainty.
