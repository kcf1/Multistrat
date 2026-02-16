# How OMS Works with Blocking Reads

## What "Blocking" Means

**Blocking read** = The OMS asks Redis: "Give me the next message, and **wait** until one arrives (up to X milliseconds)".

When Redis receives a blocking read request:
- If a message exists → Returns it immediately (event-driven!)
- If no message → **Waits** (blocks) until:
  - A new message arrives → Returns immediately
  - OR timeout expires → Returns empty

## How OMS Main Loop Works Now

**Important:** Orders and cancels run in the **same thread**, so we handle them sequentially:

```python
while True:
    # Step 1: BLOCK waiting for new order (up to 1000ms)
    result = process_one(redis, store, registry, block_ms=1000)
    # ↑ This calls XREADGROUP BLOCK 1000
    #   - Thread BLOCKS here waiting for orders
    #   - If message exists: returns immediately
    #   - If no message: waits up to 1000ms for one to arrive
    #   - When message arrives: Redis wakes up OMS immediately!
    
    # Step 2: Quick NON-BLOCKING check for cancel requests
    cancel_result = process_one_cancel(redis, store, registry, block_ms=0)
    # ↑ Non-blocking (block_ms=0) because:
    #   - We can't block on both streams simultaneously
    #   - Cancels are checked after every order check/timeout
    #   - This ensures cancels are still processed promptly
    
    # Step 3: Periodic maintenance (every 20 iterations)
    if iteration % 20 == 0:
        trim_oms_streams(redis)  # Clean up old stream entries
```

**Why this design?**
- Single-threaded: Simpler, no thread synchronization needed
- Can't block on both: Redis XREADGROUP can only block on one stream at a time
- Priority: Block on orders (more frequent), then quickly check cancels

## Comparison: Polling vs Blocking

### Before (Polling - 500ms interval)
```
Time    | OMS Action                          | What Happens
--------|-------------------------------------|------------------
T=0.0s  | Check Redis: "Any messages?"        | No → Sleep 500ms
T=0.5s  | Check Redis: "Any messages?"        | No → Sleep 500ms
T=1.0s  | Check Redis: "Any messages?"        | No → Sleep 500ms
T=1.5s  | Check Redis: "Any messages?"        | **YES!** → Process
        |                                     | 
        | Message arrived at T=1.1s           |
        | But OMS didn't check until T=1.5s   |
        | **Delay: 400ms wasted**             |
```

### After (Blocking - Event-Driven)
```
Time    | OMS Action                          | What Happens
--------|-------------------------------------|------------------
T=0.0s  | XREADGROUP BLOCK 1000              | **Waits** for message...
T=0.0s  | (Redis connection is idle)          | 
T=1.1s  | **Message arrives in Redis**        | 
T=1.1s  | Redis immediately wakes up OMS!     | Returns message instantly
T=1.1s  | OMS processes order                 | **Delay: ~1ms (Redis propagation)**
```

## The Redis Command: XREADGROUP BLOCK

When OMS calls `read_one_risk_approved_cg()` with `block_ms=1000`:

```python
# This translates to Redis command:
XREADGROUP GROUP oms CONSUMER oms-1 BLOCK 1000 STREAMS risk_approved >
```

**What Redis does:**
1. Checks if there's a new message in `risk_approved` stream
2. **If yes**: Returns it immediately (no waiting)
3. **If no**: 
   - Keeps the connection open
   - **Waits** (blocks) for up to 1000ms
   - As soon as ANY message arrives → Returns it immediately
   - If 1000ms passes with no message → Returns empty

## Why Blocking is Event-Driven

**Event-driven** = Reacts immediately when an event (new message) happens.

With blocking reads:
- OMS doesn't waste CPU checking repeatedly
- OMS doesn't waste time sleeping when messages are available
- Redis **pushes** the message to OMS as soon as it arrives
- OMS wakes up **immediately** (within milliseconds)

## The Timeout (1000ms)

The `block_ms=1000` timeout serves two purposes:

1. **Allows periodic tasks**: After timeout, OMS can:
   - Check for cancel requests
   - Trim old stream entries
   - Handle shutdown signals

2. **Prevents indefinite blocking**: If Redis connection issues occur, OMS won't hang forever

## Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    OMS Main Loop                           │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │  XREADGROUP BLOCK 1000           │
        │  (Wait for new order)            │
        └─────────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
    Message              Timeout
    arrives              (1000ms)
        │                   │
        ▼                   ▼
┌───────────────┐   ┌───────────────┐
│ Process order │   │ Check cancels │
│ Stage in Redis│   │ Trim streams  │
│ Place order   │   │ (maintenance) │
│ Publish fill  │   └───────────────┘
└───────────────┘
```

## Why Single Thread? (Orders + Cancels)

**Question:** Why not use separate threads for orders and cancels?

**Answer:** We could, but single-threaded is simpler and sufficient:

1. **Simplicity**: No thread synchronization, no race conditions
2. **Redis limitation**: Can't block on multiple streams simultaneously anyway
3. **Fast enough**: Cancels are checked after every order (or timeout), so they're still prompt
4. **Event-driven**: Orders wake up immediately when they arrive

**Alternative (if needed):**
If cancel latency becomes critical, we could:
- Use separate threads (one for orders, one for cancels)
- Or use Redis pub/sub for cancels (push-based)
- But current design is sufficient for most use cases

## Benefits of Blocking Reads

✅ **Instant response**: Messages processed within milliseconds of arrival  
✅ **CPU efficient**: No busy polling, no wasted cycles  
✅ **Scalable**: Works efficiently even with low message rates  
✅ **Event-driven**: True reactive architecture  
✅ **Simple**: Single-threaded, no synchronization complexity  

## Configuration

You can adjust the blocking timeout via environment variable:

```bash
OMS_BLOCK_MS=500  # Block for 500ms instead of 1000ms
```

Or disable blocking (use polling) if needed:

```bash
OMS_BLOCK_MS=0
OMS_POLL_SLEEP_SECONDS=0.05  # Poll every 50ms
```

## Technical Details

**Redis Streams Consumer Groups:**
- `GROUP oms`: The consumer group name
- `CONSUMER oms-1`: This specific OMS instance
- `>`: Read only new messages (not pending retries)
- Messages are **acknowledged** (XACK) after processing, so they're not redelivered

**Why Consumer Groups?**
- Multiple OMS instances can run in parallel (load balancing)
- Each message is delivered to only one consumer
- If a consumer crashes, messages can be retried by another consumer
