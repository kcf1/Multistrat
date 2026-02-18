# Account Management Integration into OMS

## Overview

Account Management (balances, positions) is **integrated into OMS** rather than being a separate service. This document summarizes the integration and how it extends OMS.

## Rationale

1. **Shared broker infrastructure:** OMS already connects to broker user data streams (e.g. Binance ListenKey). The same stream sends both `executionReport` (fills) and `outboundAccountPosition` / `balanceUpdate` (account events).
2. **Consolidated state management:** Orders and account state are both broker state; keeping them together simplifies reconciliation and repairs.
3. **Single broker connection:** One WebSocket connection per broker handles both order fills and account updates (multiplexed by event type).

## Architecture Changes

### Extended Broker Adapter Interface

OMS broker adapters now support account management methods:

- `start_account_listener(callback)` - Start receiving account/balance/position events
- `get_account_snapshot(account_id)` - REST call for periodic refresh
- `stop_account_listener()` - Stop account listener

### New Components

- **`oms/storage/redis_account_store.py`** - Redis storage for accounts, balances, positions
- **`oms/account_flow.py`** - Account event callback handler
- **`oms/account_sync.py`** - Sync accounts/balances/positions to Postgres
- **`oms/account_repair.py`** - Fix flawed account fields from payload
- **`oms/brokers/binance/account_listener.py`** - Parse Binance account events

### Main Loop Extensions

The OMS main loop (`oms/main.py`) now:

1. **Bootstrap:** Starts both fill listeners and account listeners; waits for all connected
2. **Account callback:** Runs in adapter thread; updates Redis account store on each event
3. **Periodic account refresh:** Calls `get_account_snapshot()` every N seconds (e.g. 60s)
4. **Periodic sync:** Syncs both orders and accounts to Postgres; runs repairs for both

### Redis Key Layout

Account data stored in Redis:

- `account:{broker}:{account_id}` - Account metadata
- `account:{broker}:{account_id}:balances` - Balances per asset
- `account:{broker}:{account_id}:positions` - Positions per symbol
- `accounts:by_broker:{broker}` - Index set

### Postgres Tables

Account sync writes to:

- `accounts` - Account metadata
- `balances` - Balances per account/asset
- `positions` - Positions per account/symbol/side
- `margin_snapshots` (optional) - Margin snapshots for futures

## Data Flow

```
Broker User Data Stream
├── executionReport → Fill Listener → OMS (orders)
└── outboundAccountPosition / balanceUpdate → Account Listener → OMS (accounts)

Periodic REST Refresh
└── get_account_snapshot() → Merge into Redis → Sync to Postgres
```

## Configuration

New environment variables:

- `OMS_ACCOUNT_REFRESH_INTERVAL_SECONDS` - Periodic REST refresh (default 60)
- `OMS_ACCOUNT_SYNC_TTL_AFTER_SECONDS` - TTL on Redis account keys after sync (default 300)

## Benefits

1. **No duplicate broker connections** - Single WebSocket per broker
2. **Consistent patterns** - Same event-driven + periodic refresh + sync + repairs pattern
3. **Easier reconciliation** - Orders and accounts in same service, same sync cycle
4. **Simpler deployment** - One service instead of two

## References

- **Complete OMS architecture:** `docs/oms/OMS_ARCHITECTURE.md`
- **Account DB fields:** `docs/ams/AMS_DB_FIELDS.md`
- **Account architecture details:** `docs/ams/AMS_ARCHITECTURE.md` (describes components, now integrated into OMS)
