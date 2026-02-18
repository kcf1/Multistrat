# Balance Changes Historical Tracking

## Overview

The `balance_changes` table provides **historical tracking** of all balance modifications (deposits, withdrawals, transfers, adjustments) separate from the current state stored in the `balances` table.

## Table Design

### Schema

```sql
balance_changes
├── id (BIGSERIAL PK)
├── account_id (FK → accounts)
├── asset (TEXT)                    -- Asset symbol (e.g. USDT, BTC)
├── change_type (TEXT)              -- deposit, withdrawal, transfer, adjustment, snapshot
├── delta (NUMERIC)                 -- Amount changed: positive for deposit, negative for withdrawal
├── balance_before (NUMERIC NULL)   -- Balance before change (optional, for audit)
├── balance_after (NUMERIC NULL)    -- Balance after change (optional, for audit)
├── event_type (TEXT)               -- balanceUpdate, outboundAccountPosition, manual_adjustment
├── broker_event_id (TEXT NULL)     -- Broker's event/trade ID if available
├── event_time (TIMESTAMPTZ)        -- Timestamp from broker event
├── created_at (TIMESTAMPTZ)        -- When record was created in our system
└── payload (JSONB NULL)            -- Raw broker event blob (for repairs and audit)
```

### Indexes

- `(account_id, asset, event_time)` - For historical queries by account/asset/time
- `(account_id, change_type)` - For filtering deposits/withdrawals
- `event_time` - For time-range queries

## Change Type Derivation

| Change Type | Source | Condition |
|------------|--------|-----------|
| `deposit` | `balanceUpdate` | `delta > 0` (positive change) |
| `withdrawal` | `balanceUpdate` | `delta < 0` (negative change) |
| `transfer` | `balanceUpdate` | During account transfers (may need broker-specific logic) |
| `adjustment` | Manual | Manual adjustments or broker corrections |
| `snapshot` | `outboundAccountPosition` | Full account snapshot (delta = 0 or calculated from previous state) |

## Data Flow

### Event-Driven (Real-time)

1. **Binance `balanceUpdate` event** arrives via WebSocket
   - Contains: `asset`, `delta` (d field), `event_time` (E or T)
   - Example: `{"e": "balanceUpdate", "a": "USDT", "d": "100.5", "E": 1564034571105}`

2. **Account listener** parses event → unified `BalanceUpdateEvent`
   - `event_type`: `"balance_update"`
   - `balances`: `[{"asset": "USDT", "available": "100.5", "locked": "0.0"}]`

3. **Account callback** updates Redis account store
   - Updates `account:{broker}:{account_id}:balances` hash

4. **Account sync** writes to Postgres:
   - **`balances` table**: UPSERT current state (by `account_id`, `asset`)
   - **`balance_changes` table**: INSERT historical record
     - `change_type`: `"deposit"` if `delta > 0`, `"withdrawal"` if `delta < 0`
     - `delta`: from `balanceUpdate.d` field
     - `event_time`: from `balanceUpdate.E` or `T` (converted from ms to timestamp)
     - `payload`: raw Binance event blob

### Periodic Snapshot (Reconciliation)

1. **REST API call** (`get_account_snapshot`) every 60s
   - Returns full account snapshot (all balances)

2. **Account sync** writes to Postgres:
   - **`balances` table**: UPSERT all balances (current state)
   - **`balance_changes` table**: Optionally INSERT snapshot record
     - `change_type`: `"snapshot"`
     - `delta`: `0` or calculated difference from previous state
     - Used for reconciliation audit trail

## Usage Examples

### Query All Deposits

```sql
SELECT * FROM balance_changes
WHERE account_id = 1
  AND change_type = 'deposit'
ORDER BY event_time DESC;
```

### Query Withdrawals for Specific Asset

```sql
SELECT * FROM balance_changes
WHERE account_id = 1
  AND asset = 'USDT'
  AND change_type = 'withdrawal'
ORDER BY event_time DESC;
```

### Calculate Net Deposits/Withdrawals

```sql
SELECT 
  asset,
  SUM(CASE WHEN change_type = 'deposit' THEN delta ELSE 0 END) as total_deposits,
  SUM(CASE WHEN change_type = 'withdrawal' THEN ABS(delta) ELSE 0 END) as total_withdrawals,
  SUM(delta) as net_change
FROM balance_changes
WHERE account_id = 1
  AND event_time >= NOW() - INTERVAL '30 days'
GROUP BY asset;
```

### Audit Trail: Balance Changes Over Time

```sql
SELECT 
  event_time,
  asset,
  change_type,
  delta,
  balance_before,
  balance_after
FROM balance_changes
WHERE account_id = 1
  AND asset = 'BTC'
ORDER BY event_time DESC
LIMIT 100;
```

## Relationship with `balances` Table

| Table | Purpose | Update Frequency | Data Type |
|-------|---------|------------------|-----------|
| `balances` | **Current state** | Every balance change | One row per (account_id, asset) |
| `balance_changes` | **Historical record** | Every balance change | Append-only (INSERT only) |

- `balances` table: Stores **current** balance per asset (UPSERT by `account_id`, `asset`)
- `balance_changes` table: Stores **historical** changes (INSERT only, never updated)

## Implementation Notes

### Idempotency

- Use `broker_event_id` + `event_time` to prevent duplicate inserts
- If same `balanceUpdate` event processed twice, second INSERT should be ignored (unique constraint or application-level check)

### Performance

- `balance_changes` table will grow over time
- Consider partitioning by `event_time` (monthly/quarterly) for large-scale deployments
- Archive old records (> 1 year) to separate archive table if needed

### Reconciliation

- Periodic REST snapshots (`outboundAccountPosition` or `get_account_snapshot`) can be written to `balance_changes` with `change_type='snapshot'`
- Helps identify gaps or discrepancies in event stream
- Can calculate expected balance: `SUM(delta) FROM balance_changes WHERE account_id=X AND asset=Y`

## References

- **Schema definition:** `docs/PHASE2_DETAILED_PLAN.md` §3.3
- **Account sync implementation:** `oms/account_sync.py` (task 12.2.7)
- **Binance events:** `docs/BINANCE_API_RULES.md` (balanceUpdate event structure)
