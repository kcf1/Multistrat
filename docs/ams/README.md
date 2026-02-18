# Account Management System (AMS) — Docs

**Note:** Account Management is **integrated into OMS** (not a separate service). These documents describe the account management components and architecture within OMS.

## Documents

| Doc | Description |
|-----|-------------|
| [AMS_ARCHITECTURE.md](./AMS_ARCHITECTURE.md) | Data flow, Redis key layout, broker adapter (account interface), main loop, sync, repairs, cleanup, config, file map. |
| [AMS_DB_FIELDS.md](./AMS_DB_FIELDS.md)   | Postgres column sources for `accounts`, `balances`, `margin_snapshots` (from Redis / broker stream / REST). OMS has no `positions` table (positions in Redis only). |

## Integration

- **Integration details:** See `docs/oms/ACCOUNT_MANAGEMENT_INTEGRATION.md` for how account management is integrated into OMS.
- **Complete OMS architecture:** See `docs/oms/OMS_ARCHITECTURE.md` for the full OMS architecture including orders and accounts.

## Alignment with OMS

Account management reuses the same patterns as order management:

- **Event-driven:** Broker user data stream (e.g. Binance `outboundAccountPosition`, `balanceUpdate`) → account callback → Redis.
- **Periodic:** REST account/balance/position snapshot (e.g. every 60s) → merge into Redis; periodic sync Redis → Postgres.
- **Data fixes:** Post-sync repairs from payload (e.g. fix NULL/zero from broker raw data).
- **Storage:** Redis as primary store; Postgres for audit and downstream (Booking, Position Keeper).
