# Account Management System (AMS) — Docs

Plan and architecture for the **Account Management System**: receive account events from broker streams and periodic REST refresh, store in Redis, sync to Postgres.

## Documents

| Doc | Description |
|-----|-------------|
| [AMS_ARCHITECTURE.md](./AMS_ARCHITECTURE.md) | Data flow, Redis key layout, broker adapter (account interface), main loop, sync, repairs, cleanup, config, file map. |
| [AMS_DB_FIELDS.md](./AMS_DB_FIELDS.md)   | Postgres column sources for `accounts`, `balances`, `positions`, `margin_snapshots` (injected from Redis / broker stream / REST). |

## Alignment with OMS

AMS reuses the same patterns as OMS:

- **Event-driven:** Broker user data stream (e.g. Binance `outboundAccountPosition`, `balanceUpdate`) → AMS callback → Redis.
- **Periodic:** REST account/balance/position snapshot (e.g. every 60s) → merge into Redis; periodic sync Redis → Postgres.
- **Data fixes:** Post-sync repairs from payload (e.g. fix NULL/zero from broker raw data).
- **Storage:** Redis as primary store; Postgres for audit and downstream (Booking, Position Keeper).
