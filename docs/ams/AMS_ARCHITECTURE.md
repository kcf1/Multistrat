# AMS Architecture: Data Flow, Interfaces & Design

**Note:** Account Management is **integrated into OMS** (not a separate service). This document describes the account management components within OMS. See `docs/oms/OMS_ARCHITECTURE.md` for the complete OMS architecture including orders and accounts.

Single reference for Account Management within OMS: data flow, Redis/Postgres interfaces, broker adapter contract, and architecture details. Account management follows the same patterns as order management: **event-driven from broker streams**, **periodic REST refresh**, **Redis as primary store**, **sync to Postgres**, and **data fixes (repairs)**.

---

## 1. High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Broker (e.g. Binance)                              │
│  User data stream (WebSocket)          REST API                              │
│  • outboundAccountPosition             • GET /api/v3/account                  │
│  • balanceUpdate                       • GET /fapi/v2/account (futures)      │
│  • executionReport (OMS only)         • GET /fapi/v2/balance                 │
└──────────────┬────────────────────────────────┬─────────────────────────────┘
               │ event-driven                    │ periodic refresh
               ▼                                ▼
       ┌───────────────────────────────────────────────────┐
       │                      AMS                           │
       │  - account event callback (stream)                 │
       │  - periodic snapshot (REST)                        │
       │  - store → Redis                                    │
       │  - sync → Postgres (trigger + periodic)             │
       │  - repairs (fix flawed fields from payload)         │
       └─────────────────────────┬───────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         ▼                       ▼                       ▼
  ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
  │ Redis Store   │      │ (optional)    │      │ Postgres      │
  │ account:*     │      │ ams_snapshots │      │ accounts      │
  │ balances:*    │      │ Redis Stream  │      │ balances      │
  │ positions:*   │      │ (downstream)  │      │ positions     │
  └───────────────┘      └───────────────┘      └───────────────┘
```

- **Inbound (event-driven):** Broker user data stream events: `outboundAccountPosition` (balance per asset), `balanceUpdate` (delta), and broker-specific position/balance events. OMS continues to consume `executionReport` only; AMS consumes account/balance/position events (same WebSocket can be shared or separate listener per broker).
- **Inbound (periodic):** REST snapshot: e.g. `get_account()`, `get_futures_account()`, `get_futures_balance()` at a configurable interval (e.g. 60s) to reconcile and backfill.
- **Store:** Redis hashes/sets for account metadata, balances, and positions per `(broker, account_id)`.
- **Outbound:** Postgres `accounts`, `balances`, `balance_changes` (historical deposits/withdrawals). OMS does not have a `positions` or `margin_snapshots` table (positions in Redis only; margin_snapshots dropped). Optional Redis stream `ams_snapshots` for downstream (Risk / Position Keeper) if needed.

---

## 2. Data Flows (Aligned with OMS)

| Flow type            | OMS pattern                    | AMS equivalent                                      |
|----------------------|---------------------------------|-----------------------------------------------------|
| **Event-driven**     | Fill listener (user data stream)| Account/balance/position listener (same or separate stream) |
| **Periodic**         | sync_terminal_orders every N s  | Periodic REST account/balance/position refresh      |
| **Sync to DB**       | On terminal + periodic sync    | On event (optional) + periodic sync to Postgres     |
| **Data fixes**       | run_all_repairs after sync     | run_all_repairs (fix flawed fields from payload)    |
| **Cleanup / TTL**    | Trim streams; TTL on order keys | Trim optional stream; TTL on Redis account keys (optional) |

AMS does **not** consume Redis streams from Risk (no `risk_approved`-style input). It is fed only by **broker streams** and **periodic REST**. Optionally, AMS can produce an outbound stream (e.g. `ams_snapshots`) for consumers that need account state.

---

## 3. Redis Key Layout (Account Store)

Proposed layout (broker-agnostic; extend per broker as needed):

| Key pattern                          | Type  | Purpose |
|--------------------------------------|-------|--------|
| `account:{broker}:{account_id}`      | Hash  | Account metadata: broker, account_id, updated_at, optional config. |
| `account:{broker}:{account_id}:balances` | Hash | Asset → balance info (available, locked, etc.). Field = asset, value = JSON or pipe-separated. |
| `account:{broker}:{account_id}:positions`| Hash | Symbol (or symbol_side) → position info (quantity, entry_price_avg, etc.). |
| `accounts:by_broker:{broker}`         | Set   | Set of `account_id` for that broker (index). |

- **Pipelines:** Multi-key updates (e.g. apply balance update + update account updated_at) use Redis pipelines for atomicity.
- **TTL:** TTL on Redis account keys is set only when processing `balanceUpdate` events (in main's `on_balance_change` callback), not after periodic sync. Configurable via `OMS_ACCOUNT_TTL_AFTER_BALANCE_CHANGE_SECONDS` (default from sync TTL).

---

## 4. Broker User Data Stream Events (Account-Related)

Binance user data stream sends (among others):

| Event type                  | Description                    | AMS action |
|----------------------------|--------------------------------|------------|
| `outboundAccountPosition`  | Balance per asset (full snapshot) | Update Redis balances for that account; mark updated_at. Optionally write to `balance_changes` table with `change_type='snapshot'` for reconciliation audit trail. |
| `balanceUpdate`            | Single-asset balance delta (deposit/withdrawal/transfer) | Update Redis balance for asset; **write to `balance_changes` table** with `change_type` derived from delta sign (deposit if d > 0, withdrawal if d < 0); optional event_id for idempotency. |
| `executionReport`          | Order/fill events              | Consumed by OMS only; AMS ignores. |

Other brokers (e.g. futures) may add position update events; AMS maps them to a unified internal shape and writes to `account:*:positions`.

---

## 5. Broker Adapter Extension (Account Interface)

**Location:** Same registry as OMS; extend `BrokerAdapter` or add an **AccountAdapter** protocol.

| Method | Purpose |
|--------|--------|
| **start_account_listener(callback)** | Start receiving account/balance/position events (e.g. from user data stream). Callback receives unified event dict (event_type, account_id, balances[], positions[], payload). |
| **get_account_snapshot(account_id)** | REST: return current account snapshot (balances, positions, margin if applicable). Used for periodic refresh and reconciliation. |
| **stop_account_listener()** | Stop the account event listener. |

- **Unified event shape:** `event_type` (e.g. `balance_update`, `account_position`), `broker`, `account_id`, `balances` (list of {asset, available, locked}), `positions` (list of {symbol, side, quantity, entry_price}), `updated_at`, `payload` (raw broker blob for repairs).
- **Shared stream:** If the broker uses a single user data stream for both orders and account, the adapter can multiplex: OMS gets executionReport, AMS gets outboundAccountPosition / balanceUpdate. Alternatively, separate ListenKey per service if the broker supports it.

---

## 6. Main Loop and Processing

**Entrypoint:** `ams/main.py` — `main()` → `run_ams_loop()`.

1. **Bootstrap:** Redis client, Redis account store, AdapterRegistry (brokers that support account interface). Start **account listeners** with a single callback that updates Redis store (balances/positions); wait for listeners connected (same pattern as OMS wait_for_fill_listeners_connected).
2. **Loop (each iteration):**
   - **Event-driven:** Account listener runs in adapter thread; callback updates Redis immediately (apply_balance_update, apply_account_position). Optionally, on significant change, call `on_account_updated(account_key)` to trigger sync of that account to Postgres.
   - **Periodic refresh:** Every `refresh_interval_seconds` (e.g. 60), call `get_account_snapshot` per registered (broker, account_id), merge into Redis store (overwrite or merge by event_id/timestamp to avoid overwriting newer stream data).
   - **Periodic sync:** Every `sync_interval_seconds` (e.g. 60), run `sync_accounts_to_postgres(redis, store, pg_connect)` (sync all accounts with updated state from Redis to Postgres `accounts`, `balances`, `positions`). Then run `run_all_repairs(pg_connect)` to fix flawed fields from payload.
   - **Trim / cleanup:** Every `trim_every_n` iterations, trim optional `ams_snapshots` stream (if used); optionally set TTL on Redis account keys after sync.

**Blocking:** The main loop can block on a condition or sleep for `min(refresh_interval_seconds, sync_interval_seconds)`; event-driven updates happen in the listener thread. No Redis stream consumer group for inbound (broker pushes via WebSocket); optional outbound stream `ams_snapshots` can use XADD and be trimmed.

---

## 7. Sync to Postgres

**Module:** `ams/sync.py`.

- **Trigger:** Optional: on account update from stream (`on_account_updated`); plus **periodic** `sync_accounts_to_postgres` every `sync_interval_seconds`.
- **Logic:** For each account in Redis (or each account updated since last sync): read account hash, balances hash, positions hash; map to Postgres rows (`_account_to_row`, `_balance_to_row`, `_position_to_row`); UPSERT into `accounts` by (broker, account_id or id), then `balances` (by account_id, asset), then `positions` (by account_id, symbol, side). **After UPSERTing positions, DELETE any positions for the account that are not present in Redis** (handles flattened positions where brokers exclude zero positions from events). Set TTL on Redis keys after sync if configured.
- **Balance changes:** When processing `balanceUpdate` events (deposits/withdrawals), write to `balance_changes` table with `change_type` derived from delta sign. Optionally write `outboundAccountPosition` snapshots to `balance_changes` with `change_type='snapshot'` for reconciliation audit trail.
- **Flattened positions:** When syncing positions from Redis to Postgres, positions not present in Redis (missing from the latest `outboundAccountPosition` event) are **deleted** from Postgres. This handles cases where brokers exclude zero/flattened positions from account events (e.g., selling all BTC removes BTC from the event, so BTC position should be deleted from Postgres).
- **DB columns / sources:** See `docs/ams/AMS_DB_FIELDS.md`.

**Repairs (post-sync):** `ams/repair.py` — `run_all_repairs(pg_connect)` runs periodically after sync. Fix flawed Postgres fields (e.g. numeric/empty) by recovering from `payload` or raw broker data for the relevant broker (e.g. `broker = 'binance'`).

---

## 8. Cleanup and TTL

**Module:** `ams/cleanup.py`.

- **trim_ams_streams(redis):** If `ams_snapshots` stream exists, XTRIM to MAXLEN (e.g. 10000).
- **set_account_key_ttl(redis, broker, account_id, ttl_seconds):** Set TTL on `account:{broker}:{account_id}` and related balance/position keys after sync. Default TTL from `AMS_SYNC_TTL_AFTER_SECONDS` (e.g. 300). Optional: only set TTL if periodic refresh will re-populate.

---

## 9. Optional Outbound Stream: ams_snapshots

If downstream (Risk, Position Keeper) need account state via Redis:

| Stream         | Producer | Consumer   | Schema / Notes |
|----------------|----------|------------|----------------|
| **ams_snapshots** | AMS      | Risk / PK  | Snapshot message: broker, account_id, balances[], positions[], updated_at. Trimmed by AMS. |

AMS can XADD to `ams_snapshots` on each periodic sync or on each stream update (with rate limiting to avoid flood). Consumer group optional.

---

## 10. Configuration (Environment)

| Variable | Purpose |
|----------|--------|
| REDIS_URL | Redis connection (default redis://localhost:6379). |
| DATABASE_URL | Postgres; enables sync and repairs. |
| BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_BASE_URL | Binance adapter (same as OMS; reuse or extend). |
| AMS_REFRESH_INTERVAL_SECONDS | Periodic REST account snapshot interval (default 60). |
| AMS_SYNC_INTERVAL_SECONDS | Periodic sync Redis → Postgres (default 60). |
| AMS_SYNC_TTL_AFTER_SECONDS | TTL on Redis account keys after sync (default 300; 0 = no TTL). |
| AMS_TRIM_EVERY_N | Iterations between stream trim (default 200). |
| LOG_LEVEL | e.g. DEBUG (default INFO). |

---

## 11. File Map (Proposed)

| Area | Files |
|------|--------|
| Entry & loop | `ams/main.py` |
| Flow (apply events, refresh, sync) | `ams/redis_flow.py` (or `account_flow.py`) |
| Account event callback | `ams/account_callback.py` or inside redis_flow |
| Stream helpers | `ams/streams.py` (if ams_snapshots used) |
| Schemas | `ams/schemas.py`, `ams/schemas_pydantic.py` |
| Store | `ams/storage/redis_account_store.py` |
| Registry & adapter | Reuse OMS registry; `ams/brokers/base.py` (AccountAdapter), `ams/brokers/binance/account_adapter.py` or extend existing adapter |
| Binance account events | `ams/brokers/binance/account_listener.py` (parse outboundAccountPosition, balanceUpdate) |
| Sync to DB | `ams/sync.py` |
| Repairs | `ams/repair.py` |
| Cleanup | `ams/cleanup.py` |
| Logging | `ams/log.py` |

---

## 12. Architecture Notes

- **Single process:** One AMS process; one account listener per broker (same as OMS one fill listener per broker). Scaling: multiple AMS instances would need per-broker partitioning or a single writer to Redis.
- **At-least-once / idempotency:** Balance/position updates are applied by key (e.g. asset, symbol_side). Use `updated_at` or event_id to avoid overwriting newer data with older periodic snapshot.
- **Event-driven + periodic:** Stream gives low-latency updates; REST refresh reconciles and fills gaps after disconnect or missed events.
- **Alignment with OMS:** Same patterns: Redis as source of truth, sync to Postgres, repairs from payload, optional TTL and stream trim.

This document focuses on data flow and interfaces. For DB column sources see `docs/ams/AMS_DB_FIELDS.md`.
