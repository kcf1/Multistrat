# OMS Architecture: Data Flow, Interfaces & Design

Single reference for the Order Management System (OMS) codebase: data flow, Redis/Postgres interfaces, broker adapter contract, and architecture details.

**Scope:** OMS manages **all broker state interactions**: orders (place/cancel/status) and **account state** (balances, positions). Market data and historical data are handled by separate services.

---

## 1. High-Level Data Flow

```
┌─────────────────┐     risk_approved      ┌──────────────────────────────────────────────┐
│ Risk (upstream)  │ ───────────────────►  │                                              │
└─────────────────┘   Redis Stream          │              OMS                            │
                                           │  • Order management (place/cancel/status)     │
┌─────────────────┐     cancel_requested   │  • Account management (balances/positions)   │
│ Risk / Admin    │ ───────────────────►   │  • Broker adapter registry                   │
└─────────────────┘   Redis Stream         │  • User data stream (fills + account)       │
                                           └──────────────┬───────────────────────────────┘
                                                          │
                    ┌─────────────────────────────────────┼─────────────────────────────────────┐
                    │                                     │                                     │
                    ▼                                     ▼                                     ▼
           ┌───────────────────┐              ┌───────────────────┐              ┌───────────────────┐
           │   Broker          │              │   Redis Store     │              │   Postgres        │
           │  (Binance)        │              │                   │              │                   │
           │                   │              │  orders:*         │              │  orders           │
           │  REST API:         │              │  account:*        │              │  accounts         │
           │  • place_order     │              │  balances:*       │              │  balances         │
           │  • cancel_order    │              │  positions:*      │              │                   │
           │  • get_account     │              │                   │              │  (no positions     │
           │                    │              │                   │              │   table in P2)    │
           │                    │              └───────────────────┘              └───────────────────┘
           │  User data stream:  │
           │  • executionReport  │              ┌───────────────────┐
           │  • outboundAccount  │              │  oms_fills        │
           │    Position         │              │  Redis Stream     │
           │  • balanceUpdate    │              └─────────┬─────────┘
           └──────────┬──────────┘                        │
                      │                                   │
                      │ WebSocket                         ▼
                      └───────────────────────────►  PMS / downstream consumers
```

- **Inbound (orders):** `risk_approved` (orders to execute), `cancel_requested` (cancel by order_id or broker_order_id+symbol).
- **Inbound (account):** Broker user data stream events (`outboundAccountPosition`, `balanceUpdate`) + periodic REST (`get_account`, `get_futures_account`).
- **Outbound:** `oms_fills` (fill/reject/cancelled/expired events for PMS and downstream), Postgres `orders`, `accounts`, `balances` (sync); **positions** in Redis only (no Postgres positions table in Phase 2). See **docs/pms/PMS_ARCHITECTURE.md**.
- **Broker:** Binance REST (place/cancel, account snapshots) + user data stream (execution reports + account updates). Other brokers plug in via the same adapter interface.

---

## 2. Redis Streams (Interfaces)

| Stream              | Producer     | Consumer | Schema / Key constants      | Notes |
|---------------------|-------------|----------|-----------------------------|-------|
| **risk_approved**   | Risk        | OMS      | `RISK_APPROVED_STREAM`      | Order to execute. Consumer group `oms`, XREADGROUP + XACK. Trimmed by `trim_oms_streams`. |
| **cancel_requested**| Risk/Admin  | OMS      | `CANCEL_REQUESTED_STREAM`  | Cancel by order_id or (broker_order_id + symbol). Consumer group `oms`. **Not** trimmed by cleanup. |
| **oms_fills**       | OMS         | PMS / downstream | `OMS_FILLS_STREAM`          | Unified fill/reject/cancelled/expired. Trimmed by `trim_oms_streams`. |

- **Stream helpers:** `oms/streams.py` — `add_message`, `read_messages`, `read_messages_group`, `ack_message`, `ensure_consumer_group`, `get_pending_delivery_count`.
- **Consumer groups:** Created at startup for `risk_approved` and `cancel_requested` (idempotent XGROUP CREATE MKSTREAM). Single consumer name (e.g. `oms-1`).

---

## 3. Redis Key Layout

### 3.1 Order Store

Defined in `oms/storage/redis_order_store.py`:

| Key pattern                         | Type  | Purpose |
|-------------------------------------|-------|--------|
| `orders:{order_id}`                 | Hash  | Full order (internal_id, broker, symbol, side, status, executed_qty, payload, etc.). Optional TTL after sync. |
| `orders:by_status:{status}`          | Set   | order_ids in that status (pending, sent, partially_filled, filled, rejected, cancelled, expired). |
| `orders:by_book:{book}`             | Set   | order_ids for that book (optional index). |
| `orders:by_broker_order_id:{id}`     | String| Maps broker_order_id → order_id (for fill callback and cancel resolution). |

- **Pipelines:** All multi-key updates (stage, update_status, update_fill_status) use Redis pipelines for atomicity.
- **Retry key:** `oms:retry:risk_approved:{entry_id}` used for place_order retry count (compared to `OMS_PLACE_ORDER_MAX_RETRIES`).

### 3.2 Account Store

Defined in `oms/storage/redis_account_store.py`:

| Key pattern                          | Type  | Purpose |
|--------------------------------------|-------|--------|
| `account:{broker}:{account_id}`      | Hash  | Account metadata: broker, account_id, env, updated_at, optional config. |
| `account:{broker}:{account_id}:balances` | Hash | Asset → balance info (available, locked, etc.). Field = asset, value = JSON or structured string. |
| `account:{broker}:{account_id}:positions` | Hash | Symbol (or symbol_side) → position info (quantity, entry_price_avg, etc.). |
| `accounts:by_broker:{broker}`         | Set   | Set of `account_id` for that broker (index). |

- **Pipelines:** Multi-key updates (apply balance update + update account updated_at) use Redis pipelines for atomicity.
- **TTL (optional):** After sync to Postgres, set TTL on Redis keys (e.g. `OMS_ACCOUNT_SYNC_TTL_AFTER_SECONDS`) to avoid unbounded growth; re-populate from REST or stream on next refresh.

---

## 4. Message Schemas (Stream Payloads)

### 4.1 risk_approved (input)

- **Fields:** `order_id` (optional), `broker`, `account_id`, `symbol`, `side`, `quantity`, `order_type`, `price` (→ limit_price), `time_in_force`, `book`, `comment`, `strategy_id`, `created_at`.
- **Validation:** `RiskApprovedOrder` in `oms/schemas_pydantic.py`; parsed in `consumer.parse_risk_approved_message`.

### 4.2 cancel_requested (input)

- **Fields:** `order_id` (optional) OR (`broker_order_id` + `symbol`); `broker` required.
- **Validation:** `CancelRequest` in `oms/schemas_pydantic.py`; parsed in `cancel_consumer.parse_cancel_request_message`.

### 4.3 oms_fills (output)

- **Fields:** `event_type` (fill | reject | cancelled | expired), `order_id`, `broker_order_id`, `symbol`, `side`, `quantity`, `price`, `fee`, `fee_asset`, `executed_at`, `fill_id`, `reject_reason`, `book`, `comment`.
- **Validation:** `OmsFillEvent` in `oms/schemas_pydantic.py`; produced via `producer.produce_oms_fill`.

---

## 5. Broker Adapter Interface

**Location:** `oms/brokers/base.py` — `BrokerAdapter` (Protocol).

### 5.1 Order Management Methods

| Method | Purpose |
|--------|--------|
| **place_order(order)** | Submit order. Returns unified dict: `broker_order_id`, `status`, … or `rejected: True`, `reject_reason`, optional `payload`. |
| **cancel_order(broker_order_id, symbol)** | Cancel open order. Returns status/reject in unified form. |
| **start_fill_listener(callback)** | Start receiving fill/reject/cancel/expire events; callback receives unified event dict. Typically background thread. |

### 5.2 Account Management Methods

| Method | Purpose |
|--------|--------|
| **start_account_listener(callback)** | Start receiving account/balance/position events (e.g. from user data stream). Callback receives unified event dict (`event_type`, `account_id`, `balances[]`, `positions[]`, `payload`). Typically background thread (can share WebSocket with fill listener). |
| **get_account_snapshot(account_id)** | REST: return current account snapshot (balances, positions, margin if applicable). Used for periodic refresh and reconciliation. Returns unified dict. |
| **stop_account_listener()** | Stop the account event listener. |

- **Registry:** `oms/registry.py` — `AdapterRegistry` maps broker name (e.g. `binance`) → adapter. OMS routes by message field `broker`.
- **Binance:** `oms/brokers/binance/adapter.py` uses `BinanceAPIClient` (REST) and `create_fills_listener` / `create_account_listener` (ListenKey or WebSocket API). The user data stream sends multiple event types: `executionReport` (fills), `outboundAccountPosition` (account snapshot), `balanceUpdate` (balance delta). The adapter multiplexes: fill listener processes `executionReport`; account listener processes `outboundAccountPosition` / `balanceUpdate`. Converts all responses to unified shapes.

---

## 6. Main Loop and Processing

**Entrypoint:** `oms/main.py` — `main()` → `run_oms_loop()`.

1. **Bootstrap:** Redis client, `RedisOrderStore`, `AdapterRegistry`, register Binance if `BINANCE_API_KEY` set. Start fill listeners with `make_fill_callback(redis, store, …)`; wait for listeners connected.
2. **Loop (each iteration):**
   - **process_many:** Read up to `batch_size` from `risk_approved` (XREADGROUP BLOCK `block_ms`), then pending; for each: stage → place_order via registry → **write payload (and broker_order_id) to store immediately** so fill callback enrichment can see them even if the WebSocket fill arrives first (race); then update status to `sent` (or keep terminal if fill already ran) → produce oms_fill on reject → XACK. On terminal status, optional `on_terminal_sync(order_id)` (sync to Postgres + TTL).
   - **process_many_cancel:** Read up to `batch_size` from `cancel_requested` (non-blocking); resolve order_id (from message or by broker_order_id); adapter.cancel_order; update store to cancelled; optionally produce cancelled to oms_fills; XACK.
   - **Trim:** Every `trim_every_n` iterations, `trim_oms_streams(redis)` (risk_approved + oms_fills, MAXLEN 20000).
   - **Periodic sync:** If `pg_connect` and `sync_interval_seconds` set, `sync_terminal_orders(redis, store, pg_connect, …)` (sync terminal orders to Postgres, set TTL on Redis keys); then `run_all_repairs(pg_connect)` (fix flawed order fields from payload for Binance orders).

**Blocking:** When `block_ms > 0`, `process_many` uses XREADGROUP BLOCK for event-driven wake-up; cancel is non-blocking so it’s checked after orders each iteration.

---

## 7. Fill Callback Flow (Broker → OMS)

1. **Adapter** (e.g. Binance) receives events on the user data stream. The stream sends multiple event types (e.g. `executionReport`, `outboundAccountPosition`). The **fills_listener** only processes `executionReport` (NEW, TRADE, REJECTED, CANCELED, EXPIRED); other types are ignored without logging.
2. **fills_listener** parses execution reports to unified event (`FillEvent`, `RejectEvent`, `CancelledEvent`, `ExpiredEvent` in `brokers/binance/schemas_pydantic.py`). For TRADE (fill) events it also parses **time_in_force** (Binance field `f`) and **binance_cumulative_quote_qty** (Binance field `Z`) from the execution report so the event can carry them even when the store payload is not yet available.
3. **make_fill_callback** (in `redis_flow.py`): validates event, resolves `order_id` (from event or store by broker_order_id), updates store (`update_fill_status`), produces to `oms_fills`, on terminal status calls `on_terminal_sync(order_id)` or sets TTL. Uses only the event’s `price` (no broker-specific fallback in core). **Binance:** When `start_fill_listener(callback, store=store)` is used, the adapter wraps the callback and enriches fill events from the order payload when the event has missing/zero/empty values: **price** (avgPrice / fills / fill.price), **time_in_force** (timeInForce), **binance_cumulative_quote_qty** (cumulativeQuoteQty). The OMS callback then receives the enriched event. The store is updated with payload (and broker_order_id) **immediately** after place_order returns (in `process_one` / `process_many`) so enrichment usually sees the payload even if the WebSocket fill arrives quickly.

Terminal statuses: `filled`, `rejected`, `cancelled`, `expired` (`TERMINAL_STATUSES` in redis_flow).

---

## 8. Sync to Postgres

### 8.1 Order Sync

**Module:** `oms/sync.py`.

- **Trigger:** On terminal status from fill callback (`on_terminal_sync`) or from process_one reject path; plus **periodic** `sync_terminal_orders` every `sync_interval_seconds` (e.g. 60s).
- **Logic:** For each terminal order: read from Redis store, map to Postgres row (`_order_to_row`), UPSERT by `internal_id`, then set TTL on `orders:{order_id}`.
- **DB columns / sources:** See `docs/oms/OMS_ORDERS_DB_FIELDS.md` (risk_approved, place_order response, fills → columns).

**Order repairs (post-sync):** `oms/repair.py` — `run_all_repairs(pg_connect)` runs periodically after sync. Fixes flawed Postgres order fields (price, time_in_force, binance_cumulative_quote_qty, status) by recovering values from the `payload` JSONB when they are NULL/0/empty. Status is set from payload (payload.binance.status or .X) whenever payload has it; no null check on current DB status. Applies only to `broker = 'binance'` orders.

### 8.2 Account Sync

**Module:** `oms/account_sync.py`.

- **Trigger:** Optional: on account update from stream (`on_account_updated`); plus **periodic** `sync_accounts_to_postgres` every `sync_interval_seconds` (e.g. 60s).
- **Logic:** For each account in Redis (or each account updated since last sync): read account hash, balances hash, positions hash; map to Postgres rows (`_account_to_row`, `_balance_to_row`, `_position_to_row`); UPSERT into `accounts` by (broker, account_id or id), then `balances` (by account_id, asset), then `positions` (by account_id, symbol, side). Set TTL on Redis keys after sync if configured.
- **DB columns / sources:** See `docs/ams/AMS_DB_FIELDS.md` (broker stream vs REST, payload → columns).

**Account repairs (post-sync):** `oms/account_repair.py` — `run_all_account_repairs(pg_connect)` runs periodically after sync. Fixes flawed Postgres account/balance/position fields (e.g. numeric/empty) by recovering from `payload` or raw broker data for the relevant broker (e.g. `broker = 'binance'`).

---

## 9. Cleanup and TTL

**Module:** `oms/cleanup.py`.

- **trim_oms_streams(redis):** XTRIM risk_approved and oms_fills to MAXLEN (default 20000). cancel_requested is **not** trimmed.
- **set_order_key_ttl(redis, order_id, ttl_seconds):** Set TTL on `orders:{order_id}` after terminal status (e.g. after sync). Default TTL from `OMS_SYNC_TTL_AFTER_SECONDS` (default 300).
- **set_account_key_ttl(redis, broker, account_id, ttl_seconds):** Set TTL on `account:{broker}:{account_id}` and related balance/position keys after sync. Default TTL from `OMS_ACCOUNT_SYNC_TTL_AFTER_SECONDS` (default 300; 0 = no TTL). Optional: only set TTL if periodic refresh will re-populate.

---

## 10. Retry and Error Handling

- **place_order failure:** Retry count in `oms:retry:risk_approved:{entry_id}`; delivery count from XPENDING. After `OMS_PLACE_ORDER_MAX_RETRIES` (3): mark rejected, produce oms_fill reject, XACK, delete retry key, call `on_terminal_sync` if set.
- **Parse errors:** risk_approved/cancel_requested parse failures log and skip message (no XACK for that entry when using consumer group — message stays pending; pending is re-read in next iteration with id `0`).
- **Fill callback validation:** Invalid or unknown event_type logs and skips; no store update.

---

## 11. Configuration (Environment)

| Variable | Purpose |
|----------|--------|
| REDIS_URL | Redis connection (default redis://localhost:6379). |
| BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_BASE_URL | Binance adapter (testnet vs main). |
| BINANCE_FILLS_STREAM | `wsapi` (default) or `listenkey` for user data stream (shared for fills + account events). |
| DATABASE_URL | Postgres; enables sync and on_terminal_sync. |
| OMS_SYNC_TTL_AFTER_SECONDS | TTL on Redis order key after sync (default 300). |
| OMS_SYNC_INTERVAL_SECONDS | Periodic sync interval for orders and accounts (default from sync.py, e.g. 60). |
| OMS_ACCOUNT_REFRESH_INTERVAL_SECONDS | Periodic REST account snapshot interval (default 60). |
| OMS_ACCOUNT_SYNC_TTL_AFTER_SECONDS | TTL on Redis account keys after sync (default 300; 0 = no TTL). |
| OMS_BLOCK_MS | Block timeout for risk_approved read (default 100). |
| OMS_POLL_SLEEP_SECONDS | Used only when block_ms=0. |
| OMS_BATCH_SIZE | Max messages per process_many / process_many_cancel (default 50). |
| LOG_LEVEL | e.g. DEBUG (default INFO). |

---

## 12. File Map (Concise)

| Area | Files |
|------|--------|
| Entry & loop | `main.py` |
| Flow (process_one/many, fill callback) | `redis_flow.py` |
| Account flow (account callback, periodic refresh) | `account_flow.py` |
| Consumers | `consumer.py`, `cancel_consumer.py` |
| Producer | `producer.py` |
| Stream helpers | `streams.py` |
| Schemas (names, field lists) | `schemas.py` |
| Pydantic validation | `schemas_pydantic.py`, `brokers/binance/schemas_pydantic.py` |
| Order store | `storage/redis_order_store.py` |
| Account store | `storage/redis_account_store.py` |
| Registry & adapter contract | `registry.py`, `brokers/base.py` |
| Binance | `brokers/binance/adapter.py`, `api_client.py`, `fills_listener.py`, `account_listener.py` |
| Order sync to DB | `sync.py` |
| Account sync to DB | `account_sync.py` |
| Order repairs (Binance payload recovery) | `repair.py` |
| Account repairs | `account_repair.py` |
| Cleanup | `cleanup.py` |
| Logging | `log.py` |

---

## 13. Architecture Notes

- **Single process:** One OMS process; one consumer group with one consumer name. Scaling would require multiple consumer names in the same group or separate groups (not currently designed).
- **At-least-once:** risk_approved/cancel_requested are XACK’ed only after successful processing; on crash, pending messages are redelivered. Idempotency is by order_id (stage_order no-op if order already exists).
- **Event-driven + batch:** Blocking read on risk_approved for low latency; batch size and trim interval tune throughput and stream growth.
- **Fill listener before orders:** Main loop waits for fill listeners to report connected before processing, so user data stream is active before orders are placed.

This document focuses on data flow, interfaces, and architecture; for blocking behavior and tuning see `docs/OMS_BLOCKING_EXPLAINED.md`, and for DB column sources see `docs/OMS_ORDERS_DB_FIELDS.md`.
