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
           │                   │              │  orders:*         │              │  oms.orders       │
           │  REST API:         │              │  account:*        │              │  oms.accounts     │
           │  • place_order     │              │  (+ :balances /   │              │  oms.balances     │
           │  • cancel_order    │              │    :positions)   │              │                   │
           │  • account snapshot│              │                   │              │  (no positions     │
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
- **Inbound (account):** Broker user data stream events (`outboundAccountPosition`, `balanceUpdate`) + periodic REST via adapter **`get_account_snapshot`** (same unified shape as listener snapshots; no separate `get_futures_account` in this codebase).
- **Outbound:** `oms_fills` (fill/reject/cancelled/expired), Postgres **`oms.orders`**, **`oms.accounts`**, **`oms.balances`** (when `sync_balances=True`), **`oms.balance_changes`** (from balanceUpdate); **positions** in Redis only. Connections use **`pgconn.configure_for_oms`** so same-schema SQL stays unqualified in code. PMS reads OMS tables with explicit **`oms.*`** qualifiers. See **docs/pms/PMS_ARCHITECTURE.md** and **docs/POSTGRES_SCHEMA_GROUPING_PLAN.md**.
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
- **Retry key:** `oms:retry:risk_approved:{entry_id}` used for place_order retry count (compared to constant **`OMS_PLACE_ORDER_MAX_RETRIES`** = 3 in `redis_flow.py`, not an environment variable).

### 3.2 Account Store

Defined in `oms/storage/redis_account_store.py`:

| Key pattern                          | Type  | Purpose |
|--------------------------------------|-------|--------|
| `account:{broker}:{account_id}`      | Hash  | Account metadata: broker, account_id, updated_at, optional `payload` (broker blob, JSON string). |
| `account:{broker}:{account_id}:balances` | Hash | Asset → balance info (available, locked, etc.). Field = asset, value = JSON or structured string. |
| `account:{broker}:{account_id}:positions` | Hash | Symbol (hash field) → position info JSON (quantity, side, entry price fields as emitted by the adapter). |
| `accounts:by_broker:{broker}`         | Set   | Set of `account_id` for that broker (index). |

- **Pipelines:** Multi-key updates (apply balance update + update account updated_at) use Redis pipelines for atomicity.
- **TTL:** In `main()`, account Redis keys get TTL from **`OMS_ACCOUNT_TTL_AFTER_BALANCE_CHANGE_SECONDS`** (fallback: same as order sync TTL) **after** `balanceUpdate` → `write_balance_change`, not from periodic account sync. `sync_accounts_to_postgres(..., ttl_after_sync_seconds=...)` accepts TTL in the signature but **does not apply it** today; periodic sync does not expire account keys.

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

1. **Bootstrap:** Redis client, `RedisOrderStore`, `AdapterRegistry`, register Binance if `BINANCE_API_KEY` set. Start fill listeners with `make_fill_callback(redis, store, …)` and **wait until fill listeners report connected**; start account listeners (and optional `on_balance_change` when `DATABASE_URL` is set) and **wait until account listeners report connected** (or timeout). One-time symbol sync when Postgres + Binance are configured.
2. **Loop (each iteration):**
   - **process_many:** Read up to `batch_size` from `risk_approved` (XREADGROUP BLOCK `block_ms`); if no new messages, **re-read pending** (`id="0"`) so stuck deliveries are retried. For each valid parsed message: stage (only if order not already in store) → place_order → **write payload (and broker_order_id) to store immediately** (race with WebSocket) → update status to `sent` (or keep terminal if fill already ran) → produce oms_fill on reject → XACK. On terminal status, optional `on_terminal_sync(order_id)` (sync to Postgres + TTL).
   - **process_many_cancel:** Read up to `batch_size` from `cancel_requested` (non-blocking, `id=">"` only); **no separate pending sweep** like `risk_approved`. Resolve order_id; adapter.cancel_order; update store to cancelled; optionally produce cancelled to oms_fills; XACK.
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

OMS-owned tables live in the **`oms`** schema. **`pgconn.configure_for_oms`** sets `search_path` to `oms, public` on each connection so application code uses unqualified names (`orders`, `accounts`, …). **`public`** holds **`alembic_version`** and extension objects. Cross-service SQL in other packages qualifies **`oms.*`** as needed. See **docs/POSTGRES_SCHEMA_GROUPING_PLAN.md** §7.8.

### 8.1 Order Sync

**Module:** `oms/sync.py`.

- **Trigger:** On terminal status from fill callback (`on_terminal_sync`) or from process_one reject path; plus **periodic** `sync_terminal_orders` every `sync_interval_seconds` (e.g. 60s).
- **Logic:** For each terminal order: read from Redis store, map to Postgres row (`_order_to_row`), UPSERT by `internal_id` into **`oms.orders`** (unqualified in code), then set TTL on `orders:{order_id}`.
- **DB columns / sources:** See `docs/oms/OMS_ORDERS_DB_FIELDS.md` (risk_approved, place_order response, fills → columns).

**Order repairs (post-sync):** `oms/repair.py` — `run_all_repairs(pg_connect)` runs periodically after sync. Fixes flawed Postgres order fields (price, time_in_force, binance_cumulative_quote_qty, status) by recovering values from the `payload` JSONB when they are NULL/0/empty. Status is set from payload (payload.binance.status or .X) whenever payload has it; no null check on current DB status. Applies only to `broker = 'binance'` orders.

### 8.2 Account Sync

**Module:** `oms/account_sync.py`.

- **Trigger:** Periodic `sync_accounts_to_postgres` every `account_sync_interval_seconds` (e.g. 60s). Balance changes from stream → `write_balance_change` (main's `on_balance_change`).
- **Logic:** For each account in Redis: UPSERT `accounts`; optionally **balances** when `sync_balances=True` (main loop currently uses `sync_balances=False`). **No Postgres positions table** in OMS (positions in Redis only). **Symbol sync:** At startup, `sync_symbols_from_binance` runs once to populate **symbols** from broker exchangeInfo.
- **DB columns:** See **docs/oms/OMS_ACCOUNT_DB_FIELDS.md** (accounts, balances). **balance_changes:** deposit/withdrawal/transfer only; see **docs/oms/BALANCE_CHANGES_HISTORY.md**.

**Account repairs:** `oms/account_repair.py` — `run_all_account_repairs(pg_connect)` after sync; fixes flawed fields from payload.

**Account Redis TTL:** Periodic `sync_accounts_to_postgres` does **not** set key TTL. TTL is applied from **`cleanup.set_account_key_ttl`** when **`main()`** handles a balance-change event (see §3.2).

---

## 9. Cleanup and TTL

**Module:** `oms/cleanup.py`.

- **trim_oms_streams(redis):** XTRIM risk_approved and oms_fills to MAXLEN (default 20000). cancel_requested is **not** trimmed.
- **set_order_key_ttl(redis, order_id, ttl_seconds):** Set TTL on `orders:{order_id}` after terminal status (e.g. after sync). Only applied when the key has no TTL yet. Defaults via `OMS_SYNC_TTL_AFTER_SECONDS` (default 300) in `main()` / `sync.py`.
- **set_account_key_ttl(redis, broker, account_id, ttl_seconds):** Set TTL on `account:{broker}:{account_id}` and related `:balances` / `:positions` keys (only if the key exists and has no TTL yet). **`main()`** uses this after **`write_balance_change`**; **not** invoked from periodic account sync.

---

## 10. Retry and Error Handling

- **place_order failure:** Retry count in `oms:retry:risk_approved:{entry_id}`; delivery count from XPENDING. After **`OMS_PLACE_ORDER_MAX_RETRIES`** (constant **3** in `redis_flow.py`): mark rejected, produce oms_fill reject, XACK, delete retry key, call `on_terminal_sync` if set.
- **Parse errors:** Parse failures log and skip that entry (**no XACK** — message stays in the consumer group pending list). **`risk_approved`:** `process_many` also reads pending with `id="0"`, so poison messages keep retrying until handled (e.g. `XCLAIM`/trim). **`cancel_requested`:** batch reads use `id=">` only; there is **no** matching pending sweep — invalid entries stay pending until **`XCLAIM`**, another consumer, or operational cleanup.
- **Fill callback validation:** Invalid or unknown event_type logs and skips; no store update.

---

## 11. Configuration (Environment)

| Variable | Purpose |
|----------|--------|
| REDIS_URL | Redis connection (default redis://localhost:6379). |
| BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_BASE_URL | Binance adapter (testnet vs main). |
| BINANCE_FILLS_STREAM | `wsapi` (default) or `listenkey` for user data stream (shared for fills + account events). |
| DATABASE_URL | Postgres; enables sync, `on_terminal_sync`, account listeners’ `on_balance_change`, symbol sync, and periodic loops below. |
| OMS_SYNC_TTL_AFTER_SECONDS | TTL on Redis order keys after sync (default 300). Also the default fallback for account TTL after balance change when **`OMS_ACCOUNT_TTL_AFTER_BALANCE_CHANGE_SECONDS`** is unset. |
| OMS_ACCOUNT_TTL_AFTER_BALANCE_CHANGE_SECONDS | TTL (seconds) on account Redis keys after **`balanceUpdate`** → `write_balance_change`. If unset, **`main()`** uses **`OMS_SYNC_TTL_AFTER_SECONDS`** (order sync TTL). |
| OMS_BLOCK_MS | Block timeout for risk_approved read (default 100). |
| OMS_POLL_SLEEP_SECONDS | Used only when block_ms=0. |
| OMS_BATCH_SIZE | Max messages per process_many / process_many_cancel (default 50). |
| LOG_LEVEL | e.g. DEBUG (default INFO). |

**Intervals (code constants today, not environment variables):** When `DATABASE_URL` is set, `main()` passes **`DEFAULT_SYNC_INTERVAL_SECONDS`** (60, from `oms/sync.py`) for order **`sync_terminal_orders` + repairs**, and **`DEFAULT_ACCOUNT_REFRESH_INTERVAL_SECONDS` / `DEFAULT_ACCOUNT_SYNC_INTERVAL_SECONDS`** (60 each, in `main.py`) for REST account refresh and **`sync_accounts_to_postgres`**. To change these, edit `main()` / call **`run_oms_loop`** with different arguments (or add env wiring).

**Not environment-driven:** **`OMS_PLACE_ORDER_MAX_RETRIES`** is **`3`** in **`redis_flow.py`** only.

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
- **At-least-once:** risk_approved/cancel_requested are XACK’ed only after successful processing; on crash, pending messages are redelivered. **Idempotency:** callers avoid duplicate **`stage_order`** by checking **`store.get_order(order_id)`** before staging; an existing Redis order short-circuits re-staging for the same id.
- **Event-driven + batch:** Blocking read on risk_approved for low latency; batch size and trim interval tune throughput and stream growth.
- **User-data streams before loop:** **`main()`** waits for **fill** listeners **and** **account** listeners to connect (subject to timeout), so subscriptions are up before the processing loop runs.

For DB column sources see **docs/oms/OMS_ORDERS_DB_FIELDS.md**. Blocking: `process_many` uses XREADGROUP BLOCK when `block_ms > 0` for event-driven wake-up; cancel is non-blocking.
