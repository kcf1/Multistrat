# Phase 2: Detailed Plan — OMS, Account Management, Position (Binance first)

**Goal:** End-to-end order flow: approved orders → **generic OMS** (routes to broker adapters) → first broker **Binance** → fills → Postgres orders; OMS also manages account state (balances, positions) from broker streams → Postgres accounts/balances, positions in Redis; **PMS** as source of truth for PnL/margin. The OMS is broker-agnostic and handles all broker state (orders + accounts); Binance is the first broker adapter. Minimal Risk pass-through for testing.

---

## 1. Overview

| Item | Choice |
|------|--------|
| **OMS** | Generic order router: consumes `risk_approved`, stages orders in **Redis** (hashes + indexes), dispatches to a **broker adapter** by `broker`/account; receives fills from adapter and publishes `oms_fills`. Periodically syncs orders to Postgres for audit. Extensible to more brokers later. |
| **Broker adapters** | Pluggable implementations (e.g. Binance first; others later). Each adapter: place order, subscribe to fills/rejects, report back to OMS in a unified format. |
| **First broker** | Binance (testnet first; spot or futures — decide one for Phase 2). |
| **Order path** | Redis `risk_approved` → OMS (router) → broker adapter (Binance) → broker API → fills back to OMS → Redis `oms_fills` stream (for downstream consumers); OMS syncs orders to Postgres `orders` table. |
| **Account path** | Broker user data stream (`outboundAccountPosition`, `balanceUpdate`) → OMS account listener → Redis account store → sync to Postgres (`accounts`, `balances`); positions in Redis only. Periodic REST refresh for reconciliation. |
| **OMS persistence** | **Redis** (staging: order hashes, account hashes, status indexes, broker_order_id lookup); **sync to Postgres** `orders` table (trigger on terminal status + periodic) via `oms/sync.py`; **sync to Postgres** `accounts`, `balances` (periodic) via `oms/account_sync.py`; positions in Redis only; optional repairs via `oms/repair.py` and `oms/account_repair.py`. |
| **PMS** | Reads Postgres/Redis; **source of truth** for PnL and margin |
| **Risk (Phase 2)** | Minimal: consume `strategy_orders`, pass through to `risk_approved` (or use test inject for E2E) |

---

## 2. Dependencies

- **Phase 1 complete:** Postgres, Redis, Alembic, Docker network `multistrat`, `.env` with `DATABASE_URL` and `REDIS_URL`.
- **Binance:** API key and secret; decide **spot** or **futures** (e.g. futures for margin/leverage). Use **testnet** for development.
- **Language/runtime:** Choose one for OMS, PMS, Risk (e.g. Python for consistency with Alembic; or Go/Node per service). Account management is integrated into OMS.
- **Testing approach:** Build **bottom-up** with unit tests at each layer (mock dependencies) before integration. See §12 Task checklist and §16 Testing for the order and test strategy.

---

## 3. Postgres schema (OMS / PMS)

**Note:** Postgres schema is **defined and implemented as part of OMS** (orders sync: §12.1.10, account sync: §12.2.1). PMS reads from this schema but does not modify it. This section describes the schema design; implementation happens in the OMS task checklist.

All schema changes are **Alembic revisions** (same as Phase 1). Add one or more revisions for the following.

### 3.1 Tables (minimal set)

- **accounts**  
  - `id` (PK), `account_id` (broker’s account id), `name` (display; derived as `broker:account_id` if not set), `broker` (e.g. `binance`), `created_at`, optional `config` (JSONB). *(No `env` column; dropped in revision e5f6a7b8c9d0.)*  
  - One row per broker account; unique `(broker, account_id)` for sync UPSERT.

- **orders** (audit / recovery; **populated by OMS order sync only**)  
  - See table in §3.1 above: includes `book` (strategy/book id), `comment` (freetext), generic broker fields (`executed_qty`, `time_in_force`, etc.), and Binance-specific columns with `binance_` prefix (`binance_cumulative_quote_qty`, `binance_transact_time`).  
  - OMS syncs orders to this table via `sync_one_order` / `sync_terminal_orders` (`oms/sync.py`) on terminal status (from fill callback or reject path) and periodically. Column sources: **docs/oms/OMS_ORDERS_DB_FIELDS.md**. Post-sync repairs for Binance: `oms/repair.py` (`run_all_repairs`).

- **fills**  
  - `id` (PK), `order_id` (FK to orders or internal reference), `account_id` (FK), `symbol`, `side`, `quantity`, `price`, `fee`, `fee_asset`, `broker_fill_id`, `executed_at`, `created_at`.  
  - Source of truth for what was executed; populated by downstream consumers of `oms_fills` stream (e.g. PMS or future services). OMS does not write to `fills` table.

- **positions** *(OMS: no Postgres table.)* Positions are stored in Redis only (`account:{broker}:{account_id}:positions`). The name `positions` was dropped from OMS Postgres schema for use by PMS (Position Management System). See revision d4e5f6a7b8c9.

- **balances**  
  - `id` (PK), `account_id` (FK), `asset`, `available`, `locked`, `updated_at`.  
  - OMS account sync updates from broker balance events (`outboundAccountPosition`, `balanceUpdate`) and periodic REST refresh.  
  - **Current state only:** Stores current balance per asset; historical changes tracked in `balance_changes` table.

- **balance_changes** (historical tracking)  
  - `id` (PK), `account_id` (FK), `asset`, `change_type` (deposit, withdrawal, transfer, adjustment, snapshot), `delta` (NUMERIC: positive for deposit, negative for withdrawal), `balance_before` (NUMERIC NULL), `balance_after` (NUMERIC NULL), `event_type` (balanceUpdate, outboundAccountPosition, manual_adjustment), `broker_event_id` (TEXT NULL), `event_time` (TIMESTAMPTZ), `created_at` (TIMESTAMPTZ), `payload` (JSONB NULL).  
  - **Historical record:** Tracks all balance changes (deposits, withdrawals, transfers, adjustments). Populated from `balanceUpdate` events via main's `on_balance_change` callback (writes to Postgres and sets TTL on Redis account keys only for balance change events).  
  - Indexes: `(account_id, asset, event_time)`, `(account_id, change_type)`, `event_time`.

*(**margin_snapshots** table was dropped in revision f6a7b8c9d0e1; unused. PMS can use a dedicated `pnl_snapshots` or Redis keys if needed.)*

**orders** — Extended columns for book (strategy) and freetext comment. Broker-agnostic columns for routing/audit; **Binance-specific** columns use the `binance_` prefix so other brokers can add their own (e.g. `bybit_*`) without collision.

| Column | Type | Source | Notes |
|--------|------|--------|--------|
| `id` | BIGSERIAL PK | internal | |
| `account_id` | FK → accounts | risk_approved / config | |
| `internal_id` | TEXT UNIQUE | OMS (UUID) | Our order id; sent as clientOrderId to Binance |
| `broker` | TEXT | risk_approved | e.g. `binance` |
| `broker_order_id` | TEXT | broker response | Broker’s order id (e.g. Binance `orderId`) |
| `symbol` | TEXT | risk_approved / broker | |
| `side` | TEXT | risk_approved / broker | BUY/SELL |
| `order_type` | TEXT | risk_approved / broker | MARKET, LIMIT, … |
| `quantity` | NUMERIC | risk_approved / broker | |
| **`price`** | NUMERIC NULL | broker / fills | **Executed (average fill) price**; from broker order response `avgPrice` or fill events. |
| **`limit_price`** | NUMERIC NULL | risk_approved / broker | **Order limit price** (for LIMIT orders); sent in place_order request; from risk_approved or broker response. |
| `time_in_force` | TEXT NULL | broker | GTC, IOC, FOK (generic) |
| `status` | TEXT | broker | Normalized or broker-specific status |
| `executed_qty` | NUMERIC NULL | broker | Filled quantity (generic) |
| **`binance_cumulative_quote_qty`** | NUMERIC NULL | Binance `cumulativeQuoteQty` | Quote asset amount (Binance-specific) |
| **`binance_transact_time`** | BIGINT NULL | Binance `transactTime` | Order time in ms (Binance-specific) |
| **`book`** | TEXT NULL | risk_approved | Strategy / book identifier (e.g. `ma_cross`, `manual`) |
| **`comment`** | TEXT NULL | risk_approved | Freetext comment for the order |
| `created_at` | TIMESTAMPTZ | internal | |
| `updated_at` | TIMESTAMPTZ | internal | |
| `payload` | JSONB NULL | optional | Full broker response or extra; e.g. `payload.binance` for raw Binance blob |

Indexes: `account_id`, `symbol`, `(account_id, book)`, `created_at`, `broker_order_id` (unique per broker if needed).

### 3.2 Order schemas: Binance response → internal mapping

Binance **place_order** returns (relevant): `orderId`, `symbol`, `status`, `clientOrderId`, `side`, `type`, `origQty`, `price`, `executedQty`, `timeInForce`, `transactTime`, and optionally `avgPrice`, `cumulativeQuoteQty`.  
Binance **query_order** adds: `cumulativeQuoteQty`, `origQty`, etc.  
Binance **cancel_order** returns same shape as query.

- **Generic columns** (used for any broker): `internal_id`, `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, **`price`** (executed / average fill), **`limit_price`** (order limit price), `time_in_force`, `status`, `executed_qty`.
- **Price semantics:** In **stored orders** and **broker order response**: `price` = executed (average fill) price; `limit_price` = order limit price. In **risk_approved** input, the optional field is the limit price (stored as `limit_price`). In **oms_fills** events, `price` remains the fill (executed) price per event.
- **Binance-specific columns** (prefix `binance_`): `binance_cumulative_quote_qty` ← Binance `cumulativeQuoteQty`; `binance_transact_time` ← Binance `transactTime`. Other brokers get their own prefix (e.g. `bybit_*`).
- Internal `internal_id` = our UUID; we send it as `newClientOrderId` → Binance returns it as `clientOrderId`.
- Internal `broker_order_id` = Binance `orderId`.
- `book` and `comment` are **not** sent to Binance; they are carried from `risk_approved` through OMS and stored in Postgres/Redis for audit and strategy attribution.

### 3.3 Balance changes table schema

**balance_changes** — Historical tracking of deposits, withdrawals, transfers, and balance adjustments.

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `id` | BIGSERIAL PK | internal | |
| `account_id` | FK → accounts | account event | |
| `asset` | TEXT | balanceUpdate / outboundAccountPosition | Asset symbol (e.g. USDT, BTC) |
| `change_type` | TEXT | derived | `deposit`, `withdrawal`, `transfer`, `adjustment`, `snapshot` |
| `delta` | NUMERIC | balanceUpdate `d` field | Amount changed: positive for deposit, negative for withdrawal |
| `balance_before` | NUMERIC NULL | calculated | Balance before change (optional, for audit) |
| `balance_after` | NUMERIC NULL | calculated | Balance after change (optional, for audit) |
| `event_type` | TEXT | account event | `balanceUpdate`, `outboundAccountPosition`, `manual_adjustment` |
| `broker_event_id` | TEXT NULL | broker event | Broker's event/trade ID if available |
| `event_time` | TIMESTAMPTZ | broker event `E` or `T` | Timestamp from broker event (ms converted to timestamp) |
| `created_at` | TIMESTAMPTZ | internal | When record was created in our system |
| `payload` | JSONB NULL | broker event | Raw broker event blob (for repairs and audit) |

**Change type derivation:**
- `deposit`: `balanceUpdate` with positive `delta` (d > 0)
- `withdrawal`: `balanceUpdate` with negative `delta` (d < 0)
- `transfer`: `balanceUpdate` during account transfers (may need broker-specific logic)
- `adjustment`: Manual adjustments or broker corrections
- `snapshot`: From `outboundAccountPosition` (full account snapshot; delta = 0 or calculated from previous state)

**Indexes:** `(account_id, asset, event_time)` for historical queries, `(account_id, change_type)` for filtering deposits/withdrawals, `event_time` for time-range queries.

**Populated by:** OMS main wires `on_balance_change` when `DATABASE_URL` is set: on each `balanceUpdate` event the callback writes to `balance_changes` (via `write_balance_change`) and sets TTL on Redis account keys. Periodic sync does not set TTL.

### 3.4 Alembic workflow

- Create revision: `alembic revision -m "add_account_tables"` (or split). Note: `orders` table is synced by OMS order sync (`oms/sync.py`); `accounts`, `balances`, `balance_changes` by OMS account sync (`oms/account_sync.py`). No `positions` or `margin_snapshots` table in OMS Postgres (positions in Redis only; margin_snapshots dropped in f6a7b8c9d0e1).
- Implement `upgrade()` and `downgrade()` in the new file under `alembic/versions/`.
- Run `alembic upgrade head` after deploying the revision.

### 3.4 Price vs limit_price — detailed implementation plan

**Goal:** In stored orders and broker order response, **`price`** = executed (average fill) price; **`limit_price`** = order limit price. `oms_fills` already uses `price` as fill price; no change there.

| Layer | Current behaviour | Change |
|-------|-------------------|--------|
| **risk_approved (input)** | Optional `price` = limit for LIMIT orders | Document as limit price; in code store in order as **`limit_price`**. |
| **Consumer parse** | Sets `order["price"]` from stream | Set **`order["limit_price"]`** from stream; do not set `order["price"]` from input (filled from broker/fills). |
| **Redis order hash** | `price` only | Add **`limit_price`**; **`price`** = executed (from broker response or fill updates). |
| **Binance adapter unified response** | `price` = Binance `price` (limit) | **`limit_price`** = Binance `price`; **`price`** = Binance `avgPrice` or derived from `cumulativeQuoteQty`/`executedQty` when filled. |
| **place_order request** | `order.get("price")` for REST param | Use **`order.get("limit_price") or order.get("price")`** for limit sent to broker. |
| **Fills listener / oms_fills** | `price` = fill price | No change (already executed price). |
| **Postgres `orders`** | `price` column | Keep **`price`** = executed; add column **`limit_price`** (NUMERIC NULL). |
| **Sync _order_to_row** | Maps `price` only | Map **`price`** and **`limit_price`**; INSERT/UPDATE both columns. |

**File-level changes:**

1. **`oms/schemas.py`** — In comments: risk_approved `price` = limit price; stored order and oms_fills: `price` = executed, `limit_price` = limit.
2. **`oms/consumer.py`** — In `parse_risk_approved_message`: set **`order["limit_price"]`** from `fields.get("price")` when present; do not set `order["price"]` from stream.
3. **`oms/storage/redis_order_store.py`** — In `stage_order`: add **`limit_price`** from `order_data`; keep **`price`** from `order_data` only if present (e.g. from broker response). In `update_status` / `extra_fields`: accept **`limit_price`** and **`price`**. In **_unflatten_order**: treat **`limit_price`** as numeric.
4. **`oms/brokers/binance/adapter.py`** — In **binance_order_response_to_unified**: map Binance **`price`** → **`limit_price`**; map Binance **`avgPrice`** → **`price`** (executed); if `avgPrice` missing and `cumulativeQuoteQty`/`executedQty` present with executedQty > 0, set **`price`** = cumulativeQuoteQty / executedQty. In **place_order**: send limit as **`order.get("limit_price") or order.get("price")`**.
5. **`oms/redis_flow.py`** — After place_order, pass **`price`** and **`limit_price`** from unified response into store `extra_fields`. Fill callback: when updating from fill event, pass event **`price`** (executed) into store update as **`price`**.
6. **`oms/sync.py`** — In **_order_to_row**: **`row["limit_price"] = order.get("limit_price")`**; **`row["price"] = order.get("price")`**. In sync SQL: add **limit_price** column to INSERT/ON CONFLICT.
7. **Alembic** — New revision: add **`limit_price`** (NUMERIC NULL) to **orders** table.
8. **Tests** — **test_consumer.py**: assert parsed order has **limit_price** (not `price` from stream). **test_redis_order_store.py**: stage with **limit_price**; update with **price** and **limit_price**. **test_adapter.py**: mock response with **price** (limit) and **avgPrice** (executed); assert unified **limit_price** and **price**. **test_sync.py**: _order_to_row and sync with **price** and **limit_price**. Integration/testnet tests: use **limit_price** for limit orders where applicable.

**Task:** Implement as 12.1.12 (or follow-on) after 12.1.11b; unit tests per component, then integration check.

---

## 4. Redis Streams

**Reference:** Stream names and consumer groups are implemented in OMS as constants in `oms/schemas.py` (`RISK_APPROVED_STREAM`, `CANCEL_REQUESTED_STREAM`, `OMS_FILLS_STREAM`) and used via `oms/streams.py`. See **docs/oms/OMS_ARCHITECTURE.md** §2.

### 4.1 Stream names

| Stream | Producer | Consumer | Purpose |
|--------|----------|----------|---------|
| `strategy_orders` | Strategies (Phase 5) or test inject | Risk | Order intents from strategies |
| `risk_approved` | Risk | OMS | Orders approved (or pass-through in P2). Consumer group `oms`, XREADGROUP + XACK. Trimmed by OMS. |
| `cancel_requested` | Risk / Admin | OMS | Cancel by order_id or (broker_order_id + symbol). Consumer group `oms`. Not trimmed. |
| `oms_fills` | OMS | Downstream (PMS, future services) | Fill, reject, cancelled, and expired events. Trimmed by OMS. |

### 4.2 Message schemas (JSON)

**Note:** Schemas are **defined and implemented per service** (see §12 Task checklist). OMS defines `risk_approved` input and `oms_fills` output; downstream consumers of `oms_fills` must match OMS output schema. OMS also owns account management schemas (Redis account store, Postgres accounts/balances/positions). This ensures each service owns its contract.

**risk_approved (order to execute)** — defined in OMS (12.1.5)  
- `order_id` (internal UUID or string; optional, OMS can generate)  
- `broker` (e.g. `binance`) — OMS uses this to select the broker adapter.  
- `account_id` (optional; default account if single)  
- `symbol`, `side` (BUY/SELL), `quantity`, `order_type` (MARKET, LIMIT, etc.)  
- `price` (optional; **limit price** for LIMIT orders — stored as `limit_price` in order; executed price comes from broker/fills).  
- `time_in_force` (optional; GTC, IOC, FOK)  
- **`book`** (optional) — Strategy / book identifier (e.g. `ma_cross`, `manual`) for attribution.  
- **`comment`** (optional) — Freetext comment; stored with order in Redis and Postgres.  
- `strategy_id` (optional), `created_at` (ISO)

**oms_fills (fill / reject / cancelled / expired)** — defined in OMS (12.1.5), consumed by downstream services (e.g. PMS, future services). Validation: `OmsFillEvent` in `oms/schemas_pydantic.py`.  
- `event_type`: `fill` | `reject` | `cancelled` | `expired`  
- `order_id` (internal), `broker_order_id`, `symbol`, `side`, `quantity`, **`price`** (executed/fill price for this event), `fee`, `fee_asset`  
- `executed_at` (ISO), `fill_id` (broker), optional `reject_reason` for rejections  
- **`book`** (optional) — Pass-through from order for attribution.  
- **`comment`** (optional) — Pass-through from order for audit.  

Note: OMS account management receives account state directly from broker streams, not from `oms_fills`. Downstream consumers of `oms_fills` may use fill events for their own purposes (e.g. PMS for PnL calculations).

Each service documents its schemas in code or a service README (e.g. `oms/README.md`). Streams are created on first XADD.

### 4.3 Consumer groups (optional)

For scaling, use Redis consumer groups (XREADGROUP) per stream so each message is processed once. For Phase 2 solo dev, a single consumer with XREAD (block) is enough.

---

## 5. OMS service (generic)

The OMS is **broker-agnostic**. It routes orders to the appropriate broker adapter and emits a unified fill/reject stream.

### 5.1 Role

- Consume from Redis stream `risk_approved` using a **consumer group** (XREADGROUP + XACK) so each message is delivered once; no double-processing when OMS reads again. Create group with XGROUP CREATE … MKSTREAM if needed; XACK after successful process.
- For each message: read `broker` (and optional `account_id`); **select broker adapter** for that broker; generate internal `order_id` if not set; persist to **Redis** (order hash, status indexes), including `broker` and `account_id`.
- Call the adapter’s **place_order** (or equivalent); adapter talks to the broker API; OMS updates Redis (e.g. status `sent`).
- Adapter notifies OMS of **fills** and **rejects** (unified format); OMS publishes to Redis stream `oms_fills` and updates Redis order status (`filled` / `rejected` / `cancelled`).
- **Partial/full fill:** Listener (Binance) exposes broker order status (`X`: PARTIALLY_FILLED, FILLED) and cumulative executed qty (`z`) in the unified event. OMS callback maps `X` to Redis status `partially_filled` or `filled`; uses cumulative qty when present, otherwise accumulates per-fill quantity so `executed_qty` in Redis is always cumulative.
- **Periodic sync:** Background job syncs orders from Redis to Postgres `orders` table (completed orders, or all); supports audit, reconciliation, and recovery.
- If no adapter exists for the message’s `broker`, log and optionally publish a reject to `oms_fills` or a dead-letter stream.

### 5.2 Redis staging schema (OMS, broker-agnostic)

- **orders:{order_id}** — Hash: `internal_id`, `broker`, `account_id`, `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, **`price`** (executed / average fill), **`limit_price`** (order limit for LIMIT orders), `time_in_force`, `status` (pending, sent, **partially_filled**, filled, rejected, cancelled), **`book`**, **`comment`**, `created_at`, `updated_at`, optional **`executed_qty`** (cumulative), **`binance_cumulative_quote_qty`**, **`binance_transact_time`**, `payload` (JSON). Broker-agnostic fields plus `binance_*` when broker is Binance; other brokers use their own prefix in payload or separate keys.
- **orders:by_status:{status}** — Set of order_id; e.g. `orders:by_status:pending`, `orders:by_status:partially_filled`, `orders:by_status:filled`.
- **orders:by_book:{book}** — Set of order_id; list orders by strategy/book.
- **orders:by_broker_order_id:{broker_order_id}** — String value = order_id; O(1) lookup when fill event only has broker order id.
- Use pipelines for atomic multi-key updates. No table structure; Redis hashes are O(1) for get/set.

### 5.3 Broker adapter interface

- Adapters implement a common interface (e.g. `place_order(order)`, `start_fill_listener(callback)`, or `get_fills_since(...)`). OMS calls the adapter for the message’s `broker`.
- **Place order:** adapter returns broker order id (or reject).  
- **Fills:** adapter delivers fill/reject events to OMS in a **unified shape** (symbol, side, quantity, price, fee, executed_at, fill_id, event_type fill|reject) so OMS can always publish the same `oms_fills` schema.
- Registry: OMS holds a map `broker_name → adapter instance` (or factory). Binance is registered as the first adapter.

### 5.4 Config (generic)

- `REDIS_URL` (required). `DATABASE_URL` (optional; for periodic sync of orders to Postgres). Per-broker config (e.g. Binance API keys) is loaded by the adapter or via OMS config keyed by broker/account.

### 5.5 Deployment

- Single **loop process**: XREADGROUP `risk_approved` (consumer group) → Redis order store → route to adapter → handle adapter callbacks for fills/rejects → Redis status update → XADD `oms_fills` → XACK. Optional background task: sync Redis orders to Postgres (e.g. every 30s or on terminal status).
- Docker: service `oms`; image from `oms/`; connect to `multistrat` network; env: `REDIS_URL`, `DATABASE_URL` (if sync enabled). No broker-specific env in the generic OMS image if adapters load their own config (or pass broker vars through env with a prefix, e.g. `BINANCE_*`).

---

## 6. Broker adapter: Binance (first implementation)

Binance is the **first broker adapter** plugged into the generic OMS. Other brokers (e.g. Bybit, IBKR) can be added later with the same adapter contract.

### 6.1 Role

- Implement the OMS broker adapter interface: place order (REST), report fills/rejects (websocket or poll).
- **Place order:** call Binance API (spot or futures); return broker order id or reject reason.
- **Fills:** subscribe to user data stream (websocket) or poll “my trades” / order status; convert Binance execution reports into the unified fill/reject format and pass to OMS.

### 6.2 Binance client details

- **Auth:** HMAC-SHA256 signed requests (API key + secret). Support testnet and production base URLs (e.g. `https://testnet.binance.vision` vs `https://api.binance.com` for spot; futures URLs similarly).
- **Endpoints:** Place order (POST), query order (GET), cancel (DELETE). For futures: `/fapi/v1/order` etc.
- **Fills:** Prefer **user data stream** (websocket) for execution reports; fallback to polling.
- **Ids:** Send `clientOrderId` = internal `order_id`; store `orderId` from Binance for reconciliation.

### 6.3 Config

- `BINANCE_BASE_URL`, `BINANCE_API_KEY`, `BINANCE_API_SECRET`, `BINANCE_TESTNET` (bool). Optional: per-account keys if multiple Binance accounts. Add to `.env.example` and `.env`; Binance adapter reads from env (or OMS passes through).

---

## 7. Account Management (integrated into OMS)

**Note:** Booking service (separate consumer of `oms_fills`) is **discarded**. Account management (balances, positions) is **integrated into OMS** instead. This consolidates broker state management in one service and eliminates duplicate broker connections.

**Context:** OMS extends broker adapters to receive account events from the same user data stream that provides fills. OMS owns syncing both **orders** and **accounts/balances/positions** to Postgres. See **docs/oms/OMS_ARCHITECTURE.md** and **docs/oms/ACCOUNT_MANAGEMENT_INTEGRATION.md** for architecture details.

### 7.1 Role (within OMS)

- **Account event listener:** Receive account/balance/position events from broker user data stream (`outboundAccountPosition`, `balanceUpdate`) via broker adapter. Same WebSocket connection as fills listener (multiplexed by event type).
- **Periodic REST refresh:** Call `adapter.get_account_snapshot(account_id)` periodically (e.g. every 60s) to reconcile and backfill account state.
- **Redis account store:** Store account metadata, balances, positions in Redis (`account:{broker}:{account_id}`, `...:balances`, `...:positions`).
- **Sync to Postgres:** Periodically sync accounts/balances/positions from Redis to Postgres (`accounts`, `balances`, `positions` tables). Same pattern as order sync: trigger on update + periodic sync.
- **Repairs:** Fix flawed account/balance/position fields from payload (similar to order repairs).

### 7.2 Data Sources

- **Event-driven:** Broker user data stream events (`outboundAccountPosition` = full snapshot, `balanceUpdate` = single-asset delta).
- **Periodic:** REST account snapshot (`get_account()`, `get_futures_account()` for futures) for reconciliation.

### 7.3 Idempotency

- Use `updated_at` or event_id to avoid overwriting newer stream data with older periodic snapshot. Account updates are applied by key (asset, symbol_side).

### 7.4 Deployment

- Integrated into OMS process; no separate service. OMS main loop handles both orders and accounts.

---

## 8. PMS service

**Core vs optional:** See **docs/pms/PMS_ARCHITECTURE.md** §2. **Core** = derive positions from orders → compute PnL (realized + unrealized, one mark source) → one granular table (`pms_positions`) + Redis (`pnl:{account_id}`, `margin:{account_id}`) → timer loop. **Optional** = fills table, symbols table, book_allocations, pnl_snapshots, margin_snapshots, book_cash, repairs, multiple mark price sources, Pydantic. Implement **core** first; add optional as needed.

**Data-model notes:** Order **symbol** (pair, e.g. BTCUSDT) vs balance **asset** (single asset, e.g. BTC, USDT) are not the same; building positions from orders and reconciling two tables (e.g. order-derived positions vs broker positions, balance_changes vs balances) are documented in **docs/pms/PMS_DATA_MODEL.md**.

### 8.1 Role

- Periodically (e.g. every few seconds) read **orders** and **balances** from **Postgres**; **derive positions from the orders table** (PMS does **not** read OMS Redis positions — that path is dummy). Compute **realized PnL** (from fills), **unrealized PnL** (from current mark price if available), and **margin** (for futures).
- Write results to Postgres (e.g. a dedicated `pnl_snapshots` table) and/or to Redis (e.g. `pnl:{account_id}`, `margin:{account_id}`) for Admin/UI. *(OMS `margin_snapshots` table was dropped; unused.)*

### 8.2 Mark price (unrealized PnL)

- **Interface first:** Define a **mark price provider interface** (e.g. get mark prices per symbol). PMS depends only on this contract; implementations are swappable via config.
- **Phase 2:** Implement the interface by **wrapping Binance** (REST or WebSocket). PMS gets real unrealized PnL without a separate Market Data service.
- **Phase 4:** Add an implementation that **reads from Redis or DB** (fed by the Market Data service). Switch with `PMS_MARK_PRICE_SOURCE` (e.g. `binance` vs `redis` or `market_data`). No change to PMS calculation logic.

### 8.3 Granular store, views, and reference data

- **One granular store (core):** PMS writes a **position/snapshot table** at grain **(account_id, book, symbol, side)** (e.g. `pms_positions`). One row per grain; updated after each calculation. **Grouping** (by symbol, by book/symbol, by broker/symbol) is **on request** (SQL GROUP BY or API); optional DB VIEWs on PMS-written tables. See **docs/pms/PMS_ARCHITECTURE.md** §6.
- **Reference data (optional for core):** **Symbols table** (symbol → base_asset, quote_asset, tick_size, etc.): populated by sync or config; PMS reads. **Allocations** (cash by book): **managed outside**; system **provided with** via interface (table, API, or file); PMS reads only.
- **Cash vs positions:** Cash stays in OMS **balances** table; do not put cash in positions table. **Capital by book** = asset_value(book) + cash(book); cash(book) from allocation or internal book_cash ledger (optional).

### 8.4 Deployment

- Loop process; Docker service `pms`; env: `DATABASE_URL`, `REDIS_URL`; connect to `multistrat` network.

---

## 9. Risk service (minimal for Phase 2)

### 9.1 Role

- Consume from Redis stream `strategy_orders` (or skip and use **test inject** only).
- **Pass-through:** For each message, optionally validate (e.g. symbol format); then publish the same (or enriched) payload to `risk_approved`. No position/margin checks yet.
- Enables the pipeline Strategy → Risk → OMS for Phase 5; in Phase 2 you can **inject test orders** directly into `risk_approved` to drive OMS/PMS.

### 9.2 Test inject

- Small script or Redis CLI: XADD into `risk_approved` with a JSON body including `broker: "binance"`, symbol, side, quantity, order_type, etc., so OMS routes to the Binance adapter and sends to testnet. No Strategy or Risk process required for E2E.

### 9.3 Deployment

- Optional in Phase 2: run Risk as a loop process, or omit and use only test inject for E2E.

---

## 10. Binance-specific notes

- **Spot vs Futures:** Choose one for Phase 2 (e.g. **futures** for margin/leverage). Use the corresponding base URL and endpoints (e.g. `/fapi/v1/order` for futures).
- **Order types:** Implement at least MARKET; add LIMIT and reduce-only (futures) as needed.
- **Testnet:** Use Binance testnet for API keys and base URL; no real funds. For API key, signature, and symbol filter rules (e.g. -2014, -1022, -1013), see **docs/BINANCE_API_RULES.md**.
- **Rate limits:** Respect Binance weight/limit; back off or queue if needed.
- **Reconciliation:** Periodically compare OMS Redis orders and Postgres `orders`/fills with Binance “my trades” or order history.

---

## 11. Environment and config

### 11.1 `.env.example` additions

```env
# Binance (first broker adapter)
BINANCE_TESTNET=true
BINANCE_BASE_URL=https://testnet.binance.vision
BINANCE_API_KEY=
BINANCE_API_SECRET=

# Optional: account id used by OMS if multiple accounts
# DEFAULT_ACCOUNT_ID=1
```

### 11.2 Per-service config

- OMS (generic): `REDIS_URL`, `DATABASE_URL` (optional, for order sync to Postgres). Broker-specific vars (e.g. Binance) are used by the Binance adapter.
- Binance adapter: `BINANCE_*` vars (or passed from OMS env).
- OMS account management: `REDIS_URL`, `DATABASE_URL` (same as OMS orders).
- PMS: `DATABASE_URL`, `REDIS_URL`.

---

## 12. Task checklist (implementation order)

**Bottom-up approach:** Build and unit-test each layer before integrating. Start with the lowest-level component (Binance API), then build up: adapter → OMS pieces (orders + accounts) → OMS integration → PMS pieces → PMS integration. Each step should have unit tests before moving to the next.

**Schema ownership:** Each service defines and implements its own schemas (Redis structures, Postgres, Redis streams, Redis cache keys) as part of its implementation. OMS owns Redis order staging keys, Redis account store keys, `risk_approved` input and `oms_fills` output schemas, Postgres `orders` table sync, and Postgres `accounts`/`balances` sync (positions in Redis only); PMS owns its read/write schemas (PnL/margin snapshots).

### 12.1 OMS (build backwards: Binance API → adapter → OMS pieces → integration)

- [x] **12.1.1** **Binance API client** (lowest level): HTTP client for Binance REST (place order, query order, cancel); HMAC-SHA256 signing; support testnet/production URLs. **Server-time sync:** GET `/api/v3/time` to align request timestamp with Binance (avoids -1021); offset refreshed periodically; on fetch failure existing offset is preserved. **Unit test:** mock HTTP responses; verify signing, request format, error handling.
- [x] **12.1.2** **Binance fills listener** (websocket or poll): subscribe to user data stream (default: WebSocket API `userDataStream.subscribe.signature`) or classic listenKey; parse execution reports to unified fill/reject. **Time sync:** subscribe message uses same client time offset as REST; listener pre-syncs before connecting. See `docs/BINANCE_API_RULES.md` §6 for ws-api connection failure reasons. **Unit test:** mock websocket messages or REST responses; verify fill/reject parsing.
- [x] **12.1.3** **Binance broker adapter** (uses Binance client): implement adapter interface (`place_order`, `start_fill_listener(callback)`, `stop_fill_listener()`); convert Binance responses to unified fill/reject format. **Unit test:** mock Binance client and create_fills_listener; verify adapter calls client correctly, formats output, and stop_fill_listener clears listener. **Testnet:** adapter place_order then cancel; adapter fill listener receives fill then stop.
- [x] **12.1.4** **OMS Redis order store**: define Redis key layout (`orders:{order_id}` hash, `orders:by_status:{status}` set, `orders:by_broker_order_id:{broker_order_id}` for lookup); implement stage_order, update_status, update_fill_status, get_order, find_order_by_broker_order_id. Use pipelines for atomic updates. **Adapter alignment:** Order hash holds risk_approved fields plus adapter place_order response (`broker_order_id`, `status`, `executed_qty`, `binance_transact_time`, `binance_cumulative_quote_qty`); `find_order_by_broker_order_id` supports fill callback lookup when event has only `broker_order_id`; `update_fill_status` for status/executed_qty on fill/reject. See §5.2. **Unit test:** fakeredis or mock Redis; verify CRUD and index updates.
- [x] **12.1.5** **OMS Redis stream schemas** (input/output): define `risk_approved` input schema (`broker`, `account_id`, `symbol`, `side`, `quantity`, `order_type`, `price`, `book`, `comment`, etc.); define `oms_fills` output schema (`event_type`, `order_id`, `broker_order_id`, `symbol`, `side`, `quantity`, `price`, `fee`, `fee_asset`, `executed_at`, `fill_id`, `reject_reason`, `book`, `comment`, etc.). **Adapter alignment:** risk_approved fields match adapter place_order input; oms_fills fields match adapter fill/reject callback (book/comment added by OMS from order store when publishing). Align with §3.1 and §4.2. Document in code or `oms/README.md`. Streams are created on first XADD.
- [x] **12.1.6** **OMS Redis consumer** (XREAD from `risk_approved`): parse messages per `risk_approved` schema, handle blocking read. **Unit test:** mock Redis client; verify message parsing and error handling.
- [x] **risk_approved consumer group:** Use XREADGROUP + XACK so each message is delivered once; XGROUP CREATE with MKSTREAM; XACK after process_one success. Prevents double-processing when OMS reads again; extendable for multiple OMS. **Unit test:** consumer group read/ack; **integration test:** no double-process (read two messages in order, second read returns next message not first).
- [x] **12.1.7** **OMS broker adapter registry**: interface definition; registry (map `broker_name` → adapter); route by `broker`. **Unit test:** mock adapters; verify routing and error handling (unknown broker).
- [x] **12.1.8** **OMS Redis producer** (XADD to `oms_fills`): format unified fill/reject events per `oms_fills` schema. **Unit test:** mock Redis client; verify message format.
- [x] **12.1.9** **OMS integration** (wire pieces): Redis consumer → Redis order store → adapter registry → adapter.place_order → adapter fill callback → Redis status update → Redis producer. **Integration test:** mock Redis streams and adapter; verify full flow for one order.
- [x] **Partial/full fill (OMS + Binance listener):** Listener exposes Binance `order_status` (X) and `executed_qty_cumulative` (z); OMS callback maps to Redis status `partially_filled` | `filled` and stores cumulative `executed_qty` (or accumulates when z absent). See §5.1. **Unit test:** fills listener parser (order_status, executed_qty_cumulative); OMS integration (partial then full, accumulate fallback).
- [x] **12.1.9a** **Config:** Add `RUN_BINANCE_TESTNET=0` to `.env.example` so testnet test gate is documented. **Test:** N/A (doc/config).
- [x] **12.1.9b** **Redis/stream cleanup:** XTRIM `risk_approved` and `oms_fills` (e.g. MAXLEN ~ 10000); optional TTL on terminal order keys (e.g. after sync or on status filled/rejected/cancelled). **Unit test:** `oms/tests/test_cleanup.py` (fakeredis): trim_oms_streams trims both streams, respects maxlen/flags; set_order_key_ttl sets EXPIRE on order key, returns False when key missing. **Call sites (in task list):** main loop (12.1.11b) calls `trim_oms_streams(redis)` periodically (e.g. each N iterations or timer); fill callback or Postgres sync calls `set_order_key_ttl(redis, order_id, ttl_seconds)` when order reaches terminal status (filled/rejected/cancelled/expired).
- [x] **12.1.9c** **CANCELED/EXPIRED handling:** Listener parses executionReport with exec_type CANCELED or order_status EXPIRED; return unified event (e.g. event_type `cancelled`); OMS callback updates Redis status to `cancelled`/`expired` and optionally publishes to `oms_fills`. When callback sets any terminal status (filled/rejected/cancelled/expired), optionally call `set_order_key_ttl(redis, order_id, ttl_seconds)` (12.1.9b). **Unit test:** `test_fills_listener.py` parser returns event for CANCELED/EXPIRED; **integration test:** `test_oms_integration.py` fill callback updates store to cancelled/expired and publishes to oms_fills.
- [x] **12.1.9d** **process_one error handling:** When `adapter.place_order` raises, do not XACK (message stays in PEL). After max retries (Redis PEL delivery count + per-entry INCR `oms:retry:risk_approved:{entry_id}`): update store to rejected, publish reject to `oms_fills`, then XACK (Option B). **Integration test:** `test_oms_integration_place_order_raises_retry_then_reject_after_max` (XREAD path with `consumer_group=None`; FakeRedis XREADGROUP `">"` is buggy so CG path tested with real Redis / testnet).
- [x] **12.1.9e** **Cancel order:** Add `cancel_order(broker_order_id, symbol)` to broker adapter interface; Binance adapter calls client.cancel_order; OMS (or admin) can call adapter.cancel_order for open orders. **Unit test:** `test_adapter.py` TestBinanceBrokerAdapterCancelOrder (mock client, correct args, API error, missing args); **testnet:** `test_testnet.py` test_adapter_place_order_then_cancel uses adapter.cancel_order.
- [x] **12.1.9f** **OMS consumes cancel command from Redis:** New stream `cancel_requested` (schema: `order_id` and/or `broker_order_id`, `symbol`, `broker`). Consumer reads one message; resolve order from store (by order_id or broker_order_id); get adapter by broker; call `adapter.cancel_order(broker_order_id, symbol)`; update store to `cancelled`; optionally publish `cancelled` to `oms_fills`. Main loop (12.1.11b) can call `process_one_cancel` after `process_one` (or on a separate iteration). **Unit test:** `oms/tests/test_cancel_consumer.py` parse + read_one_cancel_request; **integration test:** `test_oms_integration_process_one_cancel_from_redis` and `test_oms_integration_process_one_cancel_by_broker_order_id` (stage order, XADD cancel_requested, process_one_cancel → store cancelled, oms_fills).
- [x] **12.1.10** **OMS → Postgres order sync**: Sync on trigger (terminal status from fill callback or process_one reject) and/or every 60s via `sync_terminal_orders`. UPSERT by `internal_id`; after sync, set TTL on Redis key so it expires. **Implementation:** `oms/sync.py` (`sync_one_order`, `sync_terminal_orders`, `get_terminal_order_ids`); Alembic revision `orders` table; `make_fill_callback(..., on_terminal_sync)` and `process_one(..., on_terminal_sync)` for trigger; caller runs `sync_terminal_orders` every 60s for periodic. **Unit test:** `oms/tests/test_sync.py` (UPSERT, idempotent, TTL after sync).
- [x] **12.1.11a** **Fill listener started by OMS:** Bootstrap code starts fill listener for each registered adapter (e.g. one callback from `make_fill_callback(redis, store)` per adapter). **Integration test:** OMS bootstrap starts listener; inject order; assert fill path (mock or testnet).
- [x] **12.1.11b** **OMS main loop and Binance registration:** Runnable entrypoint (e.g. `oms/main.py`) that loads REDIS_URL, creates store and registry, registers Binance adapter, starts fill listener(s), then loop: `process_one(redis, store, registry, block_ms=5000)` until shutdown. In the loop (or on a timer), call `trim_oms_streams(redis)` periodically to cap stream length (12.1.9b). Add `BINANCE_*` to `.env.example` (already present); document in README. **Integration test:** run main with fakeredis + mock adapter; inject one message to `risk_approved`; assert order in store and/or oms_fills. **E2E:** run OMS with real Redis + testnet; inject script; assert fill on `oms_fills`.
- [x] **12.1.12** **Price vs limit_price (order and broker response):** In stored orders and broker order response, **`price`** = executed (average fill) price; **`limit_price`** = order limit price. See §3.4 for full plan. **Changes:** consumer parse risk_approved → `limit_price`; Redis order store + Binance adapter (unified: `limit_price` from Binance `price`, `price` from `avgPrice` or derived); place_order request uses `limit_price` or `price`; sync + Postgres add `limit_price` column; fill callback/store update `price` from fill event. **Unit tests:** consumer, store, adapter, sync; **integration:** limit order has `limit_price` and after fill `price` populated.
- [x] **12.1.13** **OMS logging:** Structured logging (loguru) across OMS: **main** (startup/shutdown, brokers, fill listeners, trim); **redis_flow** (process_one: message read, place/reject, retries; fill callback: event + terminal status; process_one_cancel: read, cancel result); **sync** (sync_one_order, sync_terminal_orders count, failures); **consumer/cancel_consumer** (parse errors); **producer** (DEBUG: produce_oms_fill); **redis_order_store** (DEBUG: stage_order, update_status). INFO for business events, WARNING for recoverable issues, DEBUG for audit. **Implementation:** `oms/log.py` (loguru logger), `loguru` in requirements.
- [x] **12.1.14** **Add Pydantic dependency:** Add `pydantic>=2.0.0` to `requirements.txt` (root and/or `oms/requirements.txt`). Pydantic v2 provides runtime validation, type safety, and better error messages for Redis Stream messages and broker WebSocket events. **Implementation:** Update `requirements.txt`; verify Docker build installs Pydantic. **Unit test:** N/A (dependency addition).
- [x] **12.1.15** **Create Pydantic models for Redis Streams:** Define Pydantic models for Redis Stream message schemas. **Models:** `RiskApprovedOrder` (for `risk_approved` stream: broker, account_id, symbol, side, quantity, order_type, limit_price, time_in_force, book, comment, etc.); `CancelRequest` (for `cancel_requested` stream: order_id or broker_order_id+symbol, broker); `OmsFillEvent` (for `oms_fills` stream: event_type, order_id, broker_order_id, symbol, side, quantity, price, fee, fee_asset, executed_at, fill_id, reject_reason, book, comment). Use `Field()` for validation (e.g. `quantity > 0`, required fields, optional defaults). **Implementation:** Create `oms/schemas_pydantic.py` (or add to existing `oms/schemas.py`); define BaseModel classes with proper field types and validators. **Unit test:** `oms/tests/test_schemas_pydantic.py` — verify models validate correct data, reject invalid data (missing required fields, invalid types, negative quantities), provide clear error messages.
- [x] **12.1.16** **Integrate Pydantic validation into Redis Stream parsers:** Replace manual validation in `parse_risk_approved_message()` and `parse_cancel_request_message()` with Pydantic model validation. Update `produce_oms_fill()` to validate output using `OmsFillEvent` before publishing. **Implementation:** `oms/consumer.py` — use `RiskApprovedOrder(**fields)` in `parse_risk_approved_message()`; catch `ValidationError` and raise `RiskApprovedParseError` with Pydantic error details. `oms/cancel_consumer.py` — use `CancelRequest(**fields)` in `parse_cancel_request_message()`; catch `ValidationError` and raise `CancelRequestParseError`. `oms/producer.py` — validate event dict with `OmsFillEvent(**event)` before `XADD`. **Unit test:** `oms/tests/test_consumer.py` — verify Pydantic validation catches invalid messages (missing broker, invalid quantity, etc.); `oms/tests/test_cancel_consumer.py` — verify Pydantic validation for cancel requests; `oms/tests/test_producer.py` — verify `produce_oms_fill` validates before publishing. **Integration test:** Invalid messages in `risk_approved` or `cancel_requested` are logged and skipped (not processed).
- [x] **12.1.17** **Create Pydantic models for broker streams (Binance WebSocket):** Define Pydantic models for Binance WebSocket execution report events. **Models:** `BinanceExecutionReport` (raw Binance event: e, x, X, c, i, s, S, q, p, l, L, z, t, T, n, N, r, etc.); `FillEvent` (unified fill: event_type="fill", order_id, broker_order_id, symbol, side, quantity, price, fee, fee_asset, executed_at, fill_id, order_status, executed_qty_cumulative); `RejectEvent` (unified reject: event_type="reject", order_id, broker_order_id, symbol, side, quantity, price, reject_reason, executed_at); `CancelledEvent` (unified cancelled: event_type="cancelled", order_id, broker_order_id, symbol, side, quantity, price, executed_at, reject_reason); `ExpiredEvent` (unified expired: event_type="expired", order_id, broker_order_id, symbol, side, quantity, price, executed_at, reject_reason). Use `Literal` for event_type values; `Field()` for validation (quantity > 0 for fills, required fields). **Implementation:** Create `oms/brokers/binance/schemas_pydantic.py` (or add to existing fills_listener module); define BaseModel classes. **Unit test:** `oms/brokers/binance/tests/test_schemas_pydantic.py` — verify models validate correct Binance events, reject invalid events (missing required fields, invalid exec_type, negative quantities), handle optional fields (executed_qty_cumulative, fee_asset).
- [x] **12.1.18** **Integrate Pydantic validation into broker stream parsers:** Replace manual parsing in `parse_execution_report()` with Pydantic model validation. Update `make_fill_callback()` to validate fill/reject/cancelled/expired events using Pydantic models before processing. **Implementation:** `oms/brokers/binance/fills_listener.py` — use `BinanceExecutionReport(**event)` to validate raw Binance event; catch `ValidationError` and log/skip invalid events. Return `FillEvent`, `RejectEvent`, `CancelledEvent`, or `ExpiredEvent` models (or convert to dict for backward compatibility). `oms/redis_flow.py` — in `make_fill_callback()`, validate event dict with appropriate Pydantic model (`FillEvent`, `RejectEvent`, etc.) before processing; catch `ValidationError` and log error, skip callback. **Unit test:** `oms/brokers/binance/tests/test_fills_listener.py` — verify `parse_execution_report` validates Binance events, rejects invalid events, returns correct unified event models. `oms/tests/test_redis_flow.py` — verify fill callback validates events, skips invalid events with error log. **Integration test:** Invalid Binance WebSocket events are logged and skipped (not processed by OMS).

### 12.2 Account Management Integration into OMS (build backwards: adapter extension → Redis store → Postgres schema)

**Integration approach:** Account management (balances, positions) is **integrated into OMS** rather than being a separate service. OMS extends broker adapters to handle account events from the same user data stream that provides fills. This consolidates broker state management (orders + accounts) in one service. See **docs/oms/ACCOUNT_MANAGEMENT_INTEGRATION.md** for rationale and architecture.

**Alignment with OMS:** OMS owns `orders` table sync (`oms/sync.py`); OMS also owns `accounts`, `balances`, `positions` sync (`oms/account_sync.py`). Account state comes from broker user data stream events (`outboundAccountPosition`, `balanceUpdate`) and periodic REST refresh (`get_account_snapshot`). Same patterns as orders: event-driven + periodic refresh + sync to Postgres + repairs.

**Build order:** Start with broker adapter extension (define what data we get from broker), then build Redis store (store what adapter provides), then build Postgres schema (based on Redis store structure), then wire everything together.

- [x] **12.2.1** **Broker adapter account interface** (`oms/brokers/base.py`): extend `BrokerAdapter` protocol with account methods: `start_account_listener(callback)`, `get_account_snapshot(account_id)`, `stop_account_listener()`. Document unified event shape: `event_type` (e.g. `balance_update`, `account_position`), `broker`, `account_id`, `balances[]`, `positions[]`, `updated_at`, `payload`. Define return shape for `get_account_snapshot`: same unified structure. **Unit test:** verify protocol definition, method signatures, documentation.
- [x] **12.2.2** **Binance account listener** (`oms/brokers/binance/account_listener.py`): parse Binance user data stream events (`outboundAccountPosition`, `balanceUpdate`). Convert to unified event shape (`AccountPositionEvent`, `BalanceUpdateEvent` in `brokers/binance/schemas_pydantic.py`). Share WebSocket connection with fills listener (multiplex by event type). **Unit test:** mock WebSocket messages; verify parsing, event type routing, unified shape conversion.
- [x] **12.2.3** **Binance adapter account extension** (`oms/brokers/binance/adapter.py`): implement `start_account_listener(callback)` (uses `account_listener.py`), `get_account_snapshot(account_id)` (REST calls: `get_account()`, `get_futures_account()` if futures), `stop_account_listener()`. **Unit test:** mock Binance API client; verify REST calls, listener start/stop, callback invocation with unified event shape.
- [x] **12.2.4** **Redis account store** (`oms/storage/redis_account_store.py`): implement Redis storage for accounts, balances, positions based on unified event shape from adapter. Key patterns: `account:{broker}:{account_id}`, `account:{broker}:{account_id}:balances`, `account:{broker}:{account_id}:positions`, `accounts:by_broker:{broker}`. Methods: `apply_account_position`, `apply_balance_update`, `get_account`, `get_balances`, `get_positions`. Use Redis pipelines for atomicity. Store structure should match unified event shape from adapter. **Unit test:** mock Redis or fakeredis; verify key formats, CRUD operations, pipeline atomicity, data structure matches adapter events.
- [x] **12.2.5** **Account callback** (`oms/account_flow.py`): implement `make_account_callback(redis, account_store, on_account_updated, ...)` that validates account events (using unified shape from adapter), updates Redis account store (`apply_account_position`, `apply_balance_update`), optionally calls `on_account_updated(broker, account_id)` to trigger sync. Use `updated_at` or event_id to avoid overwriting newer data with older periodic snapshot. **Unit test:** mock account store and callback; verify event validation, store updates, idempotency.
- [x] **12.2.6** **Account Postgres schema** (Alembic): create revision(s) for `accounts`, `balances`, `balance_changes` (historical deposits/withdrawals). *(Positions table was created then dropped in d4e5f6a7b8c9; `accounts.env` dropped in e5f6a7b8c9d0; `margin_snapshots` dropped in f6a7b8c9d0e1.)* Schema documented in §3.1. **Signed off.**
- [x] **12.2.7** **Account sync to Postgres** (`oms/account_sync.py`): implement `sync_accounts_to_postgres(redis, account_store, pg_connect, ...)` that reads accounts/balances from Redis and UPSERTs to Postgres (`accounts`, `balances`). No positions table (dropped for PMS). **Balance cleanup:** After UPSERTing balances, DELETE any balances for the account not in current Redis (flattened assets). **Balance changes:** Main wires `on_balance_change` when `DATABASE_URL` is set; callback writes to `balance_changes` (via `write_balance_change` + `get_account_pk_by_broker_and_id`) and sets TTL on Redis account keys only for balance change events (periodic sync does not set TTL). **Signed off.**
- [x] **12.2.8** **Account repairs** (`oms/account_repair.py`): implement `run_all_account_repairs(pg_connect)` that fixes flawed Postgres account/balance/position fields (e.g. NULL/zero) by recovering from `payload` or raw broker data. Similar pattern to `oms/repair.py` for orders. **Signed off:** Dummy implementation in place: `run_all_account_repairs(pg_connect)` returns 0; unit tests in `oms/tests/test_account_repair.py`. Full payload-based recovery can follow later.
- [x] **12.2.9** **OMS main loop integration**: extend `oms/main.py` to start account listeners alongside fill listeners; add periodic account refresh (`get_account_snapshot` every `account_refresh_interval_seconds`); add periodic account sync (`sync_accounts_to_postgres`) and repairs (`run_all_account_repairs`) in main loop. Wait for account listeners connected before processing orders. **Signed off:** `get_account_store`, `start_account_listeners`, `stop_account_listeners`, `wait_for_account_listeners_connected`; `run_oms_loop(account_store=..., account_refresh_interval_seconds=..., account_sync_interval_seconds=..., account_sync_ttl_seconds=...)` with refresh/sync/repair logic; `main()` creates account_store, starts account listeners after fill listeners, waits for account listeners, passes env `OMS_ACCOUNT_REFRESH_INTERVAL_SECONDS` / `OMS_ACCOUNT_SYNC_INTERVAL_SECONDS`, stops account listeners in `finally`. Integration test optional.
- [x] **12.2.10** **Account cleanup** (`oms/cleanup.py`): extend cleanup module with `set_account_key_ttl(redis, broker, account_id, ttl_seconds)` for TTL on account keys after sync. **Signed off:** `set_account_key_ttl` implemented in `oms/cleanup.py`; unit tests in `oms/tests/test_cleanup.py` (`TestSetAccountKeyTtl`).

### 12.3 PMS (build backwards: calculations → reads → writes → loop)

**Core vs optional:** Implement **Core** tasks first for a minimal PMS; add **(Optional)** tasks when needed. See **docs/pms/PMS_ARCHITECTURE.md** §2.

- [ ] **12.3.1** **(Core)** **PMS PnL/margin calculation**: realized PnL (from fills or orders), unrealized PnL (mark price), margin (futures). **Unit test:** given positions/balances/fills, verify calculations.
- [ ] **12.3.1a** **(Core)** **Mark price provider**: define **interface** (e.g. get mark prices per symbol); **Phase 2:** implement by wrapping Binance (REST/WS). **(Optional)** Phase 4: add implementation reading from Redis/DB (Market Data). Switch via `PMS_MARK_PRICE_SOURCE`. See **docs/pms/PMS_ARCHITECTURE.md** §11.
- [ ] **12.3.1b** **(Optional)** **Pydantic in PMS**: Use Pydantic (same as OMS) for config, mark price, internal shapes, snapshot writes. See **docs/pms/PMS_ARCHITECTURE.md** §12.
- [ ] **12.3.2** **(Core)** **PMS Postgres schema** (reads): orders, balances, accounts. **(Optional):** fills, **symbols** (reference), **book_allocations** or equivalent. Grain includes **book**. Document in **docs/pms/PMS_ARCHITECTURE.md** §8.
- [ ] **12.3.3** **(Core)** **PMS Postgres reads**: query orders, balances; **derive positions** at grain (account_id, book, symbol, side). **(Optional):** fills, symbols, allocations. **Unit test:** verify queries and position derivation.
- [ ] **12.3.4** **(Core)** **PMS granular store** (writes): **pms_positions** (or equivalent) at grain (account_id, book, symbol, side); update after each calculation. Grouping on request. Document in **docs/pms/PMS_ARCHITECTURE.md** §6.
- [ ] **12.3.5** **(Core)** **PMS Redis**: PMS writes PnL/margin keys only (does not read OMS Redis). **Unit test:** verify writes only.
- [ ] **12.3.6** **(Core)** **PMS Postgres schema** (writes): **pms_positions**. **(Optional):** pnl_snapshots, margin_snapshots; book_cash if using internal ledger. Document in **docs/pms/PMS_ARCHITECTURE.md** §6–8.
- [ ] **12.3.7** **(Core)** **PMS Postgres/Redis writes**: write granular position table + Redis (pnl/margin keys). **(Optional):** pnl_snapshots, margin_snapshots. **Unit test:** test Postgres and mock Redis; verify writes.
- [ ] **12.3.8** **(Core)** **PMS integration** (loop): periodic read → derive → calculate → write (granular table + Redis). **(Optional):** snapshots. **Integration test:** mock data; verify loop runs and writes results.

### 12.4 Test harness and deployment

- [ ] **12.4.1** **Write test script** (`scripts/inject_test_order.py`): connect to Redis (`REDIS_URL`), XADD test order to `risk_approved` (broker, symbol, side, quantity, order_type, etc.). Use for manual E2E and automated tests. Optionally add assertions (poll `oms_fills` or Postgres). See §16 Testing.
- [ ] **12.4.2** Implement **Risk (minimal)** or use test inject only: pass-through `strategy_orders` → `risk_approved`, or rely on test script.
- [ ] **12.4.3** Add Docker services for OMS, PMS (and optionally Risk) to `docker-compose.yml`; same network `multistrat`; env from `.env`. Note: Account management is integrated into OMS, no separate Booking service.
- [ ] **12.4.4** **E2E:** run test script to inject one test order (broker `binance`) → OMS → Binance adapter → Binance testnet → fill → OMS publishes `oms_fills`; OMS account listener receives account events → OMS syncs orders and accounts to Postgres → PMS reads orders/balances from Postgres, derives positions from orders, and shows PnL/margin. Verify in pgAdmin and RedisInsight.

---

## 13. Verification (acceptance)

- [ ] `docker compose up -d` brings up infra + OMS, PMS (and optionally Risk).
- [ ] **Test script** runs successfully: connects to Redis, XADDs one test order to `risk_approved` with `broker: "binance"`.
- [ ] One test order injected via script flows: OMS routes to Binance adapter → Binance testnet → fill (or reject) → OMS publishes to `oms_fills` (event_type fill/reject/cancelled/expired as applicable).
- [ ] **OMS order sync** (trigger on terminal status and/or periodic `sync_terminal_orders`) populates Postgres `orders` from Redis; after sync, `orders` table has the test order row.
- [ ] **OMS account sync** (periodic `sync_accounts_to_postgres`) populates Postgres `accounts`, `balances` from Redis account store (updated by account listener from broker stream events). No `positions` table (positions in Redis only). **Balance changes:** `balanceUpdate` events are written to `balance_changes` via main's `on_balance_change` callback; TTL on Redis account keys is set only when processing balance change events (not after periodic sync).
- [ ] PMS reads orders/balances/symbols/allocations from Postgres, derives positions at grain (account_id, book, symbol, side), writes granular position table (e.g. pms_positions) and PnL/margin to Postgres or Redis (does not read OMS Redis positions — dummy).
- [ ] In pgAdmin: `orders` (from OMS order sync), `accounts`, `balances` (from OMS account sync) reflect the test order and account state.
- [ ] In RedisInsight: OMS order hashes (e.g. `orders:*`), account hashes (e.g. `account:binance:*`); `oms_fills` stream has the fill (and optionally reject/cancelled/expired) events.

---

## 14. Suggested repo layout (Phase 2)

```
multistrat/
├── docker-compose.yml      # add services: oms, pms, risk
├── .env.example
├── alembic/
│   └── versions/
│       └── xxxxx_add_account_tables.py  # accounts, balances, balance_changes (margin_snapshots dropped in f6a7b8c9d0e1; no positions table)
├── oms/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py             # loop: risk_approved → Redis order store → adapter → fills → oms_fills; account listener → Redis account store; sync orders + accounts to Postgres
│   ├── adapter.py          # broker adapter interface + registry
│   ├── storage/
│   │   ├── redis_order_store.py
│   │   └── redis_account_store.py
│   ├── account_flow.py      # account callback handler
│   ├── account_sync.py      # sync accounts/balances/positions to Postgres
│   ├── account_repair.py   # fix flawed account fields from payload
│   ├── brokers/
│   │   ├── __init__.py
│   │   └── binance/
│   │       ├── adapter.py      # Binance adapter (orders + accounts)
│   │       ├── account_listener.py  # parse account events
│   │       └── ...
├── pms/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py             # loop: read positions/balances → PnL/margin → Postgres/Redis
├── risk/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py             # loop: strategy_orders → risk_approved (pass-through)
└── scripts/
    └── inject_test_order.py  # required for E2E: XADD test order to risk_approved (broker, symbol, side, qty, etc.); see §16.1
```

---

## 15. Handoff to Phase 3

- Admin (Phase 3) will consume or query: positions, balances, margin, open orders, recent fills from Postgres or Redis cache (read-only) and publish commands to Redis streams. Phase 2 leaves Postgres and Redis cache as the source of truth for positions/balances; Phase 3 adds the Admin UI/CLI and command streams.

---

## 16. Testing (Phase 2)

**Bottom-up unit-testing approach:** Each component is unit-tested in isolation before integration. Mock dependencies (HTTP, Redis, Postgres) so tests run fast and don't require external services. Then integration tests wire pieces together with mocks, and finally E2E uses real services (testnet).

### 16.1 Unit tests (per component)

- **Binance API client:** Mock HTTP (e.g. `responses` in Python, `nock` in Node); test signing, request format, error handling, response parsing.
- **Binance adapter:** Mock Binance client; test adapter calls client correctly, converts responses to unified format, handles errors.
- **OMS Redis order store:** Mock Redis or fakeredis; test stage_order, update_status, update_fill_status, get_order, find_order_by_broker_order_id; verify index updates and pipelines.
- **OMS Redis consumer/producer:** Mock Redis client (e.g. `fakeredis`); test XREAD parsing, XADD formatting, error handling.
- **OMS adapter registry:** Mock adapters; test routing by `broker`, unknown broker handling.
- **OMS stream/Redis cleanup:** Mock Redis; verify XTRIM and TTL on terminal order keys (12.1.9b).
- **OMS CANCELED/EXPIRED:** Parser unit test for CANCELED/EXPIRED; integration: callback updates store to cancelled/expired (12.1.9c).
- **OMS process_one error:** Mock adapter that raises; verify no XACK and optional reject/store (12.1.9d).
- **OMS cancel order:** Mock client; adapter cancel_order calls client; testnet: place then cancel (12.1.9e).
- **OMS cancel from Redis:** Parse cancel_requested message; read_one_cancel_request; process_one_cancel → store cancelled, oms_fills (12.1.9f).
- **OMS → Postgres sync:** Unit test: sync writes correct rows; idempotent (12.1.10).
- **OMS main loop:** Integration: fakeredis + mock adapter, inject one message, assert process_one and XACK; E2E: real Redis + testnet, inject script, assert oms_fills (12.1.11a/12.1.11b).
- **OMS account store:** Mock Redis or fakeredis; test key formats (`account:*`, `...:balances`, `...:positions`), CRUD operations, pipeline atomicity.
- **OMS account callback:** Mock account store and callback; verify event validation, store updates, idempotency (updated_at handling).
- **OMS account sync:** Mock Redis and Postgres; verify mapping to Postgres rows, UPSERT logic, TTL setting.
- **PMS calculations:** Pure functions; test PnL/margin math given positions/balances/fills.
- **PMS reads/writes:** Mock Postgres/Redis or use test DB; test queries and writes.

### 16.2 Integration tests (wire pieces)

- **OMS integration:** Mock Redis streams (`risk_approved`) and adapter; verify: consumer → Redis order store → adapter.place_order → adapter callback → Redis status update → producer (`oms_fills`).
- **OMS bootstrap and main loop:** Start fill listener(s) and run process_one loop; inject one message; assert processed and (with mock) no double-process (12.1.11a, 12.1.11b).
- **OMS error path:** Adapter place_order raises → no XACK; optional reject to store/oms_fills (12.1.9d).
- **OMS cancel from Redis:** XADD cancel_requested → process_one_cancel → store status cancelled, oms_fills event (12.1.9f).
- **OMS account integration:** Mock broker adapter account listener; verify: account events → callback → Redis account store updates → periodic sync → Postgres writes (accounts, balances, positions).
- **PMS integration:** Mock Postgres/Redis data; verify: read → calculate → write loop.

### 16.3 Test script (required for E2E)

**You need to write a test script to test the flow.** Redis is not HTTP; services consume streams via a Redis client (XREAD/XADD). To drive the pipeline you either run a Risk process that forwards from `strategy_orders`, or (simpler for Phase 2) **a script that publishes a test order into `risk_approved`**. Use the same script for manual E2E and, if you add assertions (e.g. poll `oms_fills` or Postgres), for automated E2E.

- **Purpose:** Inject one or more test orders into `risk_approved` so OMS (and then PMS) can be tested end-to-end without running a live Strategy or Risk.
- **Implementation:** Script (e.g. `scripts/inject_test_order.py`) that: connects to Redis using `REDIS_URL`; builds a JSON payload per stream schema (`broker`, `symbol`, `side`, `quantity`, `order_type`, etc.); calls XADD on stream `risk_approved`. Run after OMS and (optionally) PMS are up.
- **Optional:** After inject, poll Redis stream `oms_fills` or query Postgres (fills, positions) with a timeout and assert expected state for automated E2E.

### 16.4 E2E (real services, testnet)

- **E2E:** Run the test script to inject one order with `broker: "binance"` → OMS → Binance adapter → Binance testnet → fill → OMS publishes `oms_fills`; OMS account listener receives account events → OMS syncs orders and accounts to Postgres; PMS reads and reflects state. Use testnet only; no real funds.

### 16.5 Test classification and file mapping

Tests are classified into four categories based on scope and dependencies:

#### 16.5.1 Unit tests (isolated components, mocked dependencies)

**Purpose:** Test individual functions/components in isolation with all dependencies mocked.

**Files:**
- `oms/brokers/binance/tests/test_api_client.py` — Binance API client (mocked HTTP)
  - `test_place_order_market_success` — MARKET order placement
  - `test_place_order_limit_success` — LIMIT order placement
  - Request signing, error handling, query/cancel endpoints
- `oms/tests/test_consumer.py` — Redis consumer message parsing
- `oms/tests/test_producer.py` — Redis producer message formatting
- `oms/tests/test_redis_order_store.py` — Redis order store CRUD operations
- `oms/tests/test_registry.py` — Broker adapter registry routing
- `oms/tests/test_sync.py` — Postgres sync operations
- `oms/tests/test_cleanup.py` — Stream trimming and TTL
- `oms/tests/test_cancel_consumer.py` — Cancel command parsing
- `oms/brokers/binance/tests/test_adapter.py` — Binance adapter (mocked client)
- `oms/brokers/binance/tests/test_fills_listener.py` — Fills listener parsing (mocked websocket)

**Characteristics:**
- Fast execution (no network I/O)
- No external dependencies
- Mock all external services (HTTP, Redis, Postgres)

#### 16.5.2 Integration tests (multiple components, mocked external services)

**Purpose:** Test multiple components wired together, but still using mocked external services.

**Files:**
- `oms/tests/test_oms_integration.py` — Full OMS flow with fakeredis and mock adapters
  - `test_oms_integration_consumer_store_registry_producer` — Full flow: consumer → store → adapter → producer
  - `test_oms_integration_sent_then_fill_callback_produces` — Fill callback updates store and produces to `oms_fills`
  - `test_oms_integration_partial_then_full_fill_status_and_cumulative` — Partial/full fill handling
  - `test_oms_integration_process_one_cancel_from_redis` — Cancel flow
  - `test_oms_integration_consumer_group_no_double_process` — Consumer group deduplication
  - `test_oms_main_loop_integration` — Main loop with fakeredis

**Characteristics:**
- Uses fakeredis (in-memory Redis) or mocked Redis client
- Mock broker adapters
- Tests component interactions without real services
- Faster than E2E but validates integration logic

#### 16.5.3 E2E tests — code-level (real services, OMS code runs in-process)

**Purpose:** Test full system flow with real services, but OMS code runs directly in the test process.

**Files:**
- `oms/tests/test_oms_redis_testnet.py` — Real Redis + Binance testnet, OMS code imported
  - Order-flow: `test_trigger_testnet_via_redis_listen_oms_fills` — MARKET order through Redis → testnet → `oms_fills`
  - Order-flow: `test_full_pipeline_place_then_cancel_via_redis` — LIMIT order → place → cancel
  - Order-flow: `test_full_pipeline_redis_order_testnet_status_sync_to_postgres` — MARKET order with Postgres sync
  - Order-flow: `test_full_pipeline_with_main_loop` — MARKET order via main loop
  - Order-flow: `test_place_order_raises_retry_then_reject_consumer_group` — Error handling with real Redis
  - Order-flow: `test_error_order_rejected_via_redis_testnet` — Rejection flow
  - Account-flow: `test_account_refresh_e2e` — get_account_snapshot (testnet) → apply to Redis account store → assert account/balances in Redis
  - Account-flow: `test_account_sync_to_postgres_e2e` — get_account_snapshot → apply to Redis → sync_accounts_to_postgres → assert Postgres `accounts` and `balances` (requires `DATABASE_URL`)
  - Account-flow: `test_order_fill_matches_balance_change_e2e` — place MARKET BUY → wait for fill → snapshot before/after → assert BTC balance increased by executed_qty and USDT did not increase (order matched to balance change)

**Characteristics:**
- Real Redis (from `REDIS_URL`)
- Real Binance testnet (requires `BINANCE_API_KEY`, `BINANCE_API_SECRET`)
- Real Postgres (optional, requires `DATABASE_URL`)
- OMS code imported and executed in test process
- Fill listeners started in test
- Requires `RUN_BINANCE_TESTNET=1` to execute

**Setup:**
```bash
export RUN_BINANCE_TESTNET=1
export REDIS_URL=redis://localhost:6379
export DATABASE_URL=postgresql://...  # optional
export BINANCE_API_KEY=...
export BINANCE_API_SECRET=...
pytest oms/tests/test_oms_redis_testnet.py
```

#### 16.5.4 E2E tests — service-level (black-box, services running separately)

**Purpose:** Test the running system as a black box without importing OMS code.

**Files:**
- `scripts/full_pipeline_test.py` — Service-level E2E script
  - Supports `--market` flag for MARKET orders
  - Default LIMIT order flow (place → cancel)
  - Checks downstreams: `oms_fills` stream, Redis order store
  - Account-flow: polls Redis account store (OMS periodic refresh) and optionally Postgres `accounts`/`balances`; `--no-account` to skip

**Characteristics:**
- Assumes OMS service is already running (e.g., `docker compose up -d oms`)
- Does not import OMS code
- Interacts with system via Redis/Postgres only
- Validates deployed/running system
- Useful for production-like validation

**Usage:**
```bash
# Start services first
docker compose up -d oms pms

# Run script
python scripts/full_pipeline_test.py --market  # MARKET order
python scripts/full_pipeline_test.py           # LIMIT order (default)
```

**What it validates:**
- Order injection → `risk_approved` stream
- OMS processes order (running service)
- Order appears in Redis store
- Fill/reject appears in `oms_fills` stream
- Order synced to Postgres `orders` table (if `DATABASE_URL` set)

#### 16.5.5 Test matrix summary

| Test Type | Files | Dependencies | Speed | Use Case |
|-----------|-------|--------------|-------|----------|
| **Unit** | `test_api_client.py`, `test_consumer.py`, `test_redis_order_store.py`, etc. | None (mocked) | Fastest | Development, CI |
| **Integration** | `test_oms_integration.py` | fakeredis, mocks | Fast | Component integration |
| **E2E (code-level)** | `test_oms_redis_testnet.py` | Real Redis, Binance testnet, Postgres | Slow | Full flow validation |
| **E2E (service-level)** | `scripts/full_pipeline_test.py` | Running services | Slow | Production-like validation |

**Test execution order (recommended):**
1. Run unit tests (fast, no setup)
2. Run integration tests (fakeredis, no external services)
3. Run E2E code-level tests (requires testnet credentials)
4. Run E2E service-level script (requires running services)

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md#testing-corresponding-to-each-phase) for the overall test matrix.

**Comprehensive test documentation:** See [TESTING.md](TESTING.md) for complete test inventory, classification, and execution instructions.
