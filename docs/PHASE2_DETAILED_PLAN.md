# Phase 2: Detailed Plan — OMS, Booking, Position (Binance first)

**Goal:** End-to-end order flow: approved orders → **generic OMS** (routes to broker adapters) → first broker **Binance** → fills → Booking → Postgres positions/balances/margin; Position Keeper for PnL/margin. The OMS is broker-agnostic; Binance is the first broker adapter. Minimal Risk pass-through for testing.

---

## 1. Overview

| Item | Choice |
|------|--------|
| **OMS** | Generic order router: consumes `risk_approved`, stages orders in **Redis** (hashes + indexes), dispatches to a **broker adapter** by `broker`/account; receives fills from adapter and publishes `oms_fills`. Periodically syncs orders to Postgres for audit. Extensible to more brokers later. |
| **Broker adapters** | Pluggable implementations (e.g. Binance first; others later). Each adapter: place order, subscribe to fills/rejects, report back to OMS in a unified format. |
| **First broker** | Binance (testnet first; spot or futures — decide one for Phase 2). |
| **Order path** | Redis `risk_approved` → OMS (router) → broker adapter (Binance) → broker API → fills back to OMS → Redis `oms_fills` → Booking → Postgres + Redis cache |
| **OMS persistence** | **Redis** (staging: order hashes, status indexes, broker_order_id lookup); **periodic sync to Postgres** `orders` table for audit and recovery. |
| **Booking persistence** | Postgres (orders, positions, balances, margin, fills); Redis cache for Risk/Position Keeper |
| **Position Keeper** | Reads Postgres/Redis; aggregates PnL and margin |
| **Risk (Phase 2)** | Minimal: consume `strategy_orders`, pass through to `risk_approved` (or use test inject for E2E) |

---

## 2. Dependencies

- **Phase 1 complete:** Postgres, Redis, Alembic, Docker network `multistrat`, `.env` with `DATABASE_URL` and `REDIS_URL`.
- **Binance:** API key and secret; decide **spot** or **futures** (e.g. futures for margin/leverage). Use **testnet** for development.
- **Language/runtime:** Choose one for OMS, Booking, Position Keeper, Risk (e.g. Python for consistency with Alembic; or Go/Node per service).
- **Testing approach:** Build **bottom-up** with unit tests at each layer (mock dependencies) before integration. See §12 Task checklist and §16 Testing for the order and test strategy.

---

## 3. Postgres schema (Booking / Position Keeper)

**Note:** Postgres schema is **defined and implemented as part of Booking** (see §12.2.1). Position Keeper reads from this schema but does not modify it. This section describes the schema design; implementation happens in the Booking task checklist.

All schema changes are **Alembic revisions** (same as Phase 1). Add one or more revisions for the following.

### 3.1 Tables (minimal set)

- **accounts**  
  - `id` (PK), `name`, `broker` (e.g. `binance`), `env` (e.g. `testnet`/`mainnet`), `created_at`, optional `config` (JSONB).  
  - One row per broker account if you support multiple; else a single default account.

- **orders** (audit / recovery; populated by OMS from Redis or on status updates)  
  - See table in §3.1 above: includes `book` (strategy/book id), `comment` (freetext), generic broker fields (`executed_qty`, `time_in_force`, etc.), and Binance-specific columns with `binance_` prefix (`binance_cumulative_quote_qty`, `binance_transact_time`).  
  - OMS syncs orders to this table for audit, reconciliation, and recovery.

- **fills**  
  - `id` (PK), `order_id` (FK to orders or internal reference), `account_id` (FK), `symbol`, `side`, `quantity`, `price`, `fee`, `fee_asset`, `broker_fill_id`, `executed_at`, `created_at`.  
  - Source of truth for what was executed; Booking reads from `oms_fills` stream and writes here.

- **positions**  
  - `id` (PK), `account_id` (FK), `symbol`, `side` (long/short for futures), `quantity`, `entry_price_avg`, `updated_at`.  
  - Booking updates on each fill (add/reduce). For futures, one row per (account, symbol, side) or net quantity per symbol.

- **balances**  
  - `id` (PK), `account_id` (FK), `asset`, `available`, `locked`, `updated_at`.  
  - Booking updates from balance events or derived from fills/cash.

- **margin_snapshots** (optional for spot; useful for futures)  
  - `id` (PK), `account_id` (FK), `total_margin`, `available_balance`, `timestamp`.  
  - Position Keeper or Booking can write periodically.

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
| `price` | NUMERIC NULL | risk_approved / broker | |
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

Binance **place_order** returns (relevant): `orderId`, `symbol`, `status`, `clientOrderId`, `side`, `type`, `origQty`, `price`, `executedQty`, `timeInForce`, `transactTime`.  
Binance **query_order** adds: `cumulativeQuoteQty`, `origQty`, etc.  
Binance **cancel_order** returns same shape as query.

- **Generic columns** (used for any broker): `internal_id`, `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, `price`, `time_in_force`, `status`, `executed_qty`.
- **Binance-specific columns** (prefix `binance_`): `binance_cumulative_quote_qty` ← Binance `cumulativeQuoteQty`; `binance_transact_time` ← Binance `transactTime`. Other brokers get their own prefix (e.g. `bybit_*`).
- Internal `internal_id` = our UUID; we send it as `newClientOrderId` → Binance returns it as `clientOrderId`.
- Internal `broker_order_id` = Binance `orderId`.
- `book` and `comment` are **not** sent to Binance; they are carried from `risk_approved` through OMS and stored in Postgres/Redis for audit and strategy attribution.

### 3.3 Alembic workflow

- Create revision: `alembic revision -m "add_booking_tables"` (or split: `add_accounts_orders`, `add_fills_positions_balances`).
- Implement `upgrade()` and `downgrade()` in the new file under `alembic/versions/`.
- Run `alembic upgrade head` after deploying the revision.

---

## 4. Redis Streams

### 4.1 Stream names

| Stream | Producer | Consumer | Purpose |
|--------|----------|----------|---------|
| `strategy_orders` | Strategies (Phase 5) or test inject | Risk | Order intents from strategies |
| `risk_approved` | Risk | OMS | Orders approved (or pass-through in P2) |
| `oms_fills` | OMS | Booking | Fill and rejection events |

### 4.2 Message schemas (JSON)

**Note:** Schemas are **defined and implemented per service** (see §12 Task checklist). OMS defines `risk_approved` input and `oms_fills` output; Booking defines `oms_fills` input (must match OMS output). This ensures each service owns its contract.

**risk_approved (order to execute)** — defined in OMS (12.1.5)  
- `order_id` (internal UUID or string; optional, OMS can generate)  
- `broker` (e.g. `binance`) — OMS uses this to select the broker adapter.  
- `account_id` (optional; default account if single)  
- `symbol`, `side` (BUY/SELL), `quantity`, `order_type` (MARKET, LIMIT, etc.)  
- `price` (optional; for LIMIT), `time_in_force` (optional; GTC, IOC, FOK)  
- **`book`** (optional) — Strategy / book identifier (e.g. `ma_cross`, `manual`) for attribution.  
- **`comment`** (optional) — Freetext comment; stored with order in Redis and Postgres.  
- `strategy_id` (optional), `created_at` (ISO)

**oms_fills (fill or reject)** — defined in OMS (12.1.5), consumed by Booking (12.2.5)  
- `event_type`: `fill` | `reject`  
- `order_id` (internal), `broker_order_id`, `symbol`, `side`, `quantity`, `price`, `fee`, `fee_asset`  
- `executed_at` (ISO), `fill_id` (broker), optional `reject_reason` for rejections  
- **`book`** (optional) — Pass-through from order for attribution.  
- **`comment`** (optional) — Pass-through from order for audit.  

Each service documents its schemas in code or a service README (e.g. `oms/README.md`, `booking/README.md`). Streams are created on first XADD.

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

- **orders:{order_id}** — Hash: `internal_id`, `broker`, `account_id`, `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, `price`, `time_in_force`, `status` (pending, sent, **partially_filled**, filled, rejected, cancelled), **`book`**, **`comment`**, `created_at`, `updated_at`, optional **`executed_qty`** (cumulative), **`binance_cumulative_quote_qty`**, **`binance_transact_time`**, `payload` (JSON). Broker-agnostic fields plus `binance_*` when broker is Binance; other brokers use their own prefix in payload or separate keys.
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

## 7. Booking service

### 7.1 Role

- Consume from Redis stream `oms_fills` (XREAD block).
- For each **fill** event: update **positions** (add or reduce quantity, update avg price), update **balances** (cash/asset), optionally write **margin_snapshots**; insert row into **fills** in Postgres.
- Update **Redis cache**: e.g. `positions:{account_id}`, `balance:{account_id}:{asset}`, `margin:{account_id}` (JSON or hash) so Risk and Position Keeper can read without hitting Postgres every time.

### 7.2 Idempotency

- Use `fill_id` (broker) or (order_id + executed_at) as unique key; ignore duplicate fill events.

### 7.3 Deployment

- Loop process; Docker service `booking`; env: `REDIS_URL`, `DATABASE_URL`; connect to `multistrat` network.

---

## 8. Position Keeper service

### 8.1 Role

- Periodically (e.g. every few seconds) read **positions** and **balances** from Postgres or from Redis cache; compute **realized PnL** (from fills), **unrealized PnL** (from current mark price if available), and **margin** (for futures).
- Write results to Postgres (`margin_snapshots` or a dedicated `pnl_snapshots` table) and/or to Redis (e.g. `pnl:{account_id}`, `margin:{account_id}`) for Admin/UI.

### 8.2 Mark price

- For futures, mark price can come from Binance (REST or websocket) or from Market Data service (Phase 4). For Phase 2, optional: fetch mark price from Binance in Position Keeper or leave unrealized PnL as zero until Phase 4.

### 8.3 Deployment

- Loop process; Docker service `position_keeper`; env: `DATABASE_URL`, `REDIS_URL`; connect to `multistrat` network.

---

## 9. Risk service (minimal for Phase 2)

### 9.1 Role

- Consume from Redis stream `strategy_orders` (or skip and use **test inject** only).
- **Pass-through:** For each message, optionally validate (e.g. symbol format); then publish the same (or enriched) payload to `risk_approved`. No position/margin checks yet.
- Enables the pipeline Strategy → Risk → OMS for Phase 5; in Phase 2 you can **inject test orders** directly into `risk_approved` to drive OMS/Booking/Position Keeper.

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

# Optional: account id used by OMS/Booking if multiple accounts
# DEFAULT_ACCOUNT_ID=1
```

### 11.2 Per-service config

- OMS (generic): `REDIS_URL`, `DATABASE_URL` (optional, for order sync to Postgres). Broker-specific vars (e.g. Binance) are used by the Binance adapter.
- Binance adapter: `BINANCE_*` vars (or passed from OMS env).
- Booking: `REDIS_URL`, `DATABASE_URL`.
- Position Keeper: `DATABASE_URL`, `REDIS_URL`.

---

## 12. Task checklist (implementation order)

**Bottom-up approach:** Build and unit-test each layer before integrating. Start with the lowest-level component (Binance API), then build up: adapter → OMS pieces → OMS integration → Booking pieces → Booking integration → Position Keeper pieces → Position Keeper integration. Each step should have unit tests before moving to the next.

**Schema ownership:** Each service defines and implements its own schemas (Redis structures, Postgres, Redis streams, Redis cache keys) as part of its implementation. OMS owns Redis order staging keys, `risk_approved` input and `oms_fills` output schemas; Booking owns Postgres schema (Alembic) and `oms_fills` input schema (must match OMS output); Position Keeper owns its read/write schemas.

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
- [ ] **12.1.9c** **CANCELED/EXPIRED handling:** Listener parses executionReport with exec_type CANCELED or order_status EXPIRED; return unified event (e.g. event_type `cancelled`); OMS callback updates Redis status to `cancelled`/`expired` and optionally publishes to `oms_fills`. When callback sets any terminal status (filled/rejected/cancelled/expired), optionally call `set_order_key_ttl(redis, order_id, ttl_seconds)` (12.1.9b). **Unit test:** parser returns event for CANCELED/EXPIRED payload; **integration test:** fill callback updates store to cancelled/expired.
- [ ] **12.1.9d** **process_one error handling:** When `adapter.place_order` raises, do not XACK (message stays in PEL); optionally update store to rejected and publish reject to `oms_fills` or dead-letter stream. **Unit test:** mock adapter that raises; verify no XACK, optional store/stream reject.
- [ ] **12.1.9e** **Cancel order:** Add `cancel_order(order_id)` or `cancel_order(broker_order_id, symbol)` to broker adapter interface; Binance adapter calls client.cancel_order; OMS (or admin) can call adapter.cancel_order for open orders. **Unit test:** mock client; adapter cancel_order calls client with correct args; **testnet:** place limit order then cancel, verify cancelled.
- [ ] **12.1.10** **OMS → Postgres order sync**: background task that syncs orders from Redis to Postgres `orders` table (e.g. completed orders only, or all; interval e.g. 30s). UPSERT by `internal_id`. After syncing an order, optionally call `set_order_key_ttl(redis, order_id, ttl_seconds)` so Redis can expire the key. **Unit test:** verify sync writes correct rows; idempotent on re-run.
- [ ] **12.1.11a** **Fill listener started by OMS:** Bootstrap code starts fill listener for each registered adapter (e.g. one callback from `make_fill_callback(redis, store)` per adapter). **Integration test:** OMS bootstrap starts listener; inject order; assert fill path (mock or testnet).
- [ ] **12.1.11b** **OMS main loop and Binance registration:** Runnable entrypoint (e.g. `oms/main.py`) that loads REDIS_URL, creates store and registry, registers Binance adapter, starts fill listener(s), then loop: `process_one(redis, store, registry, block_ms=5000)` until shutdown. In the loop (or on a timer), call `trim_oms_streams(redis)` periodically to cap stream length (12.1.9b). Add `BINANCE_*` to `.env.example` (already present); document in README. **Integration test:** run main with fakeredis + mock adapter; inject one message to `risk_approved`; assert order in store and/or oms_fills. **E2E:** run OMS with real Redis + testnet; inject script; assert fill on `oms_fills`.

### 12.2 Booking (build backwards: Postgres writes → Redis cache → consumer → integration)

- [ ] **12.2.1** **Booking Postgres schema** (Alembic): create revision(s) for `accounts`, `orders` (with `book`, `comment`, and Binance-mapped columns per §3.1), `fills`, `positions`, `balances`, optional `margin_snapshots`; implement `upgrade()` and `downgrade()`. Run `alembic upgrade head`. **Unit test:** verify schema creation and indexes.
- [ ] **12.2.2** **Booking Postgres writes**: insert fills, update positions (add/reduce, avg price), update balances. **Unit test:** test Postgres (or test DB); verify SQL, idempotency (duplicate fill_id).
- [ ] **12.2.3** **Booking Redis cache schema**: define key formats (`positions:{account_id}`, `balance:{account_id}:{asset}`, `margin:{account_id}`) and value formats (JSON or hash). Document in code or `booking/README.md`.
- [ ] **12.2.4** **Booking Redis cache updates**: write cache keys per schema. **Unit test:** mock Redis client; verify key format and updates.
- [ ] **12.2.5** **Booking Redis stream schema** (input): define `oms_fills` input schema (must match OMS output schema from 12.1.5). Document in code or `booking/README.md`.
- [ ] **12.2.6** **Booking Redis consumer** (XREAD from `oms_fills`): parse fill/reject events per `oms_fills` schema. **Unit test:** mock Redis client; verify message parsing.
- [ ] **12.2.7** **Booking integration** (wire pieces): Redis consumer → Postgres writes → Redis cache updates. **Integration test:** mock Redis stream; verify Postgres and Redis cache updated for one fill.

### 12.3 Position Keeper (build backwards: calculations → reads → writes → loop)

- [ ] **12.3.1** **Position Keeper PnL/margin calculation**: realized PnL (from fills), unrealized PnL (mark price), margin (futures). **Unit test:** given positions/balances/fills, verify calculations.
- [ ] **12.3.2** **Position Keeper Postgres schema** (reads): define queries for positions, balances, fills (uses Booking schema from 12.2.1). Document in code or `position_keeper/README.md`.
- [ ] **12.3.3** **Position Keeper Postgres reads**: query positions, balances, fills. **Unit test:** test Postgres; verify queries.
- [ ] **12.3.4** **Position Keeper Redis cache schema** (reads): define key formats to read (`positions:{account_id}`, `balance:{account_id}:{asset}`) — must match Booking cache schema from 12.2.3. Document in code or `position_keeper/README.md`.
- [ ] **12.3.5** **Position Keeper Redis reads**: read cache keys (positions, balances). **Unit test:** mock Redis client; verify reads.
- [ ] **12.3.6** **Position Keeper Postgres/Redis schema** (writes): define PnL/margin snapshot schema (Postgres table or Redis keys like `pnl:{account_id}`, `margin:{account_id}`). Document in code or `position_keeper/README.md`.
- [ ] **12.3.7** **Position Keeper Postgres/Redis writes**: write PnL/margin snapshots. **Unit test:** test Postgres and mock Redis; verify writes.
- [ ] **12.3.8** **Position Keeper integration** (loop): periodic read → calculate → write. **Integration test:** mock data; verify loop runs and writes results.

### 12.4 Test harness and deployment

- [ ] **12.4.1** **Write test script** (`scripts/inject_test_order.py`): connect to Redis (`REDIS_URL`), XADD test order to `risk_approved` (broker, symbol, side, quantity, order_type, etc.). Use for manual E2E and automated tests. Optionally add assertions (poll `oms_fills` or Postgres). See §16 Testing.
- [ ] **12.4.2** Implement **Risk (minimal)** or use test inject only: pass-through `strategy_orders` → `risk_approved`, or rely on test script.
- [ ] **12.4.3** Add Docker services for OMS, Booking, Position Keeper (and optionally Risk) to `docker-compose.yml`; same network `multistrat`; env from `.env`.
- [ ] **12.4.4** **E2E:** run test script to inject one test order (broker `binance`) → OMS → Binance adapter → Binance testnet → fill → OMS publishes `oms_fills` → Booking updates Postgres/Redis → Position Keeper shows position and PnL/margin. Verify in pgAdmin and RedisInsight.

---

## 13. Verification (acceptance)

- [ ] `docker compose up -d` brings up infra + OMS, Booking, Position Keeper (and optionally Risk).
- [ ] **Test script** runs successfully: connects to Redis, XADDs one test order to `risk_approved` with `broker: "binance"`.
- [ ] One test order injected via script flows: OMS routes to Binance adapter → Binance testnet → fill (or reject) → OMS publishes to `oms_fills` → Booking writes to Postgres (fills, positions, balances) and updates Redis cache.
- [ ] OMS sync job (if enabled) populates Postgres `orders` from Redis; after sync, `orders` table has the test order row.
- [ ] Position Keeper reads positions/balances and writes PnL/margin to Postgres or Redis.
- [ ] In pgAdmin: `orders`, `fills`, `positions`, `balances` (and optional `margin_snapshots`) reflect the test order and fill.
- [ ] In RedisInsight: OMS order hashes (e.g. `orders:*`); `oms_fills` stream has the fill event; cache keys (e.g. `positions:1`) are present and updated.

---

## 14. Suggested repo layout (Phase 2)

```
multistrat/
├── docker-compose.yml      # add services: oms, booking, position_keeper, risk
├── .env.example
├── alembic/
│   └── versions/
│       └── xxxxx_add_booking_tables.py
├── oms/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py             # loop: risk_approved → Redis order store → adapter → fills → oms_fills; optional sync to Postgres
│   ├── adapter.py          # broker adapter interface + registry
│   ├── storage/            # optional: redis_order_store.py, sync job
│   ├── brokers/
│   │   ├── __init__.py
│   │   └── binance.py      # Binance adapter (first broker)
├── booking/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py             # loop: oms_fills → Postgres + Redis cache
├── position_keeper/
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
- **OMS → Postgres sync:** Unit test: sync writes correct rows; idempotent (12.1.10).
- **OMS main loop:** Integration: fakeredis + mock adapter, inject one message, assert process_one and XACK; E2E: real Redis + testnet, inject script, assert oms_fills (12.1.11a/12.1.11b).
- **Booking Postgres writes:** Test Postgres (or testcontainers/test DB); test SQL, idempotency (duplicate fill_id).
- **Booking Redis cache:** Mock Redis; test key format, updates.
- **Position Keeper calculations:** Pure functions; test PnL/margin math given positions/balances/fills.
- **Position Keeper reads/writes:** Mock Postgres/Redis or use test DB; test queries and writes.

### 16.2 Integration tests (wire pieces)

- **OMS integration:** Mock Redis streams (`risk_approved`) and adapter; verify: consumer → Redis order store → adapter.place_order → adapter callback → Redis status update → producer (`oms_fills`).
- **OMS bootstrap and main loop:** Start fill listener(s) and run process_one loop; inject one message; assert processed and (with mock) no double-process (12.1.11a, 12.1.11b).
- **OMS error path:** Adapter place_order raises → no XACK; optional reject to store/oms_fills (12.1.9d).
- **Booking integration:** Mock Redis stream (`oms_fills`); verify: consumer → Postgres writes → Redis cache updates.
- **Position Keeper integration:** Mock Postgres/Redis data; verify: read → calculate → write loop.

### 16.3 Test script (required for E2E)

**You need to write a test script to test the flow.** Redis is not HTTP; services consume streams via a Redis client (XREAD/XADD). To drive the pipeline you either run a Risk process that forwards from `strategy_orders`, or (simpler for Phase 2) **a script that publishes a test order into `risk_approved`**. Use the same script for manual E2E and, if you add assertions (e.g. poll `oms_fills` or Postgres), for automated E2E.

- **Purpose:** Inject one or more test orders into `risk_approved` so OMS (and then Booking, Position Keeper) can be tested end-to-end without running a live Strategy or Risk.
- **Implementation:** Script (e.g. `scripts/inject_test_order.py`) that: connects to Redis using `REDIS_URL`; builds a JSON payload per stream schema (`broker`, `symbol`, `side`, `quantity`, `order_type`, etc.); calls XADD on stream `risk_approved`. Run after OMS and (optionally) Booking/Position Keeper are up.
- **Optional:** After inject, poll Redis stream `oms_fills` or query Postgres (fills, positions) with a timeout and assert expected state for automated E2E.

### 16.4 E2E (real services, testnet)

- **E2E:** Run the test script to inject one order with `broker: "binance"` → OMS → Binance adapter → Binance testnet → fill → OMS → Booking → Postgres and Redis updated; Position Keeper reflects state. Use testnet only; no real funds.

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md#testing-corresponding-to-each-phase) for the overall test matrix.
