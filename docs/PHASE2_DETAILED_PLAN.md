# Phase 2: Detailed Plan — OMS, Booking, Position (Binance first)

**Goal:** End-to-end order flow: approved orders → **generic OMS** (routes to broker adapters) → first broker **Binance** → fills → Booking → Postgres positions/balances/margin; Position Keeper for PnL/margin. The OMS is broker-agnostic; Binance is the first broker adapter. Minimal Risk pass-through for testing.

---

## 1. Overview

| Item | Choice |
|------|--------|
| **OMS** | Generic order router: consumes `risk_approved`, stages in SQLite, dispatches to a **broker adapter** by `broker`/account; receives fills from adapter and publishes `oms_fills`. Extensible to more brokers later. |
| **Broker adapters** | Pluggable implementations (e.g. Binance first; others later). Each adapter: place order, subscribe to fills/rejects, report back to OMS in a unified format. |
| **First broker** | Binance (testnet first; spot or futures — decide one for Phase 2). |
| **Order path** | Redis `risk_approved` → OMS (router) → broker adapter (Binance) → broker API → fills back to OMS → Redis `oms_fills` → Booking → Postgres + Redis cache |
| **OMS persistence** | SQLite (staging: orders, status); broker-agnostic schema. |
| **Booking persistence** | Postgres (positions, balances, margin, fills); Redis cache for Risk/Position Keeper |
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

- **orders** (audit / reference; OMS has its own SQLite copy for staging)  
  - `id` (PK), `account_id` (FK), `internal_id` (unique, from OMS), `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, `price` (nullable), `status`, `created_at`, `updated_at`.  
  - Optional: store here when OMS sends to broker, or only in SQLite and rely on fills for audit.

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

Indexes: `account_id`, `symbol`, `executed_at` (fills), `(account_id, symbol)` (positions).

### 3.2 Alembic workflow

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
- `order_id` (internal UUID or string)  
- `broker` (e.g. `binance`) — OMS uses this to select the broker adapter.  
- `account_id` (optional; default account if single)  
- `symbol`, `side` (BUY/SELL), `quantity`, `order_type` (MARKET, LIMIT, etc.)  
- `price` (optional; for LIMIT)  
- `strategy_id` (optional), `created_at` (ISO)

**oms_fills (fill or reject)** — defined in OMS (12.1.5), consumed by Booking (12.2.5)  
- `event_type`: `fill` | `reject`  
- `order_id` (internal), `broker_order_id`, `symbol`, `side`, `quantity`, `price`, `fee`, `fee_asset`  
- `executed_at` (ISO), `fill_id` (broker), optional `reject_reason` for rejections  

Each service documents its schemas in code or a service README (e.g. `oms/README.md`, `booking/README.md`). Streams are created on first XADD.

### 4.3 Consumer groups (optional)

For scaling, use Redis consumer groups (XREADGROUP) per stream so each message is processed once. For Phase 2 solo dev, a single consumer with XREAD (block) is enough.

---

## 5. OMS service (generic)

The OMS is **broker-agnostic**. It routes orders to the appropriate broker adapter and emits a unified fill/reject stream.

### 5.1 Role

- Consume from Redis stream `risk_approved` (XREAD block).
- For each message: read `broker` (and optional `account_id`); **select broker adapter** for that broker; generate internal `order_id` if not set; persist to **SQLite** (orders table, status `pending`), including `broker` and `account_id`.
- Call the adapter’s **place_order** (or equivalent); adapter talks to the broker API; OMS updates SQLite (e.g. `sent`).
- Adapter notifies OMS of **fills** and **rejects** (unified format); OMS publishes to Redis stream `oms_fills` and updates SQLite status (`filled` / `rejected` / `cancelled`).
- If no adapter exists for the message’s `broker`, log and optionally publish a reject to `oms_fills` or a dead-letter stream.

### 5.2 SQLite schema (OMS local, broker-agnostic)

- **orders:** `id` (PK), `internal_id` (UUID, unique), `broker` (e.g. `binance`), `account_id` (optional), `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, `price`, `status` (pending, sent, filled, rejected, cancelled), `created_at`, `updated_at`, optional payload (JSON).  
- Same schema regardless of broker; adapter-specific details can live in payload or a separate table if needed.

### 5.3 Broker adapter interface

- Adapters implement a common interface (e.g. `place_order(order)`, `start_fill_listener(callback)`, or `get_fills_since(...)`). OMS calls the adapter for the message’s `broker`.
- **Place order:** adapter returns broker order id (or reject).  
- **Fills:** adapter delivers fill/reject events to OMS in a **unified shape** (symbol, side, quantity, price, fee, executed_at, fill_id, event_type fill|reject) so OMS can always publish the same `oms_fills` schema.
- Registry: OMS holds a map `broker_name → adapter instance` (or factory). Binance is registered as the first adapter.

### 5.4 Config (generic)

- `REDIS_URL`, SQLite path (e.g. `/data/oms.db`). Per-broker config (e.g. Binance API keys) is loaded by the adapter or via OMS config keyed by broker/account.

### 5.5 Deployment

- Single **loop process**: XREAD `risk_approved` → route to adapter → handle adapter callbacks for fills/rejects → XADD `oms_fills`.  
- Docker: service `oms`; image from `oms/`; connect to `multistrat` network; env: `REDIS_URL`, SQLite path; optional volume for SQLite. No broker-specific env in the generic OMS image if adapters load their own config (or pass broker vars through env with a prefix, e.g. `BINANCE_*`).

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
- **Testnet:** Use Binance testnet for API keys and base URL; no real funds.
- **Rate limits:** Respect Binance weight/limit; back off or queue if needed.
- **Reconciliation:** Periodically compare OMS SQLite and Postgres fills with Binance “my trades” or order history.

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

- OMS (generic): `REDIS_URL`, SQLite path. Broker-specific vars (e.g. Binance) are used by the Binance adapter.
- Binance adapter: `BINANCE_*` vars (or passed from OMS env).
- Booking: `REDIS_URL`, `DATABASE_URL`.
- Position Keeper: `DATABASE_URL`, `REDIS_URL`.

---

## 12. Task checklist (implementation order)

**Bottom-up approach:** Build and unit-test each layer before integrating. Start with the lowest-level component (Binance API), then build up: adapter → OMS pieces → OMS integration → Booking pieces → Booking integration → Position Keeper pieces → Position Keeper integration. Each step should have unit tests before moving to the next.

**Schema ownership:** Each service defines and implements its own schemas (SQLite, Postgres, Redis streams, Redis cache keys) as part of its implementation. OMS owns `risk_approved` input and `oms_fills` output schemas; Booking owns Postgres schema (Alembic) and `oms_fills` input schema (must match OMS output); Position Keeper owns its read/write schemas.

### 12.1 OMS (build backwards: Binance API → adapter → OMS pieces → integration)

- [ ] **12.1.1** **Binance API client** (lowest level): HTTP client for Binance REST (place order, query order, cancel); HMAC-SHA256 signing; support testnet/production URLs. **Unit test:** mock HTTP responses; verify signing, request format, error handling.
- [ ] **12.1.2** **Binance fills listener** (websocket or poll): subscribe to user data stream or poll “my trades”; parse execution reports. **Unit test:** mock websocket messages or REST responses; verify fill/reject parsing.
- [ ] **12.1.3** **Binance broker adapter** (uses Binance client): implement adapter interface (`place_order`, `start_fill_listener(callback)`); convert Binance responses to unified fill/reject format. **Unit test:** mock Binance client; verify adapter calls client correctly and formats output.
- [ ] **12.1.4** **OMS SQLite schema and persistence**: define schema (broker-agnostic: `broker`, `account_id`, order fields, status); implement store/update orders. **Unit test:** in-memory SQLite; verify CRUD operations.
- [ ] **12.1.5** **OMS Redis stream schemas** (input/output): define `risk_approved` input schema (`broker`, `account_id`, `symbol`, `side`, `quantity`, `order_type`, `price`, etc.); define `oms_fills` output schema (`event_type`, `order_id`, `broker_order_id`, `symbol`, `side`, `quantity`, `price`, `fee`, `executed_at`, etc.). Document in code or `oms/README.md`. Streams are created on first XADD.
- [ ] **12.1.6** **OMS Redis consumer** (XREAD from `risk_approved`): parse messages per `risk_approved` schema, handle blocking read. **Unit test:** mock Redis client; verify message parsing and error handling.
- [ ] **12.1.7** **OMS broker adapter registry**: interface definition; registry (map `broker_name` → adapter); route by `broker`. **Unit test:** mock adapters; verify routing and error handling (unknown broker).
- [ ] **12.1.8** **OMS Redis producer** (XADD to `oms_fills`): format unified fill/reject events per `oms_fills` schema. **Unit test:** mock Redis client; verify message format.
- [ ] **12.1.9** **OMS integration** (wire pieces): Redis consumer → SQLite → adapter registry → adapter.place_order → adapter fill callback → SQLite update → Redis producer. **Integration test:** mock Redis streams and adapter; verify full flow for one order.
- [ ] **12.1.10** Register Binance adapter in OMS; add `BINANCE_*` to `.env.example` and `.env`.

### 12.2 Booking (build backwards: Postgres writes → Redis cache → consumer → integration)

- [ ] **12.2.1** **Booking Postgres schema** (Alembic): create revision(s) for `accounts`, `orders`, `fills`, `positions`, `balances`, optional `margin_snapshots`; implement `upgrade()` and `downgrade()`. Run `alembic upgrade head`. **Unit test:** verify schema creation and indexes.
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
- [ ] Position Keeper reads positions/balances and writes PnL/margin to Postgres or Redis.
- [ ] In pgAdmin: `fills`, `positions`, `balances` (and optional `margin_snapshots`) reflect the test fill.
- [ ] In RedisInsight: `oms_fills` stream has the fill event; cache keys (e.g. `positions:1`) are present and updated.

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
│   ├── main.py             # generic loop: risk_approved → SQLite → adapter.place_order → adapter fills → oms_fills
│   ├── adapter.py          # broker adapter interface + registry
│   ├── brokers/
│   │   ├── __init__.py
│   │   └── binance.py      # Binance adapter (first broker)
│   └── oms.db              # SQLite (or mount volume)
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
- **OMS SQLite:** In-memory SQLite; test CRUD, queries, idempotency.
- **OMS Redis consumer/producer:** Mock Redis client (e.g. `fakeredis`); test XREAD parsing, XADD formatting, error handling.
- **OMS adapter registry:** Mock adapters; test routing by `broker`, unknown broker handling.
- **Booking Postgres writes:** Test Postgres (or testcontainers/test DB); test SQL, idempotency (duplicate fill_id).
- **Booking Redis cache:** Mock Redis; test key format, updates.
- **Position Keeper calculations:** Pure functions; test PnL/margin math given positions/balances/fills.
- **Position Keeper reads/writes:** Mock Postgres/Redis or use test DB; test queries and writes.

### 16.2 Integration tests (wire pieces)

- **OMS integration:** Mock Redis streams (`risk_approved`) and adapter; verify: consumer → SQLite → adapter.place_order → adapter callback → SQLite update → producer (`oms_fills`).
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
