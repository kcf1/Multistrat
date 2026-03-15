# Implementation Plan

Phased rollout for the multistrategy trading system. Each phase is designed to deliver a working slice that can be tested and extended.

---

## Phase 1: Docker, Postgres, Redis

**Goal:** Run infra locally with Docker Compose; Postgres with **version-controlled migrations**; Redis ready for streams and persistence.

**Detailed plan:** [docs/PHASE1_DETAILED_PLAN.md](PHASE1_DETAILED_PLAN.md) — task order, migration layout, and acceptance steps.

### Deliverables

- [ ] **Docker Compose stack**
  - `docker-compose.yml` with services: `postgres`, `redis`, `pgadmin` (Postgres UI at http://localhost:5050), `redisinsight` (Redis UI at http://localhost:5540)
  - Health checks and restart policies
  - Named volumes for Postgres data; (optional) Redis persistence later
- [ ] **Postgres (version-controlled)**
  - Image: `postgres:16-alpine` (or current LTS)
  - Env: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
  - Port: 5432 (host mapping optional for dev)
  - **Migrations:** [Alembic](https://alembic.sqlalchemy.org/): `alembic init`, `alembic/versions/`, `DATABASE_URL` in env; apply with `alembic upgrade head`. No schema in `docker-entrypoint-initdb.d/` — all schema via Alembic only.
- [ ] **Redis**
  - Image: `redis:7-alpine`
  - Port: 6379
  - No persistence required initially; enable AOF later if needed
- [ ] **Networking**
  - Single `multistrat` network so future app containers can reach Postgres and Redis
- [ ] **Local dev**
  - `.env.example` with `POSTGRES_*`, `REDIS_URL`, `DATABASE_URL`
  - Run stack with `docker-compose up -d` (Compose pulls images when missing; optional pre-pull: `docker-compose pull`).
  - README/Getting Started: `docker-compose up -d`, then `alembic upgrade head`; verify Postgres and Redis from host

### Out of scope (later phases)

- Application containers (Phase 2+)
- Stream creation (streams created when first service uses them)
- Application Postgres schema (Phase 2: Booking/positions) — Phase 1 only has `alembic_version`

### Acceptance

- [ ] `docker-compose up -d` succeeds; `docker-compose ps` shows postgres, redis, pgadmin, and redisinsight healthy (or running)
- [ ] Alembic applies initial revision; `alembic_version` has one row; `alembic upgrade head` re-run is idempotent
- [ ] Can connect to Postgres and Redis from host (e.g. psql, redis-cli) using `.env` values

---

## Phase 2: OMS, Booking, Position (Binance first)

**Goal:** End-to-end order flow: approved orders → **generic OMS** (routes to broker adapters) → **Binance** (first broker) → fills → Booking → Postgres positions/balances/margin; Position Keeper for PnL/margin. OMS is broker-agnostic; Binance is the first adapter.

**Detailed plan:** [docs/PHASE2_DETAILED_PLAN.md](PHASE2_DETAILED_PLAN.md) — schema, streams, generic OMS + broker adapters, task order (build OMS first), and acceptance.

### Dependencies

- Phase 1 complete (Postgres, Redis, Docker network)
- Binance API keys (testnet or mainnet) and decision on spot vs futures

### Deliverables

- [ ] **Postgres schema (Booking / Position Keeper)**
  - Tables: e.g. `accounts`, `balances`, `balance_changes`, `fills`, `orders` (positions in Redis only; margin_snapshots dropped in OMS)
  - Migrations: single initial migration or versioned (e.g. `migrations/` with numbering)
- [ ] **Redis Streams**
  - Define and document stream names: `strategy_orders`, `risk_approved`, `oms_fills`
  - Message schemas (JSON) for: order intent, risk-approved order, fill/reject
- [ ] **OMS service (generic)**
  - Single process, loop: consume from `risk_approved` (XREAD block); route by `broker` to a **broker adapter**; stage orders in **Redis** (hashes + status indexes, broker-agnostic); receive fills/rejects from adapter; publish unified events to `oms_fills`. **Periodic sync** of orders from Redis to Postgres `orders` table for audit and recovery.
  - **Broker adapter interface:** place_order, fill/reject callback; registry (e.g. `broker_name` → adapter). **Binance** is the first adapter: place order (REST), websocket or poll for fills, convert to unified format
  - Config: REDIS_URL, DATABASE_URL (optional, for order sync); Binance adapter: base URL, API key/secret, testnet
  - Deploy as Docker service; connect to Redis and Postgres (for sync)
- [ ] **Booking service**
  - Single process, loop: consume from `oms_fills` (XREAD block)
  - For each fill: update positions (add/subtract), update balances, compute margin if applicable
  - Write to Postgres: positions, balances, margin snapshots, and raw fills for audit
  - Update Redis cache: e.g. `positions:{account}`, `balance:{account}`, `margin:{account}` for Risk and Position Keeper
- [ ] **PMS (Portfolio Management System)** — *Core vs optional: **docs/pms/PMS_ARCHITECTURE.md** §2; task list **docs/PHASE2_DETAILED_PLAN.md** §12.3*
  - **Core:** Single process, loop: read orders + balances from Postgres → derive positions (signed open_qty, position_side per account/book/symbol) → one mark price provider (e.g. Binance) → compute PnL/margin → write **positions** table (open_qty signed, position_side) + Redis (`pnl:{account_id}`, `margin:{account_id}`)
  - **Optional:** fills table, symbols table, book_allocations, pnl_snapshots, margin_snapshots, book_cash, Pydantic, repairs, second mark price source (Phase 4 Redis/DB)
  - **Mark price:** Interface first; Phase 2 wrap Binance; Phase 4 read from Redis/DB (Market Data). Config: `PMS_MARK_PRICE_SOURCE`
  - Expose via simple API (optional) or just write to Postgres/Redis for Admin/UI later
- [ ] **Risk service (Phase 2: no-rule interface first, then add rules on demand)**
  - **Interface first:** Consume `strategy_orders`, validate schema, forward to `risk_approved` in the same (or richer) schema OMS expects. No rules required for E2E.
  - **Add rules on demand:** Rule engine runs an ordered list of checks; empty list = pass-through. Add rules (e.g. min/max qty, allowed venues) when needed; see **docs/risk/RISK_SERVICE_PLAN.md**. Test inject to `risk_approved` remains available when Risk is omitted.
- [ ] **E2E test path**
  - Manually or script: push a test order to `risk_approved` → OMS picks up → Binance testnet execution → fill → Booking updates Postgres/Redis → Position Keeper shows updated PnL/margin

### Binance-specific notes

- **Spot vs Futures:** Decide one first (e.g. futures for margin/leverage). Implement order types used (market, limit, reduce-only if futures).
- **Auth:** Use API key + secret; sign REST requests; support testnet and production base URLs.
- **Time sync:** Binance rejects requests if timestamp is outside server-time window (-1021). The API client syncs via GET `/api/v3/time` and applies offset to all signed requests (REST and ws-api subscribe); on sync fetch failure the last good offset is preserved. See `docs/BINANCE_API_RULES.md` §6 for ws-api connection issues.
- **Fills:** Prefer websocket for fills (e.g. user data stream via WebSocket API or listenKey) to reduce latency; fallback to REST poll. Fills listener uses same time sync for subscribe message.
- **Ids:** Map Binance `clientOrderId` / `orderId` to internal order id in Redis (and Postgres via sync) for idempotency and reconciliation.

### Acceptance

- [ ] One test order flows: Risk (or test inject) → OMS → Binance (testnet) → fill → Booking → Postgres and Redis updated; Position Keeper reflects position and PnL/margin.

---

## Phase 3: Admin

**Goal:** Admin interface (GUI or CLI) to send commands and view state; commands target Redis streams and/or services.

### Dependencies

- Phase 1 and 2 (streams exist; Booking/Position Keeper maintain state)

### Deliverables

- [ ] **Command model**
  - Define admin commands: e.g. “cancel order”, “flush risk queue”, “manual order”, “start/stop strategy”, “refresh balance/margin”
  - Map each command to a target: Redis stream (e.g. `admin_commands`) or dedicated streams per domain (e.g. `oms_commands`)
- [ ] **Admin service**
  - Backend that publishes to the appropriate Redis stream(s) with payload (command type, params, id, timestamp)
  - Optional: thin HTTP or WebSocket server for GUI
- [ ] **Consumers (in existing services)**
  - OMS: consume `oms_commands` (or `admin_commands` filtered) for cancel/manual order
  - Risk: consume for flush/override (if allowed)
  - Strategy runner (Phase 5): consume for start/stop
- [ ] **Admin UI or CLI**
  - **Option A – CLI:** Scripts (e.g. Python) that publish to Redis (e.g. `redis-cli` or small app)
  - **Option B – Web GUI:** Simple SPA or server-rendered page that lists positions, balances, open orders, recent fills; buttons/forms that call Admin service to publish commands
- [ ] **Read-only views**
  - Positions, balances, margin, open orders, recent fills from Postgres or Redis cache (read-only for Admin)

### Acceptance

- [ ] Admin can send at least: manual order (to `risk_approved` or OMS stream), cancel order; view positions and recent fills from Postgres/Redis.

---

## Phase 4: Market Data

**Goal:** Ingest market data into Postgres and Redis so strategies can query it.

### Dependencies

- Phase 1 (Postgres, Redis)

### Deliverables

- [ ] **Market data service**
  - Single process, loop: connect to Binance (or chosen venue) market data (REST + WebSocket)
  - **REST:** Historical candles (and/or trades) on a schedule; write to Postgres (e.g. `candles`, `trades` tables)
  - **WebSocket:** Live ticker/candles/trades; write to Redis (e.g. `market:{symbol}:ticker`, `market:{symbol}:candle_1m`) and optionally latest to Postgres (e.g. latest candle per symbol)
- [ ] **Schema (Postgres)**
  - Tables: e.g. `candles` (symbol, interval, open_time, o/h/l/c/v), `trades` if needed
  - Indexes for strategy queries (symbol, time range)
- [ ] **Redis key layout**
  - Document keys and TTLs; keep hot path minimal (e.g. last N candles, current ticker)
- [ ] **Config**
  - Symbols to subscribe; intervals; Binance (or other) endpoints
- [ ] **Deploy**
  - Docker service; same network as Postgres/Redis

### Acceptance

- [ ] After running Market Data service: Postgres has historical candles for selected symbols; Redis has up-to-date ticker/latest candle; no dependency on strategies yet.

---

## Phase 5: Strategy Modules

**Goal:** One or more strategies that read market data, produce order intents to `strategy_orders`; Risk consumes and approves/rejects to `risk_approved`.

### Dependencies

- Phase 1, 2, 4 (Redis streams; OMS/Booking/Position; Market Data in Postgres/Redis)
- Phase 3 optional but recommended (Admin to start/stop strategies)

### Deliverables

- [ ] **Strategy runner / harness**
  - Single process per strategy (or one process that runs multiple strategies in threads/async): loop over each strategy’s interval
  - For each strategy: read from Postgres/Redis (candles, ticker, positions from Redis cache); compute signals; produce order intents (symbol, side, qty, type, etc.) to `strategy_orders`
  - Config: which strategies enabled; params per strategy
- [ ] **Risk service (full)**
  - Same interface as Phase 2 (no-rule first); **add rules on demand**: position limits, max order size, margin/balance checks using Redis/Postgres from OMS/PMS
  - Publish approved to `risk_approved`; optionally publish rejections to a stream or log for debugging
- [ ] **At least one strategy**
  - Example: simple MA cross or fixed-size test strategy that emits one order type (e.g. market) to Binance
  - Shared interface: e.g. `next_signal(market_state, position_state) -> list[OrderIntent]`
- [ ] **Strategy contract**
  - Order intent schema (JSON) matching what Risk and OMS expect (symbol, side, quantity, order_type, optional price, strategy_id, etc.)
- [ ] **Deploy**
  - Docker services: `strategy_runner`, `risk`; both on same network, Redis and (for strategy) Postgres/Redis for market data and positions

### Acceptance

- [ ] Strategy produces an order intent; Risk approves it; OMS sends to Binance; fill is booked and Position Keeper updates; end-to-end automated flow works with at least one strategy.

---

## Summary Table

| Phase | Focus                         | Depends on | Key output                                      |
|-------|-------------------------------|------------|--------------------------------------------------|
| 1     | Docker, Postgres, Redis       | —          | Infra up; `.env`; `docker-compose up`            |
| 2     | OMS, Booking, Position (Binance) | 1       | E2E order → fill → positions/balances/margin    |
| 3     | Admin                         | 1, 2       | Commands via streams; view positions/fills      |
| 4     | Market data                   | 1          | Postgres + Redis populated with market data     |
| 5     | Strategy modules              | 1, 2, 4    | Strategies → Risk → OMS → Booking automated     |

---

## Testing (corresponding to each phase)

The plans do not yet define automated tests for every deliverable. Below is a suggested test mapping; add or automate as you go.

| Phase | Suggested tests | Notes |
|-------|-----------------|--------|
| **1** | **Smoke / verification:** Compose up succeeds; Postgres accepts connections; Redis `PING`; `alembic upgrade head` idempotent. | Can be a small script or CI step (e.g. `docker-compose up -d && psql $DATABASE_URL -c 'SELECT 1' && redis-cli -u $REDIS_URL ping && alembic current`). |
| **2** | **Unit:** Component tests with mocked dependencies (API client, Redis store, consumer/producer). **Integration:** OMS flow with fakeredis and mock adapters (`test_oms_integration.py`). **E2E (code-level):** Real Redis + Binance testnet (`test_oms_redis_testnet.py`). **E2E (service-level):** Black-box script (`scripts/full_pipeline_test.py`) assumes running services. See [PHASE2_DETAILED_PLAN.md](PHASE2_DETAILED_PLAN.md#165-test-classification-and-file-mapping) for test classification. | Mock or testnet only; avoid real money. |
| **3** | **Integration:** Admin publishes command to stream; target service consumes and reacts. **Smoke:** Manual order and cancel from CLI or GUI. | |
| **4** | **Integration:** Market data service writes candles to Postgres and latest to Redis; query by symbol/interval. **Smoke:** After run, candles exist for configured symbols. | |
| **5** | **Unit:** Strategy `next_signal()` given mock state; Risk checks (limits, margin). **Integration:** Strategy → Risk → `risk_approved`; full E2E with testnet. | |

- **Where to put tests:** Per-service (e.g. `oms/tests/`, `booking/tests/`) or a top-level `tests/` with subdirs per phase/service. Use one runner (e.g. pytest) and `requirements-dev.txt` if needed.
- **CI:** Run Phase 1 verification on every PR; add Phase 2+ tests as those services land.
- **Test documentation:** See [TESTING.md](docs/TESTING.md) for complete test inventory, classification, and execution instructions.

---

## Suggested repo layout (by end of Phase 2)

```
multistrat/
├── docker-compose.yml
├── .env.example
├── README.md
├── docs/
│   └── IMPLEMENTATION_PLAN.md
├── migrations/          # Postgres migrations
├── oms/                  # OMS service (Redis staging + Postgres sync, Binance adapter)
├── booking/              # Booking service
├── pms/                  # PMS (Portfolio Management System) — source of truth for PnL/margin
├── risk/                 # Risk service (minimal in P2, full in P5)
├── admin/                # Admin service + CLI or GUI (Phase 3)
├── market_data/          # Market data service (Phase 4)
├── strategies/           # Strategy runner + strategy modules (Phase 5)
└── config/               # Shared config files (optional)
```

Each service can have its own `Dockerfile`, `requirements.txt` or `go.mod`, and small README for run instructions.
