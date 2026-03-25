# Multistrategy Trading System

A minimal yet extendable multistrategy trading system designed for solo development, featuring Docker-based isolation, Redis Streams for asynchronous communication, and Postgres for persistent data storage.

## Architecture Overview

The system is built on a microservices architecture with event-driven communication patterns, optimized for reliability and scalability while maintaining simplicity for individual developers.

### Infrastructure Stack

- **Docker Compose**: Orchestrates all services in isolated containers
- **Redis Streams**: Handles asynchronous event-driven communication between services
  - `strategy_orders`: Order intents from strategies
  - `risk_approved`: Risk-approved orders
  - `oms_fills`: Order fills from the Order Management System
- **Postgres**: Central database for persistent data storage (orders audit, fills, positions, balances)
- **Redis**: OMS order staging (hashes + indexes); periodic sync of orders to Postgres for audit

## Core Services

All core services operate as infinite loop-based processes that consume and produce Redis Streams:

### Market Data Service
- Fetches market data feeds from external sources (e.g. Binance spot klines, perps basis & open interest)
- Updates Postgres database with historical data (`ohlcv`, `basis_rate`, `open_interest`, …)
- Maintains Redis cache for real-time data access

### Strategies Service
- Queries market data from Postgres/Redis
- Generates trading signals and order intents
- Publishes order intents to `strategy_orders` Redis Stream

### Risk Management Service
- Consumes order intents from `strategy_orders` stream
- Performs risk checks (position limits, margin requirements, etc.)
- Queries Redis cache for balance/margin data pre-approval
- Publishes approved/rejected orders to `risk_approved` stream

### Order Management System (OMS)
- Consumes approved orders from `risk_approved` stream
- Stages orders in Redis (fast updates); periodically syncs to Postgres `orders` table for audit
- Executes orders via broker API (polling/websocket)
- Publishes fills and rejections to `oms_fills` stream

### Booking Service
- Consumes fills from `oms_fills` stream
- Builds and maintains positions, balances, and margin calculations
- Writes position data to Postgres (source of truth)
- Updates Redis cache for real-time access

### Position Keeper Service
- Aggregates real-time PnL and margin calculations
- Provides position tracking and monitoring
- Queries booking data for position updates

### Admin Service
- Provides GUI/CLI interface for system administration
- Publishes commands to targeted Redis streams
- Handles manual interventions and recommendations

## Data Flows

```
Market Data → Postgres/Redis Cache
                ↓
Strategies → Redis.strategy_orders → Risk → Redis.risk_approved
                                                      ↓
OMS → Broker API (poll/websocket) → Redis.oms_fills → Booking → Postgres
                                                                   ↓
                                                          Position Keeper
                                                          
Admin → Redis Streams (for operational commands)
```

### Key Data Flow Points

1. **Market Data Flow**: External feeds → Postgres (persistent) + Redis (cache)
2. **Order Flow**: Strategies → Risk → OMS → Broker → Booking → Postgres
3. **Position Tracking**: Booking maintains positions/balances/margin in Postgres (truth), Redis (cache)
4. **Risk Checks**: Risk service queries Redis cache for balance/margin before approval
5. **Administration**: Admin service publishes commands to targeted streams for operational control

## Design Principles

- **Minimal**: Core functionality without unnecessary complexity
- **Extendable**: Easy to add new strategies, services, or features
- **Isolated**: Docker containers provide service isolation
- **Event-Driven**: Redis Streams enable loose coupling between services
- **Persistent**: Postgres ensures data durability
- **Real-Time**: Redis cache enables fast data access for trading decisions

## Implementation Phases

Implementation is planned in five phases:

| Phase | Focus | Key deliverable |
|-------|--------|------------------|
| **1** | Docker, Postgres, Redis | Infra stack; `docker-compose up` |
| **2** | OMS, Booking, Position (Binance) | E2E order → fill → positions/balances/margin |
| **3** | Admin | Commands via streams; view positions/fills |
| **4** | Market data | Postgres + Redis populated with market data |
| **5** | Strategy modules | Strategies → Risk → OMS → Booking automated |

See **[docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md)** for detailed tasks, dependencies, and acceptance criteria for each phase.

## Getting Started

### Prerequisites

- **Docker** and **Docker Compose** (or `docker compose` v2)
- **Python 3.x** (for Alembic migrations; optional for later phases)

### Installation (Phase 1: infra + migrations)

1. **Clone the repository**
   ```bash
   git clone <repo-url>
   cd Multistrat
   ```

2. **Configure environment**
   - Copy `.env.example` to `.env`
   - Edit `.env` if you need different Postgres/pgAdmin credentials or ports

3. **Start the stack**
   ```bash
   docker compose up -d
   ```
   This starts Postgres, Redis, pgAdmin, and RedisInsight. Images are pulled automatically if missing.

4. **Run database migrations**
   ```bash
   pip install -r requirements.txt
   python -m alembic upgrade head
   ```
   (Use a virtualenv if you prefer: `python -m venv .venv`, then activate and run the same commands.)

5. **Verify**
   - **Compose:** `docker compose ps` — all services should be up (and healthy where applicable). Phase 2 adds the **oms** service; start it with `docker compose up -d oms` if not already running.
   - **Postgres:** `docker compose exec postgres psql -U multistrat -d multistrat -c "SELECT 1"`
   - **Redis:** `docker compose exec redis redis-cli ping` (should return `PONG`)
   - **pgAdmin:** http://localhost:5050 — log in with `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` from `.env`; add server host `postgres`, port `5432`, user/password/db from `.env`.
   - **RedisInsight:** http://localhost:5540 — add Redis host `redis`, port `6379`.

### Running the OMS (Phase 2)

The OMS consumes `risk_approved`, places orders via the registered broker adapter(s), and publishes fills to `oms_fills`. It can run as a **Docker service** (recommended for E2E) or locally.

1. **Environment:** Copy `.env.example` to `.env`. Set:
   - **REDIS_URL** — Redis connection (e.g. `redis://localhost:6379` on host; OMS container uses `redis://redis:6379` via compose)
   - **DATABASE_URL** — optional; if set, terminal orders are synced to Postgres and Redis keys get a TTL after sync
   - **BINANCE_API_KEY**, **BINANCE_API_SECRET** — required for Binance; **BINANCE_BASE_URL** (e.g. `https://testnet.binance.vision` for testnet)
2. **Run as Docker service (recommended for pipeline test):**
   ```bash
   docker compose up -d oms pms risk market_data
   ```
   Or use the deployment script: `.\scripts\update_and_deploy.ps1` (builds and starts oms, pms, risk, market_data). The **risk** service consumes `strategy_orders` and publishes to `risk_approved`; **oms** consumes `risk_approved`. **market_data** runs scheduled OHLCV ingest to Postgres. Running `docker compose up -d` starts all defined services including these app services.
3. **Full pipeline test (against the OMS service):** With OMS running in Docker (and Postgres/Redis up), from repo root:
   ```bash
   python scripts/full_pipeline_test.py
   ```
   The script injects one order into `risk_approved`, waits for the OMS to process it (polls up to 60s), then checks downstreams: `oms_fills`, Redis order store, and Postgres `orders`. Requires **REDIS_URL** (and optionally **DATABASE_URL** for the Postgres check) on the host; no Binance keys needed in the script (the container has them).
4. **Run locally (development):** `python -m oms.main`  
   Starts fill listeners and the main loop (`process_one` / `process_one_cancel`, periodic stream trim). Stop with Ctrl+C or SIGTERM.

### Configuration (later phases)

- Configure market data sources in `config/market_data.yml`
- Set broker API credentials in environment variables (see `.env.example` for `BINANCE_*`)
- Adjust risk parameters in `config/risk.yml`

## Development

This system is designed for solo development with a focus on:
- Clear service boundaries
- Simple debugging (each service runs independently)
- Easy testing (services can be mocked via Redis Streams)
- Incremental development (add strategies/services as needed)

## License

See [LICENSE](LICENSE) file for details.
