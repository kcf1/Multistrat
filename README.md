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
- **Postgres**: Central database for persistent data storage
- **SQLite**: Staging database within the OMS for order management

## Core Services

All core services operate as infinite loop-based processes that consume and produce Redis Streams:

### Market Data Service
- Fetches market data feeds from external sources
- Updates Postgres database with historical data
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
- Stages orders in SQLite database
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

- Docker and Docker Compose
- Access to market data feeds
- Broker API credentials

### Installation

1. Clone the repository
2. Configure environment variables (see `.env.example`)
3. Start services with Docker Compose:
   ```bash
   docker-compose up -d
   ```

### Configuration

- Configure market data sources in `config/market_data.yml`
- Set broker API credentials in environment variables
- Adjust risk parameters in `config/risk.yml`

## Development

This system is designed for solo development with a focus on:
- Clear service boundaries
- Simple debugging (each service runs independently)
- Easy testing (services can be mocked via Redis Streams)
- Incremental development (add strategies/services as needed)

## License

See [LICENSE](LICENSE) file for details.
