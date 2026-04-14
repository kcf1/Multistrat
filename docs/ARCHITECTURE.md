# Current Architecture — Modules, Data Stores, and Interfaces

Single reference for the multistrategy trading system: directory layout by module, Postgres layout (schemas + tables), Redis streams and keys, and pointers to detailed docs.

**See also:** [POSTGRES_SCHEMA_GROUPING_PLAN.md](POSTGRES_SCHEMA_GROUPING_PLAN.md) (per-service `search_path`, cross-schema `schema.table`), [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md), [PHASE2_DETAILED_PLAN.md](PHASE2_DETAILED_PLAN.md), [PHASE3_DETAILED_PLAN.md](PHASE3_DETAILED_PLAN.md), [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md), [DATASET_INGESTION_STEPS.md](market_data/DATASET_INGESTION_STEPS.md), [oms/OMS_ARCHITECTURE.md](oms/OMS_ARCHITECTURE.md), [pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md), [risk/RISK_SERVICE_PLAN.md](risk/RISK_SERVICE_PLAN.md), [strategies/STRATEGY_MODULE_ARCHITECTURE.md](strategies/STRATEGY_MODULE_ARCHITECTURE.md).

---

## 1. High-level flow

```
┌─────────────────┐   strategy_orders    ┌─────────────────┐   risk_approved    ┌──────────────────────────────────────────────┐
│ Strategies /    │ ───────────────────► │ Risk            │ ─────────────────► │ OMS                                            │
│ Test inject     │   Redis Stream        │ (optional)      │   Redis Stream     │ • risk_approved → place/cancel → broker       │
└─────────────────┘                       └─────────────────┘                    │ • Broker user stream → account/positions     │
                                                                                  │ • Sync: orders, accounts, balances → Postgres │
┌─────────────────┐   cancel_requested    └─────────────────┬────────────────────┘
│ Risk / Admin    │ ───────────────────►                    │
└─────────────────┘   Redis Stream                          │
                                                             ▼
                    ┌───────────────────────────────────────┼───────────────────────────────────────┐
                    │                                       │                                       │
                    ▼                                       ▼                                       ▼
           ┌───────────────────┐                ┌───────────────────┐                ┌────────────────────────────┐
           │ Broker (Binance)  │                │ Redis             │                │ Postgres (schemas)         │
           │ REST + User stream│                │ orders:*,         │                │ oms: orders, accounts, …   │
           └───────────────────┘                │ account:*,        │                │ pms: assets, positions     │
                                               │ oms_fills stream  │                │ market_data: ohlcv, …      │
                                               └─────────┬─────────┘                │ scheduler: scheduler_runs  │
                                                         │                          │ public: alembic_version    │
                                                         │
                                                         ▼
                                                ┌───────────────────┐   Cross-schema reads (e.g. `oms.orders`,
                                                │ PMS                │   `oms.symbols`) — see grouping plan §7.8.
                                                │ • Read Postgres    │ ◄───────────────────────┘
                                                │   (OMS + PMS       │
                                                │    tables)       │
                                                │ • Derive positions │
                                                │ • Write `pms` home │
                                                └───────────────────┘
```

---

## 2. Directory layout by module

| Directory | Owner | Purpose |
|-----------|--------|---------|
| **oms/** | OMS | Order management: consume `risk_approved` / `cancel_requested`, broker adapters, Redis order/account store, sync to Postgres, produce `oms_fills`. |
| **pms/** | PMS | Portfolio: read Postgres (**`oms.*`** cross-schema + **`pms`** home) → derive positions → write **`pms.positions`**. Does not consume Redis. |
| **risk/** | Risk | Pre-trade: consume `strategy_orders`, rule engine (optional), produce `risk_approved`. |
| **market_data/** | (Phase 4) | Public market feeds → Postgres (`ohlcv`, …) + Redis hot keys (`market:{symbol}:…`). Default symbol universe: [`market_data/universe.py`](../market_data/universe.py) (`DATA_COLLECTION_SYMBOLS`, USDT spot only). See [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md). |
| **strategies/** | (Phase 6) | Dedicated strategy domain package (isolated from infra modules): strategy runner, shared strategy contracts, and per-strategy modules producing `strategy_orders`. See [strategies/STRATEGY_MODULE_ARCHITECTURE.md](strategies/STRATEGY_MODULE_ARCHITECTURE.md). |
| **admin/** | (Phase 3) | Admin CLI/API: publish commands to streams, read-only views over Postgres/Redis. |
| **scheduler/** | (Phase 5) | Scheduled **reports**, **reconciliation** (orders/positions vs venue), and **misc** batch jobs; not streaming OMS/PMS. See [scheduler/SCHEDULER_ARCHITECTURE.md](scheduler/SCHEDULER_ARCHITECTURE.md) and [PHASE5_DETAILED_PLAN.md](PHASE5_DETAILED_PLAN.md). |
| **alembic/** | Shared | Postgres migrations; revisions create tables then **move** them into app schemas **`oms`**, **`pms`**, **`market_data`**, **`scheduler`** (see [POSTGRES_SCHEMA_GROUPING_PLAN.md](POSTGRES_SCHEMA_GROUPING_PLAN.md)). |
| **scripts/** | Shared | E2E tests, deploy, reset (e.g. `e2e_with_risk.py`, `update_and_deploy.ps1`, `reset_redis_postgres.ps1`). |
| **docs/** | Shared | Architecture and phase plans; `docs/oms/`, `docs/pms/`, `docs/risk/` for module-specific docs. |

---

## 3. Postgres schema (current)

All application tables are created by **Alembic** then moved into **owner schemas** (`oms`, `pms`, `market_data`, `scheduler`). Each service sets `SET search_path TO <home>, public` on connect (**`pgconn`**); SQL against another module’s tables uses qualified names (e.g. `oms.orders`, `pms.positions`, `market_data.ohlcv`). **`public.alembic_version`** stays in `public` for default Alembic behavior. Full mapping: [POSTGRES_SCHEMA_GROUPING_PLAN.md](POSTGRES_SCHEMA_GROUPING_PLAN.md) §5–§7.8.

### 3.1 Schema `oms` (OMS home)

| Table | Purpose | Key columns / grain |
|-------|---------|----------------------|
| **orders** | Audit and recovery; synced from OMS Redis on terminal status + periodic | `internal_id` (UUID), `account_id` (TEXT, broker account id), `broker`, `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, `price`, `limit_price`, `time_in_force`, `status`, `executed_qty`, `book`, `comment`, `binance_*`, `payload` (JSONB). See [oms/OMS_ORDERS_DB_FIELDS.md](oms/OMS_ORDERS_DB_FIELDS.md). |
| **accounts** | One row per broker account | `id` (PK), `account_id` (broker id), `name`, `broker`, `env`, `created_at`, `config` (JSONB). Unique `(broker, account_id)`. |
| **balances** | Current balance per asset | `id`, `account_id` (FK → accounts.id), `asset`, `available`, `locked`, `updated_at`. Unique `(account_id, asset)`. |
| **balance_changes** | History of deposits/withdrawals/transfers | `id`, `account_id` (TEXT broker id), `asset`, `book`, `change_type`, `delta`, … See [oms/BALANCE_CHANGES_HISTORY.md](oms/BALANCE_CHANGES_HISTORY.md). |
| **symbols** | Trading pair metadata; OMS writes; PMS reads via **`oms.symbols`** | `symbol` (PK), `base_asset`, `quote_asset`, `tick_size`, `lot_size`, `min_qty`, `product_type`, `broker`, `updated_at`. |

### 3.2 Schema `pms` (PMS home)

| Table | Purpose | Key columns / grain |
|-------|---------|----------------------|
| **assets** | Per-asset USD price for position valuation | `asset` (PK), `usd_symbol`, `usd_price`, `price_source`, `updated_at`. Populated by PMS asset init + asset price feed. See [pms/ASSET_PRICE_FEED_PLAN.md](pms/ASSET_PRICE_FEED_PLAN.md). |
| **positions** | Derived positions at asset level | Grain `(broker, account_id, book, asset)`; `usd_notional` generated. Written by `granular_store.write_pms_positions`. See [pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md). |

### 3.3 Schema `market_data` (ingest + cursors)

| Table family | Purpose |
|--------------|---------|
| **ohlcv** | Historical OHLCV bars per symbol and interval (PK `(symbol, interval, open_time)`). PMS OHLCV price provider reads **`market_data.ohlcv`**. |
| **Cursors** | `ingestion_cursor`, `basis_cursor`, `open_interest_cursor`, `taker_buy_sell_volume_cursor`, `top_trader_long_short_cursor` — ingest high-water marks. |
| **Series** | `basis_rate`, `open_interest`, `taker_buy_sell_volume`, `top_trader_long_short`. |

Details: [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md), [market_data/DATASET_INGESTION_STEPS.md](market_data/DATASET_INGESTION_STEPS.md).

### 3.4 Schema `scheduler` (scheduler home)

| Table | Purpose | Key columns / grain |
|-------|---------|----------------------|
| **scheduler_runs** | Audit log per job invocation | `id` (PK), `job_id`, `started_at`, `finished_at`, `status`, `error`, `payload` (JSONB). Written by `scheduler/run_history.py`. See [PHASE5_DETAILED_PLAN.md](PHASE5_DETAILED_PLAN.md) §4.4. |

### 3.5 Schema `public` (migrations only)

| Table | Purpose |
|-------|---------|
| **alembic_version** | Alembic revision pointer (convention: leave here). |

**Reports / reconciliation (CSV, not in Postgres):** under **`scheduler/reports_out/`** (gitignored): `position_snapshot_hourly` reads **`pms.positions`** and writes four CSVs; **`order_reconciliation_binance`** reads **`oms.orders`**; **`position_reconciliation_binance`** aggregates **`pms.positions`**. Details: [scheduler/SCHEDULER_ARCHITECTURE.md](scheduler/SCHEDULER_ARCHITECTURE.md) §8.

---

## 4. Redis

### 4.1 Streams

| Stream | Producer | Consumer | Purpose |
|--------|----------|----------|---------|
| **strategy_orders** | Strategies / test inject | Risk | Order intents from strategies. |
| **risk_approved** | Risk (or test inject) | OMS | Orders approved for execution; same schema as OMS expects. |
| **cancel_requested** | Risk / Admin | OMS | Cancel by `order_id` or `(broker_order_id, symbol)`. |
| **oms_fills** | OMS | Downstream (optional) | Unified fill/reject/cancelled/expired events. PMS does not consume Redis; it derives positions from Postgres orders. |

Constants: `risk/schemas.py` (STRATEGY_ORDERS_STREAM, RISK_APPROVED_STREAM); `oms/schemas.py` (RISK_APPROVED_STREAM, CANCEL_REQUESTED_STREAM, OMS_FILLS_STREAM).

### 4.2 Key patterns (OMS)

- **Order store** (`oms/storage/redis_order_store.py`): `orders:{order_id}` (Hash), `orders:by_status:{status}` (Set), `orders:by_book:{book}` (Set), `orders:by_broker_order_id:{broker_order_id}` (String → order_id).
- **Account store** (`oms/storage/redis_account_store.py`): `account:{broker}:{account_id}` (Hash), `account:{broker}:{account_id}:balances` (Hash asset→balance), `account:{broker}:{account_id}:positions` (Hash symbol→position), `accounts:by_broker:{broker}` (Set).
- **Retry:** `oms:retry:risk_approved:{entry_id}` for place-order retry count.

### 4.3 Key patterns (Market Data, Phase 4)

- **Namespace:** `market:{symbol}:…` (ticker, `ohlcv_bar`, `ohlcv_recent`, optional mark)—**planned** when **`market_data`** implements Redis (currently **deferred**; see [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md) §0). OMS must not reuse this prefix.

---

## 5. Module summaries

### 5.1 OMS (`oms/`)

- **Inputs:** Redis streams `risk_approved`, `cancel_requested`; broker user data stream (fills, account/balance updates).
- **Outputs:** Redis order store and account store; stream `oms_fills`; Postgres **`oms`** tables `orders`, `accounts`, `balances`, `balance_changes`, `symbols` (sync).
- **Details:** [oms/OMS_ARCHITECTURE.md](oms/OMS_ARCHITECTURE.md).

### 5.2 PMS (`pms/`)

- **Inputs:** Postgres: cross-schema reads **`oms.orders`**, **`oms.balance_changes`**, **`oms.symbols`**, **`oms.balances`** (optional); home **`pms.assets`**. (PMS does not consume Redis.)
- **Outputs:** Postgres **`pms.positions`** (broker, account_id, book, asset, open_qty, position_side, usd_price, usd_notional, updated_at).
- **Logic:** Each tick: derive positions from orders + balance_changes (FIFO, symbol→base/quote via `oms.symbols`), resolve USD price from `assets`, write via `granular_store.write_pms_positions`. Asset init and price feed: [pms/ASSET_PRICE_FEED_PLAN.md](pms/ASSET_PRICE_FEED_PLAN.md).
- **Details:** [pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md).

### 5.3 Risk (`risk/`)

- **Inputs:** Redis stream `strategy_orders`.
- **Outputs:** Redis stream `risk_approved`.
- **Logic:** Rule engine (optional); empty pipeline = pass-through. See [risk/RISK_SERVICE_PLAN.md](risk/RISK_SERVICE_PLAN.md).

### 5.4 Market data (Phase 4, `market_data/`)

- **Inputs:** Binance (or other) **public** REST + WebSocket; config symbols/intervals.
- **Outputs:** Postgres **`market_data`** tables (`ohlcv`, cursors, series — see §3.3); Redis keys under `market:{symbol}:…` (§4.3, **deferred** until PHASE4 §9.7–9.8).
- **Details:** [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md).

### 5.5 Admin (Phase 3, `admin/`)

- **Commands:** Publish to `risk_approved` (manual order), `cancel_requested` (cancel). Optional: flush risk queue, refresh account.
- **Read-only:** Postgres **`oms.*`** / **`pms.*`** as needed (`orders`, `accounts`, `balances`, `positions`, `symbols`, `assets`, …); Redis order/account store; optional `oms_fills` tail.
- **Details:** [PHASE3_DETAILED_PLAN.md](PHASE3_DETAILED_PLAN.md).

---

## 6. Deployment

- **Docker:** Single app image (e.g. `oms`) used for `oms`, `pms`, `risk`, and **`market_data`**; entrypoints: `python -m oms.main`, `python -m pms.main`, `python -m risk.main`, `python -m market_data.main` — see [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md) §8.
- **Scripts:** `scripts/update_and_deploy.ps1` builds once and starts `oms`, `pms`, `risk`, `market_data`; `docker compose up -d oms pms risk market_data`.
- **E2E:** `scripts/e2e_with_risk.py` injects to `strategy_orders`, asserts risk_approved → oms_fills → orders table; optional `--runs N` for timing stats.
