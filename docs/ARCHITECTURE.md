# Current Architecture — Modules, Data Stores, and Interfaces

Single reference for the multistrategy trading system: directory layout by module, Postgres schema (all tables), Redis streams and keys, and pointers to detailed docs.

**See also:** [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md), [PHASE2_DETAILED_PLAN.md](PHASE2_DETAILED_PLAN.md), [PHASE3_DETAILED_PLAN.md](PHASE3_DETAILED_PLAN.md), [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md), [DATASET_INGESTION_STEPS.md](market_data/DATASET_INGESTION_STEPS.md), [oms/OMS_ARCHITECTURE.md](oms/OMS_ARCHITECTURE.md), [pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md), [risk/RISK_SERVICE_PLAN.md](risk/RISK_SERVICE_PLAN.md).

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
           ┌───────────────────┐                ┌───────────────────┐                ┌───────────────────┐
           │ Broker (Binance)  │                │ Redis             │                │ Postgres           │
           │ REST + User stream│                │ orders:*,         │                │ orders, accounts,  │
           └───────────────────┘                │ account:*,        │                │ balances,         │
                                               │ oms_fills stream  │                │ balance_changes,  │
                                               └─────────┬─────────┘                │ symbols, assets,  │
                                                         │                          │ positions (PMS)   │
                                                         │
                                                         ▼
                                                ┌───────────────────┐   Read: orders, balances,
                                                │ PMS                │   symbols (→ legs), assets (→ usd_price)
                                                │ • Read Postgres    │ ◄───────────────────────┘
                                                │   (orders,         │
                                                │   balance_changes, │
                                                │   symbols, assets) │
                                                │ • Derive positions │
                                                │ • Write positions  │
                                                └───────────────────┘
```

---

## 2. Directory layout by module

| Directory | Owner | Purpose |
|-----------|--------|---------|
| **oms/** | OMS | Order management: consume `risk_approved` / `cancel_requested`, broker adapters, Redis order/account store, sync to Postgres, produce `oms_fills`. |
| **pms/** | PMS | Portfolio: read Postgres only (orders, balance_changes, symbols, assets) → derive positions → write `positions` table. Does not consume Redis. |
| **risk/** | Risk | Pre-trade: consume `strategy_orders`, rule engine (optional), produce `risk_approved`. |
| **market_data/** | (Phase 4) | Public market feeds → Postgres (`ohlcv`, …) + Redis hot keys (`market:{symbol}:…`). Default symbol universe: [`market_data/universe.py`](../market_data/universe.py) (`DATA_COLLECTION_SYMBOLS`, USDT spot only). See [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md). |
| **admin/** | (Phase 3) | Admin CLI/API: publish commands to streams, read-only views over Postgres/Redis. |
| **scheduler/** | (Phase 5) | Scheduled **reports**, **reconciliation** (orders/positions vs venue), and **misc** batch jobs; not streaming OMS/PMS. See [PHASE5_DETAILED_PLAN.md](PHASE5_DETAILED_PLAN.md). |
| **alembic/** | Shared | Postgres migrations; schema owned by OMS (orders, accounts, balances, balance_changes) and PMS (symbols, assets, positions). |
| **scripts/** | Shared | E2E tests, deploy, reset (e.g. `e2e_with_risk.py`, `update_and_deploy.ps1`, `reset_redis_postgres.ps1`). |
| **docs/** | Shared | Architecture and phase plans; `docs/oms/`, `docs/pms/`, `docs/risk/` for module-specific docs. |

---

## 3. Postgres schema (current)

All tables are managed via **Alembic** under `alembic/versions/`. OMS writes orders, accounts, balances, balance_changes; OMS or a sync job populates **symbols**; PMS (or asset feed) populates **assets** and writes **positions**.

### 3.1 OMS-owned tables

| Table | Purpose | Key columns / grain |
|-------|---------|----------------------|
| **orders** | Audit and recovery; synced from OMS Redis on terminal status + periodic | `internal_id` (UUID), `account_id` (TEXT, broker account id), `broker`, `broker_order_id`, `symbol`, `side`, `order_type`, `quantity`, `price`, `limit_price`, `time_in_force`, `status`, `executed_qty`, `book`, `comment`, `binance_*`, `payload` (JSONB). See [oms/OMS_ORDERS_DB_FIELDS.md](oms/OMS_ORDERS_DB_FIELDS.md). |
| **accounts** | One row per broker account | `id` (PK), `account_id` (broker id), `name`, `broker`, `env`, `created_at`, `config` (JSONB). Unique `(broker, account_id)`. |
| **balances** | Current balance per asset | `id`, `account_id` (FK → accounts.id), `asset`, `available`, `locked`, `updated_at`. Unique `(account_id, asset)`. |
| **balance_changes** | History of deposits/withdrawals/transfers | `id`, `account_id` (FK), `asset`, `book`, `change_type`, `delta`, `balance_before`, `balance_after`, `event_type`, `broker_event_id`, `event_time`, `created_at`, `payload`. See [oms/BALANCE_CHANGES_HISTORY.md](oms/BALANCE_CHANGES_HISTORY.md). |

### 3.2 Reference and valuation tables

| Table | Purpose | Key columns / grain |
|-------|---------|----------------------|
| **symbols** | Trading pair metadata; used for order→legs (base/quote) in position derivation | `symbol` (PK), `base_asset`, `quote_asset`, `tick_size`, `lot_size`, `min_qty`, `product_type`, `broker`, `updated_at`. Populated by OMS symbol sync (e.g. from exchange info) or reference job. See §12.2.14 in Phase 2 plan. |
| **assets** | Per-asset USD price for position valuation | `asset` (PK), `usd_symbol`, `usd_price`, `price_source`, `updated_at`. Populated by PMS asset init + asset price feed (e.g. Binance, manual, CoinGecko). See [pms/ASSET_PRICE_FEED_PLAN.md](pms/ASSET_PRICE_FEED_PLAN.md). |

### 3.3 PMS-owned table

| Table | Purpose | Key columns / grain |
|-------|---------|----------------------|
| **positions** | Derived positions at asset level; source of truth for PnL/valuation | Grain: `(broker, account_id, book, asset)`. Columns: `id`, `broker`, `account_id`, `book`, `asset`, `open_qty`, `position_side`, `usd_price`, `usd_notional` (generated: `open_qty * usd_price`), `updated_at`. Written by PMS `granular_store.write_pms_positions`. See [pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md), [pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md](pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md). |

### 3.4 Market data (Phase 4)

| Table | Purpose | Key columns / grain |
|-------|---------|---------------------|
| **ohlcv** | Historical OHLCV bars per symbol and interval | PK `(symbol, interval, open_time)`; columns `open`–`close`, `volume`, optional `quote_volume`, `trades`, `close_time`, `ingested_at`. See [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md) §4. |

### 3.5 Scheduler / batch jobs (Phase 5)

| Table | Purpose | Key columns / grain |
|-------|---------|----------------------|
| **scheduler_runs** | Audit log for each scheduled job invocation | `id` (PK), `job_id`, `started_at`, `finished_at`, `status` (`ok` / `error`), `error`, `payload` (JSONB). Written by `scheduler/run_history.py`. See [PHASE5_DETAILED_PLAN.md](PHASE5_DETAILED_PLAN.md) §4.4. |

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
- **Outputs:** Redis order store and account store; stream `oms_fills`; Postgres `orders`, `accounts`, `balances`, `balance_changes` (sync). Optionally syncs **symbols** from broker (e.g. `oms/symbol_sync.py`).
- **Details:** [oms/OMS_ARCHITECTURE.md](oms/OMS_ARCHITECTURE.md).

### 5.2 PMS (`pms/`)

- **Inputs:** Postgres only: `orders`, `balance_changes`, `symbols`, `assets`. (PMS does not consume Redis.)
- **Outputs:** Postgres `positions` (broker, account_id, book, asset, open_qty, position_side, usd_price, usd_notional, updated_at).
- **Logic:** Each tick: derive positions from orders + balance_changes (FIFO, symbol→base/quote via `symbols`), resolve USD price from `assets`, write via `granular_store.write_pms_positions`. Asset init and price feed: [pms/ASSET_PRICE_FEED_PLAN.md](pms/ASSET_PRICE_FEED_PLAN.md).
- **Details:** [pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md).

### 5.3 Risk (`risk/`)

- **Inputs:** Redis stream `strategy_orders`.
- **Outputs:** Redis stream `risk_approved`.
- **Logic:** Rule engine (optional); empty pipeline = pass-through. See [risk/RISK_SERVICE_PLAN.md](risk/RISK_SERVICE_PLAN.md).

### 5.4 Market data (Phase 4, `market_data/`)

- **Inputs:** Binance (or other) **public** REST + WebSocket; config symbols/intervals.
- **Outputs:** Postgres **`ohlcv`**; Redis keys under `market:{symbol}:…` (§4.3, **deferred** until PHASE4 §9.7–9.8).
- **Details:** [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md).

### 5.5 Admin (Phase 3, `admin/`)

- **Commands:** Publish to `risk_approved` (manual order), `cancel_requested` (cancel). Optional: flush risk queue, refresh account.
- **Read-only:** Postgres `orders`, `accounts`, `balances`, `positions`, **symbols**, **assets**; Redis order/account store; optional `oms_fills` tail.
- **Details:** [PHASE3_DETAILED_PLAN.md](PHASE3_DETAILED_PLAN.md).

---

## 6. Deployment

- **Docker:** Single app image (e.g. `oms`) used for `oms`, `pms`, `risk`, and **`market_data`**; entrypoints: `python -m oms.main`, `python -m pms.main`, `python -m risk.main`, `python -m market_data.main` — see [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md) §8.
- **Scripts:** `scripts/update_and_deploy.ps1` builds once and starts `oms`, `pms`, `risk`, `market_data`; `docker compose up -d oms pms risk market_data`.
- **E2E:** `scripts/e2e_with_risk.py` injects to `strategy_orders`, asserts risk_approved → oms_fills → orders table; optional `--runs N` for timing stats.
