# OMS (Order Management System)

Generic order router that consumes from `risk_approved` stream and dispatches to broker adapters. Orders are staged in **Redis** (hashes + indexes) for fast updates; a periodic sync writes orders to Postgres `orders` table for audit and recovery.

## Structure

- `brokers/` - Broker adapter implementations
  - `base.py` - Broker adapter interface (Protocol: `place_order`, `start_fill_listener`)
  - `binance/` - Binance broker adapter (task 12.1.3)
    - `api_client.py` - Low-level Binance REST API client; server-time sync (task 12.1.1)
    - `fills_listener.py` - User data stream (ws-api or listenKey); unified fill/reject parsing (task 12.1.2)
    - `adapter.py` - BinanceBrokerAdapter: place_order, start_fill_listener, stop_fill_listener
    - `tests/` - Unit tests (mocked) and testnet integration tests

## Running as a service

The OMS runs as a Docker service (see repo root **README.md**). Start it with `docker compose up -d oms`. To test the full pipeline against the running service: from repo root run `python scripts/full_pipeline_test.py` (injects one order to `risk_approved`, polls until the OMS processes it, then checks `oms_fills`, Redis store, and Postgres).

## Development

### Setup

```bash
pip install -r requirements.txt
```

### Run tests

```bash
pytest oms/brokers/binance/tests/
```

### Run tests against Binance testnet (optional)

Uses real testnet for user data stream and fills listener. Requires testnet API keys.

```bash
# From repo root; .env can provide BINANCE_API_KEY, BINANCE_API_SECRET
set RUN_BINANCE_TESTNET=1
pytest oms/brokers/binance/tests/test_testnet.py -v
```

Set `RUN_BINANCE_TESTNET=1` and `BINANCE_API_KEY` / `BINANCE_API_SECRET` (and optionally `BINANCE_BASE_URL`). If `python-dotenv` is installed, variables are loaded from the repo root `.env`. Fill listener uses WebSocket API (ws-api) by default; if ws-api does not connect on testnet, see `docs/BINANCE_API_RULES.md` §6 (timestamp sync, network, testnet availability).

## Redis Stream Schemas (task 12.1.5)

Defined in `oms/schemas.py`; see PHASE2_DETAILED_PLAN §4.2.

### Input: `risk_approved`
Fields: `order_id`, `broker`, `account_id`, `symbol`, `side`, `quantity`, `order_type`, `price`, `time_in_force`, `book`, `comment`, `strategy_id`, `created_at`. Consumed by OMS; order payload is passed to adapter `place_order`.

### Output: `oms_fills`
Fields: `event_type` (fill|reject), `order_id`, `broker_order_id`, `symbol`, `side`, `quantity`, `price`, `fee`, `fee_asset`, `executed_at`, `fill_id`, `reject_reason`, `book`, `comment`. Produced by OMS from adapter fill/reject callback; Booking consumes.

## Order staging (Redis + Postgres sync)

- **Redis order store** (`oms/storage/redis_order_store.py`, task 12.1.4): `orders:{order_id}` (hash), `orders:by_status:{status}` (set), `orders:by_book:{book}` (set), `orders:by_broker_order_id:{broker_order_id}` (lookup). Operations: stage_order, update_status, update_fill_status, get_order, find_order_by_broker_order_id. Pipelines for atomic updates.
- **Postgres:** Periodic sync (e.g. completed orders) to `orders` table for audit and recovery (task 12.1.10).
