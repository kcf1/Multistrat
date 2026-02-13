# OMS (Order Management System)

Generic order router that consumes from `risk_approved` stream and dispatches to broker adapters. Orders are staged in **Redis** (hashes + indexes) for fast updates; a periodic sync writes orders to Postgres `orders` table for audit and recovery.

## Structure

- `brokers/` - Broker adapter implementations
  - `binance/` - Binance broker adapter
    - `api_client.py` - Low-level Binance REST API client (task 12.1.1)
    - `tests/` - Unit tests

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

Set `RUN_BINANCE_TESTNET=1` and `BINANCE_API_KEY` / `BINANCE_API_SECRET` (and optionally `BINANCE_BASE_URL`). If `python-dotenv` is installed, variables are loaded from the repo root `.env`.

## Redis Stream Schemas

### Input: `risk_approved`
Defined in OMS (see task 12.1.5).

### Output: `oms_fills`
Defined in OMS (see task 12.1.5).

## Order staging (Redis + Postgres sync)

- **Redis:** `orders:{order_id}` (hash), `orders:by_status:{status}` (set), `orders:by_broker_order_id:{broker_order_id}` (lookup). O(1) updates; see PHASE2_DETAILED_PLAN §5.2.
- **Postgres:** Periodic sync (e.g. completed orders) to `orders` table for audit and recovery.
