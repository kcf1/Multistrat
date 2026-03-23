# Testing Documentation

Comprehensive test suite documentation for the Multistrat trading system. Tests are classified into four categories based on scope and dependencies.

## Table of Contents

1. [Test Classification](#test-classification)
2. [Unit Tests](#unit-tests)
3. [Integration Tests](#integration-tests)
4. [E2E Tests - Code-Level](#e2e-tests---code-level)
5. [E2E Tests - Service-Level](#e2e-tests---service-level)
6. [Running Tests](#running-tests)
7. [Test Coverage](#test-coverage)

---

## Test Classification

Tests are organized into four categories:

| Category | Description | Dependencies | Speed | Use Case |
|----------|-------------|--------------|-------|----------|
| **Unit** | Isolated components with mocked dependencies | None (all mocked) | Fastest | Development, CI, fast feedback |
| **Integration** | Multiple components wired together | fakeredis, mocks | Fast | Component integration validation |
| **E2E (Code-Level)** | Full system with real services, OMS code runs in-process | Real Redis, Binance testnet, Postgres | Slow | Full flow validation |
| **E2E (Service-Level)** | Black-box testing of running services | Running services (Docker) | Slow | Production-like validation |

---

## Unit Tests

**Purpose:** Test individual functions/components in isolation with all dependencies mocked.

**Characteristics:**
- Fast execution (no network I/O)
- No external dependencies
- Mock all external services (HTTP, Redis, Postgres)
- Run in CI/CD pipelines

### OMS Unit Tests

#### `oms/brokers/binance/tests/test_api_client.py`
Binance API client tests (mocked HTTP responses).

**Test Classes:**
- `TestBinanceAPIClientInit` - Client initialization
  - `test_init_with_base_url`
  - `test_init_testnet_default`
  - `test_init_production_default`

- `TestRequestSigning` - HMAC-SHA256 signing
  - `test_sign_request`
  - `test_sign_request_verification`
  - `test_prepare_params_adds_timestamp_and_signature`

- `TestPlaceOrder` - Order placement
  - `test_place_order_market_success` ⭐ MARKET order
  - `test_place_order_limit_success` ⭐ LIMIT order
  - `test_place_order_api_error`
  - `test_place_order_network_error`

- `TestQueryOrder` - Order querying
  - `test_query_order_by_order_id`
  - `test_query_order_by_client_order_id`
  - `test_query_order_missing_ids`

- `TestCancelOrder` - Order cancellation
  - `test_cancel_order_success`
  - `test_cancel_order_not_found`

- `TestBinanceAPIClientGetAccount` - Account endpoint
  - `test_get_account_success`
  - `test_get_account_unauthorized`

- `TestBinanceAPIClientUserDataStream` - User data stream
  - `test_start_user_data_stream_returns_listen_key`
  - `test_keepalive_user_data_stream`
  - `test_close_user_data_stream`

#### `oms/tests/test_consumer.py`
Redis consumer message parsing.

**Test Classes:**
- `TestParseRiskApprovedMessage` - Message parsing
  - `test_parses_minimal_message`
  - `test_parses_with_order_id_and_limit_price`
  - `test_parses_with_all_fields`
  - `test_parses_with_missing_optional_fields`
  - `test_parses_with_invalid_json_raises`

- `TestReadRiskApproved` - Stream reading
  - `test_read_one_message`
  - `test_read_blocks_when_no_messages`
  - `test_read_returns_none_on_error`

- `TestConsumerGroup` - Consumer group handling
  - `test_creates_consumer_group_if_not_exists`
  - `test_reads_with_consumer_group`
  - `test_acks_message_after_read`

#### `oms/tests/test_producer.py`
Redis producer message formatting.

**Test Classes:**
- `TestFormatOmsFillEvent` - Event formatting
  - `test_formats_fill_event`
  - `test_formats_reject_event`
  - `test_formats_cancelled_event`
  - `test_formats_with_all_fields`

- `TestProduceOmsFill` - Stream production
  - `test_produces_to_stream`
  - `test_produces_with_custom_stream_name`
  - `test_produces_handles_redis_error`

#### `oms/tests/test_redis_order_store.py`
Redis order store CRUD operations.

**Test Classes:**
- `TestStageOrder` - Order staging
  - `test_stages_order_with_all_fields`
  - `test_stages_order_with_minimal_fields`
  - `test_stages_order_creates_indexes`
  - `test_stages_order_with_limit_price`

- `TestUpdateStatus` - Status updates
  - `test_updates_status_atomically`
  - `test_updates_status_updates_indexes`
  - `test_updates_status_with_extra_fields`

- `TestUpdateFillStatus` - Fill status updates
  - `test_updates_fill_status`
  - `test_updates_fill_status_with_cumulative_qty`
  - `test_updates_fill_status_partial_then_full`

- `TestGetOrder` - Order retrieval
  - `test_gets_order_by_id`
  - `test_gets_order_returns_none_when_missing`
  - `test_gets_order_with_all_fields`

- `TestIdempotentIndexUpdates` - Index consistency
  - `test_index_updates_are_idempotent`
  - `test_status_index_consistency`

- `TestGetOrderIdsInStatus` - Status queries
  - `test_gets_orders_by_status`
  - `test_gets_orders_by_status_empty_set`

#### `oms/tests/test_registry.py`
Broker adapter registry routing.

**Test Functions:**
- `test_register_and_get`
- `test_unknown_broker_returns_none`
- `test_contains`
- `test_routing_uses_correct_adapter`
- `test_register_overwrites`
- `test_broker_names`

#### `oms/tests/test_sync.py`
Postgres sync operations.

**Test Classes:**
- `TestGetTerminalOrderIds` - Terminal order identification
  - `test_gets_terminal_order_ids`
  - `test_gets_terminal_order_ids_empty`
  - `test_gets_terminal_order_ids_filters_non_terminal`

- `TestSyncOneOrder` - Single order sync
  - `test_sync_one_order_inserts_new`
  - `test_sync_one_order_updates_existing`
  - `test_sync_one_order_includes_price_and_limit_price`
  - `test_sync_one_order_sets_ttl_after_sync`
  - `test_sync_one_order_idempotent`

- `TestSyncTerminalOrders` - Batch sync
  - `test_sync_terminal_orders_syncs_all`
  - `test_sync_terminal_orders_handles_errors`
  - `test_sync_terminal_orders_respects_ttl`

#### `oms/tests/test_repair.py`
Postgres order repairs: Binance payload recovery (price, time_in_force, binance_cumulative_quote_qty).

**Test Classes:**
- `TestExtractFromBinancePayload` - Payload extraction
  - `test_extracts_price_from_avg_price`
  - `test_extracts_price_from_fills_when_avg_missing`
  - `test_extracts_time_in_force`
  - `test_extracts_cumulative_quote_qty`
  - `test_empty_payload_returns_empty`

- `TestRepairFunctions` - Repair logic (mocked DB)
  - `test_repair_price_updates_when_flawed`
  - `test_repair_price_skips_when_no_valid_price_in_payload`
  - `test_repair_time_in_force_updates`
  - `test_repair_cumulative_quote_qty_updates`
  - `test_run_all_repairs_calls_each`

#### `oms/tests/test_cleanup.py`
Stream trimming and TTL management (order-flow and account-flow).

**Test Classes:**
- `TestTrimOmsStreams` - Stream trimming
  - `test_trims_risk_approved_stream`
  - `test_trims_oms_fills_stream`
  - `test_trims_both_streams`
  - `test_trims_respects_maxlen`

- `TestSetOrderKeyTtl` - Order key TTL (order-flow)
  - `test_sets_ttl_on_order_key`
  - `test_sets_ttl_returns_false_when_key_missing`
  - `test_sets_ttl_with_custom_seconds`

- `TestSetAccountKeyTtl` - Account key TTL (account-flow, task 12.2.10)
  - `test_set_account_key_ttl_sets_expire_on_all_keys`
  - `test_set_account_key_ttl_skips_missing_keys`
  - `test_set_account_key_ttl_skips_keys_already_with_ttl`

#### OMS Account-Flow Unit Tests

#### `oms/tests/test_redis_account_store.py`
Redis account store (task 12.2.4): balances and positions CRUD, broker index.

**Test Classes:** `TestApplyAccountPosition`, `TestApplyBalanceUpdate`, `TestGetAccount`, `TestGetBalances`, `TestGetPositions`, `TestGetAccountIdsByBroker`, and others.

#### `oms/tests/test_account_flow.py`
Account event callback (task 12.2.5): validation, store updates, idempotency.

**Test Classes:** `TestMakeAccountCallbackValidation`, `TestMakeAccountCallbackStoreUpdates`, `TestMakeAccountCallbackIdempotency`, `TestMakeAccountCallbackBalanceUpdate`, and others.

#### `oms/tests/test_account_sync.py`
OMS → Postgres account sync (task 12.2.7): mapping, UPSERT accounts/balances, balance cleanup, `write_balance_change`, `get_account_pk_by_broker_and_id`. Uses fakeredis and mock Postgres.

#### `oms/tests/test_account_repair.py`
Account repairs (task 12.2.8): `run_all_account_repairs` (dummy implementation; payload-based recovery can follow).

#### `oms/tests/test_account_schema.py`
Account Postgres schema (task 12.2.6): Alembic revisions and table layout (optional DB; some tests skipped without DB).

#### `oms/brokers/binance/tests/test_account_listener.py`
Binance account listener (task 12.2.2): parse_account_event for `outboundAccountPosition` and `balanceUpdate`, listener lifecycle.

**Test Classes:** `TestParseAccountEvent`, `TestBinanceAccountListener`, etc.

#### `oms/tests/test_cancel_consumer.py`
Cancel command parsing and consumption.

**Test Classes:**
- `TestParseCancelRequestMessage` - Message parsing
  - `test_parses_cancel_by_order_id`
  - `test_parses_cancel_by_broker_order_id`
  - `test_parses_cancel_with_all_fields`
  - `test_parses_cancel_invalid_json`

- `TestReadOneCancelRequest` - Stream reading
  - `test_reads_one_cancel_request`
  - `test_reads_blocks_when_no_messages`
  - `test_reads_returns_none_on_error`

- `TestCancelRequestConsumerGroup` - Consumer group handling
  - `test_creates_consumer_group_if_not_exists`
  - `test_reads_with_consumer_group`
  - `test_acks_after_read`

### Binance Adapter Unit Tests

#### `oms/brokers/binance/tests/test_adapter.py`
Binance broker adapter (mocked client).

**Test Classes:**
- `TestBinanceOrderResponseToUnified` - Response conversion
  - `test_converts_market_order_response`
  - `test_converts_limit_order_response`
  - `test_converts_filled_order_response`
  - `test_converts_with_limit_price_and_executed_price`
  - `test_converts_rejected_order_response`

- `TestBinanceBrokerAdapterPlaceOrder` - Order placement
  - `test_place_order_market_success`
  - `test_place_order_limit_success`
  - `test_place_order_api_error`
  - `test_place_order_network_error`
  - `test_place_order_with_client_order_id`

- `TestBinanceBrokerAdapterCancelOrder` - Order cancellation
  - `test_cancel_order_success`
  - `test_cancel_order_not_found`
  - `test_cancel_order_api_error`
  - `test_cancel_order_missing_args`

- `TestBinanceBrokerAdapterStartFillListener` - Fill listener
  - `test_start_fill_listener_starts_listener`
  - `test_start_fill_listener_stops_previous`
  - `test_stop_fill_listener_stops_listener`
  - `test_start_fill_listener_with_store_enriches_fill_price_when_zero` — when store is provided, fill events with price 0 are enriched from order payload before callback
  - `test_start_fill_listener_with_store_enriches_time_in_force_and_cumulative_quote` — fill events with empty time_in_force / 0 cumulative_quote are enriched from payload
- `TestFillPriceFromBinancePayload` - Binance payload extraction (price, time_in_force, cumulative_quote_qty)
  - `test_empty_or_no_payload_returns_none`
  - `test_avg_price_extracted`, `test_fills_first_price_extracted`, `test_fill_price_extracted`
  - `test_time_in_force_and_cumulative_quote_qty_extracted`, `test_enrichments_empty_payload`

#### `oms/brokers/binance/tests/test_fills_listener.py`
Fills listener parsing (mocked websocket). Fill events include **time_in_force** and **binance_cumulative_quote_qty** parsed from the execution report (Binance fields `f` and `Z`) when present.

**Test Classes:**
- `TestParseExecutionReport` - Execution report parsing
  - `test_parses_fill_event`
  - `test_parses_partial_fill_event`
  - `test_parses_cancelled_event`
  - `test_parses_expired_event`
  - `test_parses_rejected_event`
  - `test_parses_with_cumulative_qty`
  - `test_parses_invalid_json`
  - `test_non_execution_report_returns_none` — non-`executionReport` stream events (e.g. `outboundAccountPosition`, `balanceUpdate`) are ignored and return None without logging

- `TestStreamUrlFromBaseUrl` - URL construction
  - `test_constructs_stream_url_from_testnet`
  - `test_constructs_stream_url_from_production`

- `TestBinanceFillsListener` - Listener lifecycle
  - `test_listener_connects`
  - `test_listener_receives_fill`
  - `test_listener_handles_disconnect`
  - `test_listener_stops_cleanly`

---

## Integration Tests

**Purpose:** Test multiple components wired together, but still using mocked external services.

**Characteristics:**
- Uses fakeredis (in-memory Redis) or mocked Redis client
- Mock broker adapters
- Tests component interactions without real services
- Faster than E2E but validates integration logic

### OMS Integration Tests

#### `oms/tests/test_oms_integration.py`
Full OMS flow with fakeredis and mock adapters. Covers **order-flow** (tasks 12.1.x) and **account-flow** (tasks 12.2.9).

**Order-flow tests:**
- `test_oms_integration_consumer_store_registry_producer` ⭐
  - Full flow: XADD `risk_approved` → process_one → order in store, reject on `oms_fills`

- `test_oms_integration_sent_then_fill_callback_produces` ⭐
  - Place order (sent) → fill callback updates store and produces to `oms_fills`

- `test_oms_integration_place_order_raises_retry_then_reject_after_max`
  - Error handling: place_order raises → retry up to max → reject

- `test_oms_integration_unknown_broker_rejects`
  - Unknown broker: no adapter → reject in store and on `oms_fills`

- `test_oms_integration_partial_then_full_fill_status_and_cumulative` ⭐
  - Partial fill → status `partially_filled` → full fill → status `filled`

- `test_oms_integration_fill_callback_resolves_by_broker_order_id_when_event_order_id_unknown`
  - Fill callback resolves order by `broker_order_id` when event `order_id` unknown

- `test_oms_integration_fill_callback_cancelled_expired_updates_store`
  - Cancelled/expired events update store and publish to `oms_fills`

- `test_oms_integration_process_one_cancel_from_redis` ⭐
  - Cancel command from Redis: XADD `cancel_requested` → process_one_cancel → store cancelled, `oms_fills`

- `test_oms_integration_process_one_cancel_by_broker_order_id`
  - Cancel by `broker_order_id` + symbol when `order_id` not in message

- `test_oms_integration_process_one_cancel_consumer_group_no_double_process`
  - Cancel with consumer group: each message delivered once

- `test_oms_integration_consumer_group_no_double_process` ⭐
  - Consumer group: each message delivered once; second read returns next message

- `test_oms_integration_fill_callback_accumulates_when_no_cumulative`
  - Fill callback accumulates incremental quantity when no cumulative provided

- `test_oms_bootstrap_starts_fill_listeners`
  - Bootstrap starts fill listener for each registered adapter

- `test_oms_main_loop_integration` ⭐
  - Main loop with fakeredis + mock adapter; inject one message; assert order in store and `oms_fills`

**Account-flow tests (12.2.9):**
- `test_oms_bootstrap_starts_account_listeners` — start_account_listeners starts listener for adapters that support it
- `test_oms_bootstrap_skips_adapter_without_account_listener` — adapters without account listener are skipped
- `test_oms_wait_for_account_listeners_connected_no_listeners` — returns True when no adapter has account listener
- `test_oms_wait_for_account_listeners_connected_all_connected` — returns True when all listeners report stream_connected
- `test_oms_account_callback_updates_store` — make_account_callback with account_position event updates account store
- `test_oms_main_loop_integration_with_account_refresh` — run_oms_loop with account_store runs periodic get_account_snapshot and apply_account_position; snapshot appears in store

**Order types tested:** ⭐ MARKET, ⭐ LIMIT, cancellations, partial fills, error scenarios.

---

## E2E Tests - Code-Level

**Purpose:** Test full system flow with real services, but OMS code runs directly in the test process.

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
pytest oms/tests/test_oms_redis_testnet.py -v
```

### OMS E2E Tests

#### `oms/tests/test_oms_redis_testnet.py`
Real Redis + Binance testnet, OMS code imported.

**Test Class:** `TestOmsRedisTestnet`

**Test Methods:**
- `test_trigger_testnet_via_redis_listen_oms_fills` ⭐
  - Inject one MARKET order into `risk_approved` → process via OMS → assert fill appears on `oms_fills`
  - Validates: Redis → OMS → Binance testnet → fill → `oms_fills`

- `test_full_pipeline_place_then_cancel_via_redis` ⭐
  - Full pipeline: `risk_approved` → place LIMIT order → `cancel_requested` → cancel at broker → store updated
  - Validates: LIMIT order placement and cancellation flow

- `test_place_order_raises_retry_then_reject_consumer_group`
  - Real Redis: consumer group, place_order raises → no XACK until max retries, then reject + XACK
  - Validates: Error handling with consumer groups

- `test_error_order_rejected_via_redis_testnet`
  - Send invalid order (invalid symbol) → assert reject flows to store and `oms_fills`
  - Validates: Rejection handling

- `test_full_pipeline_redis_order_testnet_status_sync_to_postgres` ⭐
  - Full pipeline: `risk_approved` → testnet → Redis order status → sync to Postgres
  - Validates: Order sync to Postgres `orders` table
  - Requires: `DATABASE_URL`

- `test_full_pipeline_with_main_loop` ⭐
  - Full pipeline via main loop: start_fill_listeners + run_oms_loop; inject order; assert Redis and DB
  - Validates: Main loop integration
  - Requires: `DATABASE_URL`

**Account-flow E2E (code-level):**
- `test_account_refresh_e2e`
  - get_account_snapshot (Binance testnet REST) → apply to Redis account store → assert account and balances in Redis
  - Requires: `RUN_BINANCE_TESTNET=1`, `REDIS_URL`
- `test_account_sync_to_postgres_e2e`
  - get_account_snapshot → apply to Redis → sync_accounts_to_postgres → assert Postgres `accounts` and `balances` rows
  - Requires: `RUN_BINANCE_TESTNET=1`, `REDIS_URL`, `DATABASE_URL`
- `test_order_fill_matches_balance_change_e2e`
  - Place MARKET BUY (BTCUSDT) → wait for fill → snapshot before/after → assert base asset (BTC) balance increased by `executed_qty` and quote (USDT) did not increase (order matched to balance change)
  - Requires: `RUN_BINANCE_TESTNET=1`, `REDIS_URL`

**Order Types Tested:** ⭐ MARKET, ⭐ LIMIT, cancellations, error scenarios, Postgres sync.

### Binance Testnet Tests

#### `oms/brokers/binance/tests/test_testnet.py`
Direct Binance testnet API tests (no OMS integration).

**Test Classes:**
- `TestBinanceTestnetAuth` - Authentication
  - `test_auth_signed` - Verify API key and HMAC-SHA256 signing

- `TestBinanceTestnetAPI` - API operations
  - `test_user_data_stream_start_keepalive_close` - User data stream lifecycle
  - `test_place_order_then_cancel` ⭐ - Place LIMIT order then cancel (no fill)

- `TestBinanceTestnetFillsListener` - WebSocket fills listener
  - `test_fills_listener_connects` - WebSocket connection
  - `test_fills_listener_receives_execution_report` - Execution report reception
  - `test_fills_listener_handles_disconnect` - Disconnect handling

- `TestBinanceTestnetAdapter` - Adapter integration
  - `test_adapter_place_order_then_cancel` ⭐ - Adapter place_order then cancel
  - `test_adapter_start_fill_listener` - Adapter fill listener lifecycle

**Order Types Tested:**
- ⭐ LIMIT orders (place → cancel)

---

## E2E Tests - Service-Level

**Purpose:** Test the running system as a black box without importing OMS code.

**Characteristics:**
- Assumes OMS service is already running (e.g., `docker compose up -d oms`)
- Does not import OMS code
- Interacts with system via Redis/Postgres only
- Validates deployed/running system
- Useful for production-like validation

**Usage:**
```bash
# Start services first
docker compose up -d oms booking

# Run single-order pipeline test
python scripts/full_pipeline_test.py --market  # MARKET order
python scripts/full_pipeline_test.py           # LIMIT order (default)

# Run bulk order test (10 orders × 3 runs, with account-flow check)
python scripts/bulk_order_test.py --market --count 10 --runs 3
python scripts/bulk_order_test.py --count 10 --runs 3
```

### Service-Level E2E Scripts

#### `scripts/full_pipeline_test.py`
Service-level E2E script for black-box testing.

**Features:**
- Supports `--market` flag for MARKET orders
- Default LIMIT order flow (place → cancel)
- Checks downstreams: `oms_fills` stream, Redis order store
- **Account-flow:** Polls Redis account store (OMS periodic refresh) and optionally Postgres `accounts`/`balances`; use `--no-account` to skip

**What it validates:**
1. Order injection → `risk_approved` stream
2. OMS processes order (running service)
3. Order appears in Redis store
4. Fill/reject appears in `oms_fills` stream
5. **Account-flow:** Within 90s, Redis account store has at least one account and balance; if MARKET, BTC in balances. If `DATABASE_URL` set, Postgres has at least one account row and one balance row.

**Order Types:** MARKET (`--market`), LIMIT (default).

**Options:** `--no-account` — skip account-flow verification.

**Output:**
- Prints status of each validation step
- Returns exit code 0 on success, 1 on failure

#### `scripts/bulk_order_test.py`
Service-level E2E script for **bulk** order injection and timing.

**Features:**
- Injects **multiple orders** per run (`--count N`, default 10)
- Runs multiple test runs and averages timing (`--runs M`, default 3)
- Supports `--market` (MARKET orders, fill immediately) or default LIMIT (place only)
- Checks downstreams: Redis order store, `oms_fills` stream
- **Account-flow:** After each run, polls Redis account store (and optionally Postgres `accounts`/`balances`); use `--no-account` to skip

**What it validates:**
1. Inject N orders to `risk_approved` (and for LIMIT, no cancel in bulk script)
2. OMS processes all orders (running service)
3. Each order appears in Redis store and has fill (MARKET) or place (LIMIT) outcome on `oms_fills`
4. **Account-flow:** Within 90s, Redis account store has account and balances; if MARKET, BTC in balances. If `DATABASE_URL` set, Postgres has account and balance rows.

**Order types:** MARKET (`--market`), LIMIT (default).

**Options:** `--count N`, `--runs M`, `--verbose`, `--no-account`.

**Output:**
- Per-order timing (total, detection, processing) and aggregate stats (avg, min, max, std dev)
- Throughput (orders/second) when `--runs` > 1
- Exit code 0 if all orders and all account-flow checks pass, 1 otherwise

**Examples:**
```bash
python scripts/bulk_order_test.py --market --count 10 --runs 3
python scripts/bulk_order_test.py --count 10 --runs 3 --verbose
python scripts/bulk_order_test.py --no-account --count 5 --runs 1
```

---

## Running Tests

### Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# For E2E tests, set environment variables
export RUN_BINANCE_TESTNET=1  # Enable testnet tests
export REDIS_URL=redis://localhost:6379
export DATABASE_URL=postgresql://...  # Optional
export BINANCE_API_KEY=...  # Testnet key
export BINANCE_API_SECRET=...  # Testnet secret
```

### Run All Unit Tests

```bash
# Run all unit tests (fast)
pytest oms/tests/ oms/brokers/binance/tests/ -v -k "not testnet and not redis_testnet"

# Run specific test file
pytest oms/tests/test_consumer.py -v
```

### Run Integration Tests

```bash
# Run integration tests (fakeredis) — order-flow and account-flow
pytest oms/tests/test_oms_integration.py -v

# Account-flow integration tests only
pytest oms/tests/test_oms_integration.py -v -k "account or bootstrap_starts_account or wait_for_account or account_callback or account_refresh"
```

### Run E2E Code-Level Tests

```bash
# Requires RUN_BINANCE_TESTNET=1 and credentials
export RUN_BINANCE_TESTNET=1
export REDIS_URL=redis://localhost:6379
export BINANCE_API_KEY=...
export BINANCE_API_SECRET=...

pytest oms/tests/test_oms_redis_testnet.py -v
pytest oms/brokers/binance/tests/test_testnet.py -v
```

### Run E2E Service-Level Tests

```bash
# Start services first
docker compose up -d oms booking

# Single-order pipeline test
python scripts/full_pipeline_test.py --market
python scripts/full_pipeline_test.py
# Bulk order test (10 orders × 3 runs, order-flow + account-flow)
python scripts/bulk_order_test.py --market --count 10 --runs 3
python scripts/bulk_order_test.py --count 10 --runs 3
```

### Run Tests by Category

```bash
# Unit tests only
pytest oms/tests/test_consumer.py oms/tests/test_producer.py oms/tests/test_registry.py -v

# Account-flow unit tests (store, sync, repair, flow, cleanup TTL)
pytest oms/tests/test_redis_account_store.py oms/tests/test_account_sync.py oms/tests/test_account_repair.py oms/tests/test_account_flow.py oms/tests/test_cleanup.py -v -k "account or Account"

# Integration tests only (order-flow + account-flow)
pytest oms/tests/test_oms_integration.py -v

# E2E tests only (requires setup)
export RUN_BINANCE_TESTNET=1
pytest oms/tests/test_oms_redis_testnet.py -v
```

### Run Tests by Order Type

```bash
# Tests that cover MARKET orders
pytest -k "market or MARKET" -v

# Tests that cover LIMIT orders
pytest -k "limit or LIMIT" -v
```

---

## Test Coverage

### Order Types Coverage

| Order Type | Unit Tests | Integration Tests | E2E Code-Level | E2E Service-Level |
|------------|------------|------------------|----------------|-------------------|
| **MARKET** | ✅ `test_api_client.py` | ✅ `test_oms_integration.py` | ✅ `test_oms_redis_testnet.py` | ✅ `full_pipeline_test.py`, `bulk_order_test.py` |
| **LIMIT** | ✅ `test_api_client.py` | ✅ `test_oms_integration.py` | ✅ `test_oms_redis_testnet.py` | ✅ `full_pipeline_test.py`, `bulk_order_test.py` |

### Component Coverage

| Component | Unit Tests | Integration Tests | E2E Tests |
|-----------|------------|-------------------|------------|
| **Binance API Client** | ✅ Complete | - | ✅ Testnet |
| **Binance Adapter** | ✅ Complete | - | ✅ Testnet |
| **Redis Consumer** | ✅ Complete | ✅ Integration | ✅ E2E |
| **Redis Producer** | ✅ Complete | ✅ Integration | ✅ E2E |
| **Redis Order Store** | ✅ Complete | ✅ Integration | ✅ E2E |
| **OMS Integration (order-flow)** | - | ✅ Complete | ✅ E2E |
| **OMS Integration (account-flow)** | - | ✅ Complete | ✅ E2E |
| **Postgres Sync (orders)** | ✅ Complete | - | ✅ E2E |
| **Postgres Sync (accounts)** | ✅ Complete | - | ✅ E2E |
| **Cancel Flow** | ✅ Complete | ✅ Integration | ✅ E2E |
| **Error Handling** | ✅ Complete | ✅ Integration | ✅ E2E |
| **Redis Account Store** | ✅ Complete | ✅ Integration | - |
| **Account Listener (Binance)** | ✅ Complete | ✅ Integration | - |
| **Account Repair** | ✅ (dummy) | - | - |

### Flow Coverage

| Flow | Unit | Integration | E2E Code-Level | E2E Service-Level |
|------|-----|-------------|----------------|-------------------|
| Order placement | ✅ | ✅ | ✅ | ✅ |
| Order cancellation | ✅ | ✅ | ✅ | ✅ |
| Fill handling | ✅ | ✅ | ✅ | ✅ |
| Partial fills | ✅ | ✅ | ✅ | - |
| Error handling | ✅ | ✅ | ✅ | - |
| Postgres sync | ✅ | - | ✅ | ✅ |
| Consumer groups | ✅ | ✅ | ✅ | - |
| Account listeners / callback | ✅ | ✅ | - | - |
| Account store (Redis) | ✅ | ✅ | ✅ | ✅ |
| Account refresh (periodic snapshot) | - | ✅ | ✅ | ✅ |
| Account sync to Postgres | ✅ | - | ✅ | ✅ |
| Account repair | ✅ (dummy) | - | - | - |
| Account key TTL | ✅ | - | - | - |

### Order-flow vs account-flow

What is implemented and tested for **orders** vs **accounts**:

| Area | Order-flow | Account-flow |
|------|------------|--------------|
| **Redis store** | `RedisOrderStore` — unit, integration, E2E | `RedisAccountStore` — unit, integration, E2E |
| **Postgres sync** | `sync_one_order`, `sync_terminal_orders` — unit, E2E | `sync_accounts_to_postgres` — unit, E2E |
| **Repair** | `run_all_repairs` (payload recovery) — unit, E2E | `run_all_account_repairs` (dummy) — unit only |
| **TTL after sync** | `set_order_key_ttl` — unit (order sync) | `set_account_key_ttl` — unit (only on balance change events, not after periodic sync) |
| **Listener** | Fill listener — unit (adapter, fills_listener), integration, E2E | Account listener — unit (Binance account_listener), integration |
| **Main loop** | process_many, process_one_cancel, trim, order sync, repair — integration, E2E | start/wait/stop account listeners, periodic refresh, periodic account sync+repair — integration only |
| **E2E code-level** | test_oms_redis_testnet (Redis + testnet + optional Postgres) | test_oms_redis_testnet: test_account_refresh_e2e, test_account_sync_to_postgres_e2e |
| **E2E service-level** | full_pipeline_test.py, bulk_order_test.py (orders, oms_fills) | full_pipeline_test.py, bulk_order_test.py (Redis account store + optional Postgres); use `--no-account` to skip |

**Remaining gaps (account-flow):**
- **Account repair:** Dummy implementation only; no payload-based recovery tests.

**Consumer Groups Flow Explained:**

Consumer groups are a Redis Streams feature that ensures **exactly-once message delivery** even with multiple consumers or after crashes. The flow:

1. **XGROUP CREATE** - Create consumer group on stream (e.g., `risk_approved`)
2. **XREADGROUP** - Consumer reads messages with `id=">"` (new messages only)
3. **Process message** - OMS processes order (stage → place → update)
4. **XACK** - Acknowledge message after successful processing
5. **No redelivery** - Once ACKed, message is never redelivered

**Why it matters:**
- **Without consumer groups:** If OMS crashes after reading but before processing, message is lost. If OMS reads twice, message is processed twice (double order).
- **With consumer groups:** Message stays in "pending" list until ACKed. If OMS crashes, message is redelivered on restart. Once ACKed, never redelivered.

**Tests covering consumer groups:**
- `test_consumer.py::TestConsumerGroup` - Unit test for XREADGROUP + XACK
- `test_oms_integration.py::test_oms_integration_consumer_group_no_double_process` - Integration: two messages, each delivered once
- `test_oms_integration.py::test_oms_integration_process_one_cancel_consumer_group_no_double_process` - Cancel flow with consumer groups
- `test_oms_redis_testnet.py::test_place_order_raises_retry_then_reject_consumer_group` - E2E: error handling with consumer groups (no ACK until max retries)

---

## Test Execution Strategy

### Development Workflow

1. **Write code** → Run unit tests (fast feedback)
2. **Component complete** → Run integration tests (validate wiring)
3. **Feature complete** → Run E2E code-level tests (validate with testnet)
4. **Before deploy** → Run E2E service-level script (validate deployment)

### CI/CD Pipeline

```yaml
# Recommended CI pipeline
stages:
  - unit_tests:      # Fast, always run
      pytest -k "not testnet and not redis_testnet"
  
  - integration_tests:  # Fast, always run
      pytest oms/tests/test_oms_integration.py
  
  - e2e_tests:       # Slow, run on PR or nightly
      export RUN_BINANCE_TESTNET=1
      pytest oms/tests/test_oms_redis_testnet.py
```

### Test Maintenance

- **Unit tests:** Update when component interfaces change
- **Integration tests:** Update when component interactions change
- **E2E tests:** Update when system behavior changes
- **Service-level tests:** Update when deployment configuration changes

---

## Notes

- ⭐ Indicates tests that cover MARKET or LIMIT orders specifically
- E2E tests require Binance testnet credentials (never use mainnet keys)
- Service-level tests require running services (Docker Compose)
- All tests use pytest framework
- Mock dependencies use `responses` (HTTP), `fakeredis` (Redis), `unittest.mock` (Python)

---

## References

- [PHASE2_DETAILED_PLAN.md](PHASE2_DETAILED_PLAN.md#165-test-classification-and-file-mapping) - Detailed test classification
- [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md#testing-corresponding-to-each-phase) - Overall test matrix
- [BINANCE_API_RULES.md](oms/BINANCE_API_RULES.md) - Binance API testnet rules and requirements
