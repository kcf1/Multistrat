# AMS: DB columns and injection sources

Single reference for Postgres tables populated by AMS: column sources (broker stream vs periodic REST, Redis store → sync).

---

## 1. accounts

| Column     | Source | Injected when |
|------------|--------|----------------|
| **id**     | Internal (PK) | Alembic / app; may be surrogate; business key is (broker, account_id or name). |
| **name**   | Config / env  | Human-readable name; from AMS config or first snapshot. |
| **broker** | Broker        | e.g. `binance`; from adapter / stream event. |
| **env**    | Config        | e.g. `testnet` / `mainnet`; from BINANCE_BASE_URL or config. |
| **created_at** | Internal  | Set on first sync. |
| **updated_at** | AMS       | Last time account snapshot was applied (from stream or REST). |
| **config** | Optional      | JSONB; broker-specific config (e.g. subaccount). |

Redis key: `account:{broker}:{account_id}`. Sync: UPSERT by (broker, account_id) or by id if accounts use surrogate id and account_id is unique.

---

## 2. balances

| Column       | Source | Injected when |
|--------------|--------|----------------|
| **id**       | Internal (PK) | Surrogate. |
| **account_id** | FK → accounts | From Redis account key (broker + account_id → accounts.id). |
| **asset**    | Broker stream / REST | Asset code (e.g. BTC, USDT). |
| **available**| Broker | From `outboundAccountPosition` (Binance: walletBalance / free) or REST get_account balances[].free. |
| **locked**   | Broker | From stream or REST balances[].locked. |
| **updated_at** | AMS  | Last update from stream or REST. |

Redis key: `account:{broker}:{account_id}:balances` (hash: field = asset, value = JSON or structured string). Sync: UPSERT by (account_id, asset). Repairs: if available/locked are NULL or inconsistent, recover from payload (e.g. Binance raw event).

---

## 3. positions

| Column           | Source | Injected when |
|------------------|--------|----------------|
| **id**           | Internal (PK) | Surrogate. |
| **account_id**   | FK → accounts | From Redis account key. |
| **symbol**       | Broker | Contract or symbol (e.g. BTCUSDT). |
| **side**         | Broker | long / short for futures; optional for spot. |
| **quantity**     | Broker | Position size from stream or REST. |
| **entry_price_avg** | Broker | Average entry from stream or REST. |
| **updated_at**   | AMS    | Last update from stream or REST. |

Redis key: `account:{broker}:{account_id}:positions` (hash: field = symbol or symbol_side, value = position info). Sync: UPSERT by (account_id, symbol, side). Repairs: recover quantity/entry_price from payload when NULL/zero.

---

## 4. margin_snapshots (optional)

| Column             | Source | Injected when |
|--------------------|--------|----------------|
| **id**             | Internal (PK) | Surrogate. |
| **account_id**     | FK → accounts | From Redis account key. |
| **total_margin**   | Broker REST   | Futures account total margin. |
| **available_balance** | Broker REST | Available balance for margin. |
| **timestamp**      | AMS           | Snapshot time. |

Filled by AMS from periodic REST (e.g. get_futures_account) when broker supports margin; optional for spot-only.

---

## 5. Sync and repair flow

- **Sync:** OMS `account_sync.py` reads from Redis account store and writes to Postgres `accounts`, `balances` (and optionally `margin_snapshots`). No `positions` table in OMS (positions in Redis only). Trigger: optional on account update; periodic sync.
- **Repairs:** `ams/repair.py` — `run_all_repairs(pg_connect)` runs after sync. Fixes flawed numeric/empty fields for `broker = 'binance'` (or other brokers) by recovering from `payload` / raw broker blob stored in Redis or in an optional payload column on accounts/balances/positions.

All columns above are written from Redis to Postgres by AMS sync. Redis is populated by the account event callback (stream) and by periodic REST refresh.
