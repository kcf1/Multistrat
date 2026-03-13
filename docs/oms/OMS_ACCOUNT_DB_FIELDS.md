# OMS: Account and balance DB columns

Postgres tables populated by OMS **account sync**: column sources (broker stream, periodic REST, Redis store → sync). Account management is **integrated into OMS** (same process as order management).

---

## 1. accounts

| Column       | Source        | Notes |
|-------------|----------------|--------|
| **id**      | Internal (PK)  | Surrogate; business key is (broker, account_id). |
| **account_id** | Broker     | Broker's account id (TEXT). |
| **name**    | Config / derived | Human-readable; from metadata or `broker:account_id`. |
| **broker**  | Broker         | e.g. `binance`. |
| **created_at** | Internal   | Set on first sync. |
| **updated_at** | OMS        | Last time account snapshot was applied. |
| **config**  | Optional       | JSONB; broker-specific. |

*(No `env` column; dropped in revision e5f6a7b8c9d0.)*

Redis key: `account:{broker}:{account_id}`. Sync: UPSERT by (broker, account_id). Module: `oms/account_sync.py` (`_account_to_row`).

---

## 2. balances

| Column       | Source  | Notes |
|-------------|---------|--------|
| **id**      | Internal (PK) | Surrogate. |
| **account_id** | FK → accounts.id | From Redis account key (broker + account_id → accounts.id). |
| **asset**   | Broker stream / REST | Asset code (e.g. BTC, USDT). |
| **available** | Broker | From `outboundAccountPosition` or REST get_account balances[].free. |
| **locked**  | Broker | From stream or REST balances[].locked. |
| **updated_at** | OMS  | Last update from stream or REST. |

Redis key: `account:{broker}:{account_id}:balances` (hash: field = asset, value = JSON). Sync: UPSERT by (account_id, asset); **DELETE** balances for that account not in current Redis snapshot. Repairs: recover from payload when NULL/inconsistent. Module: `oms/account_sync.py` (`_balance_to_row`). **Note:** Main loop may call `sync_accounts_to_postgres(..., sync_balances=False)`; then `balances` is not updated (PMS cash can still use balances when sync_balances=True).

---

## 3. balance_changes

Historical deposit/withdrawal/transfer only (from broker **balanceUpdate**; not trades). Populated by main's `on_balance_change` → `write_balance_change`. See **docs/oms/BALANCE_CHANGES_HISTORY.md** for full schema and `book` column.

---

## 4. positions (Redis only; no Postgres table)

OMS keeps **positions** in Redis only (`account:{broker}:{account_id}:positions`). There is **no** `positions` table in OMS Postgres (reserved for PMS). `margin_snapshots` was dropped.

---

## 5. Sync and repair flow

- **Sync:** `oms/account_sync.py` — `sync_accounts_to_postgres(redis, account_store, pg_connect, sync_balances=...)` reads Redis account store; UPSERTs `accounts`; optionally `balances` (when `sync_balances=True`). Balance changes from stream → `write_balance_change` (separate from periodic sync).
- **Repairs:** `oms/account_repair.py` — `run_all_account_repairs(pg_connect)` after sync; fix flawed fields for `broker = 'binance'` (or others) from `payload` / raw broker data.

For **order** DB columns see **docs/oms/OMS_ORDERS_DB_FIELDS.md**.
