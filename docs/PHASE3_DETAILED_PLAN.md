# Phase 3: Detailed Plan — Admin Interface

**Goal:** Provide an Admin interface (CLI and/or Web) to send commands (manual order, cancel order, flush risk queue, refresh account) and to view read-only state (positions, balances, open orders, recent fills). Commands are published to Redis streams; existing services (OMS, Risk) consume them. No new execution logic—Admin is a control and visibility layer.

**Current architecture (data sources):** See [docs/ARCHITECTURE.md](ARCHITECTURE.md) for Postgres tables (orders, accounts, balances, balance_changes, **symbols**, **assets**, **positions**), Redis streams/key patterns, and module layout.

## 0. Implementation priority (mark first)

**Current stance — Admin deferred:** Do not require a dedicated Admin process, `admin/` package, or formal CLI before advancing other work. **Manual order** and **cancel** remain available by publishing to existing streams (`risk_approved`, `cancel_requested`) via **ad-hoc** means (e.g. `redis-cli`, one-off Python, or small throwaway scripts). **Read-only** state can use Postgres/Redis directly (e.g. Compose **pgadmin** / **redisinsight**, SQL, or `XREVRANGE` on `oms_fills`).

### Reasons for deferral

- **Not on the execution critical path:** OMS already consumes `risk_approved` and `cancel_requested`. Admin adds no new execution logic—only a safer, repeatable way to publish and inspect. Manual `XADD` / small scripts are sufficient until operator volume justifies a package.
- **Visibility already exists:** Phase 1–2 data lives in Postgres tables and Redis keys/streams; Compose includes **pgadmin** and **redisinsight** for ad-hoc read-only access. Planned Admin “read-only views” are **application-level** listings (or equivalent SQL from Python), not a prerequisite for correctness.
- **No extra runtime today:** `docker-compose.yml` has no Admin service; deferring avoids another process, image surface, and maintenance until needed—relevant for **tight dev machines** (e.g. low RAM) or **24/7 laptop** setups where fewer containers is preferable.
- **Downstream phases are unblocked:** Market data (Phase 4), strategies (Phase 5), and external batch jobs (e.g. hourly portfolio work with **pre-shipped** models) do not require a formal Admin CLI or HTTP API.
- **DB views are optional:** Ordinary Postgres `VIEW`s do not improve performance vs the same query from Python; materialized views are only worth it for heavy, stale-tolerant aggregates. Deferring Admin does not force a separate “view layer” in the database.

**When Phase 3 is picked up:** Follow the task checklist in §9 in order — shared publish helpers and unit tests (**3.1.1**), then CLI order/cancel (**3.1.2–3.1.3**), then read-only query helpers and list commands (**3.2.x**), then optional **3.3.x**.

---

## 1. Overview

| Item | Choice |
|------|--------|
| **Admin role** | Publish commands to Redis streams; read state from Postgres and/or Redis (read-only). No direct broker or strategy execution. |
| **Command delivery** | Redis streams: `risk_approved` (manual order), `cancel_requested` (cancel), optional `admin_commands` or domain-specific streams for flush/refresh. |
| **State sources** | Postgres: `orders`, `accounts`, `balances`, `positions` (PMS). Redis: OMS order store (open orders), account store (balances/positions), optional `oms_fills` tail. |
| **UI options** | **Option A – CLI:** Scripts or small app that publish to Redis and query Postgres/Redis. **Option B – Web GUI:** Thin backend (HTTP API or server-rendered) + page that lists state and sends commands. Phase 3 can ship CLI first, then add Web if needed. |

---

## 2. Dependencies

- **Phase 1:** Postgres, Redis, Docker network.
- **Phase 2:** OMS (consumes `risk_approved`, `cancel_requested`), Risk (optional; consumes `strategy_orders`), PMS (writes `positions`). Streams and Postgres schema exist.
- **No Phase 4/5 required** for basic Admin (manual order, cancel, view); strategy start/stop can be stubbed or deferred to Phase 5.

---

## 3. Command model

### 3.1 Commands and targets

| Command | Target | Stream / mechanism | Consumer |
|--------|--------|--------------------|----------|
| **Manual order** | OMS (via Risk or direct) | `risk_approved` (same schema as strategy output) | OMS |
| **Cancel order** | OMS | `cancel_requested` (existing) | OMS |
| **Flush risk queue** | Risk | Optional: `admin_commands` or Redis key/signal; Risk consumes and clears or skips pending. | Risk (Phase 3 or later) |
| **Refresh balance/margin** | OMS | Optional: trigger OMS periodic refresh (e.g. Redis key or `admin_commands`); or rely on existing periodic sync. | OMS |
| **Start/stop strategy** | Strategy runner | Phase 5; optional placeholder command in Phase 3. | Phase 5 |

**Phase 3 minimum:** Manual order (publish to `risk_approved`), cancel order (publish to `cancel_requested`). Flush/refresh/strategy can be added incrementally.

### 3.2 Payload shape

- **Manual order:** Same as `risk_approved` input: `broker`, `account_id`, `symbol`, `side`, `quantity`, `order_type`, optional `limit_price`, `time_in_force`, `book`, `comment`. Admin sets `book` (e.g. `"manual"`) and optional `order_id`.
- **Cancel order:** Same as existing `cancel_requested`: `broker`, and either `order_id` (internal) or (`broker_order_id` + `symbol`).
- **Other commands (later):** JSON payload with `command`, `params`, `id`, `timestamp` on `admin_commands` or domain stream.

---

## 4. Read-only views (data for Admin)

### 4.1 Postgres

| Source | Content | Use |
|--------|---------|-----|
| **orders** | All or recent orders (internal_id, account_id, symbol, side, quantity, status, created_at, …) | Open orders, recent history |
| **accounts** | broker, account_id, name | Account list |
| **balances** | account_id, asset, available, locked | Cash per account |
| **positions** | broker, account_id, book, asset, open_qty, position_side, usd_price, usd_notional (PMS) | Positions for Admin dashboard |
| **symbols** | symbol, base_asset, quote_asset, broker, … | Reference for symbol/asset display |
| **assets** | asset, usd_symbol, usd_price, price_source | Asset USD prices for display |

Queries: by account_id, by book, time range for orders; no writes.

### 4.2 Redis

| Source | Content | Use |
|--------|---------|-----|
| OMS order store | Open/pending orders (status, broker_order_id, symbol, side, quantity, …) | Live open orders for cancel targeting |
| OMS account store | Balances, positions (if needed before Postgres sync) | Optional fallback |
| **oms_fills** | Last N events (XREVRANGE) | Recent fills for display |
| **risk_approved** / **strategy_orders** | Length or last ID (optional) | Queue depth for “flush” UX |

Admin reads only; no XADD from Admin except for command streams.

### 4.3 PnL / margin

- If PMS writes Redis `pnl:{account_id}`, `margin:{account_id}` (Phase 2 follow-up), Admin can display them.
- Otherwise Admin shows Postgres **positions** (open_qty, usd_price, usd_notional per row) and optionally aggregates; margin can be “N/A” until PMS Redis writes exist.

---

## 5. Admin service (backend)

### 5.1 Responsibilities

- **Publish commands** to Redis streams (`risk_approved`, `cancel_requested`, optional `admin_commands`) with correct payload shape.
- **Read state** from Postgres (orders, accounts, balances, positions) and optionally Redis (open orders, last fills) for display.
- **Optional:** Expose a thin HTTP API (e.g. GET /positions, GET /orders, POST /order, POST /cancel) so a Web GUI or CLI can call it instead of talking to Redis/Postgres directly. Alternative: CLI and Web GUI talk to Redis + Postgres themselves with a small shared library.

### 5.2 Deployment options

- **A. Library + CLI only:** No long-running Admin service. CLI scripts (Python) use `REDIS_URL`, `DATABASE_URL`; publish to streams via `oms.streams.add_message`-style helpers; query Postgres for views. Simple, no extra container.
- **B. Admin API server:** One process (e.g. FastAPI/Flask) that wraps publish + read; CLI and Web GUI call the API. One extra container; consistent auth and validation in one place.

**Recommendation for Phase 3:** Start with **Option A** (CLI + shared lib) for speed; add Option B if you add a Web GUI and want a single backend.

---

## 6. CLI

### 6.1 Scope

- **Manual order:** e.g. `python -m admin.cli order --broker binance --symbol BTCUSDT --side BUY --quantity 0.001 --book manual`
  - Builds `risk_approved` payload, XADD to `risk_approved`.
- **Cancel order:** e.g. `python -m admin.cli cancel --order-id <id>` or `--broker-order-id <id> --symbol BTCUSDT`
  - XADD to `cancel_requested`.
- **List open orders:** Query Postgres or Redis order store; print table.
- **List positions:** Query Postgres `positions` (and optionally balances); print table.
- **Recent fills:** XREVRANGE `oms_fills` (or query Postgres if fills table exists later); print last N.

CLI uses `.env` for `REDIS_URL`, `DATABASE_URL`; no separate Admin server.

### 6.2 Implementation layout

- **admin/** (or **scripts/admin_*.py**): CLI entrypoint, helpers to publish to `risk_approved` / `cancel_requested`, helpers to query Postgres (orders, positions, balances, accounts). Reuse OMS stream helpers and schema where possible.

---

## 7. Web GUI (optional for Phase 3)

- Simple page (SPA or server-rendered) that:
  - Lists positions, balances, open orders, recent fills (from Admin API or direct Postgres/Redis).
  - Buttons: “Place order” (form → POST → risk_approved), “Cancel” (select order → POST → cancel_requested).
- Can be added after CLI is working; same command contract.

---

## 8. Consumers (existing services)

- **OMS:** Already consumes `cancel_requested` and `risk_approved`. No Phase 3 change required for basic Admin; Admin just publishes to those streams.
- **Risk:** If “flush risk queue” is implemented, Risk (or a separate loop) consumes from `admin_commands` or a dedicated stream and clears/skips pending strategy_orders. Can be Phase 3 or later.
- **Strategy runner (Phase 5):** Consumes start/stop from admin; Phase 3 can define the command format and leave consumer for Phase 5.

---

## 9. Task checklist (implementation order)

### 9.1 Command publishing

- [ ] **3.1.1** **Admin package or scripts:** Create `admin/` (or `scripts/admin_*`) with shared helpers: `publish_manual_order(redis, payload)` → XADD `risk_approved`, `publish_cancel(redis, payload)` → XADD `cancel_requested`. Payloads match OMS schema (see §3.2). **Unit test:** mock Redis; verify payload shape and stream name.
- [ ] **3.1.2** **CLI – manual order:** Subcommand `order` with args (broker, symbol, side, quantity, order_type, optional limit_price, book, comment). Build payload, call publish_manual_order. **Smoke:** run CLI, then verify OMS receives (e.g. order in Redis store or Postgres).
- [ ] **3.1.3** **CLI – cancel:** Subcommand `cancel` with `--order-id` or `--broker-order-id` and `--symbol`, `--broker`. Publish to `cancel_requested`. **Smoke:** place order then cancel via CLI; verify OMS cancels.

### 9.2 Read-only views

- [ ] **3.2.1** **Query helpers:** Functions to read from Postgres: list open orders (by account, status), list positions (by account/book), list balances (by account), list accounts. Use existing schema (orders, positions, accounts, balances). **Unit test:** mock DB or test DB; verify SQL and returned shape.
- [ ] **3.2.2** **CLI – list orders:** Subcommand `orders` (optional `--account-id`, `--status open`). Print table of open/recent orders.
- [ ] **3.2.3** **CLI – list positions:** Subcommand `positions` (optional `--account-id`, `--book`). Print table from Postgres `positions`.
- [ ] **3.2.4** **CLI – list balances:** Subcommand `balances` (optional `--account-id`). Print table from Postgres `balances`.
- [ ] **3.2.5** **CLI – recent fills (optional):** Subcommand `fills` (optional `--last N`). XREVRANGE `oms_fills` or query Postgres if fills table exists; print last N.

### 9.3 Optional extensions

- [ ] **3.3.1** **Flush risk queue:** Define command (e.g. `admin_commands` stream with `command=flush_risk`). Risk service consumes and clears or skips pending `strategy_orders` (or no-op if Risk reads only new messages). Lower priority.
- [ ] **3.3.2** **Refresh account:** Command to trigger OMS account refresh (e.g. set Redis key or publish to stream that OMS reads). OMS already has periodic refresh; this is “refresh now.” Lower priority.
- [ ] **3.3.3** **Web GUI:** Thin backend (HTTP API) that wraps publish + read; simple front-end that lists state and sends order/cancel. Optional for Phase 3.

---

## 10. Testing

- **Unit:** Mock Redis and Postgres; test publish payloads and query helpers.
- **Integration:** Admin CLI publishes manual order → OMS (running) processes it → order appears in Postgres; Admin CLI cancel → OMS cancels; Admin CLI list orders/positions/balances returns data from Postgres.
- **Smoke:** Manual run: place order via CLI, cancel via CLI, list orders/positions/balances.

---

## 11. Acceptance

- [ ] Admin CLI can place a manual order (to `risk_approved`); OMS executes it (or rejects); order visible in Postgres and list orders.
- [ ] Admin CLI can cancel an open order (to `cancel_requested`); OMS cancels; order status updated.
- [ ] Admin CLI can list open orders, positions, and balances from Postgres (read-only).
- [ ] (Optional) Admin CLI can show recent fills from `oms_fills` or Postgres.

---

## 12. Suggested repo layout (Phase 3)

```
admin/
  __init__.py
  cli.py          # argparse + subcommands: order, cancel, orders, positions, balances, fills
  publish.py      # publish_manual_order, publish_cancel
  queries.py      # query_orders, query_positions, query_balances, query_accounts
  requirements.txt
  # Optional: api.py (FastAPI/Flask) for Web GUI backend
```

Or keep CLI as scripts: `scripts/admin_order.py`, `scripts/admin_cancel.py`, `scripts/admin_list_orders.py`, etc., and a shared `admin/` or `scripts/admin_common.py` for publish/query helpers.

---

## 13. Link to main plan

Phase 3 corresponds to **docs/IMPLEMENTATION_PLAN.md** § Phase 3: Admin. This document is the detailed plan; the main plan’s deliverables and acceptance are satisfied by the task list above.
