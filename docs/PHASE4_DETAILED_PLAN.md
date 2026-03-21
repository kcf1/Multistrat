# Phase 4: Detailed Plan — Market Data

**Goal:** Run a dedicated **market data** process that ingests **public** market feeds from a primary venue (**Binance first**, consistent with Phase 2 OMS), writes **historical** series to **Postgres** and (when §9.3–9.4 are done) **live / hot** snapshots to **Redis**, so **Phase 5 strategies** (and optional **PMS** mark-price reads) can consume a single, documented contract. **Current tranche:** Postgres + REST only (**§0**). No order placement, no account streams—**read-only market APIs only**.

**Current architecture:** [docs/ARCHITECTURE.md](ARCHITECTURE.md). Phase 2 PMS today uses **`PMS_MARK_PRICE_SOURCE=binance`** for mark prices; Phase 4 adds a path where PMS (or strategies) reads **Redis/Postgres** fed by this service—see [docs/pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md) §11 and `PMS_MARK_PRICE_SOURCE`.

## 0. Implementation priority (defer 9.3–9.4)

**Current stance — Redis hot path + WebSocket deferred:** Ship **Postgres `ohlcv` via REST only** first (checklist **§9.1**, **§9.2**, and a **REST-only** **§9.5** runner/deploy). **Do not require** market-data **Redis** keys (§5) or **WebSocket** live ingestion until this deferral is lifted.

### Reasons for deferral

- **Sufficient for batch / hourly workflows:** Historical bars in Postgres support backtests, factor work, and scheduled rebalance without sub-second Redis or push streams.
- **Lower operational surface:** No WS reconnect logic, combined-stream limits, or TTL/staleness contract to maintain on Redis for market keys yet.
- **Phase 5 can start on SQL:** Strategies may read **`ohlcv`** from Postgres (poll or scheduled) until **§9.3–9.4** deliver the low-latency Redis contract and real-time updates.
- **PMS unchanged:** Keep **`PMS_MARK_PRICE_SOURCE=binance`** (or Postgres-derived marks later); **§4.6.1** (Redis-fed mark) stays optional and blocked on **§9.3** anyway.

**When picked up:** Implement **§9.3** (Redis publisher + tests), then **§9.4** (WS + wire to Redis + closed-bar Postgres), then extend **§9.5** to orchestrate WS alongside REST.

---

## 1. Overview

| Item | Choice |
|------|--------|
| **Venue (v1)** | **Binance** public REST + WebSocket (spot and/or USDT-M futures—pick one product line per deployment; document which). Testnet vs mainnet via existing URL/env patterns; see [docs/BINANCE_API_RULES.md](BINANCE_API_RULES.md) for limits and connection behavior. |
| **Postgres role** | **Durable history:** **`ohlcv`** table (required for Phase 4 acceptance); optional **trades** or **agg_trades** table if strategies need tick granularity later. |
| **Redis role** | **(Deferred — §0.)** Planned: low-latency hot state per §5. **Until then:** no required `market:*` keys from `market_data`; optional use of Redis only if you add an unrelated cache. |
| **Process model** | **Now:** **scheduled REST** backfill / gap repair → Postgres `ohlcv`. **Deferred:** WebSocket live path + Redis updates (§9.4 / §9.3). |
| **Consumers** | **Now:** strategies (or jobs) read **Postgres `ohlcv`**. **After deferral lift:** Redis per §5; PMS Redis mark (**§4.6.1**). |

---

## 2. Dependencies

- **Phase 1:** Postgres, Redis, Docker network, Alembic baseline.
- **Phase 2:** **Recommended** before production overlap: aligned **symbol** naming with OMS (`symbols` table / exchange filters). Not strictly required for a standalone market-data smoke test on a fixed symbol list.
- **Phase 3:** Not required.

---

## 3. Scope and non-goals

**In scope**

- Configurable **symbol list** and **bar intervals** (e.g. `1m`, `5m`, `1h`) for OHLCV rows.
- **Idempotent writes:** Postgres upserts or `ON CONFLICT` on `(symbol, interval, open_time)` (or equivalent natural key).
- **Documented Redis key schema** and TTLs (see §5).
- **Docker** service on the same Compose network as Postgres/Redis.
- **Health:** log heartbeat; optional metric counters (messages/sec, last WS message time).

**Out of scope (unless explicitly added later)**

- Private streams, order flow, OMS integration.
- Multi-venue routing in v1 (design **interfaces** so a second adapter is possible).
- Guaranteed sub-millisecond latency (aim for **correct** and **bounded resource** use first).

---

## 4. Postgres schema

### 4.1 `ohlcv` (required)

| Column | Type | Notes |
|--------|------|--------|
| `symbol` | TEXT | Exchange symbol, e.g. `BTCUSDT` |
| `interval` | TEXT | Binance interval string, e.g. `1m`, `1h` |
| `open_time` | TIMESTAMPTZ | Bar open (UTC); part of uniqueness |
| `open`, `high`, `low`, `close` | NUMERIC | Prices |
| `volume` | NUMERIC | Base asset volume |
| `quote_volume` | NUMERIC | Optional but useful |
| `trades` | INTEGER | Optional |
| `close_time` | TIMESTAMPTZ | Optional |
| `ingested_at` | TIMESTAMPTZ | Default `now()` for audit |

**Constraints:** `UNIQUE (symbol, interval, open_time)` (or PRIMARY KEY spanning the three).

**Indexes:** `(symbol, interval, open_time DESC)` for range queries; optional partial indexes if you partition by time later.

**Migration:** New Alembic revision under `alembic/versions/`; no raw SQL-only schema.

### 4.2 `trades` (optional in Phase 4)

- Only add if a concrete strategy needs **trade tape** in Postgres in Phase 4/5.
- If deferred, document **Redis-only** recent trades or omit entirely.

### 4.3 Latest OHLCV bar mirror (optional)

- Optional small table or materialized path: **latest** row per `(symbol, interval)` for SQL dashboards; otherwise strategies can use **Redis** for “current” bar.

---

## 5. Redis key layout (contract for consumers)

Document in code (e.g. `market_data/redis_keys.py`) and in this section. **Names are illustrative—finalize in implementation and keep stable for Phase 5.**

| Pattern | Content | TTL |
|---------|---------|-----|
| `market:{symbol}:ticker` | Hash: `last`, `bid`, `ask`, `volume_24h`, `quote_volume_24h`, `ts` (ISO or ms) | **60–300 s** (refresh from WS); extend on each update |
| `market:{symbol}:ohlcv_bar:{interval}` | Hash or JSON string: `open_time`, `o`, `h`, `l`, `c`, `v`, `is_closed` | **None or long** if overwritten each close; optional short TTL if only “open bar” |
| `market:{symbol}:ohlcv_recent:{interval}` | List (JSON) **last N** closed OHLCV bars (newest at head or tail—pick one convention) | Cap **N** in code (e.g. 500); optional max memory eviction |
| `market:{symbol}:mark` | Last mark price (futures) | **60–120 s** if WS stalls |

**Rules**

- **Namespace** prefix `market:` reserved for this service; avoid colliding with OMS `orders:*` / `account:*`.
- **TTL:** every ephemeral key must have a documented TTL or explicit overwrite policy so a dead WS does not leave **forever-stale** prices without consumers knowing.
- **Serialization:** prefer **JSON** strings for simplicity cross-language, or Redis Hash fields with string decimals—**one convention** per key type.

---

## 6. Service design (`market_data/`)

### 6.1 Layout (suggested)

```
market_data/
  __init__.py
  config.py          # symbols, intervals, BINANCE_WS_URL / REST base, DATABASE_URL, REDIS_URL
  main.py            # entrypoint: start REST scheduler + WS tasks
  postgres_writer.py # insert/upsert ohlcv rows, optional trades
  redis_publisher.py # set keys, TTLs
  binance_rest.py    # klines / exchangeInfo (filters)
  binance_ws.py      # combined streams, reconnect, backoff
  schemas.py         # optional Pydantic for ohlcv/ticker payloads
```

Reuse patterns from OMS where sensible (HTTP session, time sync if signed endpoints added later—**public** endpoints may not need signing; still respect [BINANCE_API_RULES.md](BINANCE_API_RULES.md)).

### 6.2 REST (historical / gap fill)

- On startup and on a **cron-like** schedule: fetch **klines** for each `(symbol, interval)` from last stored `open_time` to “now” (respect **weight limits**; batch symbols).
- **Backfill:** first run may pull **months** of data—**throttle** and **checkpoint** progress to avoid ban.

### 6.3 WebSocket (live)

- Subscribe to **ticker** and/or **kline** streams per symbol (combined URL to reduce connections).
- **Reconnect** with exponential backoff; **resync** last closed bars from REST after long gaps.
- Update **Redis** on each message; update **Postgres** `ohlcv` for **closed** bars (or buffer and flush every N seconds).

### 6.4 Config (env)

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Postgres |
| `REDIS_URL` | Redis |
| `MARKET_DATA_SYMBOLS` | Comma-separated list; **default** when unset: all **USDT** pairs in [`market_data/universe.py`](../market_data/universe.py) (`DATA_COLLECTION_SYMBOLS`, 30 bases → `BTCUSDT`, …) |
| `MARKET_DATA_INTERVALS` | Comma-separated, e.g. `1m,5m,1h` |
| `BINANCE_REST_URL` / `BINANCE_WS_URL` | Align with spot vs futures testnet/mainnet |

---

## 7. PMS integration (optional in Phase 4)

- **Goal:** Implement `PMS_MARK_PRICE_SOURCE` value that reads **mark/last** from Redis keys in §5 (e.g. `market:{symbol}:ticker` or `market:{symbol}:mark`) for symbols in open positions.
- **Fallback:** If key missing or stale (TTL expired), log and fall back to **Binance REST** (existing Phase 2 behavior) or skip unrealized update—**document** choice.
- **Deliverable:** Small provider module in `pms/` wired in `mark_price.py`; **no** change to position derivation math.

---

## 8. Docker

- **Option A:** Extend the **existing** app `Dockerfile` to `COPY market_data/` and add Compose service `market_data` with `command: ["python", "-m", "market_data.main"]` (mirrors `pms` / `risk`).
- **Option B:** Separate image only if dependencies diverge significantly (avoid unless needed).

**Compose:** `depends_on` Postgres and Redis healthy; **no** public port required unless you add a debug HTTP endpoint (optional).

---

## 9. Task checklist (implementation order)

### 9.1 Schema and config

- [ ] **4.1.1** Alembic migration: **`ohlcv`** table + indexes + unique constraint.
- [ ] **4.1.2** `market_data/config.py` (or equivalent): load symbols, intervals, URLs from env; validate at startup.

### 9.2 Postgres writes

- [ ] **4.2.1** `upsert_ohlcv` (or bulk) with **idempotent** conflict handling.
- [ ] **4.2.2** REST backfill job: from empty DB and from “last open_time” checkpoint.
- [ ] **4.2.3** **Unit test:** mock DB or use test container; verify upsert semantics.

### 9.3 Redis writes **(deferred — §0)**

- [ ] **4.3.1** Publish ticker (and optional kline) to keys in §5; set **TTL** on update.
- [ ] **4.3.2** **Unit test:** fakeredis; verify key names, TTL, JSON shape.

### 9.4 WebSocket **(deferred — §0)**

- [ ] **4.4.1** Connect combined streams; parse ticker/kline; reconnect loop.
- [ ] **4.4.2** Wire closed kline → Postgres `ohlcv` upsert + Redis `ohlcv_recent` update.

### 9.5 Runner and deploy

- [ ] **4.5.1** `python -m market_data.main` orchestrates **REST-only** until §9.3–9.4 are implemented; then add WS + graceful shutdown.
- [ ] **4.5.2** `docker-compose.yml` service **`market_data`**; document in README or `market_data/README.md`.

### 9.6 Optional

- [ ] **4.6.1** PMS Redis/market mark-price provider (`PMS_MARK_PRICE_SOURCE`).
- [ ] **4.6.2** Postgres **`trades`** table + ingest (only if required).
- [ ] **4.6.3** Prometheus / structured metrics.

---

## 10. Testing

- **Unit (current tranche):** Postgres writer (mock or transactional rollback). **Deferred with §9.3–9.4:** Redis publisher (fakeredis), WS message parsing (fixture JSON).
- **Integration (current tranche):** Service + Postgres; after **N** minutes, `SELECT COUNT(*) FROM ohlcv WHERE symbol = …` increases. **Deferred:** Redis `GET`/`HGETALL` fresh `ts`.
- **Smoke:** `docker compose up -d postgres market_data` (Redis optional until §9.3); verify `ohlcv` rows.

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) testing table for Phase 4.

---

## 11. Acceptance

- [ ] For configured symbols and at least one interval: **Postgres** `ohlcv` contains **recent historical** bars (REST backfill + scheduled refresh of closed bars).
- [ ] **(Deferred — §9.3–9.4)** **Redis** holds **up-to-date** ticker (and/or `ohlcv_bar` / `ohlcv_recent`) keys per §5 with **documented TTL** behavior.
- [ ] **market_data** service runs in **Docker** on the multistrat network (**REST-only** OK under §0).
- [ ] **(Deferred / optional)** PMS Redis-fed mark prices (**§4.6.1**); until then PMS keeps direct Binance (or other) mark source.

---

## 12. Handoff to Phase 5

- **Until §9.3–9.4:** strategies should use **SQL** against `ohlcv` (and optional `trades`) as the supported contract. **After deferral lift:** add dependence on **documented** Redis keys (§5) for low-latency reads.
- Risk / OMS unchanged; no new streams required for market data (unless you later add a **cache invalidation** stream—**not** in Phase 4 minimum).

---

## 13. Link to main plan

Phase 4 corresponds to **docs/IMPLEMENTATION_PLAN.md** § Phase 4: Market Data. This document is the detailed plan; deliverables and acceptance there should be satisfied by the task list above.
