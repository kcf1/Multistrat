# Phase 4: Detailed Plan — Market Data

**Goal:** Run a dedicated **market data** process that ingests **public** market feeds from a primary venue (**Binance first**, consistent with Phase 2 OMS), writes **historical** series to **Postgres** and (when **§9.7–9.8** are done) **live / hot** snapshots to **Redis**, so **Phase 6 strategies** (and optional **PMS** mark-price reads) can consume a single, documented contract. **Current tranche:** Postgres + REST only (**§0**). No order placement, no account streams—**read-only market APIs only**.

**Current architecture:** [docs/ARCHITECTURE.md](ARCHITECTURE.md). Phase 2 PMS today uses **`PMS_MARK_PRICE_SOURCE=binance`** for mark prices; Phase 4 adds a path where PMS (or strategies) reads **Redis/Postgres** fed by this service—see [docs/pms/PMS_ARCHITECTURE.md](pms/PMS_ARCHITECTURE.md) §11 and `PMS_MARK_PRICE_SOURCE`.

**Adding datasets:** Reusable ingest checklist (grain, schema, cursor, correction/repair, tests): [DATASET_INGESTION_STEPS.md](market_data/DATASET_INGESTION_STEPS.md).

## 0. Implementation priority (defer §9.7–9.8)

**Current stance — Redis hot path + WebSocket deferred:** Ship the **§9.1–9.6** tranche first: **schema → models/parser → provider + rate limit → storage → jobs → runner/Docker** (see **§6.0**). **Do not require** market-data **Redis** keys (§5) or **WebSocket** until **§9.7–9.8** are implemented.

### Reasons for deferral

- **Sufficient for batch / hourly workflows:** Historical bars in Postgres support backtests, factor work, and scheduled rebalance without sub-second Redis or push streams.
- **Lower operational surface:** No WS reconnect logic, combined-stream limits, or TTL/staleness contract to maintain on Redis for market keys yet.
- **Phase 6 strategies can start on SQL:** Strategies may read **`ohlcv`** from Postgres (poll or scheduled) until **§9.7–9.8** deliver the low-latency Redis contract and real-time updates.
- **PMS unchanged:** Keep **`PMS_MARK_PRICE_SOURCE=binance`** (or Postgres-derived marks later); **§9.9.1** (Redis-fed mark) stays optional and blocked on **§9.7** anyway.

**When picked up:** Implement **§9.7** (Redis publisher + tests), then **§9.8** (WS + wire to Redis + closed-bar Postgres), then extend **§9.6** (`main`) to run WS alongside the REST job scheduler.

---

## 1. Overview

| Item | Choice |
|------|--------|
| **Venue (v1)** | **Binance** public REST + WebSocket (spot and/or USDT-M futures—pick one product line per deployment; document which). Testnet vs mainnet via existing URL/env patterns; see [docs/oms/BINANCE_API_RULES.md](oms/BINANCE_API_RULES.md) for limits and connection behavior. |
| **Postgres role** | **Durable history:** **`ohlcv`** table (required for Phase 4 acceptance); optional **trades** or **agg_trades** table if strategies need tick granularity later. |
| **Redis role** | **(Deferred — §0.)** Planned: low-latency hot state per §5. **Until then:** no required `market:*` keys from `market_data`; optional use of Redis only if you add an unrelated cache. |
| **Process model** | **Now:** **scheduled REST** jobs (**§9.5**) → Postgres `ohlcv`. **Deferred:** WebSocket + Redis market keys (**§9.8**, **§9.7**). |
| **Consumers** | **Now:** strategies (or jobs) read **Postgres `ohlcv`**. **After deferral lift:** Redis per §5; PMS Redis mark (**§9.9.1**). |

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

Document in code (e.g. `market_data/redis_keys.py`) and in this section. **Names are illustrative—finalize in implementation and keep stable for Phase 6 strategies (and other consumers).**

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

### 6.0 Runner architecture (OMS-like extensibility)

**Goal:** One **market_data** process that is easy to extend with **new venues** and **new dataset jobs** without duplicating Postgres/cursor logic—similar in spirit to OMS’s **broker adapter** pattern, but **read-only** and **ingest-centric**.

| Piece | Role |
|-------|------|
| **Provider interface** | Protocol or ABC per venue: e.g. build REST requests for klines, parse responses into internal OHLCV (or other) models, optional WS URL/stream names. **No** SQL in adapters. Register implementations (e.g. `binance_spot`) in a small registry keyed by `provider_id` or env. |
| **Job scheduler / supervisor** | `main` starts a **scheduler** (or asyncio loop) that runs **job types** on a cadence: **ingest / backfill**, **correction window**, **gap repair**, optional **QC** (sanity checks, staleness metrics). Each job calls **shared** `upsert` + **cursor** helpers; adapters only fetch/parse. |
| **Rate limiting** | Limits are **per API identity** (host + API key if any), not “per dataset name.” Use a **single token bucket or queue per provider instance** so all job types (OHLCV, funding, …) **share** the budget. **Default:** **sequential** or **very low concurrency** (1–2) for one Binance REST key; prefer **batch endpoints** where the API allows. |
| **Parallelism** | **Different providers** (different hosts/keys) may run **in parallel** (separate tasks). **Same provider:** jobs **take turns** through the shared limiter—do not run unbounded parallel REST per symbol. |
| **Deferred (§0)** | **WebSocket** runners are optional later; same provider module can expose WS hooks while REST+scheduler stay the **first** tranche. |

**Reference:** Ingest checklist for new tables/jobs — [DATASET_INGESTION_STEPS.md](market_data/DATASET_INGESTION_STEPS.md).

### 6.1 Layout (suggested)

```
market_data/
  __init__.py
  universe.py        # DATA_COLLECTION_* (existing)
  config.py          # symbols, intervals, provider URLs, DATABASE_URL, REDIS_URL
  main.py            # entrypoint: scheduler + job registration; later optional WS tasks
  jobs/              # ingest_ohlcv, correct_window, repair_gap, optional qc
  providers/
    base.py          # Protocol / ABC for venue
    binance_spot.py  # REST klines, exchangeInfo; later WS helpers if enabled
  storage/
    postgres_ohlcv.py   # upsert_ohlcv (name may stay postgres_writer.py in v1)
    cursors.py          # explicit watermark table or helpers around MAX(open_time)
  redis_publisher.py # set keys, TTLs (deferred §0)
  schemas.py         # optional Pydantic for ohlcv/ticker payloads
```

Reuse patterns from OMS where sensible (HTTP session, time sync if signed endpoints added later—**public** endpoints may not need signing; still respect [BINANCE_API_RULES.md](oms/BINANCE_API_RULES.md)).

### 6.2 REST (historical / gap fill)

- On startup and on a **cron-like** schedule: fetch **klines** for each `(symbol, interval)` from last stored `open_time` to “now” (respect **weight limits**; batch symbols).
- **Backfill:** first run may pull **months** of data—**throttle** and **checkpoint** progress to avoid ban.

### 6.3 WebSocket (live)

- Subscribe to **ticker** and/or **kline** streams per symbol (combined URL to reduce connections).
- **Reconnect** with exponential backoff; **resync** last closed bars from REST after long gaps.
- Update **Redis** on each message; update **Postgres** `ohlcv` for **closed** bars (or buffer and flush every N seconds).

### 6.4 Config — `.env` (macro) vs `market_data/config.py` (micro)

**Environment (macro):** Use **service-prefixed** names for isolation (see `.cursor/rules/env-and-config.mdc`). Global `DATABASE_URL` is still accepted as a fallback.

| Variable | Purpose |
|----------|---------|
| `MARKET_DATA_DATABASE_URL` | Optional Postgres URL for this service; **overrides** `DATABASE_URL` when both are set. |
| `DATABASE_URL` | Fallback Postgres URL if `MARKET_DATA_DATABASE_URL` is unset. |
| `MARKET_DATA_BINANCE_BASE_URL` | Optional REST base for Binance **public** endpoints (e.g. testnet vs mainnet); independent of OMS `BINANCE_BASE_URL`. |
| `REDIS_URL` | Redis (when §9.7+ use Redis). |
| API keys / secrets | Only where an endpoint requires signing (not needed for public klines). |

**Code constants in [`market_data/config.py`](../market_data/config.py) (micro):** `OHLCV_SYMBOLS`, `OHLCV_INTERVALS`, `DEFAULT_BINANCE_REST_URL`. Tune datasets in code—not `.env`.

**Later (deferred WS):** e.g. `MARKET_DATA_BINANCE_WS_URL` (macro, service-specific), not shared with OMS unless you choose to duplicate values manually in `.env`.

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

Order matches **§6.0**: **contract/schema → parse → provider (fetch + rate limit) → storage (upsert + cursor) → jobs → runner**; then **deferred** live path; then **optional** extras.

### 9.1 Schema and config

- [x] **4.1.1** Alembic migration: **`ohlcv`** table + indexes + unique constraint.
- [x] **4.1.2** `market_data/config.py`: macro from env (`MARKET_DATA_DATABASE_URL` / `DATABASE_URL`, optional `MARKET_DATA_BINANCE_BASE_URL`); symbols/intervals as **module constants** (see `.cursor/rules/env-and-config.mdc`).

### 9.2 Internal models and parsing

- [x] **4.2.1** Internal OHLCV row model (e.g. Pydantic) + **Binance kline array → row** parser; **unit tests** on fixture payloads.

### 9.3 Provider adapter and rate limiting (**§6.0**)

- [x] **4.3.1** `providers/base.py`: Protocol / ABC for “fetch klines range” (and later optional WS hooks).
- [x] **4.3.2** `providers/binance_spot.py`: REST klines client; integrate **one shared rate limiter / queue per provider instance** (all jobs share it).
- [x] **4.3.3** **Unit tests:** mock HTTP responses; verify parsing and limiter sequencing.

### 9.4 Storage layer (Postgres + cursor)

- [x] **4.4.1** `upsert_ohlcv` (or bulk) with **idempotent** `ON CONFLICT` on `(symbol, interval, open_time)`.
- [x] **4.4.2** **Cursors:** explicit `ingestion_cursor` table and/or documented use of `MAX(open_time)` per `(symbol, interval)`; update after successful commits.
- [x] **4.4.3** **Unit tests:** mock DB or test container; verify upsert and idempotency.

### 9.5 Jobs (scheduled work units)

- [x] **4.5.1** **`ingest_ohlcv` job:** backfill from empty DB + incremental “catch up” using provider + storage + cursor (throttled chunks).
- [x] **4.5.2** **`correct_window` job (optional but recommended):** rolling re-fetch of last *N* bars / *H* hours → upsert; log or alert on row changes (vendor drift).
- [x] **4.5.3** **`repair_gap` job (optional):** detect or target time range → fetch range only → upsert → advance cursor.

**Implemented (code):** [`market_data/jobs/ingest_ohlcv.py`](../market_data/jobs/ingest_ohlcv.py), [`correct_window.py`](../market_data/jobs/correct_window.py), [`repair_gap.py`](../market_data/jobs/repair_gap.py) (`detect_ohlcv_time_gaps`, `run_repair_gap`, …); manual one-shot ingest [`scripts/backfill_ohlcv.py`](../scripts/backfill_ohlcv.py). **Runner:** **§9.6** [`market_data/main.py`](../market_data/main.py) + Compose service `market_data`.

### 9.6 Runner and deploy

- [x] **4.6.1** `python -m market_data.main`: **scheduler** registers **§9.5** jobs (**REST-only** until §9.8); graceful shutdown.
- [x] **4.6.2** `docker-compose.yml` service **`market_data`**; README or `market_data/README.md` run instructions.

### 9.7 Redis writes **(deferred — §0)**

- [ ] **4.7.1** Publish ticker (and optional kline) to keys in §5; set **TTL** on update.
- [ ] **4.7.2** **Unit test:** fakeredis; verify key names, TTL, JSON shape.

### 9.8 WebSocket **(deferred — §0)**

- [ ] **4.8.1** Connect combined streams; parse ticker/kline; reconnect loop.
- [ ] **4.8.2** Wire closed kline → Postgres `ohlcv` upsert + Redis `ohlcv_recent` update (shares storage path with REST jobs).

### 9.9 Optional

- [ ] **4.9.1** PMS Redis/market mark-price provider (`PMS_MARK_PRICE_SOURCE`) — depends on **§9.7**.
- [ ] **4.9.2** Postgres **`trades`** table + ingest (only if required).
- [ ] **4.9.3** Prometheus / structured metrics.

---

## 10. Testing

- **Unit (tranche §9.1–9.6):** parser, provider (mock HTTP), Postgres writer (mock or transactional rollback). **Deferred with §9.7–9.8:** Redis publisher (fakeredis), WS message parsing (fixture JSON).
- **Integration (current tranche):** Service + Postgres; after **N** minutes, `SELECT COUNT(*) FROM ohlcv WHERE symbol = …` increases. **Deferred:** Redis `GET`/`HGETALL` fresh `ts`.
- **Smoke:** `docker compose up -d postgres market_data` (Redis optional until §9.7); verify `ohlcv` rows.

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) testing table for Phase 4.

---

## 11. Acceptance

- [ ] For configured symbols and at least one interval: **Postgres** `ohlcv` contains **recent historical** bars (REST backfill + scheduled refresh of closed bars).
- [ ] **(Deferred — §9.7–9.8)** **Redis** holds **up-to-date** ticker (and/or `ohlcv_bar` / `ohlcv_recent`) keys per §5 with **documented TTL** behavior.
- [ ] **market_data** service runs in **Docker** on the multistrat network (**REST-only** OK under §0).
- [ ] **(Deferred / optional)** PMS Redis-fed mark prices (**§9.9.1**); until then PMS keeps direct Binance (or other) mark source.

---

## 12. Handoff to Phase 6 (strategies)

- **Until §9.7–9.8:** strategies should use **SQL** against `ohlcv` (and optional `trades`) as the supported contract. **After deferral lift:** add dependence on **documented** Redis keys (§5) for low-latency reads.
- **Phase 5 (scheduler)** may also read `ohlcv` or snapshots for **reports**; it does not define the real-time market contract.
- Risk / OMS unchanged; no new streams required for market data (unless you later add a **cache invalidation** stream—**not** in Phase 4 minimum).

---

## 13. Link to main plan

Phase 4 corresponds to **docs/IMPLEMENTATION_PLAN.md** § Phase 4: Market Data. This document is the detailed plan; deliverables and acceptance there should be satisfied by the task list above.
