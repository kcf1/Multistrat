# Standard steps for adding a new dataset (ingestion service)

Reusable checklist when extending **market data** or any **ingest → Postgres** (and optionally Redis) pipeline. Aligns with [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md) for OHLCV; the same pattern applies to other tables (e.g. trades, funding).

---

## 1. Define the grain and identity

- **Natural key** — e.g. `(symbol, interval, open_time)` for bars, or `(symbol, trade_id)` for ticks.
- **Time rules** — UTC; bar **open** time as stored key; venue rules for inclusive/exclusive window edges.
- **Row semantics** — closed bar only vs including partials; where partials live (e.g. Redis only).

---

## 2. Schema

- **Postgres** — table, column types, **`UNIQUE` or `PRIMARY KEY`** on the natural key.
- **Indexes** — match read paths (e.g. `(symbol, interval, open_time DESC)`).
- **Alembic** — one revision; no ad-hoc DDL in production.

---

## 3. Config

- Settings / env: **API base URL**, **universe** (symbols, intervals), **rate-limit budget**, optional feature flags.
- **Secrets** — API keys only via env or a secret store; never committed.

---

## 4. Internal model + parser

- Map raw API/WebSocket payloads → **one internal shape** (e.g. Pydantic/dataclass); avoid scattering raw dicts.
- **Unit tests** — fixture JSON: valid payloads, edge cases, malformed input.

---

## 5. API client

- REST and/or WebSocket: timeouts, retries with **backoff**, **respect provider rate limits** (weights, connections).
- Centralize auth/signature if the endpoint requires it.

---

## 6. Upsert writer (idempotent)

- **`INSERT … ON CONFLICT … DO UPDATE`** (or equivalent) on the natural key.
- **Tests** — duplicate ingest → one row; “vendor correction” payload → row updates as expected.

---

## 7. Cursor / watermark

- **Derived** — e.g. `MAX(open_time)` per `(symbol, interval)`; simple, but a failed partial batch can hide gaps.
- **Explicit** — e.g. `ingestion_cursor` table updated **after** successful commits; easier to **rewind** for repair.
- Document **restart behavior** — how the next REST `startTime` or WS resume is chosen.

---

## 8. Backfill / initialization

- **Cold start** — pull history in **chunks** with throttling; persist cursor per chunk (or transactionally with batches).
- **Smoke / integration** — small symbol, short window, verify rows in DB.

---

## 9. Real-time path (if needed)

- WebSocket or poll → **same parser and upsert** as backfill where possible.
- Decide when Postgres is written (e.g. **closed** bar only) vs hot cache (e.g. Redis) for open bars.

---

## 10. Correction + repair

- **Correction** — scheduled re-fetch of a **rolling window** (last *N* bars or *H* hours) → upsert; optional **alert** if any stored field changed beyond a threshold (vendor drift).
- **Repair** — on **gap detection** or after outage, fetch only `[t0, t1]` → upsert → **advance cursor** past the repaired range.
- Share one **“fetch range → parse → upsert”** code path with backfill and correction.

---

## 11. Observability

- **Logs** — HTTP/WS errors, parse failures, upsert failures, correction stats (“*k* rows updated”).
- **Staleness** — alert if ingest does not succeed or cursor does not advance within *X* minutes (outage signal, separate from correction drift).

---

## 12. Service runner + deploy

- Register the new **job** in the process `main` loop (schedules + optional WS task).
- **Docker / Compose** — env vars, `depends_on` for Postgres/Redis as needed.

---

## 13. Tests (usual)

- Parser unit tests, upsert/idempotency tests, optional integration against Postgres (testcontainer or transactional rollback).

---

## Suggested build order

1. **2 → 3 → 4 → 5 → 6 → 7 → 8** (durable pipeline and resume)
2. **9** if live data is required
3. **10 → 11** (production hygiene)
4. **12 → 13** (integration and CI)

---

## See also

- [PHASE4_DETAILED_PLAN.md](PHASE4_DETAILED_PLAN.md) — `ohlcv`, Binance REST, deferred Redis/WebSocket
- [ARCHITECTURE.md](ARCHITECTURE.md) — Postgres tables and Redis namespaces
