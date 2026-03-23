# Dataset Ingestion Steps (Market Data)

Reusable execution guide for adding a new dataset to `market_data`, aligned with the standard blueprint:
`config -> provider -> validation/parser -> storage upsert -> cursor/watermark -> jobs -> scheduler`.

---

## 1) Define dataset contract first

- Set the dataset grain and natural key (for example `(exchange, symbol, event_time)`).
- Define time semantics (UTC, inclusive/exclusive range behavior, closed vs partial rows).
- Define row semantics (what one row means and what updates are allowed).

---

## 2) Add schema and migration

- Create Postgres table with the natural key as `PRIMARY KEY` or `UNIQUE`.
- Add only query-critical indexes.
- Add one Alembic revision for the dataset (no ad-hoc DDL).

---

## 3) Add config (macro vs micro)

- Macro env values:
  - service DB URL (`MARKET_DATA_DATABASE_URL`)
  - provider endpoint URL(s) (service-prefixed)
  - API credentials only if required
- Micro code constants in `market_data/config.py`:
  - symbols, intervals/periods
  - backfill window, chunk limit
  - retry/backoff defaults
  - scheduler cadences

---

## 4) Implement internal model and parser

- Add one canonical internal row model in `market_data/schemas.py`.
- Parse raw payloads into that model in `market_data/validation.py` (or dataset parser module).
- Validate field invariants, timestamp normalization, and ordering assumptions.
- Add parser unit tests for valid, malformed, and edge-case payloads.

---

## 5) Implement provider interface and adapter

- Add dataset-facing provider protocol in `market_data/providers`.
- Add venue adapter module (for example `binance_spot.py` or `binance_perps.py`).
- Enforce timeouts, retries with backoff, and non-retryable client error handling.
- Use one shared `ProviderRateLimiter` per provider instance.

---

## 6) Implement storage upsert and cursor helpers

- Add idempotent upsert:
  - `INSERT ... ON CONFLICT ... DO UPDATE`
- Keep storage functions transaction-agnostic (caller commits).
- Add cursor helpers:
  - `get_cursor`
  - `upsert_cursor`
  - `max_stored_time`
- Ensure startup resume uses `max(cursor, max(stored_time))`.

---

## 7) Implement runtime jobs

- `ingest_<dataset>`:
  - cold start backfill + incremental catch-up
  - chunked fetch -> parse -> upsert -> cursor update -> commit per chunk
- `correct_window_<dataset>`:
  - rolling recent-window refetch
  - drift detection and correction upserts
- `repair_gap_<dataset>`:
  - detect missing spans in policy window
  - targeted range refetch and cursor advancement

---

## 8) Wire scheduler and operational commands

- Register jobs in `market_data/main.py`.
- Keep same runtime behavior:
  - first run immediate
  - next runs UTC-aligned
  - loop survives per-job exceptions
- Keep one-shot operation support (`--once`, optional repair pass).

---

## 9) Apply retry/failure policy

- Retry transient provider/network failures with exponential backoff.
- Treat obvious client input errors as non-retryable.
- Log parse failures and count them in job summaries.
- Commit per chunk so progress survives restarts.

---

## 10) Add data-quality checks

- Ordering and duplicate-key checks per batch.
- Window coverage and span diagnostics.
- Drift metrics from correct-window runs.
- Gap statistics from repair runs.
- Freshness checks (latest stored timestamp lag).

---

## 11) Add observability and runbook

- Structured logs per job:
  - series count
  - rows fetched/upserted
  - retries/give-ups
  - drift and gap counts
- Add staleness monitoring guidance.
- Update service README with run commands and env expectations.

---

## 12) Test plan

- Unit tests:
  - parser/model validation
  - provider request/retry behavior
  - storage upsert idempotency and cursor updates
  - window/gap helper logic
- Job tests:
  - cold start
  - incremental resume
  - no-op up-to-date path
  - partial failure and checkpoint recovery
  - correct-window and repair semantics
- Integration tests:
  - one-shot end-to-end ingest
  - scheduler cadence and restart from persisted cursor

---

## 13) Ordered implementation checklist

- [ ] Define natural key, schema, indexes, migration.
- [ ] Add macro env fields and micro config constants.
- [ ] Implement internal model + parser + parser tests.
- [ ] Implement provider protocol + venue adapter + limiter/retry behavior.
- [ ] Implement upsert + cursor helpers + storage tests.
- [ ] Implement `ingest_<dataset>`.
- [ ] Implement `correct_window_<dataset>`.
- [ ] Implement `repair_gap_<dataset>` (or explicitly defer).
- [ ] Wire jobs into scheduler runner.
- [ ] Update docs and runbook.
- [ ] Add job/integration coverage and run smoke test.

---

## Reference mapping (OHLCV baseline)

- Scheduler orchestration: `market_data/main.py`
- Ingest baseline: `market_data/jobs/ingest_ohlcv.py`
- Correction baseline: `market_data/jobs/correct_window.py`
- Gap repair baseline: `market_data/jobs/repair_gap.py`
- Provider baseline: `market_data/providers/binance_spot.py`
- Validation baseline: `market_data/validation.py`, `market_data/schemas.py`
- Storage baseline: `market_data/storage.py`
- Settings baseline: `market_data/config.py`

---

## See also

- [STANDARD_DATA_PIPELINE.md](./STANDARD_DATA_PIPELINE.md)
- [PHASE4_DETAILED_PLAN.md](../PHASE4_DETAILED_PLAN.md)
- [ARCHITECTURE.md](../ARCHITECTURE.md)
