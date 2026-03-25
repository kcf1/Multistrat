# Phase 5: Detailed Plan — Scheduler / Batch Jobs

**Goal:** Run a dedicated **scheduler** (or job runner) for **cross-cutting batch work** that does not belong inside OMS, Booking, PMS, Market Data, Risk, or Admin: **reports**, **reconciliation**, and **other scheduled tasks**. One documented service, pluggable **job** modules, shared **cron/interval** configuration, and clear **observability** (logs, optional metrics).

**Relationship to other phases:** [docs/IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) § Phase 5. **Strategies** are **Phase 6** — this phase is not the strategy runner.

---

## 1. Dependencies

- **Phase 1:** Postgres, Redis, Docker network (jobs may use Redis for locks, idempotency keys, or triggers).
- **Phase 2:** Booking / PMS / OMS data in Postgres and Redis — required for meaningful reports and reconciliation.
- **Phase 3:** Optional (Admin may trigger ad-hoc jobs via a stream or HTTP later — not required for v1).
- **Phase 4:** Optional (market-history-aware reports can read `ohlcv` when available).

---

## 2. Scope

### 2.1 In scope

- **Process model:** Long-running worker that loads a **job registry** (code + config): each job has a **schedule** (e.g. cron expression or fixed interval), **enabled** flag, and **timeout/retry** policy.
- **Reports generation**
  - Examples: position snapshots, PnL summaries, risk/exposure rollups, account-level dashboards — backed by Postgres (and Redis reads if needed).
  - Outputs: Postgres tables (e.g. `report_runs`, archived snapshots), files, or object storage — **choose minimal v1** (e.g. rows + logs) and extend later.
- **Reconciliation**
  - **Order reconciliation:** compare internal `orders` / Redis staging vs broker truth (REST), flag drift, optional Admin/Risk hooks later.
  - **Position reconciliation:** compare Booking/PMS positions vs broker balances/positions, flag breaks.
  - Idempotent runs; persist **last_run**, **diff summary**, **status**.
- **Random / miscellaneous jobs**
  - Housekeeping (purge old rows, vacuum hints), cache warm, one-off maintenance tasks registered as jobs — **same runner**, no second scheduler.

### 2.2 Out of scope (v1)

- Sub-second or streaming pipelines (use dedicated services).
- Replacing PMS as source of truth for live PnL (scheduler only **reads** and **aggregates** for reports).
- A full workflow engine (DAGs, human approval gates) — keep v1 to **independent scheduled jobs**.

---

## 3. Suggested layout

- **Package:** `scheduler/` (or `jobs/`) at repo root: `main` loop, `registry.py`, `config.py`, subpackages `jobs/reports/`, `jobs/reconciliation/`, `jobs/misc/`.
- **Config:** Per workspace rules — **intervals, enabled schedules, symbol scope** in code or small config modules; **secrets/URLs** in `.env` (e.g. `SCHEDULER_DATABASE_URL`, `SCHEDULER_REDIS_URL` if isolated).

---

## 4. Task order (suggested)

1. **Skeleton:** asyncio or threaded loop; read job list from config; structured logging; graceful shutdown.
2. **Persistence:** optional `scheduler_runs` (or reuse a minimal table): `job_id`, `started_at`, `finished_at`, `status`, `error`, `payload` JSONB.
3. **Report job (v1):** one read-only report (e.g. daily position summary) writing one row per run or append-only log.
4. **Reconciliation stub:** one job that loads OMS/Binance (or mock) and compares counts — expand to full diff in a follow-up.
5. **Docker:** `scheduler` service on Compose network; health log line or HTTP `/health` optional.
6. **Locks:** optional Redis `SET NX` per job+window to prevent overlapping runs on multiple replicas.

---

## 5. Acceptance

- [ ] Scheduler process runs in Docker and executes at least **one report** and **one reconciliation** job on a configured schedule (or manual trigger for CI).
- [ ] Each run is **logged**; failures do not crash the whole process (per-job isolation).
- [ ] Job list and schedules are **documented** in code/config (no magic in cron only).

---

## 6. Testing

| Layer | What |
|-------|------|
| **Unit** | Schedule parsing; individual job functions with mocked DB/broker client. |
| **Integration** | Fake Postgres + Redis: runner executes a job end-to-end; `scheduler_runs` (if present) updated. |
| **Smoke** | Compose: service starts; one job completes in dev with test credentials or mocks. |

---

## 7. Link to main plan

Phase 5 corresponds to **docs/IMPLEMENTATION_PLAN.md** § Phase 5: Scheduler / batch jobs. Phase 6 covers strategy modules and full Risk gate.
