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
- **Config:** Per workspace rules — **intervals, enabled schedules, symbol scope** in code or small config modules; **secrets/URLs** use shared **`DATABASE_URL`** / **`REDIS_URL`** (no scheduler-specific DB/Redis env).

---

## 4. Task checklist (implementation order)

Order: **skeleton → config → runner/observability → persistence (optional) → report job → recon jobs → misc example → Docker → tests**. Domain-heavy logic should live in **OMS/PMS** where possible; `scheduler` **calls** it.

### 4.1 Package skeleton and job contract

- [x] **5.1.1** Create `scheduler/` at repo root: package layout, `python -m scheduler` (or `scheduler.main`) entrypoint.
- [x] **5.1.2** Define a small **job interface** (callable or Protocol): `job_id`, async or sync `run(ctx)`, optional **timeout** per job.
- [x] **5.1.3** **`registry.py`:** register jobs with **schedule** (cron string or fixed interval seconds) and **`enabled`** flag; load from `scheduler/config.py` (micro), not scattered literals.

### 4.2 Configuration (macro vs micro)

- [x] **5.2.1** **`scheduler/config.py`:** intervals, default timeouts, which jobs are on — **code/constants** per [env-and-config](../.cursor/rules/env-and-config.mdc) unless you need per-deployment toggles only.
- [x] **5.2.2** **Env (macro):** shared **`DATABASE_URL`** and **`REDIS_URL`** (same as other services); document only if adding scheduler-only secrets later — no per-symbol report lists in `.env`.

### 4.3 Runner loop, isolation, and shutdown

- [x] **5.3.1** Long-running **scheduler loop** (asyncio or threads): wake jobs when due; **one failure must not crash** the process — catch, log, continue.
- [x] **5.3.2** **Structured logging:** `job_id`, run boundaries, duration, exception stack on failure.
- [x] **5.3.3** **Graceful shutdown:** handle SIGTERM; finish in-flight job or bounded wait, then exit.

### 4.4 Run history (optional v1, recommended for ops)

- [x] **5.4.1** Alembic migration: **`scheduler_runs`** (name flexible): `job_id`, `started_at`, `finished_at`, `status` (`ok`/`error`), `error` (text), `payload` (JSONB for summary stats / diff counts).
- [x] **5.4.2** Shared helper: `record_run_start` / `record_run_end` (or single upsert) called from runner or each job.

### 4.5 Reports jobs (v1)

- [x] **5.5.1** **`scheduler/jobs/reports/`:** implement **at least one** report — e.g. daily **position / exposure snapshot** — primarily **`SELECT`** from Postgres (views or queries aligned with PMS; avoid re-deriving position math inside scheduler). **Implemented:** hourly **`position_snapshot_hourly`** → **four** CSVs (`by_asset`, `by_broker`, `by_book`, `granular`) in `scheduler/reports_out/` (gitignored).
- [x] **5.5.2** **Output v1:** write **one row per run** into a snapshot/archive table and/or structured log line; defer PDF/email/S3 until needed. **Implemented as CSV** (no snapshot table); same-hour re-run overwrites the file.
- [x] **5.5.3** **Unit test:** report function with **mock DB** or fixture rows; assert shape of written summary. **Implemented:** `scheduler/tests/test_position_snapshot_hourly.py`.

### 4.6 Reconciliation jobs (v1)

- [x] **5.6.1** **`scheduler/jobs/reconciliation/order_recon.py` (or similar):** compare **internal** order state (Postgres `orders` / counts / terminal statuses) vs **broker** truth via REST — start with **counts + sample diff**; expand to full key-by-key later. Prefer **reusing OMS** HTTP/signing helpers via import from `oms/` if the repo allows, or a thin shared client. **Implemented:** `order_reconciliation_binance` — **`BinanceAPIClient.get_open_orders`**, CSV **`order_recon_summary_*.csv`** + **`order_recon_diff_*.csv`** in `scheduler/reports_out/`; optional **`ORDER_RECON_ACCOUNT_ID`** in `scheduler/config.py`.
- [ ] **5.6.2** **Position reconciliation:** **deferred** (same CSV style as orders; not implemented).
- [x] **5.6.3** **Idempotency:** safe to re-run same calendar window; no duplicate alerts for same settled state (document strategy). **Same-hour report timestamp slug** → overwrite; diff reflects current drift.
- [x] **5.6.4** **Unit tests:** mock broker responses and DB fixtures; assert diff detection logic. **`scheduler/tests/test_order_recon.py`** + **`oms/.../test_api_client.py`** (`get_open_orders`).

### 4.7 Miscellaneous jobs (example)

- [x] **5.7.1** **`scheduler/jobs/misc/`:** one **housekeeping** job — e.g. prune old `scheduler_runs` beyond **N** days, or documented **no-op** placeholder with log line — proves the registry accepts arbitrary jobs. **Implemented:** **`NoopHeartbeatJob`** in **`scheduler/jobs/misc/noop_heartbeat.py`** — **`noop_heartbeat`** in **`JOB_SPECS`** (**hourly**, **`NOOP_HEARTBEAT_INTERVAL_SECONDS`** = **`SCHEDULER_HOURLY_INTERVAL_SECONDS`**); **info** log per run.

### 4.8 Docker and Compose

- [x] **5.8.1** **`scheduler/Dockerfile`** or extend existing multi-stage pattern used by `pms` / `market_data`. **Implemented:** root **`Dockerfile`** **`COPY scheduler/`** (single image for all Python services).
- [x] **5.8.2** **`docker-compose.yml`** service **`scheduler`**: same network as Postgres (and Redis if locks); `depends_on` health where applicable.
- [x] **5.8.3** Short **runbook** in `scheduler/README.md` (or root README section): command, required env, how to run a single job once for debugging.

### 4.9 Optional / follow-up (not required for Phase 5 acceptance)

- [ ] **5.9.1** **Redis lock:** `SET NX` with TTL keyed by `job_id` + window to prevent **overlapping** runs with multiple replicas.
- [ ] **5.9.2** **HTTP `/health`** or metrics endpoint (optional).
- [ ] **5.9.3** **Ad-hoc trigger:** Admin stream, CLI, or HTTP (integrate with Phase 3 when useful).

### 4.10 Tests (see also §6)

- [x] **5.10.1** **Integration:** with **`DATABASE_URL`** and migrations at head, **`scheduler/tests/test_postgres_integration.py`** exercises **`record_run_start` / `record_run_end`** against **`scheduler.scheduler_runs`** and asserts key tables live in expected schemas (inventory). Full runner smoke (report + recon) remains optional / manual (**5.10.2**).
- [ ] **5.10.2** **Smoke:** `docker compose up scheduler` — process stays up; scheduled tick completes once in dev (or documented manual one-shot).

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

---

## 8. Local stack on macOS (bootstrap and deploy)

Use this when bringing up the **whole system** on a Mac after clone: infra, schema, minimal DB data, and app containers. The same flow works on Linux with Docker; commands assume **Docker Compose v2** (`docker compose`).

### 8.1 What you need on the Mac

| Item | Notes |
|------|--------|
| **Docker Desktop for Mac** (or Colima + compose plugin) | Enable **VirtioFS** or file sharing for the repo path if bind mounts feel slow. Give Docker enough **RAM** (e.g. 8 GB+) for Postgres + several Python services. **Apple Silicon:** images used here (`postgres:16-alpine`, `redis:7-alpine`, `python:3.12-slim`) are multi-arch; no extra setup required in normal cases. |
| **Git** | To clone the repo. |
| **`.env`** | Copy **`.env.example`** → **`.env`** and set secrets (Postgres password, Binance keys if you use live/testnet trading or **scheduler order recon**). Compose substitutes `POSTGRES_*` for the `postgres` service; app containers override `DATABASE_URL` / `REDIS_URL` to the compose network—keep **host-oriented** URLs in `.env` (`localhost`) for optional host-side tools and Alembic-style commands documented below. |
| **Optional: Python 3.12 + venv on the host** | Only if you run tests or scripts outside Docker. Not required for the all-in-one deploy script. |

### 8.2 Order of operations (mental model)

1. **Docker Compose** — bring up (or build) images; Postgres and Redis must be healthy before apps rely on them.
2. **DB schema (Alembic)** — run **after** Postgres is up: `alembic upgrade head` (project uses a single migration chain under `alembic/versions/`).
3. **`.env`** — maintain manually; see `.env.example` and [env-and-config](../.cursor/rules/env-and-config.mdc).
4. **Initial data (ingestion / seed)** — not automatic in Compose; run explicitly once per fresh DB (see §8.4).
5. **App services** — `oms`, `pms`, `risk`, `market_data`, **`scheduler`** (Phase 5). The **`deploy_stack`** script starts **`oms`** first, then runs the history backfill, then **`market_data`**, then **`pms`**, **`risk`**, and **`scheduler`** so ingestion is up before those consumers.

### 8.3 One-shot deploy script (recommended)

From the repo root (after `.env` exists):

```bash
chmod +x scripts/deploy_stack.sh   # once
./scripts/deploy_stack.sh
```

Optional flags:

- **`--no-build`** — skip image build (faster iteration if the image is already current).
- **`--with-tools`** — also start **`pgadmin`** and **`redisinsight`** (see `docker-compose.yml` ports).
- **`--skip-existing`** — only for backfill: attempt to skip contiguous existing history when possible.
- **`--dry-run`** — print commands without running.

The script: builds app images → **`docker compose up -d`** for `postgres` + `redis` → **`docker compose run --rm oms python -m alembic upgrade head`** → starts **`oms`** only → runs **`scripts/backfill_all_no_watermarks.py`** → starts **`market_data`** → starts **`pms`**, **`risk`**, and **`scheduler`**. **Asset rows** (stables + `usd_symbol` from `symbols`) are initialized when **PMS** starts; use **`§8.4`** scripts manually if you need a full **`assets`** reset or a one-off without bringing up PMS.

On Windows PowerShell, use **`.\scripts\deploy_stack.ps1`** with the same flags (**`-NoBuild`**, **`-WithTools`**, **`-SkipExisting`**, **`-DryRun`**).

Legacy behavior (seed + start *all* services immediately) is preserved as `scripts/deploy_stack_legacy_all_services.sh`.

Windows equivalent for “rebuild apps + migrate” only: **`scripts/update_and_deploy.ps1`** (assumes Postgres already running). For a full first-time Windows bootstrap, mirror the same steps as the shell script (compose up infra, `run oms … alembic`, compose up apps).

### 8.4 Initial data (seed scripts)

| Script | When | Command (host with `.env` and venv) | Same via Docker |
|--------|------|--------------------------------------|-----------------|
| **`scripts/init_assets.py`** | Idempotent: stables with USD peg | `python scripts/init_assets.py` | `docker compose run --rm oms python scripts/init_assets.py` |
| **`scripts/reset_and_seed_assets.py`** | Fresh **`assets`** table (destructive) | `python scripts/reset_and_seed_assets.py` | `docker compose run --rm oms python scripts/reset_and_seed_assets.py` |

Both require **`DATABASE_URL`**. Historical market or metrics backfills (OHLCV, open interest, etc.) are **optional** and use other `scripts/backfill_*.py` modules when you need that data— they are not part of the minimal bootstrap.

### 8.5 Lighter “core stack only” (no migrate/seed)

**`scripts/start_core_stack_mac.sh`** starts Postgres, Redis, and the app services (including **scheduler**) without running Alembic or seeds—useful when the DB is already prepared.

### 8.6 Anything else?

- **Scheduler reports path:** `scheduler` mounts **`./scheduler/reports_out`** on the host; ensure the directory exists or let Compose create it (first run).
- **Binance / API keys:** OMS, market data, and scheduler recon jobs need appropriate **`.env`** entries (`BINANCE_*`, testnet flags); without them, some jobs log errors or skip—see service READMEs.
- **Ports:** `5432`, `6379`, and any published app ports in `docker-compose.yml` must be free on the host.
- **CI / integration tests:** Phase 5 checklist **§4.10** still tracks optional integration tests and compose smoke checks; local bootstrap is separate from CI.
