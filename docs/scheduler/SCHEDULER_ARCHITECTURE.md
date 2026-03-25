# Scheduler Architecture

Single reference for the **`scheduler/`** package: how batch jobs are registered, timed, isolated, audited, and where outputs land. The scheduler is **Phase 5** work: it is **not** a realtime streaming consumer like OMS or PMS; it runs **periodic** reports, venue **reconciliation**, and **misc** jobs in one OS process.

**Related:** root [ARCHITECTURE.md](../ARCHITECTURE.md) (repo map + `scheduler_runs`), [PHASE5_DETAILED_PLAN.md](../PHASE5_DETAILED_PLAN.md), operational notes in [`scheduler/README.md`](../../scheduler/README.md).

---

## 1. High-level shape

```
┌─────────────────────────────────────────────────────────────────┐
│  python -m scheduler  →  main.py (CLI)                          │
│       │                                                         │
│       ├─ --list-jobs / --dry-run-job / --once                   │
│       └─ run_forever() → runner.run_scheduled_loop()            │
│                    │                                            │
│         ┌──────────┴──────────┐                                  │
│         │  registry           │  JOB_SPECS + _JOB_FACTORIES      │
│         │  iter_registered_jobs│  → RegisteredJob(spec, job)     │
│         └──────────┬──────────┘                                  │
│                    │                                            │
│         per due job: run_job_isolated()                          │
│         ThreadPoolExecutor(1) + timeout + try/except             │
│                    │                                            │
│         ┌──────────┼──────────┬──────────────┐                   │
│         ▼          ▼          ▼              ▼                   │
│    Postgres    Binance REST   CSV files    loguru               │
│    (orders,     (API client)   reports_out/                      │
│     positions)                                                  │
│                    │                                            │
│         optional: run_history → scheduler_runs (audit)         │
└─────────────────────────────────────────────────────────────────┘
```

- **Entry:** `scheduler/__main__.py` delegates to `scheduler/main.py`.
- **Loop:** `scheduler/runner.py` — interval scheduling, **per-job isolation** (one failure does not stop others), **timeouts**, **SIGINT/SIGTERM** shutdown.
- **Catalog:** `scheduler/config.py` defines **`JOB_SPECS`** (micro: cadence, enable flags). `scheduler/registry.py` maps each `job_id` to a **`Job`** factory.
- **Contract:** `scheduler/types.py` — `JobSpec`, `JobContext`, `Job` protocol (`run(ctx)`).

---

## 2. Module map

| Module / path | Role |
|---------------|------|
| `main.py` | Argparse: default long-lived loop; `--list-jobs`, `--dry-run-job ID`, `--once`. |
| `runner.py` | `run_forever`, `run_scheduled_loop`, `run_job_once` (propagate errors), `run_job_isolated` (swallow, log). Deadline math for hourly vs generic intervals. |
| `config.py` | **Micro:** `JOB_SPECS`, timeouts, hourly alignment minute, recon tunables, `scheduler_reports_csv_dir()`. **Macro:** `SchedulerSettings` (`DATABASE_URL`, `REDIS_URL` via pydantic-settings). |
| `registry.py` | `RegisteredJob`, `iter_registered_jobs`, `_JOB_FACTORIES` dict. |
| `types.py` | `JobContext`, `JobSpec` validation, `Job` protocol. |
| `run_history.py` | `record_run_start` / `record_run_end` → table **`scheduler_runs`**. |
| `jobs/reports/` | Report jobs (e.g. position CSV snapshots). |
| `jobs/reconciliation/` | Venue vs internal DB reconciliation jobs. |
| `jobs/misc/` | Non-report jobs (e.g. heartbeat). |
| `tests/` | Unit tests (runner alignment, jobs, etc.). |

---

## 3. Scheduling semantics

### 3.1 What gets a tick?

- Only jobs with **`enabled=True`** and **`interval_seconds > 0`** participate in **`run_scheduled_loop`**.
- If **`cron_expression`** is set but **`interval_seconds`** is missing or zero, the loop **skips** that job and logs a warning (cron execution is not wired in the loop yet; one-off debugging uses **`--dry-run-job`**).

### 3.2 First run vs subsequent

- **First fire** for each job is **immediate** when the loop starts (all jobs due at once).
- **After each completion**, the next deadline is computed from **wall time at completion** (`time.time()`).

### 3.3 Hourly alignment (UTC)

- When **`interval_seconds == SCHEDULER_HOURLY_INTERVAL_SECONDS`** (3600), the next deadline is the **next** UTC time strictly after completion that matches **`SCHEDULER_HOURLY_ALIGN_MINUTE`** (e.g. `:05` → `10:03` → `10:05`, `10:07` → `11:05`).
- This avoids pile-up on wall duration alone and keeps hourly jobs on a **clock grid**, analogous in spirit to epoch grids used in `market_data`.

### 3.4 Non-hourly intervals

- For any other positive `interval_seconds`, the next deadline uses **`floor(completed_at / p) * p + p`** (same epoch-period rule as documented alongside `market_data` ingestion).

### 3.5 Shutdown

- **`install_signal_handlers`** sets **SIGINT** and **SIGTERM** on a **`threading.Event`**; the loop polls **`SCHEDULER_LOOP_POLL_SECONDS`** so shutdown stays responsive while waiting for the next tick.

---

## 4. Execution and isolation

| Function | Use | On failure | `scheduler_runs` |
|----------|-----|------------|-------------------|
| `run_job_isolated` | Main loop, `--once` | Log + exception swallowed; other jobs continue | start/end recorded if DB URL present |
| `run_job_once` | `--dry-run-job` | Exception **propagates** after logging | start/end recorded if DB URL present |

Both paths:

1. Resolve **`database_url`** (explicit or `load_scheduler_settings()`); empty/missing URL **skips** audit writes (job still runs if it does not need DB).
2. **`record_run_start`** (best-effort; warnings on failure).
3. Submit **`job.run(JobContext(job_id=...))`** to **`ThreadPoolExecutor(max_workers=1)`** and **`future.result(timeout=...)`**.
4. **`record_run_end`** with `ok` / `error` (timeout message or `str(e)`); errors truncated to a max length in runner.

Default timeout when spec omits it: **`DEFAULT_JOB_TIMEOUT_SECONDS`** in `config.py`.

---

## 5. Registry and adding a job

1. Add a **`JobSpec`** to **`JOB_SPECS`** in `scheduler/config.py` (`job_id`, `enabled`, `interval_seconds` and/or `cron_expression`, optional `timeout_seconds`).
2. Implement a class with **`job_id`** property and **`run(self, ctx: JobContext) -> None`**.
3. Register **`"your_job_id": lambda: YourJob()`** in **`scheduler/registry.py`** → **`_JOB_FACTORIES`**.

**Disabled specs:** When iterating with **`include_disabled=True`**, the registry yields a **`NoopJob`** placeholder so listing still works without instantiating heavy dependencies.

---

## 6. Configuration: micro vs macro

Per workspace rules (**`.cursor/rules/env-and-config.mdc`**):

- **Micro (code):** job list, enable flags, intervals, alignment minute, recon constants (`ORDER_RECON_*`, `POSITION_RECON_*`), report directory name — all in **`scheduler/config.py`**.
- **Macro (`.env`):** **`DATABASE_URL`**, **`REDIS_URL`** — same names as the rest of the stack (`SchedulerSettings`). Redis is optional today.
- **Secrets / venue:** Binance jobs use OMS-style env (e.g. **`BINANCE_API_KEY`**, **`BINANCE_API_SECRET`**, base URL / testnet) as consumed by **`oms.brokers.binance.api_client`**.

---

## 7. Persistence: `scheduler_runs`

| Column | Purpose |
|--------|---------|
| `id` | Primary key (bigint). |
| `job_id` | Logical job name (matches `JobSpec.job_id`). |
| `started_at` | Set on insert (server default `now()`). |
| `finished_at`, `status` | Set on completion; `status` ∈ `ok`, `error`. |
| `error` | Optional text (failures, timeouts). |
| `payload` | Optional JSONB (API not heavily used in current jobs; available for summaries). |

Migration: `alembic/versions/n5o6p7q8r9s0_add_scheduler_runs.py`. Index: **`(job_id, started_at DESC)`** for recent history per job.

---

## 8. Built-in jobs (current)

| `job_id` | Package | Reads | Writes / side effects |
|----------|---------|-------|------------------------|
| `order_reconciliation_binance` | `jobs/reconciliation/order_recon.py` | Postgres **`orders`**; Binance **`myTrades`** in a lookback window | Summary + diff **CSV** under `scheduler/reports_out/` |
| `position_reconciliation_binance` | `jobs/reconciliation/position_recon.py` | Postgres **`positions`**; Binance spot **account balances** | Summary + diff **CSV** |
| `position_snapshot_hourly` | `jobs/reports/position_snapshot_hourly.py` | Postgres **`positions`** | Four **CSV** roll-ups (by asset, broker, book, granular) |
| `noop_heartbeat` | `jobs/misc/noop_heartbeat.py` | — | **Info** log line per run (operations sanity check) |

Report/recon jobs use **`scheduler_reports_csv_dir()`** (package-relative **`reports_out/`**, root **`.gitignore`**). Filenames include a UTC timestamp (e.g. `*_20260325T1900Z.csv`).

---

## 9. CLI summary

| Flag | Behavior |
|------|----------|
| *(default)* | `run_forever()` until SIGINT/SIGTERM. |
| `--list-jobs` | Log all specs from registry (including disabled). |
| `--dry-run-job ID` | Run **one** enabled job once; **`run_job_once`** — exceptions exit non-zero. |
| `--once` | Every **enabled** job once via **`run_job_isolated`**; failures do not block others. |

---

## 10. Testing

Tests live under **`scheduler/tests/`**, for example:

- **`test_runner_schedule.py`** — UTC hourly alignment and epoch-grid deadlines (`_next_deadline_after_run`, `_next_utc_hourly_at_minute_past`).
- **`test_position_snapshot_hourly.py`**, **`test_order_recon.py`**, **`test_position_recon.py`**, **`test_noop_heartbeat.py`** — job behavior (with mocks/fixtures as appropriate).

Run from repo root with pytest, targeting `scheduler/tests` (see root **`docs/TESTING.md`** if present).

---

## 11. Docker / compose

The scheduler image is built from the **repository root** `Dockerfile` (no separate `scheduler/Dockerfile`). Compose injects **`DATABASE_URL`** / **`REDIS_URL`** like other services. See **`scheduler/README.md`** for typical commands.
