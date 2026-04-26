# Plan: Deploy Phase 1 strategies (`factor_ls`), backfill, and a central runner

This plan covers **deploying** `strategies_daily` population (`factor_ls`), **backfill** options, a **central runner** for multiple strategies, a **strategy registry**, and how **data refresh** fits each step. It aligns with [.cursor/rules/env-and-config.mdc](../../.cursor/rules/env-and-config.mdc) (DB URLs in `.env`; tuning in code) and the existing **scheduler** pattern in [`scheduler/registry.py`](../../scheduler/registry.py).

---

## 0. Backfill `strategies_daily`

You have two families of approaches; both can land on the **same** persisted tables and PKs `(bar_ts, symbol)`.

### 0.A Simulate daily production runs (iterative)

**Idea:** For each calendar day (or each batch of 24 `bar_ts` keys), call the **same** code path as production: `run_pipeline` with an explicit `open_time_ge` / `open_time_lt` window (and optional `bar_ts_ge` / `bar_ts_le`) so intraday extract + aggregation + L1 + signals + labels + persist match live behavior.

**Pros**

- Maximum **parity** with production: identical ordering, coverage checks, and edge cases per day.
- Easier to **debug** (“which day broke?”) and to compare row counts day by day.

**Cons**

- **Slower:** repeated OHLCV reads and Python work over overlapping windows unless you optimize (e.g. widen each SQL read once per chunk, then slice `bar_ts` in memory—still one “logical” run per persist batch).
- More **DB load** if you naïvely re-query the full warmup window for every single day.

**Mitigation**

- Backfill in **chunks** (e.g. 30–90 calendar days per process): one `open_time_ge` / `open_time_lt` spanning chunk + warmup, then `run_pipeline(..., bar_ts_ge=..., bar_ts_le=...)` to persist only the chunk’s daily keys (already supported).
- Run chunks **sequentially** on one writer to avoid upsert races, or partition by non-overlapping `bar_ts` ranges if you parallelize.

### 0.B Single (or few) bulk passes (“direct calculated numbers”)

**Idea:** Load a large `open_time` range once, aggregate to daily bars once, compute L1 / signals / labels in pandas in one or a few vectorized passes, then **bulk upsert** all `(bar_ts, symbol)` rows.

**Pros**

- **Faster** for long history: one or few passes over OHLCV and daily frames.
- Less repeated SQL round-trips.

**Cons**

- **Two code paths** unless bulk is implemented *inside* `factor_ls` as an alternate entrypoint that still calls the same pure functions—otherwise drift vs production is likely.
- Harder to reproduce **per-day** production quirks (e.g. `MIN_SYMBOL_COVERAGE` fail-fast day by day) unless you explicitly re-apply those checks per `bar_ts`.

**Recommendation**

- **Default:** **0.A chunked** using existing `run_pipeline` + explicit windows (lowest risk).
- **Optional later:** add a **`run_backfill_bulk`** in `factor_ls` that reuses the **same** transform functions as `run_pipeline` but one read + one groupby path, then one persist—**no duplicate math** in a separate script.

### 0.C Backfill checklist (deployment order)

1. **Migrate:** `alembic upgrade head` on target DB.
2. **Dry run:** `python scripts/run_factor_ls.py --no-persist --open-time-ge ... --open-time-lt ...` on a short window; inspect row counts and a sample join.
3. **Choose window:** e.g. first `bar_ts` with trustworthy OHLCV through “yesterday” UTC.
4. **Execute chunks:** loop or script invoking `run_pipeline` with non-overlapping or rolling windows; log `pipeline_version` (e.g. `backfill-2026-04-26-chunk-3`).
5. **Validate:** SQL checks—row counts per table per `bar_ts`, null rates, duplicate PK count = 0, spot join `l1feats_daily` → `factors_daily` → `xsecs_daily`.
6. **Cut to production schedule:** after backfill, switch to the **central runner** (below) for incremental daily (or 24-bar) updates only.

---

## 1. Central runner for “deploying strategies”

### 1.1 Is one central runner a good idea for multiple strategies?

**Yes, as a single orchestration process** (one container or one systemd unit) that **dispatches** registered strategies by `strategy_id` / job id—**if** each strategy is **short-lived** (minutes), **sequential** execution is acceptable, and you want **one** place for logging, metrics, and DB URL handling.

**Caveats**

- **Independent scaling:** If strategy A is heavy and B is light, a naive single thread makes B wait. Mitigations: **per-strategy subprocess** from the runner, **async queue + workers**, or **separate scheduler entries** (`factor_ls_daily`, `trend_daily`) that still share a small **library** registry but not one OS process.
- **Blast radius:** One uncaught exception can kill the whole loop unless you **isolate** each run (`try/except` + exit code per job) or use subprocess per job.
- **Versioning:** Runner should log `strategy_id`, git SHA or `PIPELINE_VERSION`, and DB host—enough to audit what ran.

### 1.2 Recommended shape in *this* repo

Reuse the existing **scheduler** model:

- **`JobSpec`** rows in [`scheduler/config.py`](../../scheduler/config.py) (`job_id`, `cron` / interval, `enabled`).
- **`_JOB_FACTORIES`** in [`scheduler/registry.py`](../../scheduler/registry.py) mapping `job_id` → `Job` implementation.

Add a job such as **`strategy_factor_ls_daily`** whose `Job.run()` invokes `run_pipeline(...)` with production defaults (or reads last run watermark from `scheduler_runs` / a small state table).

**Alternative:** a thin **`strategies/runner/main.py`** that only loops over registered callables—useful if you want **zero** coupling to `scheduler` tables; still keep **one** registry module so both CLI and scheduler call the same functions.

**Multi-strategy deployment**

| Pattern | When to use |
|--------|-------------|
| **One scheduler job per strategy** | Different cadences, isolation, clear ops ownership. |
| **One meta-job “run_all_strategies”** | Same cadence, strict order, shared DB connection budget. |
| **Queue + workers** | High volume or long runtimes; out of scope until needed. |

**Recommendation:** Start with **one job per strategy** in `scheduler` (`factor_ls_daily`, later `model_daily_infer`, etc.) plus a **shared registry** (§2) so registration is not duplicated.

---

## 2. Strategy module registry

### 2.1 Contract (minimal)

Each registered strategy exposes:

- **`strategy_id: str`** — stable key (e.g. `factor_ls_daily`).
- **`run(context) -> None`** (or structured result) — `context` carries DB URL, optional date override, logger, metrics sink.
- **`describe() -> dict`** — human-readable metadata (version, tables written, schedule hint).

Optional:

- **`backfill_chunks(start, end) -> Iterator[...]`** — if you implement chunked backfill helpers per strategy.

### 2.2 Registration mechanisms (pick one)

1. **Explicit registry dict** (same spirit as `_JOB_FACTORIES`):  
   `STRATEGY_RUNNERS: dict[str, Callable[..., None]] = {"factor_ls_daily": run_factor_ls_production, ...}`  
   **Pros:** obvious, grep-friendly. **Cons:** edit central file when adding a strategy.

2. **Entry points (`pyproject.toml` / setuptools)** — plugins discover themselves. **Pros:** true multi-package. **Cons:** heavier packaging for internal modules.

3. **`pkgutil.walk_packages` under `strategies.modules.*`** — auto-discover modules that define `register()`. **Pros:** less boilerplate. **Cons:** magic, harder to reason about load order.

**Recommendation:** **(1) explicit registry** in `strategies/runner/registry.py` (or `strategies/modules/_runner_registry.py`) imported by `scheduler/registry.py` job factories—clear and sufficient until you have many third-party plugins.

### 2.3 Wiring to scheduler

- Add `StrategyFactorLsDailyJob` implementing `Job` with `job_id="strategy_factor_ls_daily"`.
- Factory: `lambda: StrategyFactorLsDailyJob()`.
- Register in `_JOB_FACTORIES` and add matching `JobSpec` in `JOB_SPECS` with the desired schedule (e.g. align **00:00 UTC** with your ops).

---

## 3. Data job: “each step” vs “same pandas-style pipeline”

### 3.1 What you have today

`factor_ls` is already a **pandas-style monolith per run**: load → daily agg → L1 → pre → factors → xsec → labels → persist in one Python pipeline. There are **no** separate SQL materialization steps per stage in the database.

### 3.2 Options

| Approach | Description | Fits |
|----------|-------------|------|
| **A. Keep monolithic pandas pipeline** (current) | One `run_pipeline` per schedule or chunk; upsert at end (and optionally per stage if you later split transactions). | **Daily refresh**, backfill chunks, parity with research. |
| **B. SQL-first / dbt per stage** | Each stage writes intermediate tables or views in DB. | Large scale, SQL-only ownership, less Python RAM—**large refactor** of Phase 1. |
| **C. Hybrid** | Read OHLCV in SQL; minimal pandas for transforms; or persist only final tables. | Incremental optimization without full rewrite. |

**Recommendation for first deploy:** **A** — keep the **same pandas pipeline** for both backfill and daily runs. Reasons: already implemented, deterministic, tests target it. Add **observability** (duration per stage, rows written) inside `pipeline.py` or the job wrapper—not new storage tiers.

### 3.3 When to revisit

- If memory blows on full-universe × long history: move to **chunked backfill (0.A)** or **bulk path (0.B)** with shared functions—not SQL rewrite first.
- If ops demands DB-only replay: introduce **staging tables** or **dbt** *after* parity is proven and cost is measured.

---

## 4. Suggested implementation order (concrete)

1. **Backfill script** (optional wrapper): CLI args `chunk-days`, `start`, `end`; loop calling `run_pipeline` with explicit windows; idempotent upserts already in place.
2. **`strategies/runner/registry.py`** + **`run_registered(strategy_id, **kwargs)`** thin API.
3. **`scheduler/jobs/strategies/factor_ls_daily.py`** implementing `Job`, calling registry / `run_pipeline` with production defaults.
4. **`scheduler/config.py` + `scheduler/registry.py`** entries + **docs** in `docs/scheduler/` one paragraph.
5. **Docker / compose** (optional): one-shot service or `cron` sidecar that runs the same command as the scheduler would—only if you do not use the main `scheduler` loop in prod.

---

## 5. Tests (for this deployment slice)

- **Registry:** unknown `strategy_id` raises; known id invokes callable (mocked).
- **Job:** `StrategyFactorLsDailyJob.run` with `DATABASE_URL` unset or mocked DB skips or uses test double per existing scheduler tests style.
- **Backfill wrapper:** dry-run two adjacent chunks; assert no duplicate PK and second chunk updates overlap `bar_ts` idempotently.

---

## 6. Open decisions (capture before coding)

- **Single DB** for market data + strategies vs split URLs (`STRATEGIES_PIPELINE_DATABASE_URL`) in prod.
- **Parallelism:** sequential chunks only until proven needed.
- **Failure policy:** fail one day vs continue rest of chunk (runner should log and optionally alert).

---

## 7. Related docs

- Phase 1 spec: [PHASE1_DETAILED_PLAN.md](PHASE1_DETAILED_PLAN.md)
- Blueprint phases: [TRADING_MODULE_BLUEPRINT.md](TRADING_MODULE_BLUEPRINT.md) (Phase 4 mentions scheduler—this plan narrows **strategies** jobs earlier)
- Existing scheduler pattern: [`scheduler/registry.py`](../../scheduler/registry.py)
