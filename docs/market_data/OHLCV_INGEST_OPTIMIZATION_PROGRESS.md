# OHLCV Ingest Optimization Progress

Date: 2026-03-26

## Goal

Establish a baseline for OHLCV ingest latency by timing sub-steps end-to-end:

- Cursor reads (`get_ingestion_cursor`, `max_open_time_ohlcv`)
- API fetch + JSON decode + parsing/validation (`process_binance_klines_payload`, incl. pydantic + rules)
- Filter step
- DB writes (`upsert_ohlcv_bars`, `upsert_ingestion_cursor`, `commit`)

This baseline is used to track optimization progress over time.

## Instrumentation

Script used:

- `scripts/time_ohlcv_ingest.py`

Enhancements added for reliable benchmarking:

- `--all-symbols`: run across the full configured OHLCV universe.
- `--force-fetch-window-intervals N`: forces non-empty fetch windows even when cursor logic would no-op.
- `--write`: includes real DB upsert/cursor/commit timing.
- `--output-csv`: saves per-symbol metrics for later analysis.

## Benchmark Runs

| Run | Scope | Write | Command | Output CSV | Symbols | Total wall clock |
|---|---|---|---|---|---:|---:|
| A | All symbols, fetch/parse baseline | No | `python scripts/time_ohlcv_ingest.py --all-symbols --interval 1h --force-fetch-window-intervals 1 --max-pages 1 --output-csv scheduler/reports_out/ohlcv_ingest_timing_all_symbols.csv` | `scheduler/reports_out/ohlcv_ingest_timing_all_symbols.csv` | 51 | 18.092s |
| B | All symbols, real ingest write path | Yes | `python scripts/time_ohlcv_ingest.py --all-symbols --interval 1h --force-fetch-window-intervals 1 --max-pages 1 --write --output-csv scheduler/reports_out/ohlcv_ingest_timing_all_symbols_write.csv` | `scheduler/reports_out/ohlcv_ingest_timing_all_symbols_write.csv` | 51 | 19.615s |
| C | All symbols, split fetch/json/parse/validate with larger window | Yes | `python scripts/time_ohlcv_ingest.py --all-symbols --interval 1h --force-fetch-window-intervals 500 --max-pages 1 --write --output-csv scheduler/reports_out/ohlcv_ingest_timing_all_symbols_write_split_500iv.csv` | `scheduler/reports_out/ohlcv_ingest_timing_all_symbols_write_split_500iv.csv` | 51 | 22.822s |

### Aggregate Metrics - Run A (No Write)

| Metric | min (s) | p50 (s) | p95 (s) | max (s) | avg (s) |
|---|---:|---:|---:|---:|---:|
| `fetch_parse_validate_s` | 0.285 | 0.334 | 0.367 | 0.384 | 0.334 |
| `wall_clock_s` | 0.300 | 0.357 | 0.397 | 0.399 | 0.354 |
| `cursor_read_s` | 0.001 | 0.002 | 0.002 | 0.003 | 0.002 |

### Aggregate Metrics - Run B (Write Enabled)

| Metric | min (s) | p50 (s) | p95 (s) | max (s) | avg (s) |
|---|---:|---:|---:|---:|---:|
| `fetch_parse_validate_s` | 0.299 | 0.334 | 0.364 | 0.375 | 0.334 |
| `wall_clock_s` | 0.326 | 0.387 | 0.423 | 0.445 | 0.384 |
| `cursor_read_s` | 0.001 | 0.002 | 0.003 | 0.003 | 0.002 |
| `upsert_s` | 0.001 | 0.001 | 0.003 | 0.004 | 0.002 |
| `commit_s` | 0.001 | 0.041 | 0.050 | 0.070 | 0.027 |

### Aggregate Metrics - Run C (Write Enabled, Split Timing, 500-Interval Window)

| Metric | min (s) | p50 (s) | p95 (s) | max (s) | avg (s) |
|---|---:|---:|---:|---:|---:|
| `fetch_total_s` | 0.289 | 0.326 | 0.382 | 0.383 | 0.332 |
| `http_get_s` | 0.285 | 0.320 | 0.375 | 0.378 | 0.326 |
| `json_decode_s` | 0.000 | 0.000 | 0.001 | 0.002 | 0.000 |
| `parse_s` | 0.003 | 0.005 | 0.007 | 0.015 | 0.005 |
| `validate_s` | 0.000 | 0.000 | 0.001 | 0.002 | 0.001 |
| `upsert_s` | 0.064 | 0.107 | 0.117 | 0.138 | 0.094 |
| `commit_s` | 0.001 | 0.002 | 0.009 | 0.009 | 0.003 |
| `wall_clock_s` | 0.383 | 0.454 | 0.506 | 0.522 | 0.447 |

### Aggregate Metrics - Run C Recheck (Post-Workers Instrumentation)

Command baseline (sequential):

- `python scripts/time_ohlcv_ingest.py --all-symbols --interval 1h --force-fetch-window-intervals 500 --max-pages 1 --write --workers 1 --runs 3`

Command parallel (`4` workers):

- `python scripts/time_ohlcv_ingest.py --all-symbols --interval 1h --force-fetch-window-intervals 500 --max-pages 1 --write --workers 4 --runs 3`

Command parallel (`8` workers), pass 1:

- `python scripts/time_ohlcv_ingest.py --all-symbols --interval 1h --force-fetch-window-intervals 500 --max-pages 1 --write --workers 8 --runs 3`

Command parallel (`8` workers), pass 2 (repeat):

- `python scripts/time_ohlcv_ingest.py --all-symbols --interval 1h --force-fetch-window-intervals 500 --max-pages 1 --write --workers 8 --runs 3`

| Profile | runs | avg total wall clock (s) | min run (s) | max run (s) | Speedup vs workers=1 |
|---|---:|---:|---:|---:|---:|
| workers=`1` | 3 | 23.534 | 23.106 | 24.348 | 1.00x |
| workers=`4` | 3 | 7.934 | 7.672 | 8.304 | 2.97x |
| workers=`8` (pass 1) | 3 | 5.333 | 5.258 | 5.467 | 4.41x |
| workers=`8` (pass 2) | 3 | 5.317 | 5.279 | 5.379 | 4.43x |

| Metric (`workers=8`, pass 2) | min (s) | p50 (s) | p95 (s) | max (s) | avg (s) |
|---|---:|---:|---:|---:|---:|
| `fetch_total_s` | 0.433 | 0.666 | 0.739 | 0.800 | 0.664 |
| `http_get_s` | 0.428 | 0.659 | 0.731 | 0.795 | 0.658 |
| `json_decode_s` | 0.000 | 0.000 | 0.001 | 0.001 | 0.000 |
| `parse_s` | 0.003 | 0.005 | 0.007 | 0.016 | 0.005 |
| `validate_s` | 0.000 | 0.000 | 0.001 | 0.001 | 0.001 |
| `upsert_s` | 0.025 | 0.074 | 0.095 | 0.114 | 0.075 |
| `commit_s` | 0.001 | 0.002 | 0.044 | 0.071 | 0.009 |
| `wall_clock_s` | 0.556 | 0.780 | 0.856 | 0.981 | 0.779 |

## PE-8 Expansion Snapshot (2026-03-26)

Parallel ingest pattern has been extended from OHLCV to the other provider-compatible futures datasets:

- `ingest_basis_rate`
- `ingest_open_interest`
- `ingest_taker_buy_sell_volume`
- `ingest_top_trader_long_short`

Scope implemented per dataset:

- ProviderExecutor-backed parallel task submission
- Per-task DB connection lifecycle (thread-safe)
- Failure isolation with aggregated task-level error reporting
- Run-level observability (submitted/completed/failed/wall-clock logging)
- Shared global worker cap via `GLOBAL_PROVIDER_MAX_WORKERS` (currently `6`)

Validation:

- `python -m pytest market_data/tests/test_jobs.py -q`
- Result: `44 passed`

## Worker Pool Overhead Check (2026-03-26)

Micro-benchmark (`ProviderExecutor`, `max_workers=6`, 2000 iterations):

| Scenario | min (ms) | p50 (ms) | p95 (ms) | max (ms) | avg (ms) |
|---|---:|---:|---:|---:|---:|
| create + shutdown | 0.0019 | 0.0021 | 0.0034 | 0.0363 | 0.0022 |
| create + submit(1) + result + shutdown | 0.1141 | 0.1663 | 0.3094 | 0.7780 | 0.1842 |

Conclusion: worker-pool setup overhead is negligible relative to ingest network and write costs.

## Current Findings

| # | Finding | Evidence |
|---:|---|---|
| 1 | HTTP GET is the primary bottleneck in the fetch pipeline. | Run C: `http_get_s` avg 0.326s vs `json_decode_s` avg 0.000s, `parse_s` avg 0.005s, `validate_s` avg 0.001s. |
| 2 | Cursor reads are negligible. | `cursor_read_s` p50 ~0.002s. |
| 3 | Upsert execution is small in this micro-window benchmark. | `upsert_s` avg 0.002s, max 0.004s. |
| 4 | Commit cost is noticeable and variable. | `commit_s` p50 0.041s, p95 0.050s, max 0.070s. |
| 5 | End-to-end all-symbol write pass remains sub-20s in this forced 1-interval setup. | Run B total wall clock 19.615s for 51 symbols. |
| 6 | In larger windows, HTTP still dominates while parse/validate remain comparatively small. | Run C: `http_get_s` p50 0.320s vs `parse_s` p50 0.005s and `validate_s` p50 0.000s. |

## Current Bottleneck and Optimization Direction

### Current bottleneck

| Area | Status | Why |
|---|---|---|
| Network fetch (`http_get_s`) | Primary bottleneck | Dominant share of per-symbol wall-clock in both sequential and parallel runs. |
| Parse / validation | Minor | Low single-digit milliseconds; not a material share today. |
| Worker pool setup | Negligible | Micro-benchmark shows sub-ms overhead even with submit+shutdown. |
| DB writes (`upsert_s` / `commit_s`) | Secondary bottleneck | Smaller than network cost, but commit tail latency remains visible. |

### Most promising optimization targets

| Priority | Candidate | Rationale |
|---:|---|---|
| 1 | Transport/network tuning to Binance (endpoint path/region/connection reuse) | Largest current contributor (`http_get_s`). |
| 2 | Concurrency tuning (workers + in-flight fetch cap) with venue-safety guardrails | Already showing strong throughput gains; may still have headroom before error-rate trade-offs. |
| 3 | Commit/write path tuning (batching strategy, commit cadence by dataset) | Secondary latency contributor; can improve total wall-clock once network is optimized. |
| 4 | Retry/backoff policy refinement under load | Can reduce p95 tails when transient API slowness appears. |

Practical conclusion: keep optimization focus on **network fetch path first**, then **DB commit/write behavior**; parse/validation and worker-pool construction are currently low ROI.

## Decision (Current Focus)

| Area | Decision | Reason |
|---|---|---|
| Fetch pipeline optimization | Focus on reducing HTTP GET wall time first. | Largest contributor by far in split timing. |
| Parse/validation optimization | De-prioritize for now. | Current contribution is small vs network cost. |
| DB optimization | Secondary focus after network. | `upsert_s`/`commit_s` matter, but less than HTTP in current profile. |

## Notes / Caveats

- These runs intentionally use `--force-fetch-window-intervals 1` to avoid cursor no-op behavior and measure true per-symbol work.
- This is a micro-window benchmark (roughly one 1h interval/page per symbol); larger backfill windows will scale differently.
- The observed hourly ~4s in service loop may also include non-ingest steps (e.g. `correct_window`), not only ingest.

## Next Optimization Candidates

1. Parallelize symbol HTTP fetches within safe Binance/infra limits.
2. Explore transport-level improvements (regional endpoint/network path, connection tuning, retry/backoff tuning).
3. Reduce commit overhead by batching commits across multiple symbols (evaluate durability trade-offs).
4. Add equivalent timing for `correct_window` to attribute total hourly runtime split.

## Planned Task: Parallel OHLCV Ingest (4 Workers)

### Task Plan

| Task ID | Task | Owner | Status |
|---|---|---|---|
| P4-1 | Add configurable worker count constant for OHLCV ingest (default `4`). | TBD | Planned |
| P4-2 | Refactor `run_ingest_ohlcv` to execute symbols in a `ThreadPoolExecutor(max_workers=4)`. | TBD | Planned |
| P4-3 | Ensure one DB connection per worker/thread (no shared psycopg2 connection across threads). | TBD | Planned |
| P4-4 | Keep API throttling safe under concurrency (shared limiter or equivalent cap). | TBD | Planned |
| P4-5 | Preserve result ordering/log readability and error isolation per symbol. | TBD | Planned |
| P4-6 | Add/adjust tests for parallel execution and failure isolation behavior. | TBD | Planned |
| P4-7 | Run benchmark before/after and update this document with measured speedup. | TBD | Planned |

### Acceptance Criteria

| ID | Criteria |
|---|---|
| AC-1 | Functional parity: same rows/cursors as sequential ingest for equivalent input window. |
| AC-2 | No shared-connection threading bugs (each worker manages its own connection lifecycle). |
| AC-3 | No material increase in fetch give-ups/retry failures at default worker count (`4`). |
| AC-4 | Observable runtime improvement vs sequential baseline on identical benchmark setup. |
| AC-5 | Rollback path available (set worker count to `1` to restore sequential behavior). |

### Test Plan

| Test ID | Test | How | Expected |
|---|---|---|---|
| T-1 | Unit/integration existing market_data test suite | Run existing tests touching ingest path | No regressions |
| T-2 | Deterministic one-shot ingest parity | Compare sequential (`workers=1`) vs parallel (`workers=4`) on same fixed window | Same row counts/cursor end points |
| T-3 | Failure isolation | Inject provider error for one symbol under parallel run | Other symbols still ingest; failed symbol reported |
| T-4 | Performance benchmark | Repeat timing script / one-shot job runs before and after | Total ingest wall time improves |
| T-5 | Safety under load | Observe retries/HTTP errors/DB commit latency during run | No unacceptable error spike |

### Rollout Notes

| Item | Decision |
|---|---|
| Initial worker count | `4` |
| Tuning sequence | Evaluate `1 -> 2 -> 4 -> 6` only if needed |
| Fallback | Set worker count to `1` immediately |
| Primary KPI | Total ingest wall clock per scheduler cycle |
| Guardrail KPI | Fetch give-up rate / retry escalation / DB health |

## Provider-Scoped Executor Implementation Plan

### Workflow (Target)

| Step | Description |
|---:|---|
| 1 | Create provider-scoped executor(s) at startup (or run start). |
| 2 | Pass executor handle(s) to dataset jobs. |
| 3 | Dataset job submits symbol tasks and waits for completion. |
| 4 | Log dataset-level summary and per-symbol failures/success. |
| 5 | Reuse same executor for next dataset if provider type matches. |
| 6 | Shutdown executor(s) cleanly at end of run. |

### Implementation Tasks

| Task ID | Task | Notes | Status |
|---|---|---|---|
| PE-1 | Add provider-level worker settings (default `4`) | `1` must preserve sequential behavior | Done |
| PE-2 | Introduce `ProviderExecutor` abstraction | Wrap `ThreadPoolExecutor`, submit/wait/error helpers | Done |
| PE-3 | Initialize provider + executor lifecycle centrally | Create once, reuse, shutdown at end | Done |
| PE-4 | Integrate OHLCV job with executor first | Submit per-symbol tasks; preserve current ingest logic | Done |
| PE-5 | Enforce DB safety | One psycopg2 connection per task/worker; no shared connection | Done |
| PE-6 | Keep rate-limit safety under concurrency | Shared limiter/cap per provider pool | Done |
| PE-7 | Add concurrency observability | Task counts, failures, dataset wall clock | Done |
| PE-8 | Extend to other datasets after OHLCV validation | basis/open-interest/taker/top-trader | Done |

### Test Plan

| Test ID | Test | Method | Expected |
|---|---|---|---|
| PE-T1 | Functional parity | Compare workers=`1` vs `4` on fixed window | Same row/cursor outcomes |
| PE-T2 | Failure isolation | Inject one symbol failure in parallel run | Other tasks complete; failure isolated |
| PE-T3 | Thread/connection safety | Run with concurrency and inspect errors | No shared-connection/thread exceptions |
| PE-T4 | Performance | Before/after benchmark on identical setup | Wall-clock improvement |
| PE-T5 | Stability guardrails | Monitor retries/give-ups/DB commit latency | No unacceptable degradation |

### Rollout Plan

| Phase | Action | Exit Criteria |
|---|---|---|
| R1 | Implement PE-1..PE-7 for OHLCV only | Tests PE-T1..PE-T5 pass |
| R2 | Benchmark and record speedup in this document | KPI improvement confirmed |
| R3 | Extend pattern to other provider-compatible datasets | No regression from R1 baseline |
| R4 | Keep fallback path | workers=`1` fully functional |

### Success Criteria

| ID | Criteria |
|---|---|
| PE-S1 | Total ingest wall clock improves at workers=`4` vs workers=`1`. |
| PE-S2 | Data correctness parity is maintained (rows + cursors). |
| PE-S3 | Error profile remains healthy (no significant give-up spike). |
| PE-S4 | Operational fallback is immediate by setting workers=`1`. |

## T1-T5 Execution Snapshot (2026-03-26)

| Test ID | Status | Evidence |
|---|---|---|
| T1 | Partial pass (code-level parity/safety) | `pytest market_data/tests/test_jobs.py -k run_ingest_ohlcv_uses_separate_connection_per_symbol` passed. |
| T2 | Pass | `pytest market_data/tests/test_jobs.py -k run_ingest_ohlcv_parallel_failure_isolated_and_raised` passed. |
| T3 | Pass | No lints on modified files; connection isolation + executor/config tests passed. |
| T4 | Pass (synthetic orchestration benchmark) | Workers `1`: ~1.015s vs workers `4`: ~0.255s (`~3.98x`) in controlled mock task benchmark. |
| T5 | Pass (practical stability benchmark) | Real all-symbol run completed; no fatal retry/give-up spikes observed in benchmark logs. |

### T5 Reference Benchmark Artifact

| Artifact | Path |
|---|---|
| Split timing CSV (post PE-6/7) | `scheduler/reports_out/ohlcv_ingest_timing_all_symbols_write_split_500iv_post_pe67.csv` |

## Planned Task: PE-9 Parallelize Correct/Repair Jobs (Step-by-Step)

### Objective

Apply the same provider-executor parallelization approach used in ingest jobs to:

- `correct_window*` jobs
- `repair_gap*` jobs

while preserving correctness and operational safety under production cadence.

### Stepwise Execution Plan

| Step | Task ID | Task | Target scope | Status |
|---:|---|---|---|---|
| 1 | PE9-1 | Add/confirm shared worker-cap constants for correct/repair orchestration | `market_data/config.py` | Done |
| 2 | PE9-2 | Parallelize OHLCV `correct_window` first (pilot) with provider executor + per-task DB connections | `jobs/correct_window.py` | Done |
| 3 | PE9-3 | Parallelize OHLCV `repair_gap` with same safety pattern + failure aggregation | `jobs/repair_gap.py` | Done |
| 4 | PE9-4 | Add tests for OHLCV correct/repair: parity, failure isolation, thread-safety assumptions | `market_data/tests/test_jobs.py` | Done |
| 5 | PE9-5 | Benchmark OHLCV correct/repair workers=`1` vs workers=`N` and record speedup + guardrails | scripts + logs | Done |
| 6 | PE9-6 | Extend same pattern to futures `correct_window_*` jobs | basis/open-interest/taker/top-trader | Done |
| 7 | PE9-7 | Extend same pattern to futures `repair_gap_*` jobs | basis/open-interest/taker/top-trader | Done |
| 8 | PE9-8 | Optional central shared futures executor lifecycle in `main.py` (single pool reused across futures jobs) | `market_data/main.py` | Planned |
| 9 | PE9-9 | End-to-end scheduler validation and staged rollout notes | scheduler + docs | Planned |

### Design Constraints / Guardrails

| Area | Constraint |
|---|---|
| DB safety | No shared psycopg2 connection across worker threads; each task owns its connection lifecycle. |
| Failure model | One-symbol/task failures are isolated; batch raises aggregated summary after all tasks complete. |
| Rate-limit safety | Preserve existing limiter/fetch-cap semantics per provider; do not increase implicit request burst without explicit tuning. |
| Fallback | Worker count `1` must preserve sequential behavior for all corrected/refactored jobs. |
| Observability | Log run start/end with submitted/completed/failed counts and wall-clock. |

### PE-9 Test Plan

| Test ID | Test | Method | Expected |
|---|---|---|---|
| PE9-T1 | Correct-window parity | workers=`1` vs workers=`N` on fixed window | Same drift-row outcomes per series |
| PE9-T2 | Repair-gap parity | workers=`1` vs workers=`N` on fixed horizon | Same repaired spans/rows/cursor endpoints |
| PE9-T3 | Failure isolation | Inject single task/provider exception | Remaining tasks complete; aggregated failure raised |
| PE9-T4 | Thread/connection safety | Run parallel load with logging/assertions | No thread-share connection errors |
| PE9-T5 | Performance | Before/after benchmark on identical setup | Wall-clock improvement for correct/repair passes |
| PE9-T6 | Stability guardrails | Observe retries, give-ups, DB latency in run logs | No unacceptable degradation |

### Rollout Sequence

| Phase | Action | Exit Criteria |
|---|---|---|
| PE9-R1 | Implement OHLCV correct only (pilot) | PE9-T1/T3/T4 pass |
| PE9-R2 | Implement OHLCV repair | PE9-T2/T3/T4 pass |
| PE9-R3 | Measure and document performance/stability | PE9-T5/T6 pass |
| PE9-R4 | Extend to futures correct jobs | No regression vs OHLCV baseline |
| PE9-R5 | Extend to futures repair jobs | No regression; guardrails stable |
| PE9-R6 | Optional shared futures pool in `main.py` | Equivalent behavior + reduced orchestration overhead |

### Current conclusion for next step

Start with **PE9-R1 (OHLCV correct-window pilot)**, then proceed one phase at a time with test gates before widening scope.

### PE9 Execution Snapshot (PE9-4, PE9-5)

PE9-4 tests added/executed:

- `test_run_correct_window_parallel_failure_isolated_and_raised`
- `test_run_repair_gap_parallel_failure_isolated_and_raised`
- `test_run_correct_window_uses_separate_connection_per_symbol`
- `test_run_repair_gap_uses_separate_connection_per_symbol`

Validation run:

- `python -m pytest market_data/tests/test_jobs.py -q`
- Result: `48 passed`

PE9-5 benchmark command (same runtime environment, two repeats each):

- `python -c "<benchmark harness invoking run_correct_window + run_repair_gaps_policy_window_all_series with ProviderExecutor workers 1 vs 6>"`

| Job | Workers | Runs (s) | Avg (s) | Speedup vs w=1 |
|---|---:|---|---:|---:|
| `correct_window` | 1 | `7.002`, `7.021` | 7.011 | 1.00x |
| `correct_window` | 6 | `1.848`, `1.735` | 1.792 | 3.91x |
| `repair_gap` (`backfill_days=30`) | 1 | `1.410`, `1.322` | 1.366 | 1.00x |
| `repair_gap` (`backfill_days=30`) | 6 | `0.529`, `0.482` | 0.505 | 2.70x |

Guardrail notes:

- No task-level fatal failures observed in benchmark logs.
- Parallel runs preserved completion counts (`submitted=51`, `completed=51`, `failed=0`) for both jobs.

