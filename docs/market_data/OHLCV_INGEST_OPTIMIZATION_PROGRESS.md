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

## Current Findings

| # | Finding | Evidence |
|---:|---|---|
| 1 | HTTP GET is the primary bottleneck in the fetch pipeline. | Run C: `http_get_s` avg 0.326s vs `json_decode_s` avg 0.000s, `parse_s` avg 0.005s, `validate_s` avg 0.001s. |
| 2 | Cursor reads are negligible. | `cursor_read_s` p50 ~0.002s. |
| 3 | Upsert execution is small in this micro-window benchmark. | `upsert_s` avg 0.002s, max 0.004s. |
| 4 | Commit cost is noticeable and variable. | `commit_s` p50 0.041s, p95 0.050s, max 0.070s. |
| 5 | End-to-end all-symbol write pass remains sub-20s in this forced 1-interval setup. | Run B total wall clock 19.615s for 51 symbols. |
| 6 | In larger windows, HTTP still dominates while parse/validate remain comparatively small. | Run C: `http_get_s` p50 0.320s vs `parse_s` p50 0.005s and `validate_s` p50 0.000s. |

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

