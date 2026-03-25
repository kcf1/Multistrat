# Scheduler (Phase 5)

Batch jobs: reports, reconciliation, misc. Runs as **`python -m scheduler`** (long-lived loop) or one-shot CLI modes.

## Docker (recommended with stack)

The image is built from the **repository root** `Dockerfile` (same layer as OMS/PMS/market_data); there is no separate `scheduler/Dockerfile`.

```bash
# From repo root — requires Postgres + Redis (e.g. compose stack)
docker compose up -d postgres redis
docker compose build scheduler
docker compose up scheduler
```

Compose wires **`DATABASE_URL`** and **`REDIS_URL`** to the `postgres` and `redis` services. Copy **`.env.example`** → **`.env`** and set **`BINANCE_API_KEY` / `BINANCE_API_SECRET`** if you run Binance recon jobs.

## Environment (macro)

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Required for jobs that query Postgres (snapshots, recons, `scheduler_runs`). |
| `REDIS_URL` | Optional today; same contract as other services. |
| `BINANCE_*` | Order/position recon against Binance (`BINANCE_BASE_URL`, `BINANCE_TESTNET`, keys). |

Micro tuning (intervals, job toggles) lives in **`scheduler/config.py`** per workspace rules.

## Local / debugging (no Docker)

```bash
# Repo root; venv with requirements installed
set DATABASE_URL=postgresql://...
python -m scheduler --list-jobs
python -m scheduler --dry-run-job position_snapshot_hourly
python -m scheduler --dry-run-job order_reconciliation_binance
python -m scheduler --once
python -m scheduler
```

**`--dry-run-job`** runs one enabled job once and exits (exceptions propagate). **`--once`** runs every enabled job with isolation (failures do not stop the rest). **`--list-jobs`** prints specs including disabled entries.

## Cadence

Hourly jobs align to **UTC** **`:MM`** past the hour after the first run (`SCHEDULER_HOURLY_ALIGN_MINUTE` in **`scheduler/config.py`**). See **`scheduler/runner.py`**.

## Outputs

CSV exports default to **`scheduler/reports_out/`** (gitignored). Mount a volume if you need files on the host from Docker.
