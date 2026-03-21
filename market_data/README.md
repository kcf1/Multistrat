# Market data (Phase 4)

REST **Binance spot** klines → Postgres **`ohlcv`**. Symbols, bar intervals, and **scheduler cadence** (`OHLCV_SCHEDULER_*`) are **code constants** in [`config.py`](config.py). Postgres URL and optional REST base come from **env** (see below).

## Run

**Scheduled service** (ingest + correct_window on an interval; optional gap repair). **First run** of each job is **immediate** on startup; **later** runs use **UTC-aligned** period boundaries (e.g. default ingest every 300 s at **:00, :05, :10, … UTC**).

```bash
python -m market_data.main
```

**One shot** (ingest + correct_window, then exit):

```bash
python -m market_data.main --once
```

Add **`--with-repair`** with `--once` to run one **policy-window** gap detect+repair pass (same window as cold-start backfill length).

**Docker** (same image as OMS/PMS/Risk):

```bash
docker compose up -d market_data
```

Requires **Postgres** healthy and migrations applied (`ohlcv`, `ingestion_cursor`). Redis is **not** required for this tranche.

**Large backfill** ( tqdm, optional `--no-watermark` / `--skip-existing`):

```bash
python scripts/backfill_ohlcv.py
```

## Environment

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Postgres (fallback) |
| `MARKET_DATA_DATABASE_URL` | Overrides `DATABASE_URL` when set |
| `MARKET_DATA_BINANCE_BASE_URL` | Public REST base (e.g. testnet) |

Scheduler timing is **not** env — edit `OHLCV_SCHEDULER_INGEST_INTERVAL_SECONDS`, `OHLCV_SCHEDULER_CORRECT_WINDOW_INTERVAL_SECONDS`, and `OHLCV_SCHEDULER_REPAIR_GAP_INTERVAL_SECONDS` in [`config.py`](config.py).

See [docs/PHASE4_DETAILED_PLAN.md](../docs/PHASE4_DETAILED_PLAN.md) §9 and [docs/BINANCE_API_RULES.md](../docs/BINANCE_API_RULES.md).

## Shutdown

The scheduler exits on **SIGINT** or **SIGTERM** (e.g. `docker compose stop market_data`).
