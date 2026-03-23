# Market data (Phase 4)

REST **Binance spot** klines + **Binance perps basis** + **Binance perps open interest** → Postgres
(**`ohlcv`**, **`basis_rate`**, **`open_interest`**).
Symbols/pairs, intervals/periods, and scheduler cadence (`OHLCV_SCHEDULER_*`, `BASIS_SCHEDULER_*`,
`OPEN_INTEREST_SCHEDULER_*`) are code constants in [`config.py`](config.py). Postgres URL and optional REST bases come from env.

## Run

**Scheduled service** runs OHLCV, basis, and open-interest jobs (ingest + correct_window on an interval; optional gap repair per dataset).
**First run** of each job is **immediate** on startup; **later** runs use **UTC-aligned** period boundaries
(e.g. default ingest every 300 s at **:00, :05, :10, ... UTC**).

```bash
python -m market_data.main
```

**One shot** (OHLCV + basis + open-interest ingest/correct_window, then exit):

```bash
python -m market_data.main --once
```

Add **`--with-repair`** with `--once` to run one **policy-window** gap detect+repair pass for OHLCV, basis, and open interest.

**Docker** (same image as OMS/PMS/Risk):

```bash
docker compose up -d market_data
```

Requires **Postgres** healthy and migrations applied (`ohlcv`, `basis_rate`, `open_interest`, cursor tables). Redis is **not** required for this tranche.

**Docker:** If `.env` sets `MARKET_DATA_DATABASE_URL` to `localhost`, override it for the container (see `docker-compose.yml` `market_data.environment`) so the service uses hostname **`postgres`**, not the container’s own loopback.

**Large OHLCV backfill** (`tqdm`, optional `--no-watermark` / `--skip-existing`):

```bash
python scripts/backfill_ohlcv.py
```

**Large basis backfill** (`tqdm`, optional `--no-watermark` / `--skip-existing`):

```bash
python scripts/backfill_basis_rate.py
```

**Large open-interest backfill** (`tqdm`, optional `--no-watermark` / `--skip-existing`):

```bash
python scripts/backfill_open_interest.py
```

Notes for perps history endpoints (basis + open interest, `period=1h`):

- Binance retention for these endpoints is on the order of **~30 days**, but we request **`BINANCE_FUTURES_LIMITED_RETENTION_BACKFILL_DAYS` (27)** as a **buffer** so the oldest `startTime` stays inside the valid window (see comment in `config.py`). Legacy alias: `BINANCE_FUTURES_30D_DATASET_BACKFILL_DAYS`.
- Some symbols may return `HTTP 400` (invalid pair, invalid `startTime` before listing, etc.); those series are skipped and logged in provider give-ups.

## Environment

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Postgres (fallback) |
| `MARKET_DATA_DATABASE_URL` | Overrides `DATABASE_URL` when set |
| `MARKET_DATA_BINANCE_BASE_URL` | Public REST base (e.g. testnet) |
| `MARKET_DATA_BINANCE_PERPS_BASE_URL` | Perps REST base for basis / funding / open-interest datasets |

Scheduler timing is **not** env — edit `OHLCV_SCHEDULER_*`, `BASIS_SCHEDULER_*`, and `OPEN_INTEREST_SCHEDULER_*`
constants in [`config.py`](config.py).

See [docs/PHASE4_DETAILED_PLAN.md](../docs/PHASE4_DETAILED_PLAN.md) §9, [docs/market_data/STANDARD_DATA_PIPELINE.md](../docs/market_data/STANDARD_DATA_PIPELINE.md), [docs/market_data/OPEN_INTEREST_IMPLEMENTATION_PLAN.md](../docs/market_data/OPEN_INTEREST_IMPLEMENTATION_PLAN.md), and [docs/oms/BINANCE_API_RULES.md](../docs/oms/BINANCE_API_RULES.md).

## Shutdown

The scheduler exits on **SIGINT** or **SIGTERM** (e.g. `docker compose stop market_data`).
