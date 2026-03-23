# Market data (Phase 4)

REST **Binance spot** klines + **Binance perps basis** → Postgres (**`ohlcv`**, **`basis_rate`**).
Symbols/pairs, intervals/periods, and scheduler cadence (`OHLCV_SCHEDULER_*`, `BASIS_SCHEDULER_*`)
are code constants in [`config.py`](config.py). Postgres URL and optional REST bases come from env.

## Run

**Scheduled service** runs OHLCV and basis jobs (ingest + correct_window on an interval; optional gap repair).
**First run** of each job is **immediate** on startup; **later** runs use **UTC-aligned** period boundaries
(e.g. default ingest every 300 s at **:00, :05, :10, ... UTC**).

```bash
python -m market_data.main
```

**One shot** (OHLCV + basis ingest/correct_window, then exit):

```bash
python -m market_data.main --once
```

Add **`--with-repair`** with `--once` to run one **policy-window** gap detect+repair pass for both datasets.

**Docker** (same image as OMS/PMS/Risk):

```bash
docker compose up -d market_data
```

Requires **Postgres** healthy and migrations applied (`ohlcv`, `ingestion_cursor`). Redis is **not** required for this tranche.

**Docker:** If `.env` sets `MARKET_DATA_DATABASE_URL` to `localhost`, override it for the container (see `docker-compose.yml` `market_data.environment`) so the service uses hostname **`postgres`**, not the container’s own loopback.

**Large OHLCV backfill** (`tqdm`, optional `--no-watermark` / `--skip-existing`):

```bash
python scripts/backfill_ohlcv.py
```

**Large basis backfill** (`tqdm`, optional `--no-watermark` / `--skip-existing`):

```bash
python scripts/backfill_basis_rate.py
```

Notes for basis endpoint behavior:

- Binance basis data is source-retention limited (about 30 days for `period=1h`).
- Some configured pairs may return `HTTP 400 Invalid pair`; those series are skipped and reported.

## Environment

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Postgres (fallback) |
| `MARKET_DATA_DATABASE_URL` | Overrides `DATABASE_URL` when set |
| `MARKET_DATA_BINANCE_BASE_URL` | Public REST base (e.g. testnet) |
| `MARKET_DATA_BINANCE_PERPS_BASE_URL` | Perps REST base for basis/funding datasets |

Scheduler timing is **not** env — edit `OHLCV_SCHEDULER_*` and `BASIS_SCHEDULER_*`
constants in [`config.py`](config.py).

See [docs/PHASE4_DETAILED_PLAN.md](../docs/PHASE4_DETAILED_PLAN.md) §9 and [docs/oms/BINANCE_API_RULES.md](../docs/oms/BINANCE_API_RULES.md).

## Shutdown

The scheduler exits on **SIGINT** or **SIGTERM** (e.g. `docker compose stop market_data`).
