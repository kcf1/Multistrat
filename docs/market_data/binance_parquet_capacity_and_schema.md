# Binance Postgres Capacity and Schema (Hourly Sampling)

This note defines a practical Postgres-first dataset design and yearly capacity estimate for Binance market data. Parquet can still be used as a cold archive tier.

## Scope and assumptions

- Universe: 50 symbols
- Sampling cadence: hourly (24 samples/day)
- Horizon: 365 days
- Primary storage: Postgres
- Datasets:
  - `orderbook_snapshot_l2`
  - `agg_trades_hourly_window`
  - `funding_rate`
  - `open_interest`

## Capacity summary (hourly, 50 symbols)

### 1) Order book snapshots (L2 depth 500, long format)

- Rows/year: `500 * 2 * 50 * 24 * 365 = 438,000,000`
- Estimated size/year (table only): ~30-70 GB
- Estimated size/year (with indexes + TOAST/WAL overhead budget): ~60-120 GB

### 2) Aggregated trades (500 trades per symbol per hourly sample)

- Rows/year: `500 * 50 * 24 * 365 = 219,000,000`
- Estimated size/year (table only): ~12-35 GB
- Estimated size/year (with indexes + overhead budget): ~25-70 GB

### 3) Funding rate

- Rows/year (hourly pull): `50 * 24 * 365 = 438,000`
- Estimated size/year: <0.5 GB (including indexes/overhead)

### 4) Open interest

- Rows/year (hourly): `50 * 24 * 365 = 438,000`
- Estimated size/year: <0.5 GB (including indexes/overhead)

### Total expected footprint

- Postgres total (practical): ~85-190 GB/year
- Safer planning budget (retries, duplicates before cleanup, schema/index changes): ~220 GB/year

## Recommended schemas (Postgres)

## `orderbook_snapshot_l2`

Purpose: deterministic L2 state per sampling timestamp.

Columns:
- `exchange` TEXT (`binance`)
- `market_type` TEXT (`spot` or `perp`)
- `symbol` TEXT
- `snapshot_ts` TIMESTAMPTZ (UTC sample time)
- `event_ts` TIMESTAMPTZ NULL (source event time)
- `side` TEXT (`bid` or `ask`)
- `level` SMALLINT (1..500)
- `price` NUMERIC(20,10)
- `qty` NUMERIC(28,12)
- `notional` NUMERIC(28,12) NULL (`price * qty`, optional precompute)
- `ingest_ts` TIMESTAMPTZ
- `source` TEXT (`rest_depth` or `ws_recon`)

Logical uniqueness key:
- `(exchange, market_type, symbol, snapshot_ts, side, level)`

Recommended partitioning/indexes:
- Range partition by `snapshot_ts` (monthly)
- Unique index on logical key
- Secondary index on `(symbol, snapshot_ts DESC)` for recent lookups

## `agg_trades_hourly_window`

Purpose: hourly microstructure sample window.

Columns:
- `exchange` TEXT
- `market_type` TEXT
- `symbol` TEXT
- `sample_ts` TIMESTAMPTZ (UTC hour sample time)
- `trade_id` BIGINT (agg trade id)
- `trade_ts` TIMESTAMPTZ
- `price` NUMERIC(20,10)
- `qty` NUMERIC(28,12)
- `notional` NUMERIC(28,12)
- `is_buyer_maker` BOOLEAN
- `ingest_ts` TIMESTAMPTZ

Logical uniqueness key:
- `(exchange, market_type, symbol, trade_id)`

Recommended partitioning/indexes:
- Range partition by `sample_ts` (monthly)
- Unique index on logical key
- Secondary index on `(symbol, trade_ts DESC)`

## `funding_rate`

Purpose: perp carry signal history.

Columns:
- `exchange` TEXT
- `symbol` TEXT
- `funding_time` TIMESTAMPTZ (effective funding time)
- `funding_rate` NUMERIC(18,10)
- `mark_price` NUMERIC(20,10) NULL
- `sample_ts` TIMESTAMPTZ (collection time)
- `ingest_ts` TIMESTAMPTZ

Logical uniqueness key:
- `(exchange, symbol, funding_time)`

Recommended partitioning/indexes:
- Range partition by `funding_time` (quarterly or yearly)
- Unique index on logical key

## `open_interest`

Purpose: leverage and crowding state over time.

Columns:
- `exchange` TEXT
- `symbol` TEXT
- `sample_ts` TIMESTAMPTZ
- `open_interest_qty` NUMERIC(28,12)
- `open_interest_value_usd` NUMERIC(28,12) NULL
- `price_ref` NUMERIC(20,10) NULL
- `ingest_ts` TIMESTAMPTZ

Logical uniqueness key:
- `(exchange, symbol, sample_ts)`

Recommended partitioning/indexes:
- Range partition by `sample_ts` (quarterly or yearly)
- Unique index on logical key

## Postgres operational settings (recommended)

- Ingest in batches (`COPY` or batched inserts), avoid single-row insert loops
- Keep only essential indexes for write-heavy tables
- Tune autovacuum for large partitions
- Use monthly partitions for high-volume datasets and drop old partitions for retention
- Prefer UTC `TIMESTAMPTZ` everywhere

## Optional archive tier (Postgres + Parquet)

- Keep hot data in Postgres (for example 90-180 days)
- Periodically export older partitions to Parquet (Snappy/ZSTD)
- Drop archived Postgres partitions after verification
- This hybrid model keeps query speed for recent data while reducing long-term cost

## Data quality checks (recommended)

- Daily row-count check per symbol and dataset
- Duplicate key rate by logical uniqueness key
- Null-rate check for required fields
- Freshness check: max `sample_ts` lag
- Coverage check: expected symbols present each sampling hour
