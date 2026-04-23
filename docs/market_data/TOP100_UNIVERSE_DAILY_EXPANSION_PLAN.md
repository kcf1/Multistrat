# Top-100 Universe Daily Expansion Plan

This plan defines a daily universe-refresh pipeline that:

- does **not** remove delisted/dropped coins from historical coverage,
- keeps expanding the universe over time,
- marks whether an asset is in the **current** top 100.

It is designed to seed from the existing static list in `market_data/universe.py`, then fully switch runtime reads to DB-managed universe tables.

---

## 1) Design goals

- Keep historical universe membership permanently (append/retain, no destructive deletes).
- Track current top-100 membership with a direct flag for easy runtime reads.
- Preserve ranking history for audit/research.
- Keep runtime ingestion stable when external source/API has transient failures.
- Reuse existing `symbols` reference table for venue-valid symbol filtering.

---

## 2) Source-of-truth decision

- **Primary runtime source:** Postgres universe tables.
- **Bootstrap seed source (one-time):** `market_data/universe.py` (`DATA_COLLECTION_BASE_ASSETS`) to initialize DB before runtime cutover.
- **Input source:** CoinMarketCap top 100 daily fetch (with API key).
- **Venue filter:** intersect candidates with Binance spot `USDT` symbols from `symbols` table.

This makes DB the only runtime authority after cutover, while keeping `universe.py` as historical/bootstrap reference.

---

## 3) Data model (expand over time, never delete)

Use two tables:

### 3.1 `market_data_universe_assets` (current-state + lifetime flags)

One row per base asset (for example `BTC`).

Suggested fields:

- `base_asset` (PK)
- `quote_asset` (default `USDT`)
- `symbol` (`BASEUSDT`)
- `source` (for example `cmc_top100`)
- `first_seen_date` (date first entered top 100)
- `last_seen_date` (latest date observed in top 100)
- `current_rank` (int nullable)
- `is_current_top100` (bool)
- `was_ever_top100` (bool, always true once observed)
- `listed_at` (timestamptz nullable; first time we confirmed venue-listing and started tracking)
- `delisted_at` (timestamptz nullable; set when symbol is no longer venue-listed/eligible)
- `updated_at` (timestamptz)

Rules:

- Never delete rows.
- If asset drops out of top 100, set `is_current_top100=false`, keep row/history.
- If asset re-enters later, set `is_current_top100=true`, update `current_rank`, `last_seen_date`.
- Set `listed_at` once (first confirmed venue-listed state); do not overwrite on later runs.
- Set `delisted_at` only when venue-listing check confirms symbol is no longer tradable.
- If venue relists later, clear `delisted_at` and keep original `listed_at` for lineage.

### 3.2 `market_data_universe_daily` (append-only membership snapshots)

One row per `(as_of_date, base_asset, source)`.

Suggested fields:

- `as_of_date` (date)
- `base_asset`
- `rank` (1..100)
- `symbol` (`BASEUSDT`)
- `source` (`cmc_top100`)
- `is_in_top100` (bool, normally true for stored rows)
- `inserted_at` (timestamptz)

Constraints:

- Unique: `(as_of_date, base_asset, source)`.
- Append-only by date; no back-deletes.

---

## 4) Daily update job flow

1. Fetch CMC top 100 for `as_of_date = today (UTC)`.
2. Normalize ticker/base values (`strip().upper()`), drop obvious invalids.
3. Build candidate Binance spot symbols (`BASEUSDT`).
4. Filter to venue-valid pairs using `symbols` table (`quote_asset='USDT'`, broker `binance`).
5. Upsert `market_data_universe_assets` for all valid daily members:
   - set `is_current_top100=true`
   - set `current_rank`
   - set `last_seen_date=today`
   - initialize `first_seen_date` if new
   - detect newly added symbols (`first_seen_date=today` / not previously present)
6. Mark previously current assets not seen today as:
   - `is_current_top100=false`
   - `current_rank=NULL`
   - keep all other historical fields
7. Insert daily snapshot rows into `market_data_universe_daily` (idempotent upsert/do-nothing on conflict).
8. Trigger initial backfill for newly added symbols across enabled datasets (OHLCV, basis, open interest, top-trader, taker buy/sell), with per-symbol retry and give-up logging.
9. Emit summary logs: requested=100, venue_valid_count, active_current_count, dropped_today_count, newly_added_count, backfill_started_count, cumulative_ever_count.

Failure policy:

- If CMC fetch fails, skip mutation for that day and keep prior state unchanged.
- Do not clear `is_current_top100` on source/API failure; only update state on successful daily run.

---

## 5) Runtime consumption

Add a runtime resolver helper (for market-data jobs) that reads from DB universe tables only.

Recommended read modes:

- `current_only`: assets with `is_current_top100=true`
- `ever_seen`: all assets with `was_ever_top100=true` (expanding historical universe)

Given requirement "do not remove delisted coins, just expand table along time", default ingestion mode should be `ever_seen` once migration is complete.

---

## 6) Integration points to update

- Add Alembic migration(s) for the two universe tables + indexes.
- Add storage helpers in `market_data/storage.py` for:
  - upsert assets state rows
  - append/upsert daily rows
  - query current/ever symbol sets
- Add daily updater job in `market_data/jobs/` (or scheduler if preferred).
- Wire daily updater cadence in `market_data/main.py` scheduler loop.
- Add "new-symbol backfill trigger" helper that runs right after a successful daily refresh.
- Update ingest jobs to read symbols from runtime resolver, not only import-time constants.
- Optional: update PMS feed universe to consume DB runtime set (if desired in same phase).

---

## 7) Configuration

Macro env vars:

- `MARKET_DATA_CMC_API_KEY`
- `MARKET_DATA_CMC_BASE_URL` (optional override)

Micro constants (`market_data/config.py`):

- daily refresh cadence (for example 86400s)
- runtime universe mode (`ever_seen` vs `current_only`)
- top-N size (`100`)

---

## 8) Rollout plan

### Phase A - schema + writer

1. Add migration for universe tables and indexes.
2. Implement storage helpers and updater job.
3. Implement/verify immediate backfill trigger for newly added symbols.
4. Run updater manually to populate initial dataset.

### Phase B - read switch

5. Add runtime universe resolver.
6. Switch market-data ingest/correct/repair jobs to runtime resolver.
7. Keep static universe fallback enabled.

### Phase C - optional PMS adoption

8. Move PMS asset feed universe read to same DB resolver (optional, gated).

---

## 9) Testing plan

### Unit tests

- CMC payload normalization and symbol mapping.
- Venue filtering via `symbols` table rules.
- State transitions:
  - new member enters top 100,
  - member drops out (flag false, row retained),
  - member re-enters.
- Idempotent daily rerun for same `as_of_date`.

### Storage tests

- `market_data_universe_assets` upsert behavior.
- `market_data_universe_daily` uniqueness by date/asset/source.
- Query helpers for `current_only` and `ever_seen`.

### Job-level tests

- successful daily refresh path,
- API failure path (no destructive state change),
- partial invalid symbols path (skip invalid, continue).
- newly added symbol path triggers backfill once,
- repeated daily run does not re-trigger duplicate initial backfill for already-known symbols.

### Integration/smoke tests

- run one-time seed + daily update + ingest once; verify symbol set expansion over days.
- verify dropped coins remain in DB and are still returned in `ever_seen` mode.
- verify startup/run failure is explicit if DB universe is not seeded.

---

## 10) Concrete to-do checklist

- [ ] Add Alembic migration for `market_data_universe_assets`.
- [ ] Add Alembic migration for `market_data_universe_daily`.
- [ ] Add storage/query helpers in `market_data/storage.py`.
- [ ] Implement CMC top-100 fetch client and parser.
- [ ] Implement daily universe updater job.
- [ ] Wire updater into `market_data/main.py` scheduler loop.
- [ ] Implement one-time seed job from `market_data/universe.py` into DB tables.
- [ ] Implement runtime universe resolver (DB-only; no static fallback).
- [ ] Unwire market_data runtime from import-time static symbol constants.
- [ ] Switch ingest/correct/repair jobs to runtime universe resolver.
- [ ] Add unit/storage/job/integration tests listed above.
- [ ] Update `market_data/README.md` with operating notes and failure behavior.

