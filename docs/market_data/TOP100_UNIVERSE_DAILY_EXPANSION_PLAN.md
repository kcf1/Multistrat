# Top-100 Universe Daily Expansion Plan

This plan defines a daily universe-refresh pipeline that:

- does **not** remove delisted/dropped coins from historical coverage,
- keeps expanding the universe over time,
- marks whether an asset is in the **current** top 100.

It is designed to seed from the existing static list in `market_data/universe.py`, then fully switch runtime reads to a DB-managed universe table.

---

## 1) Design goals

- Keep historical universe membership permanently (append/retain, no destructive deletes).
- Track current top-100 membership with a direct flag for easy runtime reads.
- Preserve membership and rank state needed for runtime and operational audit.
- Keep runtime ingestion stable when external source/API has transient failures.
- Reuse existing `symbols` reference table at runtime symbol-resolution time.

---

## 2) Source-of-truth decision

- **Primary runtime source:** Postgres universe table.
- **Bootstrap seed source (one-time):** `market_data/universe.py` (`DATA_COLLECTION_BASE_ASSETS`) to initialize DB before runtime cutover.
- **Input source:** CoinMarketCap top 100 daily fetch (with API key).
- **Venue mapping:** resolve venue tradable symbols from `symbols` during runtime reads.

This makes DB the only runtime authority after cutover, while keeping `universe.py` as historical/bootstrap reference.

Architecture contract:

- **Asset universe:** `market_data_universe_assets` stores only base-asset membership/lifecycle state.
- **Data availability/tradability:** resolved by dedicated runtime mappers/checks (for example `symbols` joins and venue-specific eligibility logic), not by the universe table.

---

## 3) Data model (expand over time, never delete)

Use one table:

### 3.1 `market_data_universe_assets` (shared base-asset reference + lifetime flags)

One row per base asset (for example `BTC`).

Suggested fields:

- `base_asset` (PK)
- `source` (for example `cmc_top100`)
- `first_seen_date` (date first entered top 100)
- `last_seen_date` (latest date observed in top 100)
- `current_rank` (int nullable)
- `is_current_top100` (bool)
- `was_ever_top100` (bool, always true once observed)
- `updated_at` (timestamptz)

Rules:

- Never delete rows.
- If asset drops out of top 100, set `is_current_top100=false`, keep row/history.
- If asset re-enters later, set `is_current_top100=true`, update `current_rank`, `last_seen_date`.
- Venue/pair tradability is not stored in this table; resolve it from `symbols` at read/use time.

---

## 4) Daily update job flow

1. Fetch CMC top 100 for `as_of_date = today (UTC)`.
2. Normalize ticker/base values (`strip().upper()`), drop obvious invalids.
3. Upsert `market_data_universe_assets` for daily top-100 base assets:
   - set `is_current_top100=true`
   - set `current_rank`
   - set `last_seen_date=today`
   - initialize `first_seen_date` if new
   - detect newly added base assets (`first_seen_date=today` / not previously present)
4. Mark previously current assets not seen today as:
   - `is_current_top100=false`
   - `current_rank=NULL`
   - keep all other historical fields
5. After each **universe refresh** scheduler tick, each **dataset** independently runs its own initial-backfill pass: resolve tradable symbols from the universe + `oms.symbols`, find series keys missing ``initial_backfill_done`` on **that dataset's cursor table**, run ``ingest_*_series(use_watermark=False, skip_existing_when_no_watermark=True)`` over that dataset's configured horizon, then mark ``initial_backfill_done`` on those cursor rows. No cross-dataset orchestrator; disabling a dataset is a code/config change in that job only.
6. Emit summary logs: requested=100, active_current_count, dropped_today_count, newly_added_count, backfill_started_count, cumulative_ever_count.

Failure policy:

- If CMC fetch fails, skip mutation for that day and keep prior state unchanged.
- Do not clear `is_current_top100` on source/API failure; only update state on successful daily run.

---

## 5) Runtime consumption

Add a runtime resolver helper (for market-data jobs) that:
- reads base assets from `market_data_universe_assets`, then
- resolves venue tradable symbols via dedicated data-availability/tradability logic (for example `symbols` table join/filter rules).

Recommended read modes:

- `current_only`: assets with `is_current_top100=true`
- `ever_seen`: all assets with `was_ever_top100=true` (expanding historical universe)

Given requirement "do not remove delisted coins, just expand table along time", default ingestion mode should be `ever_seen` once migration is complete.

---

## 6) Integration points to update

- Add Alembic migration for `market_data_universe_assets` + indexes.
- Add storage helpers in `market_data/storage.py` for:
  - upsert assets state rows
  - query current/ever base-asset sets (universe only)
  - update membership/visibility fields from daily top-100 input
- Add Pydantic schema models for universe contracts:
  - universe asset row/state payloads (asset-only),
  - daily updater input/output payloads,
  - runtime symbol-resolution results,
  - PMS admission-filter decisions (tradable + pricing-resolvable).
- Add dedicated symbol-resolution helper(s) for data jobs (separate from universe storage contract).
- Add daily updater job in `market_data/jobs/` (or scheduler if preferred).
- Wire daily updater cadence in `market_data/main.py` scheduler loop.
- Wire **per-dataset** initial backfill functions (in each ``ingest_*.py``) on the same cadence as universe refresh in ``market_data/main.py`` (each dataset owns idempotency via ``initial_backfill_done`` on its cursor table; ``market_data.universe_symbol_backfills`` removed).
- Update market-data ingest/correct/repair jobs to read runtime-resolved symbols (from base assets + separate tradability resolution), not only import-time constants.
- PMS wiring (phase-split):
  - keep current startup initialization flow (`init_assets_stables` + `sync_assets_from_symbols`) for safe cutover.
  - add follow-up periodic PMS pull from universe to refresh `assets` / `usd_symbol` mappings without requiring restart.
  - enforce PMS symbol filter on writes to `assets`: only keep rows that are both tradable (eligible symbol mapping exists) and pricing-resolvable (supported price source path exists).

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

1. Add migration for universe table and indexes. ✅
2. Implement storage helpers and updater job. ✅ (A2 core pieces)
3. Run one-time seed from `market_data/universe.py` into DB universe table. ✅ (script added)
4. Run one-time CMC upsert to reconcile seeded universe with latest top-100 before read switch. ✅ (script added)
5. Add Alembic migrations: ``initial_backfill_done`` / ``initial_backfill_at`` on each dataset cursor table; drop ``universe_symbol_backfills``. ✅
6. Implement per-dataset ``run_backfill_new_symbols_*`` in each ingest module + storage query/mark helpers. ✅
7. Run updater manually to validate initial daily refresh behavior.
8. Verify per-dataset backfills complete cursor flags for newly resolved symbols without duplicate work on subsequent ticks.

### Phase B - read switch

- Add runtime universe resolver.
- Switch market-data ingest/correct/repair jobs to runtime resolver (wiring-only rollout; behavior remains consistent).
- No static fallback: market-data symbol set is DB-driven (fail loudly if universe/symbols mapping is unavailable).

### Phase C - PMS staged adoption

- Keep PMS startup initialization wiring as-is for initial rollout.
- Add gated periodic PMS universe pull to upsert missing `assets` rows / `usd_symbol` mappings after startup.
- Apply PMS admission filter so only tradable + pricing-resolvable symbols are retained/active in PMS asset feed scope.

---

## 9) Testing plan

### Unit tests

- CMC payload normalization and base-asset extraction.
- State transitions:
  - new member enters top 100,
  - member drops out (flag false, row retained),
  - member re-enters.
- Idempotent daily rerun for same `as_of_date`.

### Storage tests

- `market_data_universe_assets` upsert behavior.
- Query helpers for `current_only` and `ever_seen`.

### Job-level tests

- successful daily refresh path,
- API failure path (no destructive state change),
- partial invalid/unknown base assets path (skip invalid, continue).
- newly added base-asset path triggers backfill once,
- repeated daily run does not re-trigger duplicate initial backfill for already-known symbols.
- tradability resolution tests (asset exists in universe but has no tradable symbol => handled gracefully by data jobs).
- per-dataset backfill tests: each ``run_backfill_new_symbols_*`` skips symbols already marked ``initial_backfill_done`` on that dataset's cursor table; scheduler runs all five on the universe-refresh tick.
- idempotency test: already-backfilled symbol is not re-enqueued/re-run on subsequent daily cycles unless policy explicitly allows.
- PMS compatibility tests: startup-only initialization path still works before periodic universe pull is enabled.
- PMS filter tests:
  - universe asset with no tradable symbol is excluded from PMS `assets` feed scope,
  - tradable symbol without pricing path is excluded or marked inactive for PMS feed,
  - asset becomes eligible later (symbol/pricing available) and is admitted on next periodic pull.

### Integration/smoke tests

- run one-time seed + one-time CMC upsert + daily update + ingest once; verify base-asset set expansion over days.
- verify dropped coins remain in DB and are still returned in `ever_seen` mode.
- verify startup/run failure is explicit if DB universe is not seeded.

---

## 10) Concrete to-do checklist

- [x] [A1] Add Alembic migration for `market_data_universe_assets`.
- [x] [A2] Add storage/query helpers in `market_data/storage.py`.
- [x] [A2] Add Pydantic schema models for universe updater/resolver/filter contracts.
- [x] [A2] Implement CMC top-100 fetch client and parser.
- [x] [A2] Implement daily universe updater job.
- [x] [A3] Implement one-time seed job from `market_data/universe.py` into DB universe table.
- [x] [A4] Implement one-time CMC bootstrap upsert before runtime read switch.
- [x] [A5] Add cursor-table ``initial_backfill_done`` / ``initial_backfill_at`` migrations + drop ``universe_symbol_backfills``.
- [x] [A5b] Per-dataset ``run_backfill_new_symbols_*`` in each ingest module + storage query/mark helpers.
- [x] [A6] Wire universe refresh + per-dataset backfills into `market_data/main.py` scheduler loop.
- [ ] [A6] Run updater manually to validate initial daily refresh behavior.
- [x] [A7] Verify per-dataset initial backfills complete without duplicate work for already-flagged symbols.
- [x] [B8] Implement runtime universe resolver (DB-only; no static fallback).
- [x] [B9] Unwire market_data runtime from import-time static symbol constants.
- [x] [B9] Switch ingest/correct/repair jobs to runtime universe resolver.
- [x] [B10] No static fallback (DB universe is required).
- [x] [C11] Keep PMS startup initialization (`init_assets_stables` + `sync_assets_from_symbols`) for rollout compatibility.
- [x] [C12] Add gated periodic PMS universe pull to refresh `assets` / `usd_symbol` mappings after startup.
- [x] [C13] Add PMS admission filter (tradable + pricing-resolvable) before writing/updating PMS `assets` feed scope.
- [x] [T] Add unit/storage/job/integration tests listed above.
- [x] [T] Update `market_data/README.md` with operating notes and failure behavior.

