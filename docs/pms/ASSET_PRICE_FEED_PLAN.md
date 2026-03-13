# Asset price feed: scalable, multi-source (Binance first)

**Goal:** Ingest USD prices per asset from one or more sources, write to a single place that PMS enrichment reads. Design is **source-agnostic** and **scalable** (add Binance first, then other providers); no PMS change when adding a new source.

**Principles:**
- Price feeds are **market data**, not part of the OMS broker adapter. Owned by PMS or a dedicated market-data process.
- **Single read path for PMS:** enrichment continues to read `assets.usd_price` (and optional `usd_symbol`). No change to `enrich_positions_with_usd_from_assets`.
- **Multiple writers:** each source can push prices for a subset of assets; resolution (which source wins per asset) is config-driven.

---

## 1. Architecture overview

```
┌─────────────────┐  ┌─────────────────┐     ┌──────────────────┐
│ Binance feed    │  │ Other feed       │ ... │ Resolution /      │
│ (REST or WS)    │  │ (e.g. CoinGecko) │     │ single writer     │
└────────┬────────┘  └────────┬─────────┘     └────────┬─────────┘
         │                    │                        │
         └────────────────────┴────────────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │ assets table      │  (usd_price, optional price_source)
                    │ (or price cache)  │
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │ PMS enrichment    │  query_assets_usd_config()
                    │ (unchanged)       │  → position.usd_price
                    └──────────────────┘
```

- **Providers:** Implement a common interface; each returns `asset → usd_price` (or symbol → price for conversion).
- **Storage:** One of two approaches (see §2).
- **Resolution:** If multiple sources can update the same asset, define priority (e.g. Binance first, fallback others) or per-asset source mapping.

---

## 2. Storage options

### Option A: Single table `assets` (current + optional column)

- **Current:** `assets(asset, usd_symbol, usd_price, updated_at)`.
- **Add (optional):** `price_source` TEXT NULL — e.g. `'binance'`, `'manual'`, `'coingecko'`. For audit and “who last updated.”
- **Writes:** Each feed updates only rows for assets it owns (e.g. config: “binance” → [BTC, ETH, BNB, …]). No conflict if asset–source mapping is disjoint. If two sources can update the same asset, use a **resolution job** (e.g. run every N seconds: for each asset, pick price from highest-priority source that has a value) and write that into `assets.usd_price`.
- **Pros:** No new table; PMS already reads `assets`.  
- **Cons:** Resolution logic needed if multiple sources per asset.

### Option B: Multi-source table + resolution into `assets`

- **New table:** `asset_prices(source, asset, usd_price, updated_at)` with unique `(source, asset)`. Each feed writes only its own `source`.
- **Resolution:** A small job (or on-read view) merges into “effective” price per asset (e.g. by source priority from config) and writes to `assets.usd_price` (or PMS reads from a view that does the merge).
- **Pros:** Full history per source; easy to add/remove sources; priority/fallback in one place.  
- **Cons:** Extra table and job/view.

**Recommendation for “Binance first, then scalable”:** Start with **Option A**: keep writing into `assets.usd_price`; add optional `price_source` for audit. Each provider has a **config-driven list of assets** it is responsible for (so no overlap, or explicit priority later). When you add a second source, introduce a **resolution step** (in-process or tiny job) that, per asset, picks one source (e.g. by priority) and writes `assets.usd_price`. If you later need per-source history or more complex rules, add Option B (asset_prices + resolution job).

**Chosen: Option A.** Single table `assets`; add `price_source` for audit. Each feed updates only assets it owns (config-driven). Resolution step when we add a second source later.

---

## 3. Implementation plan (Option A, Binance first)

Ordered tasks; each can be a PR or commit.

### 3.1 Schema

| Step | Task | Details |
|------|------|---------|
| 1 | Migration: add `price_source` to `assets` | New Alembic revision. Add column `price_source` TEXT NULL to `assets`. Downgrade drops it. |

### 3.2 Provider interface and Binance implementation

| Step | Task | Details |
|------|------|---------|
| 2 | Create `pms/asset_price_providers/` package | Add `__init__.py`, export interface and Binance provider. |
| 3 | Define `AssetPriceProvider` interface | New file `pms/asset_price_providers/interface.py`. Abstract base (ABC) or Protocol with: `get_prices(assets: List[str]) -> Dict[str, Optional[float]]`. Docstring: asset → USD price; missing/error → omit or None. |
| 4 | Implement `BinanceAssetPriceProvider` | New file `pms/asset_price_providers/binance.py`. Constructor: `base_url`, `timeout`, optional `quote_asset` (default `"USDT"`). For each asset, build symbol = `assets.usd_symbol` or `{asset}{quote}` (e.g. BTCUSDT). Call `GET /api/v3/ticker/price` (single request for all symbols or one per symbol to avoid huge URL). Parse response; return `Dict[asset, float]`. Handle network/parse errors (log, return partial dict or re-raise). Reuse patterns from `pms/mark_price.py` (requests, timeout). |
| 5 | Unit tests for Binance provider | New file `pms/tests/test_asset_price_providers.py`. Test: empty assets → empty dict; mock GET response → correct asset→price; invalid response → error or partial; symbol mapping (BTC→BTCUSDT, etc.). |

### 3.3 Feed: read assets to update, call provider, write to DB

| Step | Task | Details |
|------|------|---------|
| 6 | Query assets that this source should update | New function in `pms/reads.py` or `pms/asset_price_feed.py`: e.g. `query_assets_for_price_source(pg_connect, source: str) -> List[str]`. Returns list of `asset` where we want this source to write. Simple: from config list, or query `assets` WHERE `usd_symbol IS NOT NULL`. |
| 7 | Write prices to `assets` | New function `update_asset_prices(pg_connect, source: str, prices: Dict[str, float]) -> int` in `pms/asset_price_feed.py`. UPDATE `assets` SET `usd_price = %s`, `updated_at = now()`, `price_source = %s` WHERE `asset = %s` for each (asset, price). Batch or executemany. Return count updated. |
| 8 | Run one feed step | New function `run_asset_price_feed_step(pg_connect, provider: AssetPriceProvider, source: str, assets: List[str]) -> int`. Get `prices = provider.get_prices(assets)`; call `update_asset_prices(pg_connect, source, {k: v for k, v in prices.items() if v is not None})`; return count. |

### 3.4 Config

| Step | Task | Details |
|------|------|---------|
| 9 | Add config fields | In `pms/config.py`: `pms_asset_price_source: str = ""` (e.g. `"binance"` or `""` to disable). `pms_asset_price_interval_seconds: float = 60.0`. `binance_price_feed_base_url: Optional[str] = None`. `pms_asset_price_assets: Optional[str] = None` (comma-separated; if empty, derive from DB). |
| 10 | Provider factory / registry | New file `pms/asset_price_providers/registry.py`: `get_asset_price_provider(name: str, **kwargs) -> Optional[AssetPriceProvider]`. For `"binance"` return `BinanceAssetPriceProvider(...)` using config; for `""` or unknown return None. |

### 3.5 Wire into PMS process

| Step | Task | Details |
|------|------|---------|
| 11 | Integrate feed into PMS loop | In `pms/main.py`: if `pms_asset_price_source` is set, before or after `run_one_tick`, call `run_asset_price_feed_step` with provider from registry and asset list (from config or `query_assets_for_price_source`). Use same `pg_connect`. Run feed once per tick for simplicity. |
| 12 | Optional: ensure assets rows exist for feed | Document or add step: ensure `assets` has rows for symbols (from `init_assets_stables` for stables; from symbols table or seed for BTC, ETH, etc.). Can be one-time script or periodic. |

### 3.6 Documentation and tests

| Step | Task | Details |
|------|------|---------|
| 13 | Update this plan | Mark checklist items as done; reference implementation plan. |
| 14 | Integration test | Test: mocked provider + DB, run `run_asset_price_feed_step`, assert `assets.usd_price` and `price_source` updated. |

### 3.7 File summary

| File | Action |
|------|--------|
| `alembic/versions/xxx_add_assets_price_source.py` | New migration. |
| `pms/asset_price_providers/__init__.py` | New; export interface, Binance, registry. |
| `pms/asset_price_providers/interface.py` | New; `AssetPriceProvider` ABC/Protocol. |
| `pms/asset_price_providers/binance.py` | New; `BinanceAssetPriceProvider`. |
| `pms/asset_price_providers/registry.py` | New; `get_asset_price_provider(name, **kwargs)`. |
| `pms/asset_price_feed.py` | New; `query_assets_for_price_source`, `update_asset_prices`, `run_asset_price_feed_step`. |
| `pms/config.py` | Add: `pms_asset_price_source`, `pms_asset_price_interval_seconds`, `binance_price_feed_base_url`, `pms_asset_price_assets`. |
| `pms/main.py` | Wire feed step when source configured. |
| `pms/reads.py` | Optional: add `query_assets_for_price_source` if asset list from DB. |
| `pms/tests/test_asset_price_providers.py` | New; unit tests. |
| `pms/tests/test_asset_price_feed.py` | New (optional); integration test. |

### 3.8 Dependencies

- Step 1 (migration) first; step 7 needs the column.
- Steps 2–5 (interface + Binance + tests) after 1.
- Steps 6–8 depend on 3; 7 depends on 1.
- Steps 9–10 after 4; step 11 depends on 6–10.

---

## 4. Price source interface (provider)

Define a small interface so any source (Binance, REST, WebSocket, or other APIs) can plug in.

- **Interface:** e.g. `AssetPriceProvider` with:
  - `get_prices(assets: List[str]) -> Dict[str, Optional[float]]`  
    Return mapping asset → USD price (or symbol → price; adapter can map symbol → asset using symbols table).  
  - Optional: `supported_assets() -> List[str]` or config-driven so the runner knows which assets to request.
- **Implementations:**
  - **Binance (first):** REST `GET /api/v3/ticker/price`; map symbol (e.g. BTCUSDT) to asset (BTC); assume quote is USDT = USD. Optional later: WebSocket stream for lower latency.
  - **Others (later):** e.g. CoinGecko, Bybit, etc. Each implements the same interface and is configured with its own assets or priority.

**Location:** New module under PMS or a shared “market data” package, e.g. `pms/price_feed/` or `pms/asset_price_providers/`:
- `interface.py` — `AssetPriceProvider` (abstract).
- `binance.py` — Binance implementation (REST; reuses or mirrors logic from `pms/mark_price.py` but returns per-**asset** and is used by the feed, not by mark-price-for-positions directly).
- `registry.py` — optional: name → provider factory for config-driven selection.

PMS **enrichment** does not call these providers directly; it only reads from `assets` (and optionally current mark from existing `MarkPriceProvider` if you keep that path). The **feed** process calls the provider(s) and writes to storage.

---

## 5. Config: which source for which assets

- **Per-source asset list:** e.g. `BINANCE_PRICE_ASSETS=BTC,ETH,BNB,DOGE,...` or from symbols table (all base_asset from Binance symbols). So “Binance feed” is responsible only for those assets.
- **Priority (when you have multiple sources):** e.g. `ASSET_PRICE_SOURCE_PRIORITY=binance,coingecko`. Resolution uses first source that has a price for that asset.
- **Per-asset override (optional):** e.g. `ASSET_PRICE_SOURCE_OVERRIDE=BTC=binance,SAI=coingecko` for special cases.

Start simple: one source (Binance), one list of assets (from config or derived from symbols). Add priority/overrides when you add a second source.

---

## 6. Binance first: implementation outline

1. **Provider**
   - Add `pms/asset_price_providers/` (or under `pms/price_feed/`):
     - `AssetPriceProvider` interface: `get_prices(assets) -> Dict[str, Optional[float]]`.
     - `BinanceAssetPriceProvider`: call Binance `GET /api/v3/ticker/price`; for each asset, use `usd_symbol` from assets table (e.g. BTC → BTCUSDT) or default `{asset}USDT`; convert to float; return asset → price.
   - Reuse Binance base URL / testnet from config (e.g. same as mark price or new `BINANCE_PRICE_FEED_BASE_URL`).

2. **Feed job / loop**
   - Small loop or scheduled job (e.g. in PMS process or separate script):
     - Read list of assets to update (from config or from `assets` where `usd_symbol` is set or asset in allowed list).
     - Call `BinanceAssetPriceProvider.get_prices(assets)`.
     - Write results to `assets`: `UPDATE assets SET usd_price = %s, updated_at = now(), price_source = 'binance' WHERE asset = %s` for each asset (or batch).
   - Run every N seconds (e.g. 10–60); no need for WebSocket in v1.

3. **Assets table**
   - Optional migration: add `price_source` TEXT NULL to `assets` for audit.
   - Ensure `assets` has rows for assets you want to price (from `init_assets_stables` for stables; from symbols table or manual insert for BTC, ETH, etc.). Binance feed only updates `usd_price` (and `price_source`); it does not create rows (or you can define “upsert if missing” for feed-owned assets).

4. **PMS**
   - No change: `query_assets_usd_config()` and `enrich_positions_with_usd_from_assets()` already use `assets.usd_price`. After the feed runs, enrichment sees updated prices.

5. **Config**
   - E.g. `PMS_ASSET_PRICE_SOURCE=binance` (which provider to run).
   - `BINANCE_PRICE_FEED_BASE_URL` (optional; default testnet/main).
   - `PMS_ASSET_PRICE_ASSETS` (optional; comma-separated; if empty, derive from assets table rows that have `usd_symbol` set).

---

## 7. Extensibility (later sources)

- Add a new provider class implementing `AssetPriceProvider` (e.g. CoinGecko).
- Register it in config (e.g. `PMS_ASSET_PRICE_SOURCE=coingecko` or run two feeds with different source names).
- If using Option A with disjoint asset lists: assign a set of assets to the new source. If using overlapping sources: add resolution (e.g. priority list) and a small job that writes the chosen price into `assets.usd_price`.
- PMS and enrichment remain unchanged.

---

## 8. Where it runs

- **Option 1:** Same process as PMS: in the PMS loop, before or after `run_one_tick`, run the asset price feed (call Binance provider, write to `assets`). Simple; one process.
- **Option 2:** Separate process/script: e.g. `python -m pms.asset_price_feed` that only runs the feed loop and writes to `assets`. Better if you want to scale or deploy feed independently.
- **Option 3:** Separate “market data” service that writes to Redis or DB; PMS (or a small bridge) copies into `assets` if you want a clear boundary.

Recommendation: **Option 1** for Binance-first (run feed step inside PMS process, same host). Move to Option 2 if you add more sources or want to scale the feed independently.

---

## 9. Checklist (Binance first, Option A)

Use **§3 Implementation plan** for the full task list. Summary:

- [ ] **Schema:** Migration to add `price_source` to `assets` (step 1).
- [ ] **Provider:** `AssetPriceProvider` interface + `BinanceAssetPriceProvider` + registry (steps 2–4, 10).
- [ ] **Feed:** `query_assets_for_price_source`, `update_asset_prices`, `run_asset_price_feed_step` in `pms/asset_price_feed.py` (steps 6–8).
- [ ] **Config:** `pms_asset_price_source`, interval, Binance URL, asset list (step 9).
- [ ] **Wire:** Call feed step in PMS loop when source set (step 11).
- [ ] **Tests:** Unit tests for provider; optional integration test for feed step (steps 5, 14).

---

## 10. References

- Position valuation: `docs/pms/POSITION_VALUATION_SCHEMA_PLAN.md`.
- PMS enrichment: `pms/loop.py` (`enrich_positions_with_usd_from_assets`), `pms/reads.py` (`query_assets_usd_config`).
- Current mark price (positions): `pms/mark_price.py` (Binance REST for **symbol** ticker; used for optional mark-based enrichment). Asset feed is **per-asset** and writes to `assets`; it does not replace mark price but complements it (assets table is the single place for “current USD price per asset” for valuation).
