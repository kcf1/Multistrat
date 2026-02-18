# PMS Data Model: Symbol vs Asset, Positions, Reconciliation

PMS reads from OMS Postgres/Redis, computes PnL and margin, and writes snapshots. This document covers data-model considerations: order **symbol** vs balance **asset**, building positions from orders, and reconciling multiple views. See **docs/PHASE2_DETAILED_PLAN.md** §8 (PMS role), §12.3 (PMS tasks).

---

## References

- **Phase 2 plan:** [PHASE2_DETAILED_PLAN.md](../PHASE2_DETAILED_PLAN.md) §8 (PMS role), §12.3 (PMS tasks).
- **OMS schema:** Postgres `orders`, `accounts`, `balances`, `balance_changes`; OMS syncs **positions** to Redis only (no Postgres `positions` table in current Phase 2). See [AMS_DB_FIELDS.md](../ams/AMS_DB_FIELDS.md), [PHASE2_DETAILED_PLAN.md](../PHASE2_DETAILED_PLAN.md) §3.1.
- **Fills:** Source of truth for executions is downstream of OMS (e.g. PMS consumes `oms_fills` and may write to a `fills` table). OMS does not write to `fills`.

---

## Order symbol vs balance asset (and positions)

### The mismatch

- **Orders** use **symbol** = trading **pair** (e.g. `BTCUSDT`, `ETHUSDT`). Stored in `orders.symbol`, `oms_fills` events, and broker APIs.
- **Balances** use **asset** = single **asset** (e.g. `BTC`, `USDT`, `ETH`). Stored in `balances.asset`, `balance_changes.asset`, and broker account events (`balanceUpdate.a`, `outboundAccountPosition` balances).

So **order symbol and balance asset are not the same**: one is a pair, the other is one leg of that pair. You cannot join or compare them directly without deriving base/quote from the symbol.

### Implications for PMS

1. **Joining orders/fills to balances**  
   To answer “how much USDT did this order use?” or “how did BTC balance change after this fill?”, you need to:
   - Derive **base** and **quote** from the order/fill **symbol** (e.g. `BTCUSDT` → base `BTC`, quote `USDT`).
   - Then match **quote** or **base** to `balances.asset` / `balance_changes.asset`.

2. **Building positions from orders/fills**  
   Positions are often expressed per **symbol** (pair) and **side** (e.g. BTCUSDT, long 0.5). Balance movements are per **asset** (e.g. BTC +0.5, USDT −25k). To reconcile:
   - From **orders/fills**: aggregate by `(account_id, symbol, side)` to get a **derived position** (e.g. net quantity per symbol/side).
   - From **balances**: you see asset-level deltas (e.g. BTC, USDT). To compare with derived positions, map symbol → base/quote and check base-asset balance change vs filled quantity (and quote vs quote spent).

3. **Possible approaches**  
   - **Helper:** `symbol_to_base_quote(symbol)` (e.g. `BTCUSDT` → `("BTC", "USDT")`). Use when normalizing or joining; can be convention-based (e.g. known quote list) or a small config/table.
   - **Mapping table (DB):** If brokers use different codes (e.g. `XBT` vs `BTC`), a table `(broker, broker_asset_or_symbol, canonical_asset)` keeps one source of truth and supports reconciliation and audits.
   - **Repair / backfill:** If stored data is wrong or inconsistent, run repairs that use the same rules or mapping table to correct `orders.symbol` or `balances.asset` / `balance_changes.asset`.

Document the convention or mapping you choose (e.g. in this doc or in PMS code) so reconciliation logic is clear.

---

## Building positions from orders (and fills)

OMS does **not** expose a Postgres **positions** table in Phase 2. Positions are held in Redis only (`account:{broker}:{account_id}:positions`), populated by OMS from broker account streams.

PMS may still need to:

- **Derive positions from orders/fills** — e.g. aggregate fills (or terminal orders) by `(account_id, symbol, side)` to get net quantity per symbol/side. That gives an **order-derived** view of position (e.g. for spot: net BUY quantity − net SELL quantity per symbol).
- **Compare with account positions** — Redis (or any future Postgres positions table) holds the broker’s view; order-derived positions are your own view. Reconciling them (see below) detects missing fills, duplicate processing, or sync gaps.
- **Use both in PnL** — Realized PnL comes from fills; unrealized PnL and margin may use either broker positions or your derived positions, depending on where you get mark price and how you want to validate.

So: building positions from orders/fills is a supported and likely approach; document whether you use only order-derived positions, only broker positions, or both with reconciliation.

---

## Reconciling two tables (or two views)

PMS will often need to reconcile **two sources of truth**:

1. **Order/fill–derived positions vs broker (or Redis) positions**  
   - **Source A:** Positions built from `orders` / `oms_fills` (or a `fills` table): aggregate by `(account_id, symbol, side)`.  
   - **Source B:** Positions from Redis `account:{broker}:{account_id}:positions` (or a future Postgres positions table from OMS).  
   - **Reconciliation:** For each account and symbol, compare net quantity and side. Differences can indicate delayed sync, missing/duplicate fills, or bugs. Log and optionally alert.

2. **Balance_changes vs balances (and orders)**  
   - **Source A:** `balance_changes`: historical deltas per `(account_id, asset)`. Sum of `delta` should match current balance minus initial deposit.  
   - **Source B:** `balances`: current `available`/`locked` per `(account_id, asset)`.  
   - **Reconciliation:** For each account/asset, `SUM(balance_changes.delta)` + initial balance should equal current `balances`. Optionally cross-check with order/fill impact (e.g. fill in quote asset vs `balance_changes` for that asset).

3. **Order symbol vs balance asset when joining**  
   - When joining orders/fills to balances or balance_changes, use **base/quote derived from symbol** (see “Order symbol vs balance asset” above). Without that, joins are wrong or impossible.

Document which of these reconciliations you implement, where (e.g. PMS job, script, or admin tool), and how often (e.g. periodic job, on-demand).

---

## Summary

| Topic | Takeaway |
|-------|----------|
| **Order symbol vs balance asset** | Orders use **pair** (e.g. BTCUSDT); balances use **asset** (e.g. BTC, USDT). Derive base/quote from symbol before joining or comparing. |
| **Building positions** | OMS has no Postgres positions table in Phase 2. PMS can build positions from orders/fills (e.g. by account, symbol, side) and optionally reconcile with Redis/broker positions. |
| **Reconciling two tables** | Plan to reconcile (1) order-derived positions vs broker/Redis positions, (2) balance_changes vs balances, and (3) correct use of symbol→base/quote when joining orders to balances. |

Implement helpers or mapping (code or DB) as needed and document them in PMS so future work stays consistent.
