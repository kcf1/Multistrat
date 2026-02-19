# PMS Data Model: Symbol vs Asset, Positions, Reconciliation

PMS reads from **OMS Postgres only** (orders, balances, optional fills); **PMS does not read OMS Redis positions** (that path is dummy). Positions used by PMS are **derived from the orders table**. This document covers data-model considerations: order **symbol** vs balance **asset**, building positions from orders, and reconciling multiple views. See **docs/PHASE2_DETAILED_PLAN.md** §8 (PMS role), §12.3 (PMS tasks).

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

OMS does **not** expose a Postgres **positions** table in Phase 2. OMS may hold positions in Redis internally; **PMS does not read OMS Redis positions** (that is dummy).

PMS:

- **Derives positions from orders/fills** — e.g. aggregate fills (or terminal orders) by `(account_id, symbol, side)` to get net quantity per symbol/side. That **order-derived** view is the position source for PMS.
- **Optional reconciliation** — If another source of positions exists (e.g. future Postgres positions table), compare with order-derived for drift. PMS does not read OMS Redis.
- **Use in PnL** — Realized PnL from fills; unrealized PnL and margin use order-derived positions (and mark price when available).

---

## Reconciling two tables (or two views)

PMS will often need to reconcile **two sources of truth**:

1. **Order/fill–derived positions (PMS source)**  
   - **Source for PMS:** Positions built from `orders` (and optionally `fills`): aggregate by `(account_id, symbol, side)`. **PMS does not read OMS Redis positions** (dummy).  
   - **Optional:** If a future Postgres positions table or other store exists, reconcile order-derived vs that store; log and optionally alert on drift.

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
| **Building positions** | PMS derives positions from orders/fills (by account, symbol, side). PMS does not read OMS Redis positions (dummy). Optionally reconcile with another store or balance_changes. |
| **Reconciling two tables** | Plan to reconcile (1) order-derived vs any other position store (PMS does not use OMS Redis), (2) balance_changes vs balances, and (3) symbol→base/quote when joining orders to balances. |

Implement helpers or mapping (code or DB) as needed and document them in PMS so future work stays consistent.
