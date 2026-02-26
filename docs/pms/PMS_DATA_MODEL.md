# PMS Data Model: Build-from-Order Cash, Symbol vs Asset, Positions, Reconciliation

PMS reads from **OMS Postgres only** (orders, balance_changes); **PMS does not use a fills table** — orders are the primary data source. **PMS does not read OMS Redis positions** (that path is dummy). **Cash is not read from OMS `balances`** — OMS balance sync to Postgres is **disabled**. Instead, PMS **builds cash from orders + balance_changes** using double-entry and a symbol→base/quote mapping. Positions are **derived from the orders table** (filter by status: partially_filled, filled). This document covers data-model considerations: order **symbol** vs balance **asset**, building positions and **cash** from orders, symbol statics ownership, and reconciliation. See **docs/PHASE2_DETAILED_PLAN.md** §8 (PMS role), §12.3 (PMS tasks).

---

## References

- **Phase 2 plan:** [PHASE2_DETAILED_PLAN.md](../PHASE2_DETAILED_PLAN.md) §8 (PMS role), §12.3 (PMS tasks).
- **OMS schema:** Postgres `orders`, `accounts`, `balance_changes`; OMS may sync `balances` only when balance sync is enabled (in the build-from-order model, balance sync is **disabled**). See [AMS_DB_FIELDS.md](../ams/AMS_DB_FIELDS.md), [BALANCE_CHANGES_HISTORY.md](../oms/BALANCE_CHANGES_HISTORY.md).
- **Fills:** OMS does not write to a `fills` table. PMS **does not use a fills table**; orders (with executed_qty, price, status) are the primary source for position and cash derivation.

---

## Cash: build from orders + balance_changes (double-entry)

**Approach:** PMS is the source of truth for **cash** as well as PnL/margin. Cash is **built** from:

1. **Orders** — trade impact per (account_id, book, symbol): for each executed order, debit/credit **base** and **quote** assets using symbol→base/quote mapping (e.g. BUY 0.1 BTCUSDT: credit base BTC, debit quote USDT). Use `executed_qty`, `price` (or `binance_cumulative_quote_qty` where available) for amounts. Grain includes **book** so cash is attributed per book.
2. **balance_changes** — non-trade movements only (deposits, withdrawals, transfers, adjustments). OMS writes to `balance_changes` only from **balanceUpdate** events; per Binance API, **balanceUpdate** is triggered by deposits, withdrawals, and transfers — **not** by trades (trades trigger `outboundAccountPosition`). So **double-counting is not an issue**: balance_changes does not contain trade-related deltas when only balanceUpdate is written.
3. **Symbol mapping** — a **symbols** (or instruments) reference table provides base_asset and quote_asset per symbol. PMS uses it to convert order symbol → asset legs for double-entry cash.

**Double-entry:** Each order fill implies two legs (base asset, quote asset); each balance_change row is a single-asset delta. PMS maintains a **book_cash** (or equivalent) ledger at grain (account_id, book, asset) so that cash by book is consistent and auditable.

**Balance sync disabled:** In this model, OMS does **not** sync the Postgres `balances` table from the broker (or sync is disabled). Current cash is derived in PMS from orders + balance_changes; the `balances` table is not the source for PMS cash.

---

## Order symbol vs balance asset (and positions)

### The mismatch

- **Orders** use **symbol** = trading **pair** (e.g. `BTCUSDT`, `ETHUSDT`). Stored in `orders.symbol`.
- **Cash / balance_changes** use **asset** = single **asset** (e.g. `BTC`, `USDT`, `ETH`). Stored in `balance_changes.asset`, and in PMS book_cash by asset.

So **order symbol and balance asset are not the same**: one is a pair, the other is one leg. Derive **base** and **quote** from the symbol (via symbols table or helper) before posting cash legs.

### Implications for PMS

1. **Building cash from orders**  
   For each order (filter: partially_filled, filled), derive base and quote from symbol; then:
   - **BUY:** credit base_asset (+executed_qty), debit quote_asset (− executed_qty × price or use cumulative_quote_qty).
   - **SELL:** debit base_asset (−executed_qty), credit quote_asset (+ executed_qty × price or cumulative_quote_qty).
   Apply these legs to (account_id, book, asset) cash ledger.

2. **Building positions from orders**  
   Unchanged: filter by status **partially_filled**, **filled**; compute **signed net** per (account_id, book, symbol): **open_qty** = sum(BUY executed_qty) − sum(SELL executed_qty); **position_side** = 'long' | 'short' | 'flat'; **entry_avg** = cost basis of open quantity only (FIFO).

3. **Symbol statics (who manages)**  
   **OMS** (or a dedicated reference-data sync under OMS) **manages** the **symbols** table: sync from broker (e.g. Binance `GET /api/v3/exchangeInfo` or futures equivalent) or config; columns include symbol, base_asset, quote_asset, tick_size, lot_size, min_qty, product_type, broker. **PMS only reads** it for base/quote derivation, validation, and display. This keeps one source of truth for symbol→asset mapping and avoids duplication.

---

## Building positions from orders

OMS does **not** expose a Postgres **positions** table in Phase 2. PMS **derives positions from orders only** (no fills table):

- Filter by status **partially_filled**, **filled**; compute **signed net** per (account_id, book, symbol): **open_qty** (signed), **position_side** ('long' | 'short' | 'flat'), **entry_avg** (cost basis of the **open** quantity only; FIFO).
- **Use in PnL:** Realized PnL from executed order rows; unrealized PnL = (mark − entry_avg) × open_qty when open_qty ≠ 0. Margin uses order-derived positions and mark price when available.

---

## Account vs book: book column and default cash book

**Broker-fed data** (account events) has no notion of **book**. To support cash by book:

- **balance_changes** has a **book** column. Records written from broker **balanceUpdate** events use a **default cash book** (e.g. `__default__` or configurable). So broker-fed deposit/withdrawal/transfer is attributed to that default book.
- **Strategy books:** Cash is moved from the default book to strategy books via **book change records**: manual (or admin) entries in balance_changes (or a dedicated book-transfer table) that debit the default book and credit the target book for the same asset. So: (1) broker deposit → balance_change with book = default; (2) operator books a “transfer” from default book to book A → balance_change (or equivalent) debiting default, crediting book A.

This way, all cash is still built from orders + balance_changes with a consistent (account_id, book, asset) grain.

---

## Initial balance

When no **balance_changes** records have been received from the broker (e.g. account existed before we started recording, or testnet with no deposits), **initial balance** is established by:

- **Manual booking** or **reconciliation (recs)** entries: insert balance_changes (or equivalent) with change_type e.g. `adjustment` or `initial`, with the chosen book (default or strategy), so that PMS’s built cash matches the intended starting state.
- Document in recs process: if broker sends no balanceUpdate for an asset, initial balance for that (account, book, asset) is set via recs/manual entry.

---

## Reconciliation (recs) for the new model

Reconciliation is updated for the **build-from-order** cash model:

1. **Order-derived positions**  
   Reconcile order-derived positions (per account, book, symbol) vs any other position store (e.g. PMS-written `positions` table). Flag or repair drift; orders remain the canonical source.

2. **Order-derived cash vs balance_changes**  
   - **Cash built in PMS** = initial (from manual/recs) + SUM(balance_changes.delta per account, book, asset) + order impact (from orders, using symbol→base/quote).  
   - **Reconciliation:** For each (account, book, asset), compare PMS-built cash to expectations (e.g. broker statement or internal recs). Balance_changes should only contain deposit/withdrawal/transfer/adjustment; order impact is applied only from orders. Recs jobs can: (a) verify SUM(balance_changes) + order impact matches PMS book_cash; (b) add adjustment rows to balance_changes when fixing discrepancies.

3. **Symbol → base/quote**  
   When joining orders to cash legs, always use the **symbols** table (or same mapping) for base_asset/quote_asset. Reconcile that symbol list is in sync with broker (OMS/reference sync).

Document which reconciliations are implemented, where (PMS job, script, admin tool), and how often (periodic, on-demand).

---

## Double-counting: not an issue

- **Balance sync to Postgres `balances` is disabled** in this model; PMS does not read `balances` for cash.
- **balance_changes** is populated **only from balanceUpdate** events. Per **Binance API** (see **docs/BINANCE_API_RULES.md** §1.1), **balanceUpdate** is triggered by deposits, withdrawals, and transfers — **not** by trades. Trades trigger **outboundAccountPosition**, which we do not write to balance_changes for cash-building. So balance_changes contains only non-trade movements; trade impact is applied **only** from the orders table. **No double-counting** of trade-related cash.

---

## Symbol properties (reference data) — who manages

**Symbols table** (e.g. `symbols` or `instruments`): symbol, base_asset, quote_asset, tick_size, lot_size, min_qty, product_type (spot/futures), broker.

- **Managed by:** **OMS** or a reference-data sync component (e.g. job that calls broker `exchangeInfo` and UPSERTs into `symbols`). Same DB or schema as OMS; populated by sync or config.
- **Consumed by:** **PMS** reads only. Used for base/quote derivation when building cash from orders, validation, and display. See **PMS_ARCHITECTURE.md** §7.

---

## Cash vs positions (keep separate)

- **Positions** = open exposure in a **trading pair** (symbol). Grain (account_id, book, symbol) with **open_qty** (signed) and **position_side** ('long' | 'short' | 'flat'). Do **not** put cash into the positions table.
- **Cash** = holdings per **asset** per **book**. Built in PMS from **orders** (trade impact via symbol→base/quote) + **balance_changes** (deposit/withdrawal/transfer/adjustment). Stored in PMS **book_cash** (or equivalent) at grain (account_id, book, asset). A combined “holdings” view (positions + cash) can be derived in a VIEW or API.

---

## Capital by book

**capital(book) = asset_value(book) + cash(book)**. Asset value by book = sum of position values (qty × mark price) for that book. Cash by book = sum of book_cash for that (account_id, book, asset) across assets. No separate “allocations” interface required for core: cash by book comes from the same build-from-order + balance_changes flow, with the **book** column on balance_changes and default cash book for broker-fed records, plus book change records to move cash from default to strategy books.

---

## Summary

| Topic | Takeaway |
|-------|----------|
| **Cash source** | Build in PMS from **orders** (trade impact) + **balance_changes** (deposit/withdrawal/transfer/adjustment). Double-entry; symbol→base/quote from symbols table. OMS balance sync disabled. |
| **Double-counting** | Not an issue: balance_changes is only from balanceUpdate (deposit/withdrawal/transfer); trades are not in balance_changes. |
| **Initial balance** | Manual booking or recs (balance_changes adjustments) when no balance_change records from broker. |
| **Account vs book** | balance_changes has **book** column; default cash book for broker-fed records; book change records move cash from default to strategy books. |
| **Symbol statics** | **OMS** (or reference sync) manages **symbols** table; **PMS** reads for base/quote. |
| **Order symbol vs asset** | Derive base/quote from symbol (symbols table) before posting cash legs. |
| **Building positions** | PMS derives positions from **orders only** (filter partial/fully filled; no fills table), by account, **book**, symbol. |
| **Reconciliation** | Recs: (1) order-derived positions vs position store; (2) PMS-built cash vs balance_changes + order impact; (3) symbol mapping in sync with broker. |

Implement helpers or mapping (symbols table, book_cash grain) and document them in PMS so future work stays consistent.
