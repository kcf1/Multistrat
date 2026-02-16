# PMS: Account Data and Session Requirements

This note answers: **Does the Portfolio Management System (Position Keeper) require account data management or session handling to be built beforehand?**

---

## 1. Short Answer

**No.** You do **not** need to build a dedicated account data management service or a “session” layer before running Position Keeper. You do need:

- The **accounts** table to exist (part of Booking schema in Phase 2).
- At least one **account** row (e.g. a default account seeded by migration or script).

Position Keeper then uses **account_id** to scope all reads and writes (positions, balances, PnL, margin). No separate account CRUD service or session store is required for Phase 2.

---

## 2. What “Account Data Management” Might Mean

| Meaning | Required for Position Keeper? | Notes |
|--------|--------------------------------|--------|
| **accounts table** | Yes | Already in Phase 2 schema (§3.1). Booking creates it; Position Keeper only reads. |
| **At least one account row** | Yes | Seed via Alembic or script; e.g. default account for testnet. |
| **Account CRUD API / service** | No | Admin or Phase 3 can add this later. Position Keeper just needs account_id(s) to exist. |
| **Multi-account support** | Optional | Position Keeper can loop over all accounts from `SELECT id FROM accounts` or use a configured list (env). |
| **“Session” (user login / token)** | No | Session is an Admin/UI concern. Position Keeper is a backend loop with no user context. |
| **“Session” (which account is active)** | No | Encoded as account_id. Position Keeper can compute for one or all accounts. |

---

## 3. What Position Keeper Needs

- **account_id** on every position, balance, fill, and order row. Already in Phase 2 schema.
- A way to know **which accounts** to process:
  - **Option A:** Query `SELECT id FROM accounts` each tick or at startup.
  - **Option B:** Env var, e.g. `PMS_ACCOUNT_IDS=1,2` (optional; if empty, use all).
- No “session” store: Position Keeper does not need to know “current user” or “current session”; it just runs for a set of account_ids.

So: **account data** = accounts table + at least one row. **Session** = not required for Position Keeper.

---

## 4. Recommendation

1. **Phase 2 (Booking schema):** Implement **accounts** table and **orders** (OMS), **fills**, **positions**, **balances** per plan. Seed **one default account** (e.g. id=1, broker=binance, env=testnet) so OMS/Booking/Position Keeper have a valid account_id.
2. **Position Keeper:** Use **account_id** from env or from `SELECT id FROM accounts`. No account service or session needed.
3. **Later (Phase 3 / Admin):** Add account CRUD, “current account” or session for the UI if needed. Position Keeper remains unchanged.

---

## 5. Summary

- **Account data management** in the sense of “accounts table + seed” is required and is part of Booking schema (12.2.1); no separate service is required beforehand.
- **Session** is not required to build or run Position Keeper; it is optional and belongs to Admin/UI when you need “current user” or “current account” in the front end.

You can build Position Keeper as soon as the **accounts** table exists and at least one account row is present.
