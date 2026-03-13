# Risk Service Plan — Hard Rule Pre-Trade Checks

**Goal:** Design an expandable, rule-based pre-trade risk service that evaluates each proposed order against account state and configured limits, then returns a deterministic **ACCEPT / REJECT decision with reasons** before the order is sent to OMS/Broker.

---

## 1. Scope and role in the system

- **Input:**
  - Order intent from strategies (via `strategy_orders` stream): `account_id`, `strategy_id`/`book`, `broker`, `symbol`, `side`, `quantity`, `order_type`, optional `limit_price`, `time_in_force`, `comment`, timestamps.
  - Account & portfolio snapshot from PMS/OMS: positions, balances, margin-related metrics as available (Phase 2: minimal; Phase 5: full).
  - Risk configuration: per-account / per-strategy / global limits for order size, exposure, drawdown, etc.
- **Output:**
  - Decision:
    - `status`: `ACCEPT` or `REJECT`.
    - `reason_codes`: list of machine-readable codes (e.g. `ACCT_01_SUFFICIENT_BUYING_POWER`, `POS_01_LONG_ONLY`).
    - `messages`: optional human-readable messages for logs/ops.
  - Approved orders are written to `risk_approved` stream in the same schema OMS expects (plus optional risk metadata).
- **Position in pipeline:**
  - **Phase 2:** Optional; can still be a pass-through for initial E2E testing (see PHASE2_DETAILED_PLAN §12.4).
  - **Phase 5:** Becomes the **mandatory gate** between strategies and OMS.

---

## 2. Architecture and interfaces

### 2.1 Service boundary

- **Process:** Dedicated `risk` service (Docker container) with a simple main loop:
  - Consume order intents from `strategy_orders` (Redis stream).
  - Load/cache account snapshot and risk limits for the relevant account/strategy.
  - Run the order through a **rule engine** (ordered list of checks).
  - If accepted, publish to `risk_approved` (Redis stream) in OMS-compatible schema.
  - If rejected, optionally publish to a `risk_rejected` stream and always log.

### 2.2 Core data models (conceptual)

- `OrderIntent` (input from strategies)
  - `id` (internal UUID or from strategy)
  - `broker`, `account_id`, `strategy_id`/`book`
  - `symbol`, `side`, `quantity`, `order_type`
  - `limit_price` (optional), `time_in_force` (optional)
  - `comment` (optional), `created_at`

- `AccountSnapshot`
  - Identified by `(broker, account_id)`.
  - Balances summary (e.g. total equity, free collateral, cash by asset).
  - Positions summary (per symbol: side, size, notional).
  - Optional PMS-derived metrics (PnL, drawdown, margin usage).

- `RiskLimits`
  - Global defaults.
  - Per-account overrides.
  - Per-strategy/portfolio overrides.
  - Parameters for each rule (e.g. `max_order_notional`, `max_position_notional`, `max_daily_loss`).

- `RuleResult`
  - `rule_id`: unique code (e.g. `ACCT_01_SUFFICIENT_BUYING_POWER`).
  - `passed`: bool.
  - `message`: optional string.

- `RiskDecision`
  - `status`: `ACCEPT` / `REJECT`.
  - `reason_codes`: list of rule_ids that failed.
  - `messages`: list of human-readable messages.

---

## 3. Rule engine design (expandable)

### 3.1 Rule interface

- Every rule implements the same contract:
  - `evaluate(order: OrderIntent, account: AccountSnapshot, limits: RiskLimits) -> RuleResult`.
- Rules are **pure functions** (no side effects) to keep behavior deterministic and testable.

### 3.2 Rule registry and pipeline

- A **registry** holds all implemented rules keyed by `rule_id`.
- For each account/strategy, a **pipeline** is built from config:
  - Ordered list of `rule_id`s with enabled/disabled flags and parameters.
  - Allows per-account/strategy customization without changing code.
- Evaluation:
  - For each order, the engine iterates over the pipeline:
    - Collect all `RuleResult`s.
    - If any `passed == False`, the final decision is `REJECT`.
  - Short-circuiting is configurable:
    - Option A: Always run all rules to collect all violations (better for diagnostics).
    - Option B: Stop on first failure for latency (Phase 2 can start with A).

### 3.3 Configuration model

- Storage options (pick one for implementation; patterns stay the same):
  - Postgres tables (recommended for flexibility and sharing with Admin UI).
  - Static YAML/JSON files for early iterations.
- Typical tables for DB-backed config:
  - `risk_rules`:
    - `rule_id`, `description`, `enabled_default`, `scope` (global/account/strategy).
  - `risk_limits`:
    - `id`, `account_id`, `strategy_id` (nullable), `rule_id`, `params` (JSONB), `enabled` (bool).
- At runtime:
  - Risk service loads/refreshes config into memory.
  - For a given `(account_id, strategy_id)`:
    - Start from global defaults.
    - Apply account-level overrides.
    - Apply strategy-level overrides.

---

## 4. Recommended rule set (v1)

The list below is intentionally broad; you can enable only the rules you want for each account/strategy.

### 4.1 Basic order sanity

- **ORDER_01_MIN_QTY**
  - `qty >= min_qty(symbol)`; reject zero/tiny orders.
- **ORDER_02_MAX_QTY**
  - `qty <= max_order_qty(symbol/account)` to prevent fat-finger sizes.
- **ORDER_03_PRICE_BOUNDS (LIMIT only)**
  - `limit_price` within a configurable band around a reference price (e.g. last or mark).
  - Band can be specified as absolute distance or percentage (e.g. ±10%).

### 4.2 Account cash / buying power

- **ACCT_01_SUFFICIENT_BUYING_POWER**
  - For **buys**:
    - `order_notional <= available_buying_power`.
    - `available_buying_power` derived from balances/margin snapshot (Phase 2: may be simple spot cash; Phase 5: richer).
  - For **sells**:
    - Optional check that margin requirement does not exceed available collateral.

- **ACCT_02_MAX_GROSS_EXPOSURE**
  - Post-trade gross exposure (sum abs(notional) across all symbols) must be
    `<= max_gross_exposure` (config per account/strategy).

- **ACCT_03_MAX_NET_EXPOSURE**
  - Post-trade net exposure (signed) must be within
    `[-max_net_exposure, +max_net_exposure]`.

### 4.3 Position & symbol limits

- **POS_01_LONG_ONLY**
  - If `long_only=True` for the account/strategy:
    - Any order that would create or increase a short position is rejected.
    - For sells, post-trade position in units must be `>= 0`.

- **POS_02_MAX_POSITION_PER_SYMBOL_UNITS**
  - Post-trade absolute position size (units) for `(account, symbol)`:
    - `abs(post_trade_units) <= max_position_units`.

- **POS_03_MAX_POSITION_PER_SYMBOL_NOTIONAL**
  - Post-trade absolute notional per `(account, symbol)`:
    - `abs(post_trade_notional) <= max_position_notional`.

- **POS_04_MAX_CONCENTRATION**
  - Symbol-level concentration:
    - `symbol_exposure / portfolio_gross_exposure <= max_concentration_pct`.
  - Can be configured globally and overridden per symbol or sector.

- **POS_05_RESTRICTED_SYMBOLS**
  - If `symbol` is in a configured blocked list for the account/strategy, reject any order.

### 4.4 Margin & leverage (for futures/margin accounts)

- **MARGIN_01_MAX_LEVERAGE**
  - Post-trade leverage:
    - `post_trade_gross_exposure / equity <= max_leverage`.

- **MARGIN_02_MAX_MARGIN_USAGE**
  - Post-trade margin usage:
    - `required_margin / available_margin <= max_margin_usage`.

- **MARGIN_03_MARGIN_BUFFER**
  - Enforces a safety buffer so that post-trade margin usage stays below a strict hard cap, e.g.:
    - `required_margin / available_margin <= hard_limit - safety_buffer`.

### 4.5 P&L / drawdown rules

- **PNL_01_DAILY_LOSS_LIMIT**
  - Use daily realized + unrealized PnL for the account/strategy.
  - If `pnl <= -daily_loss_limit`, reject any **risk-increasing** orders:
    - Orders that increase net exposure or leverage.
    - Still allow strictly risk-reducing orders (closing or reducing positions).

- **PNL_02_STRATEGY_LOSS_LIMIT**
  - Same as above but scoped to `strategy_id`/`book` (per-strategy loss limits).

- **PNL_03_MAX_DRAWDOWN**
  - If equity vs recent peak exceeds `max_drawdown_pct`, block risk-increasing orders.

### 4.6 Venue / behavior rules

- **VENUE_01_ALLOWED_VENUES**
  - Per-account/strategy list of allowed venues/brokers; orders targeting disallowed venues are rejected.

- **VENUE_02_MAX_ORDER_RATE**
  - Simple throttle for fat-finger / runaway bots:
    - Max N orders per time window `(account, strategy, symbol)` or `(account, strategy)`.

### 4.7 Compliance / safety nets

- **COMP_01_GLOBAL_KILL_SWITCH**
  - Global flag: when enabled, reject **all** new orders (or all except risk-reducing).

- **COMP_02_ACCOUNT_KILL_SWITCH**
  - Per-account/strategy kill switch; can be set by ops.

- **COMP_03_RISK_REDUCING_ONLY_MODE**
  - Mode flag: only allow orders that reduce existing exposure (e.g. closing longs/shorts).
  - Can be entered automatically via P&L/drawdown rules or manually by ops.

---

## 5. Phase alignment

### 5.1 Phase 2 (minimal Risk)

- **Goal:** Keep the pipeline simple; focus on OMS/PMS stability and end-to-end flow.
- **Implementation:**
  - Either:
    - Full pass-through: `strategy_orders` → `risk_approved` with minimal schema validation.
  - Or:
    - Enable a **very small subset** of low-risk rules:
      - `ORDER_01_MIN_QTY`
      - `ORDER_02_MAX_QTY`
      - `VENUE_01_ALLOWED_VENUES`
- **Account data source:** For Phase 2, account/balance/position data comes primarily from OMS + PMS. Risk can start with **no stateful checks** (or only conservative static caps) and add state-dependent rules once PMS outputs are stable.

### 5.2 Phase 5 (full Risk)

- **Goal:** Risk acts as a robust pre-trade gate using all relevant account and portfolio signals.
- **Implementation:**
  - Enable the full rule set (or chosen subset) per account/strategy.
  - Integrate with PMS outputs (PnL, drawdown, margin usage) via:
    - Direct Postgres reads, or
    - Redis cache maintained by PMS.

---

## 6. Testing plan

### 6.1 Unit tests (per rule)

- For each rule:
  - Construct `OrderIntent`, `AccountSnapshot`, and `RiskLimits` fixtures.
  - Test:
    - Passing cases.
    - Failing cases (including edge-at-limit).
    - Boundary conditions (exactly equal to threshold).
  - Ensure `RuleResult` contains the correct `rule_id` and a useful `message` for failures.

### 6.2 Engine tests

- Build small, in-memory pipelines:
  - Single-rule pipelines (sanity).
  - Multi-rule pipelines where:
    - Multiple rules pass.
    - Multiple rules fail simultaneously.
  - Verify:
    - Aggregation of `RuleResult`s.
    - Final `RiskDecision.status` and `reason_codes`.
    - Optional short-circuit behavior if enabled.

### 6.3 Integration tests (Redis + OMS schema)

- With a running Redis (or fakeredis):
  - Write one or more `OrderIntent` messages to `strategy_orders` using the agreed schema.
  - Run the risk loop in-process:
    - Verify that accepted orders appear on `risk_approved` with the expected payload.
    - Verify that rejected orders **do not** appear on `risk_approved` and that reasons are logged/pushed to `risk_rejected` (if implemented).

### 6.4 E2E tests (with OMS/PMS)

- Reuse existing Phase 2/5 E2E harness:
  - Inject orders at `strategy_orders`:
    - Orders that should pass Risk and reach Binance via OMS.
    - Orders that should be blocked by Risk (e.g. exceeding max size, long-only violation).
  - Assert:
    - For allowed orders: end-to-end flow matches existing pipeline (Risk → OMS → broker → Booking/PMS).
    - For blocked orders: no corresponding entries in `risk_approved`, `oms` order store, or broker; risk logs/stream contain correct `reason_codes`.

---

## 7. Next steps

1. Decide the **minimal subset of rules** to enable for Phase 2 (likely just order-level sanity and venue checks).
2. Define concrete schemas in `risk` service code:
   - Pydantic models for `OrderIntent` (from `strategy_orders`) and `RiskApprovedOrder` (to `risk_approved`).
3. Implement the rule engine and 2–3 initial rules with full unit coverage.
4. Wire the `risk` service into Docker (optional in Phase 2; mandatory in Phase 5).
5. Expand configuration and rule coverage as PMS outputs mature.

