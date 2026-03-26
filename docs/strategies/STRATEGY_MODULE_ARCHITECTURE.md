# Strategy Module Architecture (Phase 6)

Purpose: define a clean, isolated structure for strategy development so strategy logic evolves independently from infrastructure modules (`oms/`, `pms/`, `market_data/`, `risk/`, `scheduler/`).

---

## 1. Isolation boundary

Strategies should live in a dedicated package (`strategies/`) and be treated as a domain layer that consumes infrastructure contracts, not internal infra implementation details.

- Strategies may read:
  - market state from Postgres (`ohlcv`) and/or Redis market keys when available
  - portfolio/account state via stable interfaces
- Strategies may write:
  - order intents to `strategy_orders` (through a shared publisher interface)
- Strategies should not:
  - directly call broker adapters from `oms/`
  - mutate OMS/PMS internal Redis key structures
  - own infra-side persistence schemas

This keeps strategy modules replaceable, testable, and independent of infra refactors.

---

## 2. Recommended repo layout

Two-level split: shared strategy runtime/core + per-strategy packages.

```text
multistrat/
├── strategies/
│   ├── core/                      # strategy-agnostic building blocks
│   │   ├── data/                  # strategy data loaders, transformers, feature pipelines
│   │   ├── portfolio/             # sizing, rebalancing policies, constraint evaluators
│   │   ├── backtest/              # local backtest harness and execution simulation
│   │   ├── optimize/              # parameter search, walk-forward helpers
│   │   ├── metrics/               # PnL/tearsheet metrics for strategy evaluation
│   │   ├── viz/                   # visualization/report helpers
│   │   ├── contracts.py           # protocol/interfaces used by all strategies
│   │   └── schemas.py             # order intent / signal payload schemas
│   ├── runner/                    # process loop/scheduler for strategy execution
│   ├── registry.py                # strategy discovery and enable/disable
│   ├── config.py                  # strategy-runner micro config
│   └── modules/
│       ├── <strategy_name_a>/
│       │   ├── config.py
│       │   ├── universe.py
│       │   ├── features.py
│       │   ├── model.py
│       │   ├── constraints.py
│       │   ├── logic.py
│       │   ├── rebalance.py
│       │   ├── pipeline.py
│       │   ├── README.md
│       │   └── tests/
│       └── <strategy_name_b>/
│           └── ...
```

Notes:
- Avoid a generic `utils.py` bucket; place reusable code by domain (`data`, `portfolio`, `backtest`, `optimize`, `metrics`, `viz`).
- Keep infra concerns (stream IO, DB clients, settings) in runner/adapters, not in per-strategy signal logic.

---

## 3. Shared contracts (stable interfaces)

Define strategy contracts once in `strategies/core/contracts.py` so each strategy is pluggable:

- `SignalGenerator`: `generate(market_state, position_state) -> list[OrderIntent]`
- `ConstraintSet`: `validate(order_intents, state) -> list[OrderIntent]`
- `PortfolioConstructor`: `target_weights(signals, state) -> dict[symbol, weight]`
- `RebalancePolicy`: `rebalance(current, target, state) -> list[OrderIntent]`
- `ModelAdapter` (optional): `fit(...)`, `predict(...)`, `load(...)`, `save(...)`

Use shared schemas for:
- signal payloads
- order intent payloads
- optional diagnostics/attribution payloads

---

## 4. Model persistence policy (recommended)

Use hybrid storage:

- Database (metadata only):
  - `model_id`, `strategy_id`, training window, feature version, params hash, metrics, created timestamp, artifact URI/path
- Artifact storage (binary blobs):
  - filesystem/object storage for model binaries (`.pkl`, `.onnx`, weights)

Rationale:
- DB remains queryable and small
- artifacts can be versioned/rotated independently
- reproducibility is preserved by metadata + immutable artifact URI

---

## 5. Config policy (aligned with workspace rules)

- Macro/infrastructure config in `.env`:
  - stream URLs, DB URLs, credentials, deployment-specific endpoints
- Micro strategy tuning in Python modules:
  - symbol lists, intervals, thresholds, lookbacks, feature flags, optimization bounds

Do not add `.env` keys for every strategy threshold/interval/symbol list unless it is truly deployment-level.

---

## 6. Implementation tasks (small and concrete)

- [x] Create `strategies/` package skeleton (`core`, `runner`, `modules`, `registry.py`, `config.py`).
- [x] Define base contracts and schemas (`contracts.py`, `schemas.py`).
- [x] Implement one reference strategy under `modules/` with `pipeline.py`.
- [ ] Wire runner to publish validated order intents to `strategy_orders`.
- [ ] Add model metadata persistence + artifact path convention.
- [ ] Add strategy registry (enabled/disabled, schedule/interval).

---

## 7. Test plan

### 7.1 Unit

- Feature/transform tests per strategy module.
- Signal logic tests for deterministic outputs given fixed inputs.
- Constraint/rebalance tests for edge cases (limits, no-op rebalance, partial rebalance).

### 7.2 Contract

- Every strategy module must satisfy `SignalGenerator` and output valid `OrderIntent` schema.
- Registry loads only valid strategies and rejects malformed configs.

### 7.3 Integration

- Strategy runner reads market + position state and emits to `strategy_orders`.
- Risk service consumes emitted intents unchanged (or with expected enrichment).

### 7.4 Determinism and regression

- Fixed seed + fixed dataset produces stable signals and backtest metrics.
- Store golden metrics for selected windows to catch behavioral regressions.

### 7.5 End-to-end

- Strategy -> `strategy_orders` -> Risk -> `risk_approved` -> OMS -> Booking/PMS completes for at least one strategy in testnet/dev environment.

---

## 8. Acceptance criteria (Phase 6 architecture)

- `strategies/` is a dedicated, isolated package from infra modules.
- At least one strategy is implemented using shared contracts.
- Order intents are schema-valid and accepted by Risk/OMS path.
- Strategy tests (unit + contract + integration) are added and runnable.
