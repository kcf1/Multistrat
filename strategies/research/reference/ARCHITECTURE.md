# MT5 Trading System — Architecture Document

## Overview

This system is an automated crypto portfolio rebalancing bot for MetaTrader 5 (MT5). It runs scheduled sessions that: (1) update market data from MT5 and Binance, (2) load pre-fitted strategy models, (3) aggregate signals into target positions, and (4) adjust MT5 positions via `adjust_position`. Designed for FTMO-style environments.

---

## 1. Data Flow

### 1.1 Data Sources → Storage

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA INGESTION                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   MT5 Terminal                    Binance API                                │
│   (api_mt5.py)                    (api_bnb.py)                               │
│        │                                 │                                   │
│        ├── symbols (Crypto specs)        │                                   │
│        ├── 1H bars (mtbars)              ├── 1H klines                       │
│        │                                 └── recent trades (optional)        │
│        │                                                                    │
│        └──────────────────────┬───────────────────────────────────────────┘
│                               ▼                                              │
│                    SQLite (data/trading.db)                                  │
│                    • symbols   • mtbars   • klines   • trades                │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Tables**

| Table   | Purpose                                             | Key Columns                                      |
|---------|-----------------------------------------------------|--------------------------------------------------|
| `symbols` | MT5 symbol specs, Binance mapping, contract value | symbol, symbol_bnb, trade_contract_value         |
| `mtbars`  | 1H MT5 bars (primary for trading)                 | symbol, time (ms), open, high, low, close, tick_volume |
| `klines`  | 1H Binance klines (optional/backfill)             | symbol, open_time, OHLCV, taker_buy_base_vol     |
| `trades`  | Binance recent trades (optional)                  | symbol, id, price, qty, time                     |

**Direction**

- **Write**: `db_load.save_mtbars`, `save_klines`, `save_symbols` (upsert semantics)
- **Read**: `db_read.read_mtbars`, `read_klines`, `get_contract_value`, `get_binance_symbol`

### 1.2 Data Flow During Rebalance

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         REBALANCE-TIME DATA FLOW                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   DB (mtbars)  ─────►  bars (OHLCV)  ─────►  Strategy models (.joblib)       │
│       │                      │                          │                    │
│       │                      │                          │  one_step_predict() │
│       │                      │                          ▼                    │
│       │                      │              Aggregate position_pct per asset  │
│       │                      │                          │                    │
│       │                      │                          ▼                    │
│       │                      │              target_lots = position_pct       │
│       │                      │                         * all_in_lots         │
│       │                      │                          │                    │
│       │                      │                          ▼                    │
│       symbols (contract_value) ───► all_in_lots     api_mt5.adjust_position  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

- Per symbol: `read_mtbars` → history for `MAX_HISTORY_HOURS`; `get_contract_value` for sizing
- Models expect DataFrame with at least `close` (and `volume` / `tick_volume`, `taker_buy_base_vol` where required)

---

## 2. Operational Flow

### 2.1 Main Session (main.py)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MAIN SESSION (main.py)                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   1. init_mt5()                                                              │
│      • Connect to MT5 (server/login/password from .env)                      │
│      • Verify login and terminal info                                        │
│                                                                              │
│   2. onhour_offset(offset_mins=0, offset_secs=-15)                           │
│      • Align execution to hourly boundary (run at :00:45)                    │
│      • If hour == 22: offset_mins=-5 (e.g. run at 21:55 for 4am market)      │
│                                                                              │
│   3. update_all_data()                                                       │
│      • update_symbol_specs() → MT5 symbols → DB                              │
│      • For each PORTFOLIO symbol:                                            │
│          - update_mt5_bars(symbol)                                           │
│          - update_binance_klines(symbol)                                     │
│                                                                              │
│   4. rebalance_portfolio()                                                   │
│      • read_balance() → capital (fixed 10k for lot sizing)                   │
│      • For each symbol: _rebalance_asset(symbol, capital_per_asset)          │
│      • Per asset: load models → aggregate signal → target lots → adjust_position │
│                                                                              │
│   5. shutdown_mt5()                                                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Scheduling Model

- **Trigger**: External scheduler (e.g. Windows Task Scheduler, cron) runs `main.py` at desired hour (typically on-the-hour or shortly before).
- **Frequency**: Intended for ~1H rebalancing (configurable via scheduler).
- **Precision**: `onhour_offset` provides fine-grained alignment to hour boundaries.

### 2.3 Per-Asset Rebalance (_rebalance_asset)

```
For each symbol in PORTFOLIO:
  1. contract_value = get_contract_value(symbol)
  2. all_in_lots = capital_per_asset / contract_value
  3. bars = read_mtbars(symbol, limit=MAX_HISTORY_HOURS)
  4. Load all .joblib models from MODEL_DIR/{symbol}/
  5. position_pct = sum(model.one_step_predict(bars) for model in models)
  6. target_lots = round(position_pct * 10) / 10 * all_in_lots
  7. target_lots = round(target_lots, LOT_PRECISION)
  8. adjust_position(symbol, target_lots)
```

---

## 3. Strategies Employed

All strategies inherit `BaseStrategy` (sklearn-style `.fit()` / `.predict()`) and produce **position as % of capital** (e.g. 0.5 = 50% long, -0.3 = 30% short).

### 3.1 Strategy Summary

| Strategy              | Type        | Purpose                                         | Fitted in fit_models? |
|-----------------------|-------------|-------------------------------------------------|------------------------|
| **VolScaleStrategy**  | Vol scaling | Target-vol scaling baseline                     | No                     |
| **WedThuStrategy**    | Calendar    | Long Wed / Short Thu seasonality                | Yes (10% weight)       |
| **EmaVolStrategy**    | Trend       | EMA crossover with vol tilt                     | Yes (20% weight)       |
| **AccelVolStrategy**  | Momentum    | EMA acceleration (momentum-of-momentum)         | Yes (15% weight)       |
| **BreakVolStrategy**  | Breakout    | Range breakout with smoothing                   | Yes (20% weight)       |
| **BlockVolStrategy**  | Trend       | Block momentum (HH + HL structure)              | Yes (15% weight)       |
| **BuySellVolStrategy**| Flow        | Taker buy/sell ratio (Binance volume flow)      | No                     |
| **RevStrategy**       | Reversal    | Short-term mean reversion with volume filter    | Yes (10% weight)       |
| **OrthAlphaStrategy** | Alpha       | Momentum-orthogonal alpha via RollingOLS        | Yes (20% weight)       |
| **WeightedOrthAlphaStrategy** | Alpha | Same, vol-weighted RollingWLS             | No                     |

---

### 3.2 Detailed Strategy Descriptions

#### VolScaleStrategy

**Purpose:** Simple volatility-targeting baseline. Inverse-volatility position sizing with no directional view; useful as a reference or building block.

**Signal flow:**
1. Volatility forecast: Rogers-Satchell (OHLC) with window ≈ 40% of `vol_window`, annualized
2. Raw position: `pos_raw = target_vol / v`
3. Calibrate `to_target_scaler` so in-sample PnL vol = `target_vol`
4. Final: `pos = pos_raw × to_target_scaler × strat_weight`

**Parameters:** `vol_window` (e.g. 24×30 h), `target_vol`, `strat_weight`  
**Inputs:** `close` (OHLC required for Rogers-Satchell)

---

#### WedThuStrategy

**Purpose:** Calendar effect — long on Wednesday, short on Thursday, neutral otherwise. Exploits weekly patterns in crypto returns.

**Signal flow:**
1. Binary: `s = +1` on Wed bars, `s = -1` on Thu bars, else 0; shift forward (predict next bar)
2. Volatility: separate EWM std for Wed and Thu bars (scaled vol window = `vol_window / 7`)
3. Raw position: `pos_raw = s × target_vol / v`; interpolate `v` across index
4. Calibrate `to_target_scaler` from in-sample PnL
5. Final: `pos = pos_raw × to_target_scaler × strat_weight`

**Parameters:** `vol_window`, `target_vol`, `strat_weight`  
**Inputs:** `close`

---

#### EmaVolStrategy

**Purpose:** Classic trend-following EMA crossover, standardized and modulated by volatility regime. Reduces size in high-vol regimes via Weibull CDF.

**Signal flow:**
1. EMA crossover: `s = fast_ema - slow_ema` (fast = `fast_ema_window`, slow = fast × `slow_ema_multiplier`)
2. Standardize: `s = s / s.ewm(vol_window).std()`, clip to ±2
3. Volatility: Rogers-Satchell, annualized
4. Vol tilt: `vol_tilt = 1 - Weibull_CDF(v; c, scale)` — reduces exposure when vol is high
5. Alpha blend: `tilt = (1-α) + α × vol_tilt`; combined `f = s × tilt`
6. Strategy decay (optional): EWM of rolling PnL → `decay ∈ [0.75, 1.0]` when strategy underperforms
7. Raw position: `pos_raw = target_vol / v × f × decay`
8. Calibrate `to_target_scaler` from in-sample PnL
9. Final: `pos = pos_raw × to_target_scaler × strat_weight`

**Parameters:** `fast_ema_window`, `slow_ema_multiplier`, `vol_window`, `weibull_c`, `alpha`, `fit_decay`, `target_vol`, `strat_weight`  
**Inputs:** `close`

---

#### AccelVolStrategy

**Purpose:** Momentum-of-momentum: uses the *acceleration* of the EMA crossover (diff of standardized level) rather than level itself. Captures trend inflection and strength changes.

**Signal flow:**
1. Level: `s_level = (fast_ema - slow_ema) / std`, standardized
2. Acceleration: `s_accel_raw = s_level.diff(diff_window)` where `diff_window = fast_ema_window × diff_multiplier`
3. Re-standardize: `s_accel = s_accel_raw / s_accel_raw.ewm(vol_window).std()`, clip ±2
4. Vol tilt: same Weibull CDF as EmaVolStrategy
5. Combined: `f = s_accel × tilt`; strategy decay as in EmaVolStrategy
6. Position: `pos_raw = target_vol / v × f × decay`; calibrate scaler; final with `strat_weight`

**Parameters:** `fast_ema_window`, `slow_ema_multiplier`, `diff_multiplier`, `vol_window`, `weibull_c`, `alpha`, `fit_decay`, `target_vol`, `strat_weight`  
**Inputs:** `close`

---

#### BreakVolStrategy

**Purpose:** Range breakout — where price sits relative to rolling high/low. Long above midpoint, short below, scaled by range and vol regime.

**Signal flow:**
1. Rolling: `h = rolling_max(breakout_window)`, `l = rolling_min(breakout_window)`, `mid = (h+l)/2`, `rng = h-l`
2. Raw breakout: `s_raw = (price - mid) / rng × 2` (normalized to ±1)
3. Smooth: `s_smooth = s_raw.ewm(span=smooth_window).mean()`
4. Volatility: Rogers-Satchell; vol tilt via Weibull; strategy decay optional
5. Combined: `f = s_smooth × tilt × decay`
6. Position: `pos_raw = target_vol / v × f`; calibrate scaler; final with `strat_weight`

**Parameters:** `breakout_window`, `smooth_window`, `vol_window`, `weibull_c`, `alpha`, `fit_decay`, `target_vol`, `strat_weight`  
**Inputs:** `close`

---

#### BlockVolStrategy

**Purpose:** Block momentum — "higher high + higher low" over blocks. Captures sustained directional structure (trend continuation) rather than single-bar breakouts.

**Signal flow:**
1. Rolling high/low: `h`, `l` over `block_window`
2. Block changes: `hh = h.diff(block_window)`, `ll = l.diff(block_window)` — how much higher current block vs prior
3. Normalized: `s_raw = (hh + ll) / 2 / (h - l)`
4. Smooth: `s = s_raw.ewm(span=smooth_window).mean()`, clip ±2
5. Vol tilt and strategy decay as in other strategies
6. Position: `pos_raw = target_vol / v × f × decay`; calibrate scaler; final with `strat_weight`

**Parameters:** `block_window`, `smooth_window`, `vol_window`, `weibull_c`, `alpha`, `fit_decay`, `target_vol`, `strat_weight`  
**Inputs:** `close`

---

#### BuySellVolStrategy

**Purpose:** Order-flow signal using taker buy volume vs total volume. High buy ratio → bullish pressure; low → bearish. Uses Binance `taker_buy_base_vol` and `volume`.

**Signal flow:**
1. Buy/sell ratio: `bsr_raw = EMA(taker_buy_base_vol) / EMA(volume)`, smoothed
2. Z-score: `bsr_z = (bsr_raw - mean) / std` over `vol_window`, clip ±2
3. Volatility: Rogers-Satchell; vol tilt via Weibull
4. Combined: `f = bsr_z × tilt`
5. Position: `pos_raw = f × target_vol / v`; calibrate scaler; final with `strat_weight`

**Parameters:** `volume_window`, `smooth_window`, `vol_window`, `weibull_c`, `alpha`, `target_vol`, `strat_weight`  
**Inputs:** `close`, `taker_buy_base_vol`, `volume` (Binance klines)

---

#### RevStrategy

**Purpose:** Short-term mean reversion. Fades extreme short-term momentum when volume decay is high (indicating exhaustion).

**Signal flow:**
1. Volatility: Rogers-Satchell (or EWM std)
2. Volume decay: min-max normalize volume EWM over `vol_window` → `vlm_z ∈ [0,1]`
3. Momentum z-score: `ret_z = EMA(returns) / vol` over `reversal_window`
4. Reversal: `rev = -sign(ret_z)` only when `|ret_z| > reversal_threshold` and `vlm_z > volume_threshold`
5. Position: `pos_raw = rev × target_vol / vol_annualized`; calibrate scaler; final with `strat_weight`

**Parameters:** `vol_window`, `reversal_window`, `reversal_threshold`, `volume_threshold`, `target_vol`, `strat_weight`  
**Inputs:** `close`, `volume`

---

#### OrthAlphaStrategy

**Purpose:** Extract alpha orthogonal to classic momentum. Regresses forward risk-adjusted returns on momentum; residuals are "pure alpha" uncorrelated with momentum.

**Signal flow:**
1. Momentum: `dp = (fast_ema - slow_ema) / std`, clip ±2
2. Target: `y = forward_return / vol` (risk-adjusted)
3. Rolling OLS: `y ~ const + dp.shift(forward_window)`; extract `const` (alpha) and t-value
4. Alpha signal: standardize alpha residual, clip ±2; optionally tilt by t-value (`t_tilt`)
5. Strategy decay: EWM of rolling PnL
6. Position: `pos_raw = target_vol / v × alpha_signal × decay`; calibrate scaler; final with `strat_weight`

**Parameters:** `forward_window`, `vol_window`, `regression_window`, `alpha` (tilt weight), `fit_decay`, `target_vol`, `strat_weight`  
**Inputs:** `close`

---

#### WeightedOrthAlphaStrategy

**Purpose:** Same as OrthAlphaStrategy but uses **RollingWLS** with weights = `1 / vol²` to down-weight high-vol periods in the regression. More robust alpha estimate.

**Signal flow:** Identical to OrthAlphaStrategy except `RollingWLS(y, X, weights=1/vol²)` instead of RollingOLS.  
**Parameters / Inputs:** Same as OrthAlphaStrategy.

---

### 3.3 Common Building Blocks

| Component       | Formula / Logic                                                                 |
|----------------|----------------------------------------------------------------------------------|
| **Volatility** | Rogers-Satchell: `fast_func.rogers_satchell_volatility(X, window)` × √ANNUAL_BARS |
| **Vol tilt**   | `vol_tilt = 1 - Weibull_CDF(v; c, scale)` — scale = `cdf_median × 1.5`; reduces size when vol is high |
| **Strategy decay** | `strat_pnl = EWM(position × returns, span=24×90)`; `decay = 0.75 + 0.25 × (-clip(strat_pnl/defactor, -2, 2) / 2)` — deflates when strategy underperforms |
| **Target vol** | `to_target_scaler = target_vol / actual_pnl_vol` fitted in-sample               |
| **Position**   | `pos = signal × target_vol / asset_vol × to_target_scaler × strat_weight`       |

---

### 3.4 Model Lifecycle

- **Fit:** `fit_models.py` runs offline — fits each strategy per symbol with configured variants (e.g. EmaVolStrategy at 24, 48, 96, 192 h), saves to `models/{symbol}/*.joblib`
- **Load:** `strat_io.load_model()` at rebalance time
- **Predict:** `model.one_step_predict(bars)` returns latest position %; aggregated by simple sum across all models per asset

---

### 3.5 Current Fit Parameters (fit_models.py)

**Global settings**
- `target_vol = 0.30`
- Data: `read_mtbars(symbol, limit=24*360*10)` (10 years of 1H bars)
- `bars['volume'] = bars['tick_volume']`

**Per-strategy configuration**

| Strategy | strat_weight | Variants | Parameters |
|----------|--------------|----------|------------|
| **EmaVolStrategy** | 0.20 | `fast_ema_window` ∈ [24, 48, 96, 192] | `slow_ema_multiplier=2`, `vol_window=720`, `weibull_c=2`, `alpha=1.0`, `fit_decay=True` |
| **AccelVolStrategy** | 0.15 | `fast_ema_window` ∈ [24, 48, 96, 192] | `slow_ema_multiplier=2`, `diff_multiplier=1.0`, `vol_window=720`, `weibull_c=2`, `alpha=1.0`, `fit_decay=True` |
| **BreakVolStrategy** | 0.20 | `breakout_window` ∈ [48, 96, 192, 384] | `smooth_window=12`, `vol_window=720`, `weibull_c=2`, `alpha=1.0`, `fit_decay=True` |
| **BlockVolStrategy** | 0.15 | `block_window` ∈ [48, 96, 192, 384] | `smooth_window=12`, `vol_window=720`, `weibull_c=2`, `alpha=1.0`, `fit_decay=True` |
| **WedThuStrategy** | 0.10 | `vol_window` ∈ [1440, 4320] (60d, 180d) | `target_vol=0.30` |
| **RevStrategy** | 0.10 | `reversal_window` ∈ [6, 9, 12, 15] | `vol_window=720`, `reversal_threshold=2.0`, `volume_threshold=0.3` |
| **OrthAlphaStrategy** | 0.20 | `forward_window` ∈ [24, 48, 96, 192] | `vol_window=720`, `regression_window=720`, `alpha=1.0`, `fit_decay=True` |

Variant weight = `strat_weight / len(variants)` (e.g. EmaVol 0.20/4 = 0.05 per variant). All strategies use `target_vol=0.30` unless noted.

---

## 4. Additional Architecture Components

### 4.1 Tech Stack

| Layer       | Technology                        |
|------------|-----------------------------------|
| Runtime    | Python 3.x                        |
| Broker     | MetaTrader 5 (MetaTrader5 package)|
| Data APIs  | MT5 Terminal, Binance REST API    |
| Storage    | SQLite (data/trading.db)          |
| Models     | scikit-learn BaseEstimator, joblib|
| Math/Stats | numpy, pandas, scipy, statsmodels |
| Logging    | Loguru (console + file rotation)  |
| Config     | python-dotenv (.env)              |

### 4.2 Configuration (config.py)

- **MT5**: `MT5_SERVER`, `MT5_LOGIN`, `MT5_PASSWORD`
- **Paths**: `DB_PATH`, `MODEL_DIR`
- **Portfolio**: `PORTFOLIO` (e.g. BTCUSD, ETHUSD, BNBUSD)
- **Rebalance**: `MAX_HISTORY_HOURS`, `MAX_POSITION_PCT`, `LOT_PRECISION`
- **Binance**: `BINANCE_BASE`
- **Time**: `TZ=UTC`, `MT5_TZ=GMT+2`

### 4.3 Execution & Risk (api_mt5.adjust_position)

- **Scenes**: New, increase, reduce, close all, reverse
- **Position sizing**: Signed lots (positive = long, negative = short)
- **Lot precision**: 2 decimals (configurable)
- **Reversal**: Close all then open opposite direction

### 4.4 Cross-Validation (strat_cv.py)

- **BlockBootstrap**: Block bootstrap CV for hyperparameter / model selection
- **BlockPermutation**: Block permutation CV for time series

### 4.5 Error Handling & Observability

- **Logging**: Loguru with rotation (7d), retention (30d), compression
- **MT5**: Retries on init, atexit shutdown
- **Data**: Graceful handling of empty bars, missing symbols, insufficient history
- **Per-asset errors**: Logged and skipped; other symbols continue

### 4.6 Security Considerations

- **Credentials**: Stored in `.env` (not committed)
- **Sensitive data**: No keys or passwords in code or logs
- **Network**: Binance public REST; MT5 local/terminal connection

---

## 5. File Map (src/)

| Module        | Role                                                |
|---------------|-----------------------------------------------------|
| `main.py`     | Entry point; session orchestration                  |
| `config.py`   | Env vars, portfolio, logging                        |
| `data_update.py` | Fetch and persist MT5 + Binance data             |
| `port_rebalance.py` | Rebalance logic, signal aggregation, adjust_position |
| `api_mt5.py`  | MT5 init, bars, symbols, orders, positions          |
| `api_bnb.py`  | Binance klines, trades                              |
| `db_load.py`  | Save klines, mtbars, symbols, trades                |
| `db_read.py`  | Read bars, klines, contract value, symbol mapping   |
| `db_schema.py`| Table DDL                                           |
| `strat_models.py` | All strategy classes                            |
| `strat_io.py` | Save/load/reset joblib models                       |
| `strat_cv.py` | Block CV for time series                            |
| `strat_scorer.py` | Pearson-based scorers                           |
| `fit_models.py` | Offline model fitting per symbol                  |
| `fast_func.py`| Rolling slope, OLS, Rogers-Satchell vol             |
| `utils.py`    | Time helpers, alignment, onhour_offset              |
| `performance.py` | PnL / risk metrics (analysis)                    |
| `plotting.py` | Visualization (plots/)                              |
| `pnl_*.py`    | PnL decomposition, permutation, simulation          |

---

## 6. Diagram: End-to-End Flow

```
                    ┌──────────────────────────────────────────────────────────┐
                    │                    SCHEDULER (external)                    │
                    │              e.g. Task Scheduler @ 00:00                  │
                    └──────────────────────────┬───────────────────────────────┘
                                               │
                                               ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                        main.py                                             │
│  init_mt5 → onhour_offset → update_all_data → rebalance_portfolio → shutdown_mt5          │
└──────────────────────────────────────────────────────────────────────────────────────────┘
         │                    │                              │
         │                    │                              │
         ▼                    ▼                              ▼
┌──────────────┐    ┌────────────────────┐         ┌────────────────────────────────────┐
│  MT5 API     │    │  data_update       │         │  port_rebalance                     │
│  Binance API │    │  • symbols → DB    │         │  • Read bars + models               │
└──────┬───────┘    │  • mtbars → DB     │         │  • Aggregate signals                │
       │            │  • klines → DB     │         │  • target_lots per symbol           │
       │            └─────────┬──────────┘         │  • adjust_position()                │
       │                      │                    └────────────────┬───────────────────┘
       │                      ▼                                     │
       │            ┌────────────────────┐                         │
       │            │  SQLite (DB)       │                         │
       │            │  symbols, mtbars,  │                         │
       │            │  klines, trades    │                         │
       │            └─────────┬──────────┘                         │
       │                      │                                    │
       │                      │  models/*.joblib                   ▼
       │                      └──────────────► strat_models ◄── api_mt5.adjust_position
       │                                                               │
       └───────────────────────────────────────────────────────────────┘
                                         MT5 Terminal (orders)
```

---

## 7. Gaps & Possible Extensions

| Area              | Current State                    | Possible Extension                         |
|-------------------|----------------------------------|--------------------------------------------|
| Portfolio-level   | Per-asset signals summed; no cross-asset | Correlation/risk parity, portfolio-level vol target |
| Model versioning  | Filename + saved_at in joblib    | Explicit version tags, A/B rollback        |
| Backtesting      | PnL scripts (pnl_*.py)          | Unified backtest harness, slippage/costs   |
| Alerts           | Logs only                       | Slack/email on failures or large drawdowns |
| Health checks    | is_mt5_ready(), basic try/except | Liveness probe, data staleness checks      |
| Rate limits      | Sleep in Binance fetcher        | Token bucket or dedicated rate limiter     |

---

*Last updated: 2025-02*
