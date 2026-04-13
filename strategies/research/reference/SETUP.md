# MT5 Trading System — Setup & Getting Started

This guide walks through setting up and running the system: environment configuration, MetaTrader 5 setup, database initialization, model fitting, and scheduling.

---

## 1. Prerequisites

- **Python 3.9+** (3.6+ supported by MetaTrader5 package)
- **MetaTrader 5 terminal** installed and able to connect to your broker (e.g. FTMO)
- **Windows** — MetaTrader5 Python package requires the MT5 desktop terminal on Windows

---

## 2. Clone & Install Dependencies

```bash
# Clone the repo
git clone <your-repo-url>
cd MT5

# Create and activate virtual environment (recommended)
python -m venv .venv
.venv\Scripts\activate    # Windows
# source .venv/bin/activate   # Linux/macOS (MT5 package requires Windows)

# Install dependencies
pip install -r requirements.txt
```

---

## 3. Environment Configuration (.env)

Create a `.env` file in the project root. Use the example below as a template:

```bash
# Copy the example and edit
copy .env.example .env
# Then edit .env with your values
```

### Example: `.env.example`

Create a file named `.env.example` in the project root with:

```env
# -------------------------------------------------------------------
# MetaTrader 5 Connection (required for trading)
# -------------------------------------------------------------------
MT5_SERVER=FTMO-Demo
MT5_LOGIN=12345678
MT5_PASSWORD=your_password_here

# -------------------------------------------------------------------
# Paths (optional — defaults shown)
# -------------------------------------------------------------------
DB_PATH=data/trading.db
MODEL_DIR=models

# -------------------------------------------------------------------
# Binance API (optional — for klines; public endpoints, no API key)
# -------------------------------------------------------------------
BINANCE_BASE=https://api.binance.com
```

### Environment Variables Reference

| Variable     | Required | Default              | Description                                      |
|-------------|----------|----------------------|--------------------------------------------------|
| `MT5_SERVER`| Yes      | `FTMO-Demo`          | MT5 server name (must match broker/server list)  |
| `MT5_LOGIN` | Yes      | `0`                  | MT5 account number (integer)                     |
| `MT5_PASSWORD` | Yes   | `""`                 | MT5 account password                             |
| `DB_PATH`   | No       | `data/trading.db`    | SQLite database path                             |
| `MODEL_DIR` | No       | `models`             | Directory for saved strategy models              |
| `BINANCE_BASE` | No    | `https://api.binance.com` | Binance REST API base URL (no key needed)  |

> **Security:** Never commit `.env`. It is listed in `.gitignore`. Use `.env.example` as a template only.

---

## 4. MetaTrader 5 Setup

The Python package connects to the **locally installed** MetaTrader 5 terminal. Ensure the terminal is configured correctly.

### 4.1 Install MT5 Terminal

1. Download MetaTrader 5 from your broker (e.g. [FTMO](https://ftmo.com)) or [MetaQuotes](https://www.metatrader5.com).
2. Install and launch the terminal.
3. Log in to your account (e.g. FTMO Demo or Live) using the same credentials as in `.env`.

### 4.2 Terminal Settings

| Setting          | Requirement                                              |
|------------------|----------------------------------------------------------|
| **Algo Trading** | Enabled — Tools → Options → Expert Advisors → Allow Algo Trading |
| **Server**       | Must match `MT5_SERVER` in `.env` (e.g. `FTMO-Demo`)     |
| **Login**        | Must match `MT5_LOGIN` in `.env`                         |
| **Running**      | Terminal must be running (or startable) when Python runs |

### 4.3 Server Name

The `MT5_SERVER` value must match the server shown in MT5’s Navigator (e.g. `FTMO-Demo`, `FTMO-Demo2`). Check:

- File → Open an Account, or  
- View → Toolbox → Accounts → right-click account → Properties  

Use the exact server string shown there.

### 4.4 Crypto Symbols

Ensure your MT5 account has access to the portfolio symbols (`config.PORTFOLIO`). For FTMO, crypto pairs like `BTCUSD`, `ETHUSD`, `BNBUSD` are typically available.

---

## 5. Database Initialization

The SQLite database and tables must be created before first run:

```bash
# From project root, with venv activated
python -m src.db_load
```

Or from `src/`:

```bash
cd src
python db_load.py
```

This creates `data/trading.db` (or your `DB_PATH`) and the tables: `klines`, `trades`, `symbols`, `mtbars`.

---

## 6. Model Fitting (Offline)

Strategy models must be fitted before the rebalancing session can trade. Run `fit_models.py` periodically (e.g. weekly or after major market changes):

```bash
# From project root
run_script.bat fit_models.py

# Or directly
python src/fit_models.py
```

Requirements:

- MT5 terminal running and logged in (for `db.read_mtbars` if using MT5 bars)
- Or pre-populated `mtbars`/`klines` in the database

This fits all strategies per symbol and saves them under `models/{symbol}/*.joblib`.

---

## 7. Running the System

### 7.1 Manual Run (Rebalance Session)

```bash
# Ensure MT5 terminal is running and logged in
run_script.bat main.py

# Or directly
python src/main.py
```

This performs:

1. Connect to MT5  
2. Align to hourly boundary (`onhour_offset`)  
3. Update data (symbols, MT5 bars, Binance klines)  
4. Rebalance portfolio (load models, aggregate signals, adjust positions)  
5. Shutdown MT5  

### 7.2 Scheduled Run (Recommended)

Use Windows Task Scheduler (or cron on Linux) to run `main.py` at the desired time (e.g. every hour at :00 or :05):

**Windows Task Scheduler:**
- Action: Start a program  
- Program: `C:\path\to\.venv\Scripts\python.exe`  
- Arguments: `F:\path\to\MT5\src\main.py`  
- Start in: `F:\path\to\MT5`  
- Trigger: Daily, repeat every 1 hour (or at specific times)

**Example (run hourly at :00):**
- Trigger: Daily, at 00:00, repeat every 1 hour for 24 hours

---

## 8. Directory Layout After Setup

```
MT5/
├── .env                 # Your secrets (create from .env.example)
├── .env.example         # Template (safe to commit)
├── data/
│   └── trading.db       # Created by init_db
├── models/
│   ├── BTCUSD/          # Created by fit_models.py
│   │   ├── EmaVolStrategy_0024.joblib
│   │   └── ...
│   ├── ETHUSD/
│   └── BNBUSD/
├── logs/
│   └── trading.log      # Created at runtime
├── src/
│   ├── main.py          # Main rebalance entry point
│   ├── fit_models.py    # Offline model fitting
│   └── ...
├── run_script.bat
└── requirements.txt
```

---

## 9. Troubleshooting

### MT5 init failed
- Ensure MT5 terminal is running.
- Check `MT5_SERVER`, `MT5_LOGIN`, `MT5_PASSWORD` in `.env`.
- Verify Algo Trading is enabled in MT5 options.

### No symbols / no contract value
- Run a full session once so `update_symbol_specs()` populates the `symbols` table.
- Ensure your account has access to the symbols in `config.PORTFOLIO`.

### No valid models
- Run `fit_models.py` before `main.py`.
- Ensure `models/{symbol}/` contains `.joblib` files.

### Insufficient data
- The system needs sufficient history (e.g. 100+ bars per symbol). Run `main.py` once to backfill; subsequent runs will append.

### Binance / klines errors
- Binance uses public endpoints; no API key needed.
- For MT5-only flow, the system can use `mtbars` instead of `klines` (see `port_rebalance.py`).

---

## 10. Quick Start Checklist

- [ ] Python 3.9+ and dependencies installed  
- [ ] MetaTrader 5 terminal installed and logged in  
- [ ] Algo Trading enabled in MT5  
- [ ] `.env` created from `.env.example` with correct MT5 credentials  
- [ ] `python -m src.db_load` run once  
- [ ] `python src/fit_models.py` run (with MT5 or pre-populated data)  
- [ ] `python src/main.py` run manually to verify  
- [ ] Task Scheduler (or cron) configured for periodic runs  
