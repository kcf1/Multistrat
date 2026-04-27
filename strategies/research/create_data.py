from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Annualized vol scaling used for normalized returns (legacy OHLCV path).
_VOL_ANN_FACTOR = 0.90 / (250**0.5)

MEGA_TOP_4 = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT")

OHLCV_SQL = "SELECT * FROM market_data.ohlcv"

_POSTGRES_HOST = "192.168.1.249"
_POSTGRES_PORT = "5432"

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Backfilled dataset contract (duplicated here to avoid importing factor_ls internals).
SCHEMA_STRATEGIES_DAILY = "strategies_daily"
XSECS_RANK_COLUMNS: tuple[str, ...] = (
    "mom_rank",
    "trend_rank",
    "breakout_rank",
    "vwaprev_rank",
    "resrev_rank",
    "skew_rank",
    "vol_rank",
    "betasq_rank",
    "maxret_rank",
    "takerratio_rank",
    "vlm_rank",
    "quotevlm_rank",
    "retvlmcor_rank",
)

# Legacy OHLCV pipeline rank features (older notebooks / local feature build).
_LEGACY_OHLCV_X_COLS: tuple[str, ...] = (
    "mom_rank",
    "trend_rank",
    "breakout_rank",
    "vwaprev_rank",
    "resrev_rank",
    "takerratio_rank",
    "vol_rank",
    "betasq_rank",
    "volume_rank",
    "quotevol_rank",
    "maxret_rank",
    "skew_rank",
)


def _x_cols_for_frame(df: pd.DataFrame) -> list[str]:
    """Feature columns for train/valid/test X: prefer DB xsecs contract, else legacy OHLCV ranks."""
    xs = [c for c in XSECS_RANK_COLUMNS if c in df.columns]
    if xs:
        return xs
    leg = [c for c in _LEGACY_OHLCV_X_COLS if c in df.columns]
    if leg:
        return leg
    raise ValueError(
        "No known feature columns found in frame "
        f"(expected XSECS_RANK_COLUMNS subset or legacy OHLCV *_rank columns). "
        f"Columns sample: {list(df.columns)[:30]}"
    )


def combine_features(X, rescale=True):
    """
    Optionally rescale each series, sum them, then scale the combined signal
    so the result stays comparable to the original feature scale.
    """
    if rescale:
        deflator = [x.abs().expanding(20).mean() for x in X]
        rescaled = [x / d for x, d in zip(X, deflator)]
    else:
        deflator = [1 for _ in X]
        rescaled = list(X)

    signal = 0
    scaler = 0
    for x, d in zip(rescaled, deflator):
        signal += x
        scaler += d

    combined = (signal * scaler) / len(X)
    return combined


def _by_symbol(df: pd.DataFrame, fn) -> pd.Series:
    return df.groupby("symbol", group_keys=False).apply(fn)


def _breakout_channel(close: pd.Series, window: int) -> pd.Series:
    hi = close.rolling(window).max()
    lo = close.rolling(window).min()
    width = hi - lo
    mid = (hi - lo) / 2
    return ((close - mid) / width).ewm(span=5).mean()


def get_mom_score(x):
    mom10m5 = (x["log_return"].ewm(span=10).sum() - x["log_return"].ewm(span=5).sum()) / 5
    mom20m10 = (x["log_return"].ewm(span=20).sum() - x["log_return"].ewm(span=10).sum()) / 10
    mom40m20 = (x["log_return"].ewm(span=40).sum() - x["log_return"].ewm(span=20).sum()) / 20
    return combine_features([mom10m5, mom20m10, mom40m20], rescale=True)


def get_resrev_score(x):
    resrev5 = x["resid_return"].ewm(span=5).sum() / 5
    resrev10 = x["resid_return"].ewm(span=10).sum() / 10
    resrev20 = x["resid_return"].ewm(span=20).sum() / 20
    return combine_features([resrev5, resrev10, resrev20], rescale=True)


def get_trend_score(x):
    vol = x["close"].diff().ewm(span=20).std()
    trend5m20 = (x["close"].ewm(span=5).mean() - x["close"].ewm(span=20).mean()) / vol
    trend10m40 = (x["close"].ewm(span=10).mean() - x["close"].ewm(span=40).mean()) / vol
    trend20m80 = (x["close"].ewm(span=20).mean() - x["close"].ewm(span=80).mean()) / vol
    return combine_features([trend5m20, trend10m40, trend20m80], rescale=True)


def get_breakout_score(x):
    c = x["close"]
    return combine_features(
        [_breakout_channel(c, 10), _breakout_channel(c, 20), _breakout_channel(c, 40)],
        rescale=True,
    )


def get_vwaprev_score(x):
    vol = x["close"].diff().ewm(span=20).std()
    vwaprev5 = (x["close"] - x["vwap"].ewm(span=5).mean()) / vol
    vwaprev10 = (x["close"] - x["vwap"].ewm(span=10).mean()) / vol
    vwaprev20 = (x["close"] - x["vwap"].ewm(span=20).mean()) / vol
    return combine_features([vwaprev5, vwaprev10, vwaprev20], rescale=True)


def get_drawdown_score(x):
    drawdown10 = (x["close"].rolling(10).max() - x["close"]) / x["close"].rolling(10).max()
    drawdown20 = (x["close"].rolling(20).max() - x["close"]) / x["close"].rolling(20).max()
    drawdown40 = (x["close"].rolling(40).max() - x["close"]) / x["close"].rolling(40).max()
    return combine_features([drawdown10, drawdown20, drawdown40], rescale=True)


def get_ddath_score(x):
    return (x["close"].cummax() - x["close"]) / x["close"].cummax()


def get_maxret_score(x):
    maxret10 = x["log_return"].rolling(10).max()
    maxret20 = x["log_return"].rolling(20).max()
    maxret40 = x["log_return"].rolling(40).max()
    return combine_features([maxret10, maxret20, maxret40], rescale=True)


def get_skew_score(x):
    skew10 = x["log_return"].rolling(10).skew()
    skew20 = x["log_return"].rolling(20).skew()
    skew40 = x["log_return"].rolling(40).skew()
    return combine_features([skew10, skew20, skew40], rescale=True)


def get_vol_score(x):
    vol10 = x["log_return"].ewm(span=10).std()
    vol20 = x["log_return"].ewm(span=20).std()
    vol40 = x["log_return"].ewm(span=40).std()
    return combine_features([vol10, vol20, vol40], rescale=True)


def get_relvol_score(x):
    relvol10 = x["log_return"].ewm(span=10).std() - x["log_return"].ewm(span=20).std()
    relvol20 = x["log_return"].ewm(span=20).std() - x["log_return"].ewm(span=40).std()
    relvol40 = x["log_return"].ewm(span=40).std() - x["log_return"].ewm(span=80).std()
    return combine_features([relvol10, relvol20, relvol40], rescale=True)


def get_volvol_score(x):
    volvol10 = x["log_return"].ewm(span=10).std().ewm(span=20).std()
    volvol20 = x["log_return"].ewm(span=20).std().ewm(span=40).std()
    volvol40 = x["log_return"].ewm(span=40).std().ewm(span=80).std()
    return combine_features([volvol10, volvol20, volvol40], rescale=True)


def get_volume_score(x):
    volume10 = x["log_volume"].ewm(span=10).mean()
    volume20 = x["log_volume"].ewm(span=20).mean()
    volume40 = x["log_volume"].ewm(span=40).mean()
    return combine_features([volume10, volume20, volume40], rescale=True)


def get_quotevol_score(x):
    quotevol10 = x["log_quote_volume"].ewm(span=10).mean()
    quotevol20 = x["log_quote_volume"].ewm(span=20).mean()
    quotevol40 = x["log_quote_volume"].ewm(span=40).mean()
    return combine_features([quotevol10, quotevol20, quotevol40], rescale=True)


def get_takervol_score(x):
    taker10 = x["taker_buy_base_volume"].ewm(span=10).mean()
    taker20 = x["taker_buy_base_volume"].ewm(span=20).mean()
    taker40 = x["taker_buy_base_volume"].ewm(span=40).mean()
    return combine_features([taker10, taker20, taker40], rescale=True)


def get_takerratio_score(x):
    ratio = x["taker_buy_base_volume"] / x["volume"]
    takerratio10 = ratio.ewm(span=10).mean()
    takerratio20 = ratio.ewm(span=20).mean()
    takerratio40 = ratio.ewm(span=40).mean()
    return combine_features([takerratio10, takerratio20, takerratio40], rescale=True)


def get_retvolcor_score(x):
    retvolcor10 = x["log_return"].rolling(10).corr(x["log_volume"])
    retvolcor20 = x["log_return"].rolling(20).corr(x["log_volume"])
    retvolcor40 = x["log_return"].rolling(40).corr(x["log_volume"])
    return combine_features([retvolcor10, retvolcor20, retvolcor40], rescale=True)


def get_revbetasq_score(x):
    revbetasq10 = (x["revbeta"] ** 2).ewm(span=10).mean()
    revbetasq20 = (x["revbeta"] ** 2).ewm(span=20).mean()
    revbetasq40 = (x["revbeta"] ** 2).ewm(span=40).mean()
    return combine_features([revbetasq10, revbetasq20, revbetasq40], rescale=True)


def get_betasq_score(x):
    betasq10 = (x["beta"] ** 2).ewm(span=10).mean()
    betasq20 = (x["beta"] ** 2).ewm(span=20).mean()
    betasq40 = (x["beta"] ** 2).ewm(span=40).mean()
    return combine_features([betasq10, betasq20, betasq40], rescale=True)


_SCORE_FUNCS = (
    ("mom_score", get_mom_score),
    ("trend_score", get_trend_score),
    ("breakout_score", get_breakout_score),
    ("vwaprev_score", get_vwaprev_score),
    ("takerratio_score", get_takerratio_score),
    ("revbetasq_score", get_revbetasq_score),
    ("drawdown_score", get_drawdown_score),
    ("ddath_score", get_ddath_score),
    ("maxret_score", get_maxret_score),
    ("skew_score", get_skew_score),
    ("vol_score", get_vol_score),
    ("volume_score", get_volume_score),
    ("quotevol_score", get_quotevol_score),
    ("retvolcor_score", get_retvolcor_score),
    ("betasq_score", get_betasq_score),
    ("resrev_score", get_resrev_score),
)

_RANK_BY_VOLUME_BIN = (
    "mom",
    "trend",
    "breakout",
    "vwaprev",
    "takerratio",
    "revbetasq",
    "drawdown",
    "ddath",
    "maxret",
    "skew",
    "vol",
    "retvolcor",
    "betasq",
    "resrev",
)


def _postgres_sqlalchemy_url(*, require_all: bool) -> str:
    load_dotenv(_REPO_ROOT / ".env")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    if require_all:
        if not user or not password or not db:
            raise RuntimeError(
                "Set POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB in .env "
                f"(host={_POSTGRES_HOST!r} port={_POSTGRES_PORT!r})."
            )
    else:
        user = user or "your_username"
        password = password or "your_password"
        db = db or "your_database"
    u = quote_plus(user)
    p = quote_plus(password)
    return f"postgresql://{u}:{p}@{_POSTGRES_HOST}:{_POSTGRES_PORT}/{db}"


def _create_postgres_engine():
    return sqlalchemy.create_engine(_postgres_sqlalchemy_url(require_all=False))


def _read_ohlcv() -> pd.DataFrame:
    engine = _create_postgres_engine()
    ohlcv_df = pd.read_sql(OHLCV_SQL, engine)
    ohlcv_df["ts"] = pd.to_datetime(ohlcv_df["open_time"])
    return ohlcv_df


def _pipeline_database_url() -> str:
    """Backfilled panel: fixed host/port in this module; credentials from .env."""
    return _postgres_sqlalchemy_url(require_all=True)


def _notebook_y_aliases(label_horizon: int) -> dict[str, str]:
    """Match naming expected by research scripts (opt_xgb / wf runner)."""
    h = int(label_horizon)
    return {
        "bar_ts": "ts",
        f"vol_weight_{h}": "vol_weight",
        f"vol_weighted_return_{h}": "vol_weighted_return",
        f"logret_fwd_{h}": "fwd_return",
    }


def load_backfilled_panel(
    *,
    label_horizon: int = 1,
    bar_ts_ge: object | None = None,
    bar_ts_le: object | None = None,
) -> pd.DataFrame:
    """
    Load model-ready panel from backfilled tables:
    `strategies_daily.labels_daily` (y/ids) joined to `strategies_daily.xsecs_daily` (x ranks).

    Output columns include: `ts`, `symbol`, `vol_weight`, `vol_weighted_return`, `fwd_return`,
    plus all `XSECS_RANK_COLUMNS` present.
    """
    schema = SCHEMA_STRATEGIES_DAILY
    rank_list = ", ".join(f'x."{c}"' for c in XSECS_RANK_COLUMNS)
    h = int(label_horizon)

    where_clauses: list[str] = []
    params: dict[str, object] = {}
    if bar_ts_ge is not None:
        where_clauses.append("l.bar_ts >= :bar_ts_ge")
        params["bar_ts_ge"] = pd.Timestamp(bar_ts_ge)
    if bar_ts_le is not None:
        where_clauses.append("l.bar_ts <= :bar_ts_le")
        params["bar_ts_le"] = pd.Timestamp(bar_ts_le)
    where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

    sql = f"""
    SELECT
        l.bar_ts,
        l.symbol,
        l.logret_fwd_{h},
        l.vol_weight_{h},
        l.vol_weighted_return_{h},
        {rank_list}
    FROM "{schema}"."labels_daily" l
    INNER JOIN "{schema}"."xsecs_daily" x
      ON l.bar_ts = x.bar_ts AND l.symbol = x.symbol
    WHERE 1=1
    {where_sql}
    ORDER BY l.bar_ts, l.symbol
    """
    engine = create_engine(_pipeline_database_url())
    df = pd.read_sql(text(sql), engine, params=params)
    if df.empty:
        return df

    df = df.rename(columns=_notebook_y_aliases(label_horizon))
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def _daily_panel(ohlcv_df: pd.DataFrame) -> pd.DataFrame:
    df = ohlcv_df.copy()
    df["date"] = df["open_time"].dt.date
    df = (
        df.groupby(["date", "symbol"])
        .agg(
            {
                "close": "last",
                "volume": "sum",
                "quote_volume": "sum",
                "taker_buy_base_volume": "sum",
                "taker_buy_quote_volume": "sum",
            }
        )
        .reset_index()
    )
    df["ts"] = pd.to_datetime(df["date"])
    return df[
        [
            "symbol",
            "ts",
            "close",
            "volume",
            "quote_volume",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        ]
    ]


def _add_returns_and_liquidity(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["log_close"] = np.log(df["close"])
    df["log_return"] = df.groupby("symbol")["log_close"].diff()
    df["fwd_return"] = df.groupby("symbol")["log_return"].shift(-1)
    df["log_vol"] = _by_symbol(
        df, lambda x: x["log_return"].ewm(span=20, adjust=False).std()
    )
    df["norm_return"] = df["log_return"] * _VOL_ANN_FACTOR / df["log_vol"]
    df["norm_close"] = df.groupby("symbol")["norm_return"].cumsum()
    df["vol_weight"] = _VOL_ANN_FACTOR / df["log_vol"]
    df["vol_weight_fwd"] = df.groupby("symbol")["vol_weight"].shift(-1)
    df["vol_weighted_return"] = df["fwd_return"] * df["vol_weight"]
    df["vol_weighted_return_fwd"] = df["fwd_return"] * df["vol_weight_fwd"]
    df["vol_weighted_return_rank"] = df.groupby("ts")["vol_weighted_return"].rank(pct=True)
    df["log_volume"] = np.log(df["volume"])
    df["log_quote_volume"] = np.log(df["quote_volume"])
    qv = df.groupby("symbol", group_keys=False)["quote_volume"].apply(lambda x: x.rolling(250).sum())
    vol_roll = df.groupby("symbol", group_keys=False)["volume"].apply(lambda x: x.rolling(250).sum())
    df["vwap"] = qv / vol_roll
    return df


def _add_market_beta_residual(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mkt_df = df[df["symbol"].isin(MEGA_TOP_4)]
    mkt_idx = mkt_df.groupby("ts")["norm_return"].mean()
    mkt_idx.name = "return"
    mkt_idx = pd.DataFrame(mkt_idx)
    mkt_idx["vol"] = mkt_idx["return"].ewm(span=20, adjust=False).std()
    mkt_idx = mkt_idx.add_prefix("mkt_").reset_index().dropna()
    df = df.merge(mkt_idx, on="ts", how="left")
    df["revbeta"] = _by_symbol(
        df,
        lambda x: x["norm_return"].rolling(250).cov(x["mkt_return"])
        / x["norm_return"].rolling(250).var(),
    )
    df["beta"] = _by_symbol(
        df,
        lambda x: x["norm_return"].rolling(250).cov(x["mkt_return"])
        / x["mkt_return"].rolling(250).var(),
    )
    df["resid_return"] = df["norm_return"] - df["beta"] * df["mkt_return"]
    return df


def _add_score_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col, fn in _SCORE_FUNCS:
        df[col] = _by_symbol(df, fn)
    return df


def _add_rank_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["quotevol_rank"] = df.groupby("ts")["quotevol_score"].rank(pct=True)
    df["volume_rank"] = df.groupby("ts")["volume_score"].rank(pct=True)
    df["volume_bin"] = (df["quotevol_rank"] * 3).astype(int).clip(0, 2)
    gcols = ["ts", "volume_bin"]
    for base in _RANK_BY_VOLUME_BIN:
        df[f"{base}_rank"] = df.groupby(gcols)[f"{base}_score"].rank(pct=True)
    return df


def _time_splits(df: pd.DataFrame):
    hi, lo = df["ts"].max(), df["ts"].min()
    span = hi - lo
    train_end = lo + span * 0.6
    valid_end = lo + span * 0.8
    train_df = df[df["ts"] < train_end]
    valid_df = df[(df["ts"] >= train_end) & (df["ts"] < valid_end)]
    test_df = df[df["ts"] >= valid_end]
    x_cols = _x_cols_for_frame(df)
    y_cols = ["ts", "symbol", "vol_weight", "vol_weighted_return", "fwd_return"]
    return (
        train_df[x_cols],
        train_df[y_cols],
        valid_df[x_cols],
        valid_df[y_cols],
        test_df[x_cols],
        test_df[y_cols],
    )


def build_ranked_panel_df() -> pd.DataFrame:
    """
    Full cross-sectional panel used by research scripts.

    **Default**: backfilled dataset (`labels_daily` + `xsecs_daily`) already contains the
    cross-sectional rank features and target columns needed by `opt_xgb.py`.

    Legacy OHLCV feature engineering is kept via `build_ranked_panel_df_legacy_ohlcv()`.
    """
    return load_backfilled_panel(label_horizon=1)


def build_ranked_panel_df_legacy_ohlcv() -> pd.DataFrame:
    """Legacy OHLCV-based feature engineering pipeline (kept for reference)."""
    ohlcv_df = _read_ohlcv()
    df = _daily_panel(ohlcv_df)
    df = _add_returns_and_liquidity(df)
    df = _add_market_beta_residual(df)
    df = _add_score_columns(df)
    df = df.dropna()
    df = _add_rank_columns(df)
    return df


def load_data(*, n_samples: int | None = None, label_horizon: int = 1):
    """
    Return train/valid/test splits for Optuna/XGB research.

    - Uses backfilled dataset by default.
    - Optional cap: keep the most-recent `n_samples` rows (after sorting by `ts`, `symbol`).
      Default ``None`` uses the full loaded panel, then applies the same 60/20/20 time split
      as the legacy pipeline (see ``_time_splits``).
    """
    df = load_backfilled_panel(label_horizon=label_horizon)
    if df.empty:
        raise SystemExit(
            "Backfilled panel is empty (check POSTGRES_* in .env and host/port in create_data.py)."
        )

    x_cols = [c for c in XSECS_RANK_COLUMNS if c in df.columns]
    missing = [c for c in XSECS_RANK_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"Backfilled panel missing xsec rank columns: {missing}")

    y_cols = ["ts", "symbol", "vol_weight", "vol_weighted_return", "fwd_return"]
    keep = y_cols + x_cols
    df = df[keep].dropna()
    df = df.sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)

    if n_samples is not None and int(n_samples) > 0 and len(df) > int(n_samples):
        df = df.iloc[-int(n_samples) :].reset_index(drop=True)

    return _time_splits(df)
