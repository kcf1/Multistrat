"""Load intraday OHLCV and aggregate to UTC-midnight daily bars."""

from __future__ import annotations

import os
from typing import Sequence

import pandas as pd
from sqlalchemy import create_engine

from . import config


def database_url() -> str:
    url = (
        os.environ.get("STRATEGIES_PIPELINE_DATABASE_URL", "").strip()
        or os.environ.get("DATABASE_URL", "").strip()
        or os.environ.get("MARKET_DATA_DATABASE_URL", "").strip()
    )
    if not url:
        raise RuntimeError(
            "Set STRATEGIES_PIPELINE_DATABASE_URL or DATABASE_URL (or MARKET_DATA_DATABASE_URL) for the pipeline."
        )
    return url


def load_intraday_ohlcv(
    *,
    symbols: Sequence[str],
    interval: str = config.DEFAULT_OHLCV_INTERVAL,
    open_time_ge: pd.Timestamp,
    open_time_lt: pd.Timestamp,
    engine=None,
) -> pd.DataFrame:
    """Return raw intraday rows from ``market_data.ohlcv`` (``open_time`` half-open range)."""
    eng = engine or create_engine(database_url())
    sym_list = [s.strip().upper() for s in symbols if s and str(s).strip()]
    if not sym_list:
        raise ValueError("symbols must be non-empty")

    t0 = pd.Timestamp(open_time_ge)
    t1 = pd.Timestamp(open_time_lt)
    if t0.tzinfo is None:
        t0 = t0.tz_localize("UTC")
    else:
        t0 = t0.tz_convert("UTC")
    if t1.tzinfo is None:
        t1 = t1.tz_localize("UTC")
    else:
        t1 = t1.tz_convert("UTC")

    q = """
    SELECT symbol, interval, open_time, open, high, low, close, volume,
           quote_volume, taker_buy_base_volume
    FROM {schema}.ohlcv
    WHERE interval = %(interval)s
      AND symbol = ANY(%(symbols)s)
      AND open_time >= %(t0)s
      AND open_time < %(t1)s
    ORDER BY symbol, open_time
    """.format(schema=config.SCHEMA_MARKET_DATA)

    df = pd.read_sql(
        q,
        eng,
        params={
            "interval": interval,
            "symbols": sym_list,
            "t0": t0,
            "t1": t1,
        },
    )
    if df.empty:
        return df
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    return df


def aggregate_to_daily_bars(intraday: pd.DataFrame) -> pd.DataFrame:
    """
    One row per (UTC calendar date, symbol): ``close`` last by ``open_time``;
    ``volume``, ``quote_volume``, ``taker_buy_base_volume`` summed.
    ``bar_ts`` = that date at 00:00 UTC.
    """
    if intraday.empty:
        return pd.DataFrame(
            columns=[
                "bar_ts",
                "symbol",
                "close",
                "volume",
                "quote_volume",
                "taker_buy_base_volume",
            ]
        )

    df = intraday.copy()
    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
    df["day"] = df["open_time"].dt.normalize()

    sum_cols = ["volume", "quote_volume", "taker_buy_base_volume"]
    for c in sum_cols:
        if c not in df.columns:
            df[c] = 0.0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df = df.sort_values(["symbol", "day", "open_time"])
    last_close = df.groupby(["symbol", "day"], as_index=False).tail(1)[["symbol", "day", "close"]]
    sums = df.groupby(["symbol", "day"], as_index=False)[sum_cols].sum()
    out = last_close.merge(sums, on=["symbol", "day"], how="left")
    out = out.rename(columns={"day": "bar_ts"})
    out["bar_ts"] = pd.to_datetime(out["bar_ts"], utc=True)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    return out[["bar_ts", "symbol", "close", "volume", "quote_volume", "taker_buy_base_volume"]]
