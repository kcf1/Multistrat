"""L1 daily features (§1 PHASE1_DETAILED_PLAN)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import config


def compute_l1_features(daily_bars: pd.DataFrame) -> pd.DataFrame:
    """
    Input: columns ``bar_ts``, ``symbol``, ``close``, ``volume``, ``quote_volume``,
    ``taker_buy_base_volume``. Output sorted by ``symbol``, ``bar_ts``.
    """
    if daily_bars.empty:
        return pd.DataFrame()

    df = daily_bars.copy()
    df["bar_ts"] = pd.to_datetime(df["bar_ts"], utc=True)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df = df.sort_values(["symbol", "bar_ts"])

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").clip(lower=0)
    df["quote_volume"] = pd.to_numeric(df["quote_volume"], errors="coerce").clip(lower=0)
    df["taker_buy_base_volume"] = pd.to_numeric(df["taker_buy_base_volume"], errors="coerce").fillna(0.0)

    df["log_close"] = np.log(df["close"].where(df["close"] > 0))
    df["log_return"] = df.groupby("symbol", group_keys=False)["log_close"].diff()

    df["ewvol_20"] = df.groupby("symbol", group_keys=False)["log_return"].transform(
        lambda s: s.ewm(span=20, adjust=False).std()
    )

    eps = 1e-12
    df["norm_return"] = np.where(df["ewvol_20"].abs() > eps, df["log_return"] / df["ewvol_20"], np.nan)
    df["norm_close"] = df.groupby("symbol", group_keys=False)["norm_return"].cumsum()

    basket = list(config.MARKET_BASKET)
    sub = df[df["symbol"].isin(basket)][["bar_ts", "symbol", "norm_return"]].copy()
    piv = sub.pivot(index="bar_ts", columns="symbol", values="norm_return")
    for s in basket:
        if s not in piv.columns:
            piv[s] = np.nan
    piv = piv[basket]
    valid = piv.notna().all(axis=1)
    mkt_ret = piv.mean(axis=1, skipna=False)
    mkt_ret = mkt_ret.where(valid)
    mkt_map = mkt_ret.rename("market_return").reset_index()
    df = df.merge(mkt_map, on="bar_ts", how="left")

    mkt_sorted = mkt_map.sort_values("bar_ts").copy()
    mkt_sorted["market_ewvol_20"] = mkt_sorted["market_return"].ewm(span=20, adjust=False).std()
    df = df.merge(mkt_sorted[["bar_ts", "market_ewvol_20"]], on="bar_ts", how="left")

    parts: list[pd.DataFrame] = []
    for _, g in df.groupby("symbol", sort=False):
        gg = g.sort_values("bar_ts").copy()
        nr = gg["norm_return"].astype(float)
        mr = gg["market_return"].astype(float)
        win = 250
        cov = nr.rolling(win, min_periods=win).cov(mr)
        var = mr.rolling(win, min_periods=win).var()
        gg["beta_250"] = cov / var.where(var.abs() > eps)
        parts.append(gg)
    df = pd.concat(parts, ignore_index=True)

    df["resid_return"] = df["norm_return"] - df["beta_250"] * df["market_return"]
    mask_resid = df["norm_return"].notna() & df["beta_250"].notna() & df["market_return"].notna()
    df.loc[~mask_resid, "resid_return"] = np.nan

    df["log_volume"] = np.where(df["volume"] > 0, np.log(df["volume"]), np.nan)
    df["log_quote_volume"] = np.where(df["quote_volume"] > 0, np.log(df["quote_volume"]), np.nan)

    vol_roll = df.groupby("symbol", group_keys=False)["volume"].transform(
        lambda s: s.rolling(250, min_periods=250).sum()
    )
    qv_roll = df.groupby("symbol", group_keys=False)["quote_volume"].transform(
        lambda s: s.rolling(250, min_periods=250).sum()
    )
    df["vwap_250"] = np.where(vol_roll.abs() > eps, qv_roll / vol_roll, np.nan)

    return df.sort_values(["symbol", "bar_ts"]).reset_index(drop=True)
