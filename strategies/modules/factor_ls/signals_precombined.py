"""Per-family columns before scalar score reduction (``signals_daily`` contract)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .validators import PRECOMBINED_COLUMNS


def _ewm_sum_diff_mom(log_return: pd.Series, n: int) -> pd.Series:
    half = n // 2
    denom = n - half
    if denom <= 0:
        return pd.Series(np.nan, index=log_return.index)
    # ``adjust=True`` required for ``.sum()`` on modern pandas (matches notebook default).
    a = log_return.ewm(span=n, adjust=True).sum()
    b = log_return.ewm(span=half, adjust=True).sum()
    return (a - b) / denom


def _close_diff_std(close: pd.Series, span: int = 20) -> pd.Series:
    return close.diff().ewm(span=span, adjust=False).std().replace(0, np.nan)


def _breakout_series(close: pd.Series, n: int) -> pd.Series:
    roll_max = close.rolling(n, min_periods=n).max()
    roll_min = close.rolling(n, min_periods=n).min()
    den = (roll_max - roll_min).replace(0, np.nan)
    raw = (close - (roll_max - roll_min) / 2.0) / den
    return raw.ewm(span=5, adjust=False).mean()


def compute_precombined(l1: pd.DataFrame) -> pd.DataFrame:
    """Build the 39 ``abbr_n`` columns from L1-shaped input (uses ``vwap_250`` for VWAP reversion)."""
    if l1.empty:
        return pd.DataFrame(columns=["bar_ts", "symbol"] + list(PRECOMBINED_COLUMNS))

    parts: list[pd.DataFrame] = []
    for sym, g in l1.groupby("symbol", sort=False):
        gg = g.sort_values("bar_ts").copy()
        gg["bar_ts"] = pd.to_datetime(gg["bar_ts"], utc=True)
        close = gg["close"].astype(float)
        log_return = gg["log_return"].astype(float)
        log_volume = gg["log_volume"].astype(float)
        log_quote_volume = gg["log_quote_volume"].astype(float)
        resid_return = gg["resid_return"].astype(float)
        beta_250 = gg["beta_250"].astype(float)
        taker_buy_base_volume = gg["taker_buy_base_volume"].astype(float)
        volume = gg["volume"].astype(float)
        vwap_250 = gg["vwap_250"].astype(float)

        base = gg[["bar_ts", "symbol"]].copy()
        d_std = _close_diff_std(close)

        for n in (10, 20, 40):
            base[f"mom_{n}"] = _ewm_sum_diff_mom(log_return, n)

        for n in (5, 10, 20):
            ewm_n = close.ewm(span=n, adjust=False).mean()
            ewm_4n = close.ewm(span=4 * n, adjust=False).mean()
            base[f"trend_{n}"] = (ewm_n - ewm_4n) / d_std

        for n in (10, 20, 40):
            base[f"breakout_{n}"] = _breakout_series(close, n)

        for n in (5, 10, 20):
            vw = vwap_250.ewm(span=n, adjust=False).mean()
            base[f"vwaprev_{n}"] = (close - vw) / d_std

        for n in (5, 10, 20):
            base[f"resrev_{n}"] = resid_return.ewm(span=n, adjust=True).sum() / n

        for n in (10, 20, 40):
            base[f"skew_{n}"] = log_return.rolling(n, min_periods=n).skew()

        for n in (10, 20, 40):
            base[f"vol_{n}"] = log_return.ewm(span=n, adjust=False).std()

        for n in (10, 20, 40):
            base[f"betasq_{n}"] = (beta_250**2).ewm(span=n, adjust=False).mean()

        for n in (10, 20, 40):
            base[f"maxret_{n}"] = log_return.rolling(n, min_periods=n).max()

        ratio = taker_buy_base_volume / volume.replace(0, np.nan)
        for n in (10, 20, 40):
            base[f"takerratio_{n}"] = ratio.ewm(span=n, adjust=False).mean()

        for n in (10, 20, 40):
            base[f"vlm_{n}"] = log_volume.ewm(span=n, adjust=False).mean()

        for n in (10, 20, 40):
            base[f"quotevlm_{n}"] = log_quote_volume.ewm(span=n, adjust=False).mean()

        for n in (10, 20, 40):
            base[f"retvlmcor_{n}"] = log_return.rolling(n, min_periods=n).corr(log_volume)

        parts.append(base)

    out = pd.concat(parts, ignore_index=True)
    for c in PRECOMBINED_COLUMNS:
        if c not in out.columns:
            out[c] = np.nan

    mat = out[list(PRECOMBINED_COLUMNS)].to_numpy(dtype=float)
    out["n_nonfinite"] = np.sum(~np.isfinite(mat), axis=1).astype(int)
    return out.sort_values(["symbol", "bar_ts"]).reset_index(drop=True)
