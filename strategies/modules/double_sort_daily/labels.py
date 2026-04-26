"""Wide forward returns and supervised columns (no lookahead on past features)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import config


def compute_labels(l1: pd.DataFrame) -> pd.DataFrame:
    """
    For each configured horizon ``h``, forward columns are aligned at ``bar_ts`` using
    the same-bar ``close`` / ``ewvol_20`` and **future** prices only inside ``logret_fwd_*``.
    """
    if l1.empty:
        return pd.DataFrame(columns=["bar_ts", "symbol", "label_asof_ts"])

    horizons = config.DEFAULT_LABEL_HORIZONS
    parts: list[pd.DataFrame] = []
    for _, g in l1.groupby("symbol", sort=False):
        gg = g.sort_values("bar_ts").copy()
        gg["bar_ts"] = pd.to_datetime(gg["bar_ts"], utc=True)
        close = gg["close"].astype(float)
        ew = gg["ewvol_20"].astype(float)
        log_close = np.log(close.where(close > 0))

        row = gg[["bar_ts", "symbol"]].copy()
        row["label_asof_ts"] = row["bar_ts"]
        row["bar_ts_venue"] = None

        eps = 1e-12
        vol_w = np.where(ew.abs() > eps, config.VOL_WEIGHT_NUMERATOR / ew, np.nan)

        for h in horizons:
            logret_fwd = log_close.diff(h).shift(-h)
            row[f"logret_fwd_{h}"] = logret_fwd
            if config.INCLUDE_SIMPRET_IN_LABELS:
                row[f"simpret_fwd_{h}"] = close.pct_change(h).shift(-h)
            else:
                row[f"simpret_fwd_{h}"] = np.nan
            row[f"normret_fwd_{h}"] = np.where(ew.abs() > eps, logret_fwd / ew, np.nan)
            row[f"vol_weight_{h}"] = vol_w
            row[f"vol_weighted_return_{h}"] = logret_fwd * vol_w

        parts.append(row)

    return pd.concat(parts, ignore_index=True).sort_values(["symbol", "bar_ts"]).reset_index(drop=True)
