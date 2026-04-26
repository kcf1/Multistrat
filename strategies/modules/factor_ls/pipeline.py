"""Orchestrate loader → L1 → signals → factors → xsec → labels → persist."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import pandas as pd

from . import config
from . import data_loader
from . import features_l1
from . import labels as labels_mod
from . import persistence
from . import signals_combined
from . import signals_precombined
from . import signals_xsection


@dataclass
class PipelineResult:
    l1: pd.DataFrame
    pre: pd.DataFrame
    factors: pd.DataFrame
    xsecs: pd.DataFrame
    labels: pd.DataFrame


def _filter_bar_ts(df: pd.DataFrame, lo: pd.Timestamp, hi: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df
    t = pd.to_datetime(df["bar_ts"], utc=True)
    return df[(t >= lo) & (t <= hi)].copy()


def _coverage_ok(l1: pd.DataFrame, symbols: Sequence[str], min_cov: float) -> None:
    if l1.empty:
        raise RuntimeError("L1 frame is empty after load; cannot check coverage")
    n_sym = len(symbols)
    for ts, g in l1.groupby("bar_ts"):
        frac = g["symbol"].nunique() / max(n_sym, 1)
        if frac < min_cov:
            raise RuntimeError(
                f"Symbol coverage {frac:.3f} at bar_ts={ts} below MIN_SYMBOL_COVERAGE={min_cov}"
            )


def run_pipeline(
    *,
    symbols: Sequence[str] | None = None,
    open_time_ge: object | None = None,
    open_time_lt: object | None = None,
    bar_ts_ge: object | None = None,
    bar_ts_le: object | None = None,
    run_at_utc: object | None = None,
    persist: bool = True,
    pipeline_version: str | None = None,
    interval: str | None = None,
) -> PipelineResult:
    """
    **Modes:**

    - **Production default:** pass ``run_at_utc`` (or omit to use current UTC). Loads warmup
      + 24 daily ``bar_ts`` keys per ``config.production_bar_ts_range`` (intended **00:00 UTC** run).
    - **Symbols:** pass ``symbols`` for a subset; omit (``None``) to load **every symbol**
      that appears in ``ohlcv`` for the intraday window (no env list).
    - **Explicit intraday window:** ``open_time_ge`` and ``open_time_lt`` on ``ohlcv.open_time``.
      If ``bar_ts_ge`` / ``bar_ts_le`` are omitted, all daily ``bar_ts`` from that window are output.
    """
    iv = interval or config.DEFAULT_OHLCV_INTERVAL

    use_explicit = open_time_ge is not None and open_time_lt is not None
    if use_explicit:
        load_ge = pd.Timestamp(open_time_ge)
        load_lt = pd.Timestamp(open_time_lt)
        if run_at_utc is not None:
            raise ValueError("Use either (open_time_ge, open_time_lt) or run_at_utc, not both.")
    else:
        ra = run_at_utc if run_at_utc is not None else pd.Timestamp.now(tz="UTC")
        load_ge, load_lt, out_lo_def, out_hi_def = config.production_bar_ts_range(ra)

    if load_ge.tzinfo is None:
        load_ge = load_ge.tz_localize("UTC")
    else:
        load_ge = load_ge.tz_convert("UTC")
    if load_lt.tzinfo is None:
        load_lt = load_lt.tz_localize("UTC")
    else:
        load_lt = load_lt.tz_convert("UTC")

    if symbols is not None:
        syms: tuple[str, ...] = tuple(s.strip().upper() for s in symbols if s and str(s).strip())
        if not syms:
            raise ValueError("symbols=() is invalid; pass None for full OHLCV universe")
    else:
        syms = data_loader.load_distinct_symbols(
            interval=iv, open_time_ge=load_ge, open_time_lt=load_lt
        )
        if not syms:
            raise RuntimeError(
                "No symbols found in market_data.ohlcv for the load window; check interval and open_time bounds"
            )

    raw = data_loader.load_intraday_ohlcv(
        symbols=None if symbols is None else syms,
        interval=iv,
        open_time_ge=load_ge,
        open_time_lt=load_lt,
    )
    daily = data_loader.aggregate_to_daily_bars(raw)
    l1_full = features_l1.compute_l1_features(daily)
    _coverage_ok(l1_full, syms, config.MIN_SYMBOL_COVERAGE)

    def _as_utc(ts: object | None, fallback: pd.Timestamp) -> pd.Timestamp:
        if ts is None:
            return fallback
        t = pd.Timestamp(ts)
        if t.tzinfo is None:
            return t.tz_localize("UTC")
        return t.tz_convert("UTC")

    ts_series = pd.to_datetime(l1_full["bar_ts"], utc=True) if not l1_full.empty else None
    if use_explicit:
        default_lo = ts_series.min() if ts_series is not None and len(ts_series) else pd.Timestamp(0, tz="UTC")
        default_hi = ts_series.max() if ts_series is not None and len(ts_series) else pd.Timestamp(0, tz="UTC")
        out_lo = _as_utc(bar_ts_ge, default_lo)
        out_hi = _as_utc(bar_ts_le, default_hi)
    else:
        out_lo = _as_utc(bar_ts_ge, out_lo_def)
        out_hi = _as_utc(bar_ts_le, out_hi_def)

    l1 = _filter_bar_ts(l1_full, out_lo, out_hi)
    pre_full = signals_precombined.compute_precombined(l1_full)
    pre = _filter_bar_ts(pre_full, out_lo, out_hi)

    factors_full = signals_combined.compute_factor_scores(pre_full)
    factors = _filter_bar_ts(factors_full, out_lo, out_hi)

    xsecs_full = signals_xsection.compute_cross_section_ranks(factors_full)
    xsecs = _filter_bar_ts(xsecs_full, out_lo, out_hi)

    labels_full = labels_mod.compute_labels(l1_full)
    labels = _filter_bar_ts(labels_full, out_lo, out_hi)

    if persist:
        persistence.persist_all(
            l1=l1,
            pre=pre,
            factors=factors,
            xsecs=xsecs,
            labels=labels,
            pipeline_version=pipeline_version,
        )

    return PipelineResult(l1=l1, pre=pre, factors=factors, xsecs=xsecs, labels=labels)
