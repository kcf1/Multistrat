"""Orchestrate loader → L1 → signals → factors → xsec → labels → persist."""

from __future__ import annotations

import logging
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

log = logging.getLogger(__name__)


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


def _valid_bar_ts_min_distinct(
    l1: pd.DataFrame,
    symbols: Sequence[str],
    *,
    min_distinct: int,
) -> frozenset:
    """``bar_ts`` values with at least ``min(min_distinct, len(symbols))`` distinct symbols in ``l1``."""
    if l1.empty:
        return frozenset()
    n_sym = len(symbols)
    need = min(min_distinct, n_sym)
    valid: set[object] = set()
    skipped: list[tuple[object, int]] = []
    for ts, g in l1.groupby("bar_ts"):
        nu = int(g["symbol"].nunique())
        if nu >= need:
            valid.add(ts)
        else:
            skipped.append((ts, nu))
    for ts, nu in skipped:
        log.info("factor_ls skip bar_ts=%s distinct_symbols=%s (need %s)", ts, nu, need)
    all_ts = l1["bar_ts"].nunique()
    log.info(
        "factor_ls coverage: kept %s / %s bar_ts (MIN_DISTINCT_SYMBOLS_PER_BAR=%s, universe=%s)",
        len(valid),
        all_ts,
        min_distinct,
        n_sym,
    )
    return frozenset(valid)


def _filter_rows_bar_ts(df: pd.DataFrame, valid: frozenset) -> pd.DataFrame:
    if df.empty or not valid:
        return df.iloc[0:0].copy() if not df.empty else df
    return df[df["bar_ts"].isin(valid)].copy()


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
    quiet: bool = False,
) -> PipelineResult:
    """
    **Modes:**

    - **Production default:** pass ``run_at_utc`` (or omit to use current UTC). Loads warmup
      + 24 daily ``bar_ts`` keys per ``config.production_bar_ts_range`` (intended **00:00 UTC** run).
    - **Symbols:** pass ``symbols`` for a subset; omit (``None``) to load **every symbol**
      that appears in ``ohlcv`` for the intraday window (no env list).
    - **Explicit intraday window:** ``open_time_ge`` and ``open_time_lt`` on ``ohlcv.open_time``.
      If ``bar_ts_ge`` / ``bar_ts_le`` are omitted, all daily ``bar_ts`` from that window are output.

    ``bar_ts`` with fewer than ``min(MIN_DISTINCT_SYMBOLS_PER_BAR, len(syms))`` symbols are **skipped**
    (logged, dropped from persisted frames). Features are still computed on the full daily series first.
    """
    _log = log.info if not quiet else lambda *a, **k: None

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

    _log("factor_ls step: resolve window open_time_ge=%s open_time_lt=%s interval=%s", load_ge, load_lt, iv)

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

    _log("factor_ls step: universe size=%s (distinct symbols in OHLCV window)", len(syms))

    raw = data_loader.load_intraday_ohlcv(
        symbols=None if symbols is None else syms,
        interval=iv,
        open_time_ge=load_ge,
        open_time_lt=load_lt,
    )
    _log("factor_ls step: loaded intraday rows=%s", len(raw))

    daily = data_loader.aggregate_to_daily_bars(raw)
    _log("factor_ls step: daily bars rows=%s", len(daily))

    l1_full = features_l1.compute_l1_features(daily)
    _log(
        "factor_ls step: L1 rows=%s bar_ts unique=%s",
        len(l1_full),
        l1_full["bar_ts"].nunique() if not l1_full.empty else 0,
    )
    if l1_full.empty:
        raise RuntimeError("L1 frame is empty after load; cannot continue")

    valid_bar_ts = _valid_bar_ts_min_distinct(
        l1_full,
        syms,
        min_distinct=config.MIN_DISTINCT_SYMBOLS_PER_BAR,
    )
    if not valid_bar_ts and not l1_full.empty:
        raise RuntimeError(
            "All bar_ts dropped by MIN_DISTINCT_SYMBOLS_PER_BAR gate; widen data, symbols, or lower the constant."
        )

    pre_full = signals_precombined.compute_precombined(l1_full)
    _log("factor_ls step: precombined rows=%s", len(pre_full))

    factors_full = signals_combined.compute_factor_scores(pre_full)
    _log("factor_ls step: factors rows=%s", len(factors_full))

    factors_for_xs = _filter_rows_bar_ts(factors_full, valid_bar_ts)
    xsecs_full = signals_xsection.compute_cross_section_ranks(factors_for_xs)
    _log("factor_ls step: xsecs rows=%s", len(xsecs_full))

    labels_full = labels_mod.compute_labels(l1_full)
    _log("factor_ls step: labels rows=%s", len(labels_full))

    l1_pass = _filter_rows_bar_ts(l1_full, valid_bar_ts)
    pre_pass = _filter_rows_bar_ts(pre_full, valid_bar_ts)
    factors_pass = _filter_rows_bar_ts(factors_full, valid_bar_ts)
    labels_pass = _filter_rows_bar_ts(labels_full, valid_bar_ts)

    def _as_utc(ts: object | None, fallback: pd.Timestamp) -> pd.Timestamp:
        if ts is None:
            return fallback
        t = pd.Timestamp(ts)
        if t.tzinfo is None:
            return t.tz_localize("UTC")
        return t.tz_convert("UTC")

    ts_series = pd.to_datetime(l1_pass["bar_ts"], utc=True) if not l1_pass.empty else None
    if use_explicit:
        default_lo = ts_series.min() if ts_series is not None and len(ts_series) else pd.Timestamp(0, tz="UTC")
        default_hi = ts_series.max() if ts_series is not None and len(ts_series) else pd.Timestamp(0, tz="UTC")
        out_lo = _as_utc(bar_ts_ge, default_lo)
        out_hi = _as_utc(bar_ts_le, default_hi)
    else:
        out_lo = _as_utc(bar_ts_ge, out_lo_def)
        out_hi = _as_utc(bar_ts_le, out_hi_def)

    _log("factor_ls step: output bar_ts filter [%s, %s]", out_lo, out_hi)

    l1 = _filter_bar_ts(l1_pass, out_lo, out_hi)
    pre = _filter_bar_ts(pre_pass, out_lo, out_hi)
    factors = _filter_bar_ts(factors_pass, out_lo, out_hi)
    xsecs = _filter_bar_ts(xsecs_full, out_lo, out_hi)
    labels = _filter_bar_ts(labels_pass, out_lo, out_hi)

    _log(
        "factor_ls step: after output filter L1=%s pre=%s factors=%s xsecs=%s labels=%s persist=%s",
        len(l1),
        len(pre),
        len(factors),
        len(xsecs),
        len(labels),
        persist,
    )

    if persist:
        _log("factor_ls step: persisting to strategies_daily …")
        persistence.persist_all(
            l1=l1,
            pre=pre,
            factors=factors,
            xsecs=xsecs,
            labels=labels,
            pipeline_version=pipeline_version,
        )
        _log("factor_ls step: persist done")

    return PipelineResult(l1=l1, pre=pre, factors=factors, xsecs=xsecs, labels=labels)
