"""Config and production window semantics."""

from __future__ import annotations

import pandas as pd

from strategies.modules.factor_ls import config
from strategies.modules.factor_ls.config import PRODUCTION_OUTPUT_BARS, production_bar_ts_range


def test_production_last_bar_ts_is_previous_utc_calendar_day():
    run = pd.Timestamp("2026-01-15 00:05:00", tz="UTC")
    _load_ge, _load_lt, first, last = production_bar_ts_range(run, output_bars=PRODUCTION_OUTPUT_BARS)
    assert last == pd.Timestamp("2026-01-14", tz="UTC")
    assert first == last - pd.Timedelta(days=PRODUCTION_OUTPUT_BARS - 1)


def test_production_load_window_covers_output_and_warmup():
    run = pd.Timestamp("2026-06-01 00:00:00", tz="UTC")
    load_ge, load_lt, first, last = production_bar_ts_range(run)
    assert load_lt == last + pd.Timedelta(days=1)
    assert load_ge < first


def test_plan_coverage_and_vol_weight_constants():
    assert config.MIN_DISTINCT_SYMBOLS_PER_BAR == 50
    assert abs(config.VOL_WEIGHT_NUMERATOR - 0.50 / (250**0.5)) < 1e-15
    assert config.INCLUDE_SIMPRET_IN_LABELS is False
