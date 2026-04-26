"""Micro-config for the daily double-sort pipeline (see env-and-config.mdc for DB URLs)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Final, Sequence

PIPELINE_VERSION: Final[str] = "1.0.0"

# Basket for L1 ``market_return`` (all four must have finite ``norm_return`` that day).
MARKET_BASKET: Final[tuple[str, ...]] = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT")

# Intraday interval stored in ``market_data.ohlcv`` used to build daily bars.
DEFAULT_OHLCV_INTERVAL: Final[str] = "1h"

SCHEMA_STRATEGIES_DAILY: Final[str] = "strategies_daily"
SCHEMA_MARKET_DATA: Final[str] = "market_data"

# Wide label columns use these integer suffixes (must match Alembic ``labels_daily`` migration).
DEFAULT_LABEL_HORIZONS: Final[tuple[int, ...]] = (1, 5, 10)

# Production batch: number of completed daily bars to persist per scheduled run.
PRODUCTION_OUTPUT_BARS: Final[int] = 24

# Extra calendar days of intraday history loaded before the output window (warmup for L1).
WARMUP_CALENDAR_DAYS: Final[int] = 320

# Fail-fast: minimum fraction of configured symbols with a non-null daily row per ``bar_ts`` (0–1).
MIN_SYMBOL_COVERAGE: Final[float] = 0.85

# Notebook-style inverse-vol weight numerator: ``vol_weight = VOL_WEIGHT_NUM / ewvol_20``.
VOL_WEIGHT_NUMERATOR: Final[float] = 0.90 / (250**0.5)

# Optional feature flags
PERSIST_SIGNALS_PRECOMBINED: Final[bool] = True
INCLUDE_SIMPRET_IN_LABELS: Final[bool] = True


@dataclass(frozen=True)
class RunWindow:
    """Inclusive ``bar_ts`` calendar range (UTC midnight timestamps) for one pipeline run."""

    bar_ts_start: object  # pd.Timestamp
    bar_ts_end: object

    def __post_init__(self) -> None:
        import pandas as pd

        if not isinstance(self.bar_ts_start, pd.Timestamp):
            object.__setattr__(self, "bar_ts_start", pd.Timestamp(self.bar_ts_start))
        if not isinstance(self.bar_ts_end, pd.Timestamp):
            object.__setattr__(self, "bar_ts_end", pd.Timestamp(self.bar_ts_end))


def production_bar_ts_range(
    run_at_utc: object,
    *,
    output_bars: int = PRODUCTION_OUTPUT_BARS,
) -> tuple[object, object, object, object]:
    """
    Return ``(load_open_start, load_open_end_exclusive, first_bar_ts, last_bar_ts)``.

    When the job runs at ``run_at_utc`` (default interpretation: UTC), the last completed
    daily key is the **previous** UTC calendar date at midnight. The batch contains
    ``output_bars`` consecutive ``bar_ts`` values ending there. Intraday load uses
    ``open_time`` in ``[load_open_start, load_open_end_exclusive)`` plus warmup days before
    ``first_bar_ts``.
    """
    import pandas as pd

    run = pd.Timestamp(run_at_utc)
    if run.tzinfo is None:
        run = run.tz_localize("UTC")
    else:
        run = run.tz_convert("UTC")

    last_bar_date = (run.normalize() - timedelta(days=1)).date()
    last_bar_ts = pd.Timestamp(last_bar_date, tz="UTC")
    first_bar_ts = last_bar_ts - timedelta(days=output_bars - 1)

    load_open_start = first_bar_ts - timedelta(days=WARMUP_CALENDAR_DAYS)
    load_open_end_exclusive = last_bar_ts + timedelta(days=1)

    return load_open_start, load_open_end_exclusive, first_bar_ts, last_bar_ts


def symbols_from_env(default: Sequence[str] | None = None) -> tuple[str, ...]:
    """Comma-separated ``DOUBLE_SORT_DAILY_SYMBOLS`` or ``default``."""
    import os

    raw = os.environ.get("DOUBLE_SORT_DAILY_SYMBOLS", "").strip()
    if raw:
        return tuple(s.strip().upper() for s in raw.split(",") if s.strip())
    if default:
        return tuple(s.upper() for s in default)
    return MARKET_BASKET
