"""Column contracts for Phase 1 tables (single source for persistence and tests)."""

from __future__ import annotations

from typing import Final

from . import config

COMMON_META: Final[frozenset[str]] = frozenset(
    ("created_at", "updated_at", "pipeline_version", "source")
)

L1FEATS_WRITE_ORDER: Final[tuple[str, ...]] = (
    "bar_ts",
    "symbol",
    "close",
    "volume",
    "quote_volume",
    "taker_buy_base_volume",
    "log_close",
    "log_return",
    "ewvol_20",
    "norm_return",
    "norm_close",
    "market_return",
    "market_ewvol_20",
    "beta_250",
    "resid_return",
    "log_volume",
    "log_quote_volume",
    "vwap_250",
)

L1FEATS_COLUMNS: Final[frozenset[str]] = frozenset(L1FEATS_WRITE_ORDER) | COMMON_META


def _precombined_names() -> tuple[str, ...]:
    specs = [
        ("mom", (10, 20, 40)),
        ("trend", (5, 10, 20)),
        ("breakout", (10, 20, 40)),
        ("vwaprev", (5, 10, 20)),
        ("resrev", (5, 10, 20)),
        ("skew", (10, 20, 40)),
        ("vol", (10, 20, 40)),
        ("betasq", (10, 20, 40)),
        ("maxret", (10, 20, 40)),
        ("takerratio", (10, 20, 40)),
        ("vlm", (10, 20, 40)),
        ("quotevlm", (10, 20, 40)),
        ("retvlmcor", (10, 20, 40)),
    ]
    out: list[str] = []
    for abbr, ns in specs:
        for n in ns:
            out.append(f"{abbr}_{n}")
    return tuple(out)


PRECOMBINED_COLUMNS: Final[tuple[str, ...]] = _precombined_names()

SIGNALS_WRITE_ORDER: Final[tuple[str, ...]] = ("bar_ts", "symbol") + PRECOMBINED_COLUMNS + ("n_nonfinite",)

SIGNALS_DAILY_COLUMNS: Final[frozenset[str]] = (
    frozenset(SIGNALS_WRITE_ORDER) | COMMON_META
)

FACTORS_SCORE_COLUMNS: Final[tuple[str, ...]] = (
    "mom_score",
    "trend_score",
    "breakout_score",
    "vwaprev_score",
    "resrev_score",
    "skew_score",
    "vol_score",
    "betasq_score",
    "maxret_score",
    "takerratio_score",
    "vlm_score",
    "quotevlm_score",
    "retvlmcor_score",
)

FACTORS_WRITE_ORDER: Final[tuple[str, ...]] = ("bar_ts", "symbol") + FACTORS_SCORE_COLUMNS + ("n_nonfinite",)

FACTORS_DAILY_COLUMNS: Final[frozenset[str]] = frozenset(FACTORS_WRITE_ORDER) | COMMON_META

XSECS_RANK_COLUMNS: Final[tuple[str, ...]] = (
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

XSECS_WRITE_ORDER: Final[tuple[str, ...]] = ("bar_ts", "symbol") + XSECS_RANK_COLUMNS + ("n_symbols_xs",)

XSECS_DAILY_COLUMNS: Final[frozenset[str]] = frozenset(XSECS_WRITE_ORDER) | COMMON_META


def _label_value_columns() -> frozenset[str]:
    cols: set[str] = set()
    for h in config.DEFAULT_LABEL_HORIZONS:
        cols.add(f"logret_fwd_{h}")
        cols.add(f"simpret_fwd_{h}")
        cols.add(f"normret_fwd_{h}")
        cols.add(f"vol_weight_{h}")
        cols.add(f"vol_weighted_return_{h}")
    return frozenset(cols)


def _labels_write_order() -> tuple[str, ...]:
    head = ("bar_ts", "symbol", "label_asof_ts", "bar_ts_venue")
    tail: list[str] = []
    for h in config.DEFAULT_LABEL_HORIZONS:
        tail.extend(
            [
                f"logret_fwd_{h}",
                f"simpret_fwd_{h}",
                f"normret_fwd_{h}",
                f"vol_weight_{h}",
                f"vol_weighted_return_{h}",
            ]
        )
    return head + tuple(tail)


LABELS_WRITE_ORDER: Final[tuple[str, ...]] = _labels_write_order()

LABELS_DAILY_COLUMNS: Final[frozenset[str]] = (
    frozenset(("bar_ts", "symbol", "label_asof_ts", "bar_ts_venue")) | _label_value_columns() | COMMON_META
)


def assert_columns_subset(table: str, columns: frozenset[str], df_columns: list[str] | set[str]) -> None:
    extra = set(df_columns) - columns
    if extra:
        raise ValueError(f"{table}: unknown columns {sorted(extra)[:20]}{'...' if len(extra) > 20 else ''}")


def assert_required_present(table: str, required: frozenset[str], df_columns: set[str]) -> None:
    missing = required - df_columns
    if missing:
        raise ValueError(f"{table}: missing required columns {sorted(missing)}")
