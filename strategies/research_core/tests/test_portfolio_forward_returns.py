from __future__ import annotations

import pandas as pd
import pytest

from strategies.research_core.forward_returns import add_forward_returns
from strategies.research_core.portfolio import add_cross_sectional_ranks, add_equal_weight_by_bin, assign_bins


def _base_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-01-01",
                    "2026-01-01",
                    "2026-01-02",
                    "2026-01-02",
                ],
                utc=True,
            ),
            "symbol": ["A", "B", "A", "B"],
            "close": [100.0, 200.0, 110.0, 180.0],
            "signal_clean": [0.1, 0.9, 0.2, 0.8],
        }
    )


def test_rank_bin_and_weights() -> None:
    df = _base_df()
    ranked = add_cross_sectional_ranks(df, signal_col="signal_clean")
    binned = assign_bins(ranked, n_bins=2)
    weighted = add_equal_weight_by_bin(binned)
    by_group = weighted.groupby(["ts", "bin"])["weight"].sum()
    assert (by_group.round(8) == 1.0).all()


def test_forward_return_h1() -> None:
    df = _base_df()
    out = add_forward_returns(df, horizons=(1,))
    a_rows = out[out["symbol"] == "A"].reset_index(drop=True)
    assert a_rows.loc[0, "fwd_ret_1"] == pytest.approx(0.1)
    assert pd.isna(a_rows.loc[1, "fwd_ret_1"])

