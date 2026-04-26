"""Notebook §2.2 momentum formula parity on synthetic series."""

from __future__ import annotations

import numpy as np
import pandas as pd

from strategies.modules.factor_ls.signals_precombined import _ewm_sum_diff_mom


def test_mom_10_matches_notebook_style():
    rng = np.random.default_rng(0)
    x = pd.Series(rng.normal(size=80))
    got = _ewm_sum_diff_mom(x, 10)
    mom10m5 = (x.ewm(span=10, adjust=True).sum() - x.ewm(span=5, adjust=True).sum()) / 5
    pd.testing.assert_series_equal(got, mom10m5, check_names=False)
