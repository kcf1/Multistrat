"""Contract helpers."""

from __future__ import annotations

import pytest

from strategies.modules.factor_ls.validators import assert_columns_subset, L1FEATS_COLUMNS


def test_rejects_unknown_columns():
    with pytest.raises(ValueError, match="unknown"):
        assert_columns_subset("t", L1FEATS_COLUMNS, ["bar_ts", "symbol", "not_a_column"])
