from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from market_data.cmc import CmcClient
from market_data.config import UNIVERSE_CMC_TOP_N


def test_cmc_client_requires_key() -> None:
    with pytest.raises(ValueError):
        CmcClient(api_key="", base_url="https://pro-api.coinmarketcap.com")


@patch("market_data.cmc.requests.get")
def test_fetch_top_n_parses_members_and_timestamp(mock_get: MagicMock) -> None:
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {
        "status": {"timestamp": "2026-04-26T00:00:00.000Z"},
        "data": [
            {"symbol": "BTC", "cmc_rank": 1},
            {"symbol": "eth", "cmc_rank": "2"},
            {"symbol": "", "cmc_rank": 3},
        ],
    }

    c = CmcClient(api_key="k", base_url="https://pro-api.coinmarketcap.com")
    res = c.fetch_top_n_by_market_cap(n=UNIVERSE_CMC_TOP_N)
    assert res.as_of_date.isoformat() == "2026-04-26"
    assert [m.base_asset for m in res.members] == ["BTC", "ETH"]
    assert [m.rank for m in res.members] == [1, 2]

