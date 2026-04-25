"""CoinMarketCap client for top-N universe refresh."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import requests

from market_data.universe_schemas import CmcTopNFetchResult, UniverseTopNMember


class CmcClient:
    def __init__(self, *, api_key: str, base_url: str) -> None:
        self.api_key = (api_key or "").strip()
        self.base_url = (base_url or "").strip().rstrip("/")
        if not self.api_key:
            raise ValueError("CmcClient requires api_key")
        if not self.base_url:
            raise ValueError("CmcClient requires base_url")

    def fetch_top_n_by_market_cap(
        self,
        *,
        n: int = 100,
        convert: str = "USD",
        timeout_seconds: int = 30,
    ) -> CmcTopNFetchResult:
        """
        Fetch CMC listings/latest and return (as_of_date, members) where members include
        (base_asset symbol, cmc_rank).
        """
        if n < 1:
            raise ValueError("n must be >= 1")
        url = f"{self.base_url}/v1/cryptocurrency/listings/latest"
        headers = {"X-CMC_PRO_API_KEY": self.api_key}
        params = {
            "start": 1,
            "limit": int(n),
            "sort": "market_cap",
            "sort_dir": "desc",
            "convert": (convert or "USD").strip().upper(),
        }
        resp = requests.get(url, headers=headers, params=params, timeout=timeout_seconds)
        resp.raise_for_status()
        payload: Any = resp.json()
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            raise ValueError("CMC response missing data list")

        members: list[UniverseTopNMember] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            sym = (row.get("symbol") or "").strip().upper()
            rank = row.get("cmc_rank")
            if not sym:
                continue
            try:
                r = int(rank)
            except (TypeError, ValueError):
                continue
            if r < 1:
                continue
            members.append(UniverseTopNMember(base_asset=sym, rank=r))

        # Treat CMC timestamp as "as_of_date" in UTC.
        as_of = date.today()
        status = payload.get("status") if isinstance(payload, dict) else None
        if isinstance(status, dict):
            ts = status.get("timestamp")
            if isinstance(ts, str) and ts.strip():
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    as_of = dt.astimezone(timezone.utc).date()
                except Exception:
                    pass

        # Sort by rank and trim to N in case of any anomalies.
        members = sorted(members, key=lambda m: m.rank)[: int(n)]
        return CmcTopNFetchResult(as_of_date=as_of, members=members)

