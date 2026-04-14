"""
Internal DB asset price provider: read latest OHLCV close from Postgres.

Maps assets -> usd_symbol from assets table, then reads latest ohlcv.close for the
configured interval. Applies a staleness guard on ingested_at.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union

from pgconn import SCHEMA_MARKET_DATA, configure_for_pms
from pms.log import logger

from .interface import AssetPriceProvider


class OhlcvDbAssetPriceProvider(AssetPriceProvider):
    """Fetch USD prices from internal ohlcv table via latest close per symbol."""

    def __init__(
        self,
        pg_connect: Union[str, Callable[[], Any]],
        *,
        interval: str = "1h",
        max_staleness_seconds: int = 7200,
    ) -> None:
        self.pg_connect = pg_connect
        self.interval = (interval or "1h").strip()
        self.max_staleness_seconds = int(max_staleness_seconds)

    def _connect(self):
        if callable(self.pg_connect):
            conn = self.pg_connect()
        else:
            import psycopg2

            conn = psycopg2.connect(self.pg_connect)
        configure_for_pms(conn)
        return conn

    def _query_asset_to_symbol(self, conn, assets: List[str]) -> Dict[str, str]:
        if not assets:
            return {}
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT asset, usd_symbol
                FROM assets
                WHERE asset = ANY(%s)
                  AND usd_symbol IS NOT NULL
                  AND TRIM(usd_symbol) != ''
                """,
                (assets,),
            )
            out: Dict[str, str] = {}
            for row in cur.fetchall():
                asset = (row[0] or "").strip().upper()
                usd_symbol = (row[1] or "").strip().upper()
                if asset and usd_symbol:
                    out[asset] = usd_symbol
            return out

    def _query_latest_close_by_symbol(
        self,
        conn,
        symbols: List[str],
    ) -> Dict[str, tuple[datetime, Decimal, datetime]]:
        if not symbols:
            return {}
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (symbol)
                    symbol, open_time, close, ingested_at
                FROM {SCHEMA_MARKET_DATA}.ohlcv
                WHERE symbol = ANY(%s)
                  AND interval = %s
                ORDER BY symbol, open_time DESC
                """,
                (symbols, self.interval),
            )
            out: Dict[str, tuple[datetime, Decimal, datetime]] = {}
            for row in cur.fetchall():
                symbol = (row[0] or "").strip().upper()
                open_time = row[1]
                close_val = row[2]
                ingested_at = row[3]
                if not symbol or open_time is None or close_val is None or ingested_at is None:
                    continue
                if isinstance(open_time, datetime) and open_time.tzinfo is None:
                    open_time = open_time.replace(tzinfo=timezone.utc)
                if isinstance(ingested_at, datetime) and ingested_at.tzinfo is None:
                    ingested_at = ingested_at.replace(tzinfo=timezone.utc)
                out[symbol] = (open_time, close_val, ingested_at)
            return out

    def get_prices(self, assets: List[str]) -> Dict[str, Optional[float]]:
        if not assets:
            return {}
        want_assets = sorted({str(a).strip().upper() for a in assets if a and str(a).strip()})
        if not want_assets:
            return {}

        conn = self._connect()
        try:
            asset_to_symbol = self._query_asset_to_symbol(conn, want_assets)
            symbols = sorted({sym for sym in asset_to_symbol.values() if sym})
            latest = self._query_latest_close_by_symbol(conn, symbols)

            now_utc = datetime.now(timezone.utc)
            out: Dict[str, Optional[float]] = {}
            for asset in want_assets:
                symbol = asset_to_symbol.get(asset)
                if not symbol:
                    continue
                row = latest.get(symbol)
                if row is None:
                    continue
                open_time, close_val, ingested_at = row
                age_sec = (now_utc - ingested_at.astimezone(timezone.utc)).total_seconds()
                if age_sec > self.max_staleness_seconds:
                    logger.debug(
                        "Skip stale OHLCV price for asset {} symbol {} by ingested_at (age_s={}, open_time={})",
                        asset,
                        symbol,
                        round(age_sec, 3),
                        open_time,
                    )
                    continue
                try:
                    out[asset] = float(close_val)
                except (TypeError, ValueError):
                    continue
            return out
        finally:
            conn.close()
