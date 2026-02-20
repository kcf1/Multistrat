"""
Pydantic models for PMS: mark price results, config, and internal shapes.

Task 12.3.1b: Use Pydantic for mark price, internal shapes, snapshot writes.
See docs/pms/PMS_ARCHITECTURE.md §12. Config lives in pms/config.py.
"""

from decimal import Decimal
from typing import Dict, Optional

from pydantic import BaseModel, Field, field_validator


class MarkPricesResult(BaseModel):
    """
    Return type for mark price provider: symbol -> mark price (for unrealized PnL).

    Used by PMS when computing unrealized PnL. Missing symbols are omitted
    (caller may treat as no price / zero unrealized).
    """

    prices: Dict[str, Decimal] = Field(
        default_factory=dict,
        description="Symbol -> mark (or last) price",
    )

    @field_validator("prices", mode="before")
    @classmethod
    def coerce_prices(cls, v):
        if v is None:
            return {}
        if isinstance(v, dict):
            out = {}
            for k, val in v.items():
                if val is None:
                    continue
                if isinstance(val, (int, float, str)):
                    out[str(k).strip().upper()] = Decimal(str(val))
                elif isinstance(val, Decimal):
                    out[str(k).strip().upper()] = val
                else:
                    out[str(k).strip().upper()] = Decimal(str(val))
            return out
        return {}

    def get(self, symbol: str) -> Optional[Decimal]:
        """Return mark price for symbol (case-insensitive), or None if missing."""
        key = symbol.strip().upper()
        return self.prices.get(key)


# --- Binance API response models (mark price provider) ---


class BinanceTickerPriceItem(BaseModel):
    """Single item from Binance GET /api/v3/ticker/price (symbol + price)."""

    symbol: str = Field(..., min_length=1)
    price: str = Field(..., min_length=1)

    @field_validator("symbol", "price", mode="before")
    @classmethod
    def strip_str(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("price", mode="before")
    @classmethod
    def price_string(cls, v):
        if v is None:
            return "0"
        return str(v).strip() or "0"


# --- Placeholders for future PMS modules (12.3.4+); document shape here ---
# DerivedPosition: account_id, book, symbol, side, open_qty, entry_avg, ...
# PnlSnapshot: account_id, book?, realized_pnl, unrealized_pnl, mark_price_used, timestamp
# MarginSnapshot: account_id, total_margin, available_balance, timestamp
# Redis pnl:{account_id} / margin:{account_id} can validate with these before write.
