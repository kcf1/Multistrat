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


# --- Order row (from Postgres) and derived position (12.3.3–12.3.4) ---
# Quantity from orders uses executed_quantity (source: DB column executed_qty).


class OrderRow(BaseModel):
    """One order row from Postgres for position derivation. Uses executed_quantity (from orders.executed_qty)."""

    account_id: str = Field(..., description="Broker account id (matches orders.account_id)")
    book: str = Field("", description="Strategy/book identifier")
    broker: str = Field("", description="Broker name (e.g. binance)")
    symbol: str = Field(..., description="Trading symbol")
    side: str = Field(..., description="BUY or SELL")
    executed_quantity: float = Field(0.0, ge=0, description="Filled quantity (from orders.executed_qty)")
    price: Optional[float] = Field(None, ge=0, description="Executed (average fill) price")
    created_at: Optional[str] = Field(None, description="Order creation timestamp")

    @field_validator("executed_quantity", mode="before")
    @classmethod
    def coerce_executed_quantity(cls, v):
        if v is None:
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0


class DerivedPosition(BaseModel):
    """One derived position at grain (broker, account_id, book, asset). Written to positions table."""

    broker: str = Field("", description="Broker name (e.g. binance); grain is (broker, account_id, book, asset)")
    account_id: str = Field(..., description="Broker account id")
    book: str = Field("", description="Book identifier")
    asset: str = Field("", description="Asset (e.g. BTC, USDT); grain is (broker, account_id, book, asset)")
    open_qty: float = Field(0.0, description="Signed net quantity: positive = long, negative = short")
    position_side: str = Field("flat", description="'long' | 'short' | 'flat'")
    usd_value: Optional[float] = Field(None, ge=0, description="Price per unit of asset in USD (set by enrichment)")
