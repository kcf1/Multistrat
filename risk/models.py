"""
Pydantic models for risk service streams.

OrderIntent: input from strategy_orders (same schema as OMS risk_approved).
Output to risk_approved uses the same field set so OMS can parse it.
"""

from typing import Optional

from pydantic import BaseModel, Field, field_validator


class OrderIntent(BaseModel):
    """
    Order intent from strategy_orders; same schema as OMS risk_approved input.

    Required: broker, symbol, side, quantity.
    Optional: order_id, account_id, order_type, limit_price (or 'price' in stream),
    time_in_force, book, comment, strategy_id, created_at.
    """

    broker: str = Field(..., min_length=1, description="Broker name (e.g. 'binance')")
    symbol: str = Field(..., min_length=1, description="Trading symbol (e.g. 'BTCUSDT')")
    side: str = Field(..., min_length=1, description="Order side (BUY or SELL)")
    quantity: float = Field(..., gt=0, description="Order quantity (must be positive)")
    order_id: Optional[str] = Field(None, description="Internal order ID (optional)")
    account_id: str = Field("", description="Account ID (optional)")
    order_type: str = Field("MARKET", description="Order type (MARKET, LIMIT, etc.)")
    limit_price: Optional[float] = Field(None, ge=0, description="Limit price for LIMIT orders")
    time_in_force: Optional[str] = Field(None, description="Time in force (GTC, IOC, FOK)")
    book: str = Field("", description="Strategy/book identifier (optional)")
    comment: str = Field("", description="Freetext comment (optional)")
    strategy_id: Optional[str] = Field(None, description="Strategy ID (optional)")
    created_at: Optional[str] = Field(None, description="Creation timestamp ISO8601 (optional)")

    @field_validator("broker", "symbol", "side", mode="before")
    @classmethod
    def strip_strings(cls, v):
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("account_id", "book", "comment", mode="before")
    @classmethod
    def empty_string_default(cls, v):
        if v is None:
            return ""
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("quantity", mode="before")
    @classmethod
    def parse_quantity(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if not v:
                raise ValueError("quantity cannot be empty")
        if v is None:
            raise ValueError("quantity is required")
        try:
            return float(v)
        except (TypeError, ValueError) as e:
            raise ValueError(f"invalid quantity: {v}") from e

    @field_validator("limit_price", mode="before")
    @classmethod
    def parse_limit_price(cls, v):
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def to_risk_approved_fields(self) -> dict:
        """
        Convert to dict suitable for XADD to risk_approved (OMS-compatible).

        Keys and values match what OMS consumer expects; numeric/optional
        fields are stringified by the producer.
        """
        out = {
            "broker": self.broker,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": str(self.quantity),
            "order_type": self.order_type,
            "account_id": self.account_id or "",
            "book": self.book or "",
            "comment": self.comment or "",
        }
        if self.order_id is not None:
            out["order_id"] = self.order_id
        if self.limit_price is not None:
            out["price"] = str(self.limit_price)  # OMS maps 'price' -> limit_price
        if self.time_in_force:
            out["time_in_force"] = self.time_in_force
        if self.strategy_id is not None:
            out["strategy_id"] = self.strategy_id
        if self.created_at is not None:
            out["created_at"] = self.created_at
        return out
