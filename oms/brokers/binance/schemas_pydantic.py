"""
Pydantic models for Binance WebSocket execution report events (task 12.1.17).

Defines validated models for raw Binance events and unified fill/reject/cancelled/expired events.
"""

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class BinanceExecutionReport(BaseModel):
    """
    Pydantic model for raw Binance executionReport WebSocket event.

    Fields match Binance API documentation for executionReport events.
    """

    e: str = Field(..., description="Event type (should be 'executionReport')")
    x: Optional[str] = Field(None, description="Execution type: NEW, CANCELED, TRADE, REJECTED, EXPIRED")
    X: Optional[str] = Field(None, description="Order status: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED")
    c: Optional[str] = Field(None, description="Client order ID")
    i: Optional[int] = Field(None, description="Order ID")
    s: Optional[str] = Field(None, description="Symbol")
    S: Optional[str] = Field(None, description="Side (BUY/SELL)")
    q: Optional[str] = Field(None, description="Order quantity")
    p: Optional[str] = Field(None, description="Order price")
    l: Optional[str] = Field(None, description="Last executed quantity (for TRADE)")
    L: Optional[str] = Field(None, description="Last executed price (for TRADE)")
    z: Optional[str] = Field(None, description="Cumulative executed quantity")
    t: Optional[int] = Field(None, description="Trade ID")
    T: Optional[int] = Field(None, description="Transaction time (ms)")
    n: Optional[str] = Field(None, description="Commission amount")
    N: Optional[str] = Field(None, description="Commission asset")
    r: Optional[str] = Field(None, description="Reject reason")

    @field_validator("e")
    @classmethod
    def validate_event_type(cls, v):
        """Ensure event type is executionReport."""
        if v != "executionReport":
            raise ValueError(f"expected 'executionReport', got '{v}'")
        return v


class FillEvent(BaseModel):
    """
    Unified fill event model (from Binance TRADE execution reports).

    Used internally by OMS for fill callbacks.
    """

    event_type: Literal["fill"] = "fill"
    order_id: str = Field(..., min_length=1, description="Internal order ID (clientOrderId)")
    broker_order_id: str = Field(..., description="Broker's order ID")
    symbol: str = Field(..., min_length=1, description="Trading symbol")
    side: str = Field(..., min_length=1, description="Order side (BUY/SELL)")
    quantity: float = Field(..., gt=0, description="Fill quantity (must be positive)")
    price: float = Field(..., ge=0, description="Fill price")
    fee: float = Field(0.0, ge=0, description="Fee amount")
    fee_asset: Optional[str] = Field(None, description="Fee asset symbol")
    executed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), description="Execution timestamp ISO8601")
    fill_id: str = Field("", description="Broker trade ID")
    order_status: Optional[str] = Field(None, description="Binance order status (PARTIALLY_FILLED, FILLED)")
    executed_qty_cumulative: Optional[float] = Field(None, ge=0, description="Cumulative executed quantity")

    @field_validator("order_id", "broker_order_id", "symbol", "side", "executed_at", "fill_id", mode="before")
    @classmethod
    def string_fields(cls, v):
        """Ensure string fields are strings."""
        if v is None:
            return "" if cls.__name__ != "executed_at" else ""
        return str(v).strip() if isinstance(v, str) else str(v)

    @field_validator("quantity", "price", "fee", "executed_qty_cumulative", mode="before")
    @classmethod
    def parse_numeric(cls, v):
        """Parse numeric fields."""
        if v is None:
            return None if cls.__name__ == "executed_qty_cumulative" else 0.0
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None if cls.__name__ == "executed_qty_cumulative" else 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return None if cls.__name__ == "executed_qty_cumulative" else 0.0

    def model_dump_dict(self) -> dict:
        """Convert model to dict compatible with existing code."""
        return self.model_dump(exclude_none=False)


class RejectEvent(BaseModel):
    """
    Unified reject event model (from Binance REJECTED execution reports).
    """

    event_type: Literal["reject"] = "reject"
    order_id: str = Field(..., description="Internal order ID (can be empty)")
    broker_order_id: str = Field(..., description="Broker's order ID")
    symbol: str = Field(..., min_length=1, description="Trading symbol")
    side: str = Field(..., min_length=1, description="Order side (BUY/SELL)")
    quantity: float = Field(0.0, ge=0, description="Order quantity")
    price: float = Field(0.0, ge=0, description="Order price")
    fee: float = Field(0.0, ge=0, description="Fee (0 for rejects)")
    fee_asset: Optional[str] = Field(None, description="Fee asset (None for rejects)")
    executed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), description="Rejection timestamp ISO8601")
    fill_id: str = Field("", description="Fill ID (empty for rejects)")
    reject_reason: str = Field(..., min_length=1, description="Rejection reason")

    @field_validator("order_id", "broker_order_id", "symbol", "side", "executed_at", "fill_id", "reject_reason", mode="before")
    @classmethod
    def string_fields(cls, v):
        """Ensure string fields are strings."""
        if v is None:
            return "" if cls.__name__ != "reject_reason" else "REJECTED"
        return str(v).strip() if isinstance(v, str) else str(v)

    @field_validator("quantity", "price", "fee", mode="before")
    @classmethod
    def parse_numeric(cls, v):
        """Parse numeric fields."""
        if v is None or v == "":
            return 0.0
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    def model_dump_dict(self) -> dict:
        """Convert model to dict compatible with existing code."""
        return self.model_dump(exclude_none=False)


class CancelledEvent(BaseModel):
    """
    Unified cancelled event model (from Binance CANCELED execution reports).
    """

    event_type: Literal["cancelled"] = "cancelled"
    order_id: str = Field(..., description="Internal order ID (can be empty)")
    broker_order_id: str = Field(..., description="Broker's order ID")
    symbol: str = Field(..., min_length=1, description="Trading symbol")
    side: str = Field(..., min_length=1, description="Order side (BUY/SELL)")
    quantity: float = Field(0.0, ge=0, description="Order quantity")
    price: float = Field(0.0, ge=0, description="Order price")
    fee: float = Field(0.0, ge=0, description="Fee (0 for cancelled)")
    fee_asset: Optional[str] = Field(None, description="Fee asset (None for cancelled)")
    executed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), description="Cancellation timestamp ISO8601")
    fill_id: str = Field("", description="Fill ID (empty for cancelled)")
    reject_reason: str = Field("CANCELED", description="Cancellation reason")

    @field_validator("order_id", "broker_order_id", "symbol", "side", "executed_at", "fill_id", "reject_reason", mode="before")
    @classmethod
    def string_fields(cls, v):
        """Ensure string fields are strings."""
        if v is None:
            return "" if cls.__name__ != "reject_reason" else "CANCELED"
        return str(v).strip() if isinstance(v, str) else str(v)

    @field_validator("quantity", "price", "fee", mode="before")
    @classmethod
    def parse_numeric(cls, v):
        """Parse numeric fields."""
        if v is None or v == "":
            return 0.0
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    def model_dump_dict(self) -> dict:
        """Convert model to dict compatible with existing code."""
        return self.model_dump(exclude_none=False)


class ExpiredEvent(BaseModel):
    """
    Unified expired event model (from Binance EXPIRED execution reports).
    """

    event_type: Literal["expired"] = "expired"
    order_id: str = Field(..., description="Internal order ID (can be empty)")
    broker_order_id: str = Field(..., description="Broker's order ID")
    symbol: str = Field(..., min_length=1, description="Trading symbol")
    side: str = Field(..., min_length=1, description="Order side (BUY/SELL)")
    quantity: float = Field(0.0, ge=0, description="Order quantity")
    price: float = Field(0.0, ge=0, description="Order price")
    fee: float = Field(0.0, ge=0, description="Fee (0 for expired)")
    fee_asset: Optional[str] = Field(None, description="Fee asset (None for expired)")
    executed_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"), description="Expiration timestamp ISO8601")
    fill_id: str = Field("", description="Fill ID (empty for expired)")
    reject_reason: str = Field("EXPIRED", description="Expiration reason")

    @field_validator("order_id", "broker_order_id", "symbol", "side", "executed_at", "fill_id", "reject_reason", mode="before")
    @classmethod
    def string_fields(cls, v):
        """Ensure string fields are strings."""
        if v is None:
            return "" if cls.__name__ != "reject_reason" else "EXPIRED"
        return str(v).strip() if isinstance(v, str) else str(v)

    @field_validator("quantity", "price", "fee", mode="before")
    @classmethod
    def parse_numeric(cls, v):
        """Parse numeric fields."""
        if v is None or v == "":
            return 0.0
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    def model_dump_dict(self) -> dict:
        """Convert model to dict compatible with existing code."""
        return self.model_dump(exclude_none=False)
