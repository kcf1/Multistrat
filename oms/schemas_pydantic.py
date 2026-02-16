"""
Pydantic models for Redis Stream message schemas (task 12.1.15).

Defines validated models for risk_approved, cancel_requested, and oms_fills streams.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class RiskApprovedOrder(BaseModel):
    """
    Pydantic model for risk_approved stream messages.

    Required fields: broker, symbol, side, quantity.
    Optional fields: order_id, account_id, order_type, limit_price (from 'price' field),
    time_in_force, book, comment, strategy_id, created_at.
    """

    broker: str = Field(..., min_length=1, description="Broker name (e.g. 'binance')")
    symbol: str = Field(..., min_length=1, description="Trading symbol (e.g. 'BTCUSDT')")
    side: str = Field(..., min_length=1, description="Order side (BUY or SELL)")
    quantity: float = Field(..., gt=0, description="Order quantity (must be positive)")
    order_id: Optional[str] = Field(None, description="Internal order ID (optional, OMS can generate)")
    account_id: str = Field("", description="Account ID (optional, defaults to empty)")
    order_type: str = Field("MARKET", description="Order type (MARKET, LIMIT, etc.)")
    limit_price: Optional[float] = Field(None, ge=0, description="Limit price for LIMIT orders (from 'price' field)")
    time_in_force: Optional[str] = Field(None, description="Time in force (GTC, IOC, FOK)")
    book: str = Field("", description="Strategy/book identifier (optional)")
    comment: str = Field("", description="Freetext comment (optional)")
    strategy_id: Optional[str] = Field(None, description="Strategy ID (optional)")
    created_at: Optional[str] = Field(None, description="Creation timestamp ISO8601 (optional)")

    @field_validator("broker", "symbol", "side", mode="before")
    @classmethod
    def strip_strings(cls, v):
        """Strip whitespace from string fields."""
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("account_id", "book", "comment", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        """Convert empty strings to empty string (not None) for optional string fields."""
        if v is None:
            return ""
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("quantity", mode="before")
    @classmethod
    def parse_quantity(cls, v):
        """Parse quantity string to float."""
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
        """Parse limit_price string to float or None."""
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

    def model_dump_dict(self) -> dict:
        """Convert model to dict compatible with existing code."""
        result = self.model_dump(exclude_none=False)
        # Remove None values for optional fields that should be omitted (not present in dict)
        # Keep empty strings for account_id, book, comment
        optional_fields_to_remove = ["order_id", "time_in_force", "strategy_id", "created_at"]
        for field in optional_fields_to_remove:
            if result.get(field) is None:
                result.pop(field, None)
        return result


class CancelRequest(BaseModel):
    """
    Pydantic model for cancel_requested stream messages.

    Requires: broker AND (order_id OR (broker_order_id AND symbol)).
    """

    broker: str = Field(..., min_length=1, description="Broker name (e.g. 'binance')")
    order_id: Optional[str] = Field(None, description="Internal order ID (required if broker_order_id not provided)")
    broker_order_id: Optional[str] = Field(None, description="Broker's order ID (required if order_id not provided)")
    symbol: Optional[str] = Field(None, description="Trading symbol (required if broker_order_id provided)")

    @field_validator("broker", mode="before")
    @classmethod
    def strip_strings(cls, v):
        """Strip whitespace from string fields."""
        if isinstance(v, str):
            return v.strip()
        return v

    @field_validator("order_id", "broker_order_id", "symbol", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        """Convert empty strings to None for optional fields."""
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            return v if v else None
        return v

    @model_validator(mode="after")
    def validate_cancel_request(self):
        """Validate that either order_id OR (broker_order_id AND symbol) is provided."""
        if self.order_id:
            return self
        if self.broker_order_id and self.symbol:
            return self
        # Raise ValueError with a clear message that will be caught and converted to CancelRequestParseError
        raise ValueError("need order_id or (broker_order_id and symbol)")

    def model_dump_dict(self) -> dict:
        """Convert model to dict compatible with existing code."""
        # Include None values for optional fields (tests expect them)
        result = self.model_dump(exclude_none=False)
        return result


class OmsFillEvent(BaseModel):
    """
    Pydantic model for oms_fills stream messages.

    Required fields: event_type, order_id, broker_order_id, symbol, side, quantity.
    Optional fields: price, fee, fee_asset, executed_at, fill_id, reject_reason, book, comment.
    """

    event_type: Literal["fill", "reject", "cancelled", "expired"] = Field(..., description="Event type")
    order_id: str = Field(..., min_length=1, description="Internal order ID")
    broker_order_id: str = Field(..., description="Broker's order ID (can be empty string)")
    symbol: str = Field(..., min_length=1, description="Trading symbol")
    side: str = Field(..., min_length=1, description="Order side (BUY or SELL)")
    quantity: float = Field(..., description="Fill quantity (can be 0 for rejects)")
    price: Optional[float] = Field(None, ge=0, description="Fill price (executed price for fills)")
    fee: float = Field(0.0, description="Fee amount")
    fee_asset: Optional[str] = Field(None, description="Fee asset symbol")
    executed_at: str = Field("", description="Execution timestamp ISO8601")
    fill_id: str = Field("", description="Broker trade ID (can be empty)")
    reject_reason: str = Field("", description="Rejection reason (for rejects)")
    book: str = Field("", description="Strategy/book identifier (pass-through)")
    comment: str = Field("", description="Freetext comment (pass-through)")

    @field_validator("order_id", "broker_order_id", "symbol", "side", "executed_at", "fill_id", "reject_reason", "book", "comment", mode="before")
    @classmethod
    def string_fields(cls, v):
        """Ensure string fields are strings."""
        if v is None:
            return ""
        return str(v).strip() if isinstance(v, str) else str(v)

    @field_validator("quantity", "price", "fee", mode="before")
    @classmethod
    def parse_numeric(cls, v):
        """Parse numeric fields, defaulting to 0.0 or None."""
        if v is None or v == "":
            return None if cls.__name__ == "price" else 0.0
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None if cls.__name__ == "price" else 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return None if cls.__name__ == "price" else 0.0

    def model_dump_dict(self) -> dict:
        """Convert model to dict compatible with existing code."""
        result = self.model_dump(exclude_none=False)
        # Ensure all fields are present (even if None/empty) for Redis stream compatibility
        return result
