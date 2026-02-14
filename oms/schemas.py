"""
OMS Redis stream schemas (task 12.1.5).

Defines risk_approved (input) and oms_fills (output) message shapes.
Aligns with PHASE2_DETAILED_PLAN §4.2 and §5.2.
Streams are created on first XADD.
"""

# risk_approved: order to execute (produced by Risk, consumed by OMS)
RISK_APPROVED_STREAM = "risk_approved"
RISK_APPROVED_FIELDS = (
    "order_id",      # internal UUID or string; optional, OMS can generate
    "broker",        # e.g. "binance" — OMS selects adapter by this
    "account_id",    # optional; default account if single
    "symbol",
    "side",          # BUY | SELL
    "quantity",
    "order_type",    # MARKET, LIMIT, etc.
    "price",         # optional; for LIMIT
    "time_in_force", # optional; GTC, IOC, FOK
    "book",          # optional — strategy/book identifier
    "comment",       # optional — freetext
    "strategy_id",   # optional
    "created_at",    # optional ISO
)

# cancel_requested: request to cancel an order (produced by Risk/admin, consumed by OMS)
# At least one of order_id (internal) or (broker_order_id + symbol) required; broker for adapter routing.
CANCEL_REQUESTED_STREAM = "cancel_requested"
CANCEL_REQUESTED_FIELDS = (
    "order_id",         # internal OMS order_id (optional if broker_order_id + symbol provided)
    "broker_order_id",  # broker's order id (optional if order_id provided)
    "symbol",           # e.g. BTCUSDT (required if using broker_order_id)
    "broker",           # e.g. "binance" — OMS selects adapter by this
)

# oms_fills: fill or reject event (produced by OMS, consumed by Booking)
OMS_FILLS_STREAM = "oms_fills"
OMS_FILLS_FIELDS = (
    "event_type",      # "fill" | "reject"
    "order_id",        # internal
    "broker_order_id",
    "symbol",
    "side",
    "quantity",
    "price",
    "fee",
    "fee_asset",
    "executed_at",     # ISO
    "fill_id",         # broker trade id
    "reject_reason",   # for rejections
    "book",            # pass-through from order
    "comment",         # pass-through from order
)
