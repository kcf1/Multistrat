"""
Stream names and constants for the risk service.

Aligns with OMS: risk_approved is consumed by OMS; strategy_orders is produced
by strategies (or test inject). Schema of messages matches OMS risk_approved input.
"""

# Consumed by Risk (from strategies or test)
STRATEGY_ORDERS_STREAM = "strategy_orders"

# Produced by Risk, consumed by OMS
RISK_APPROVED_STREAM = "risk_approved"
