"""
Risk service: pre-trade rule layer between strategy_orders and risk_approved.

Consumes from strategy_orders, optionally runs rules, publishes approved orders
to risk_approved (OMS-compatible schema). No-rule interface first; add rules on demand.
See docs/risk/RISK_SERVICE_PLAN.md and PHASE2_DETAILED_PLAN §12.5.
"""
