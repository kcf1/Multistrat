"""
Broker adapter interface for OMS.

Adapters implement place_order and start_fill_listener so the OMS can route
orders and receive unified fill/reject events regardless of broker.
"""

from typing import Any, Callable, Dict, Protocol, runtime_checkable


@runtime_checkable
class BrokerAdapter(Protocol):
    """
    Interface for broker adapters.

    - place_order: submit order to broker; returns unified response or reject.
    - start_fill_listener: start receiving fill/reject events via callback (unified format).
    """

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place an order with the broker.

        Args:
            order: Order from risk_approved schema: order_id (internal), broker, account_id?,
                   symbol, side, quantity, order_type, price?, time_in_force?, book?, comment?

        Returns:
            On success: dict with broker_order_id, status, symbol, side, order_type,
                        quantity, executed_qty?, client_order_id (order_id), and optional
                        broker-specific fields (e.g. binance_transact_time).
            On reject: dict with "rejected": True, "order_id", "reject_reason".
        """
        ...

    def start_fill_listener(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        Start listening for fill and reject events; invoke callback for each.

        Callback receives unified event dict (event_type: 'fill' | 'reject', order_id,
        broker_order_id, symbol, side, quantity, price, fee, executed_at, fill_id, etc.).
        Typically runs in a background thread.
        """
        ...

    def cancel_order(self, broker_order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Cancel an open order by broker order id and symbol.

        Args:
            broker_order_id: Broker's order id (e.g. Binance orderId).
            symbol: Trading symbol (e.g. BTCUSDT).

        Returns:
            Unified response dict with status (e.g. CANCELED), broker_order_id, symbol,
            or dict with "rejected": True and "reject_reason" on failure.
        """
        ...
