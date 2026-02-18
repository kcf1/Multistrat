"""
Broker adapter interface for OMS.

Adapters implement place_order and start_fill_listener so the OMS can route
orders and receive unified fill/reject events regardless of broker.

Adapters also support account management: start_account_listener for account/balance/position
events from broker user data streams, and get_account_snapshot for periodic REST refresh.
"""

from typing import Any, Callable, Dict, List, Optional, Protocol, runtime_checkable


# Unified account event shape (used by account listener callback and get_account_snapshot)
AccountEvent = Dict[str, Any]
"""
Unified account event shape for account listener callbacks and get_account_snapshot return.

Structure:
    {
        "event_type": str,  # e.g. "balance_update", "account_position"
        "broker": str,  # e.g. "binance"
        "account_id": str,  # Broker account identifier
        "balances": List[Dict[str, Any]],  # List of balance dicts: [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}, ...]
        "positions": List[Dict[str, Any]],  # List of position dicts: [{"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"}, ...]
        "updated_at": str,  # ISO timestamp
        "payload": Dict[str, Any],  # Raw broker event blob (for repairs)
    }

Balance dict structure:
    {
        "asset": str,  # Asset symbol (e.g. "USDT", "BTC")
        "available": str | float,  # Available balance
        "locked": str | float,  # Locked balance
    }

Position dict structure:
    {
        "symbol": str,  # Trading symbol (e.g. "BTCUSDT")
        "side": Optional[str],  # "long" | "short" | None (for futures; None for spot)
        "quantity": str | float,  # Position quantity
        "entry_price": Optional[str | float],  # Average entry price (if available)
    }
"""


@runtime_checkable
class BrokerAdapter(Protocol):
    """
    Interface for broker adapters.

    Order methods:
    - place_order: submit order to broker; returns unified response or reject.
    - start_fill_listener: start receiving fill/reject events via callback (unified format).
    - cancel_order: cancel an open order by broker order id and symbol.

    Account methods:
    - start_account_listener: start receiving account/balance/position events via callback.
    - get_account_snapshot: REST call to get current account snapshot (balances, positions).
    - stop_account_listener: stop the account event listener.
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

    def start_fill_listener(
        self,
        callback: Callable[[Dict[str, Any]], None],
        *,
        store: Optional[Any] = None,
    ) -> None:
        """
        Start listening for fill and reject events; invoke callback for each.

        Callback receives unified event dict (event_type: 'fill' | 'reject', order_id,
        broker_order_id, symbol, side, quantity, price, fee, executed_at, fill_id, etc.).
        Typically runs in a background thread. Optional store allows broker-specific
        enrichment (e.g. fill price from order payload when event price is 0).
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

    def start_account_listener(
        self,
        callback: Callable[[AccountEvent], None],
    ) -> None:
        """
        Start listening for account/balance/position events; invoke callback for each.

        Account events come from broker user data streams (e.g. Binance outboundAccountPosition,
        balanceUpdate). The same WebSocket connection may be shared with fill listener
        (multiplexed by event type).

        Args:
            callback: Function that receives unified account events. Callback receives
                     AccountEvent dict with event_type, broker, account_id, balances[],
                     positions[], updated_at, payload. See AccountEvent type annotation
                     for full structure.

        Callback receives unified event dict:
            {
                "event_type": "balance_update" | "account_position",
                "broker": str,
                "account_id": str,
                "balances": List[Dict[str, Any]],  # [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}, ...]
                "positions": List[Dict[str, Any]],  # [{"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"}, ...]
                "updated_at": str,  # ISO timestamp
                "payload": Dict[str, Any],  # Raw broker event blob
            }

        Typically runs in a background thread. Callback is invoked for each account event
        from the broker stream.
        """
        ...

    def get_account_snapshot(self, account_id: str) -> AccountEvent:
        """
        Get current account snapshot via REST API (balances, positions, margin if applicable).

        Used for periodic refresh and reconciliation. Returns the same unified structure
        as account listener events.

        Args:
            account_id: Broker account identifier (may be ignored if adapter uses single account).

        Returns:
            AccountEvent dict with same structure as account listener callback:
            {
                "event_type": "account_position",  # Typically "account_position" for snapshots
                "broker": str,
                "account_id": str,
                "balances": List[Dict[str, Any]],  # Current balances per asset
                "positions": List[Dict[str, Any]],  # Current positions per symbol
                "updated_at": str,  # ISO timestamp (current time or broker timestamp)
                "payload": Dict[str, Any],  # Raw broker REST response (for repairs)
            }

        Raises:
            Broker-specific exceptions on API errors (e.g. BinanceAPIError).
        """
        ...

    def stop_account_listener(self) -> None:
        """
        Stop the account event listener.

        Closes WebSocket connection and cleans up resources. Safe to call if listener
        is not running (no-op).
        """
        ...
