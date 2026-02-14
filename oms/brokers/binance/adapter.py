"""
Binance broker adapter.

Implements the OMS broker adapter interface using Binance API client and
fills listener. Converts Binance responses to unified fill/reject format.
"""

from typing import Any, Callable, Dict, Optional, Union

from .api_client import BinanceAPIClient, BinanceAPIError
from .fills_listener import (
    BinanceFillsListener,
    BinanceFillsListenerWsApi,
    create_fills_listener,
)


def binance_order_response_to_unified(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Binance place_order / query_order response to unified order response.

    Binance returns: orderId, symbol, status, clientOrderId, side, type,
    origQty, price, executedQty, timeInForce, transactTime, cumulativeQuoteQty (optional).

    Returns:
        Unified dict: broker_order_id, status, symbol, side, order_type,
        quantity, price, executed_qty, client_order_id, and optional
        binance_transact_time, binance_cumulative_quote_qty.
    """
    def _float(s: Any) -> float:
        if s is None:
            return 0.0
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    out: Dict[str, Any] = {
        "broker_order_id": str(resp.get("orderId", "")),
        "status": resp.get("status", ""),
        "symbol": resp.get("symbol", ""),
        "side": resp.get("side", ""),
        "order_type": resp.get("type", ""),
        "quantity": _float(resp.get("origQty")),
        "price": _float(resp.get("price")),
        "executed_qty": _float(resp.get("executedQty")),
        "client_order_id": resp.get("clientOrderId") or "",
    }
    if "transactTime" in resp and resp["transactTime"] is not None:
        out["binance_transact_time"] = resp["transactTime"]
    if "cumulativeQuoteQty" in resp and resp["cumulativeQuoteQty"] is not None:
        try:
            out["binance_cumulative_quote_qty"] = float(resp["cumulativeQuoteQty"])
        except (TypeError, ValueError):
            pass
    if resp.get("timeInForce"):
        out["time_in_force"] = resp["timeInForce"]
    return out


class BinanceBrokerAdapter:
    """
    Binance implementation of the OMS broker adapter interface.

    Uses BinanceAPIClient for place_order and create_fills_listener for
    fill/reject events. Converts all responses to unified format.
    """

    def __init__(
        self,
        client: BinanceAPIClient,
        use_ws_api: Optional[bool] = None,
    ):
        """
        Args:
            client: Binance API client (place order, user data stream).
            use_ws_api: True = WebSocket API listener, False = listenKey stream.
                        If None, uses env BINANCE_FILLS_STREAM (wsapi | listenkey).
        """
        self._client = client
        self._use_ws_api = use_ws_api
        self._listener: Optional[Union[BinanceFillsListener, BinanceFillsListenerWsApi]] = None

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place an order via Binance API; return unified response or reject.

        Args:
            order: risk_approved-style dict with order_id (internal), symbol, side,
                   quantity, order_type, optional price, time_in_force, book, comment.

        Returns:
            Unified order response (broker_order_id, status, ...) or
            reject dict (rejected=True, order_id, reject_reason).
        """
        order_id = order.get("order_id") or ""
        symbol = order.get("symbol", "")
        side = order.get("side", "")
        order_type = order.get("order_type", "MARKET")
        quantity = order.get("quantity", 0)
        price = order.get("price")
        time_in_force = order.get("time_in_force")

        if not symbol or not side or quantity <= 0:
            return {
                "rejected": True,
                "order_id": order_id,
                "reject_reason": "Missing or invalid symbol, side, or quantity",
            }

        try:
            resp = self._client.place_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                time_in_force=time_in_force,
                client_order_id=order_id or None,
            )
            return binance_order_response_to_unified(resp)
        except BinanceAPIError as e:
            return {
                "rejected": True,
                "order_id": order_id,
                "reject_reason": str(e),
            }

    def start_fill_listener(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """
        Start the Binance user data stream and invoke callback for each fill/reject.

        Callback receives unified event dict (event_type, order_id, broker_order_id,
        symbol, side, quantity, price, fee, executed_at, fill_id, reject_reason, etc.).
        """
        listener = create_fills_listener(self._client, use_ws_api=self._use_ws_api)
        self._listener = listener
        listener.start_background(callback)

    def stop_fill_listener(self) -> None:
        """Stop the fill listener if running."""
        if self._listener:
            self._listener.stop()
            self._listener = None
