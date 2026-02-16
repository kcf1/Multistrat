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
    origQty, price (order limit), executedQty, timeInForce, transactTime,
    cumulativeQuoteQty (optional), avgPrice (optional, executed avg price).

    Returns:
        Unified dict: broker_order_id, status, symbol, side, order_type,
        quantity, price (executed/avg), limit_price (order limit), executed_qty,
        client_order_id, and optional binance_transact_time, binance_cumulative_quote_qty.
    """
    def _float(s: Any) -> float:
        if s is None:
            return 0.0
        try:
            return float(s)
        except (TypeError, ValueError):
            return 0.0

    # limit_price = order's limit price (Binance "price"); price = executed avg (avgPrice or derived)
    limit_price = _float(resp.get("price"))
    executed_qty = _float(resp.get("executedQty"))
    cum_quote = resp.get("cumulativeQuoteQty")
    if cum_quote is not None:
        try:
            cum_quote_f = float(cum_quote)
        except (TypeError, ValueError):
            cum_quote_f = 0.0
    else:
        cum_quote_f = 0.0
    avg_price = resp.get("avgPrice")
    if avg_price is not None:
        try:
            price = float(avg_price)
        except (TypeError, ValueError):
            price = (cum_quote_f / executed_qty) if executed_qty else 0.0
    else:
        price = (cum_quote_f / executed_qty) if executed_qty else 0.0

    out: Dict[str, Any] = {
        "broker_order_id": str(resp.get("orderId", "")),
        "status": resp.get("status", ""),
        "symbol": resp.get("symbol", ""),
        "side": resp.get("side", ""),
        "order_type": resp.get("type", ""),
        "quantity": _float(resp.get("origQty")),
        "price": price,
        "limit_price": limit_price,
        "executed_qty": executed_qty,
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
        price = order.get("limit_price") or order.get("price")  # limit price for REST param
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
            out = binance_order_response_to_unified(resp)
            # Store raw broker response for audit/debug (Redis + Postgres payload)
            out["payload"] = {"binance": resp}
            return out
        except BinanceAPIError as e:
            rejected = {
                "rejected": True,
                "order_id": order_id,
                "reject_reason": str(e),
            }
            # Store raw broker error response for audit/debug (Redis + Postgres payload)
            if e.error_data is not None:
                rejected["payload"] = {"binance": e.error_data}
            return rejected

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

    def cancel_order(self, broker_order_id: str, symbol: str) -> Dict[str, Any]:
        """
        Cancel an open order via Binance API.

        Args:
            broker_order_id: Binance orderId (numeric string).
            symbol: Trading symbol (e.g. BTCUSDT).

        Returns:
            Unified response (status, broker_order_id, symbol, ...) or
            reject dict (rejected=True, reject_reason=...) on failure.
        """
        if not symbol or not broker_order_id:
            return {
                "rejected": True,
                "reject_reason": "Missing symbol or broker_order_id",
            }
        try:
            order_id_int = int(broker_order_id)
        except (TypeError, ValueError):
            # Binance also accepts origClientOrderId (string)
            try:
                resp = self._client.cancel_order(symbol=symbol, client_order_id=broker_order_id)
                return binance_order_response_to_unified(resp)
            except BinanceAPIError as e:
                rejected = {
                    "rejected": True,
                    "reject_reason": str(e),
                }
                if e.error_data is not None:
                    rejected["payload"] = {"binance": e.error_data}
                return rejected
        try:
            resp = self._client.cancel_order(symbol=symbol, order_id=order_id_int)
            return binance_order_response_to_unified(resp)
        except BinanceAPIError as e:
            rejected = {
                "rejected": True,
                "reject_reason": str(e),
            }
            if e.error_data is not None:
                rejected["payload"] = {"binance": e.error_data}
            return rejected
