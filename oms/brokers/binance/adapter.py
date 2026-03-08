"""
Binance broker adapter.

Implements the OMS broker adapter interface using Binance API client and
fills listener. Converts Binance responses to unified fill/reject format.
When WebSocket fill event has price 0/null, enriches event from order payload (Binance-specific).
"""

from typing import Any, Callable, Dict, Optional, Union

from oms.brokers.base import AccountEvent

from .account_listener import (
    BinanceAccountListener,
    BinanceAccountListenerWsApi,
    create_account_listener,
)
from .api_client import BinanceAPIClient, BinanceAPIError, _decimal_str
from .fills_listener import (
    BinanceFillsListener,
    BinanceFillsListenerWsApi,
    create_fills_listener,
)


def _fill_price_from_binance_payload(order: Dict[str, Any]) -> Optional[float]:
    """
    Extract executed/fill price from Binance order payload (REST response shape).

    When WebSocket executionReport has price 0/missing, use this from order payload.
    Tries: payload.binance.avgPrice, payload.binance.fills[0].price, payload.fill.price.
    Returns None if no valid price found.
    """
    en = _enrichments_from_binance_payload(order)
    return en.get("price") if isinstance(en.get("price"), (int, float)) else None


def _enrichments_from_binance_payload(order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract price, time_in_force, binance_cumulative_quote_qty from Binance order payload.

    When WebSocket fill event has these missing/zero/empty, use values from order payload
    (Binance REST response shape). Returns dict with only keys that have valid values.
    """
    out: Dict[str, Any] = {}
    payload = order.get("payload")
    if not isinstance(payload, dict):
        return out
    binance = payload.get("binance")
    try:
        if isinstance(binance, dict):
            # Price: avgPrice, fills[0].price
            avg = binance.get("avgPrice")
            if avg is not None and str(avg).strip():
                out["price"] = float(avg)
            else:
                fills = binance.get("fills")
                if isinstance(fills, list) and fills:
                    first = fills[0]
                    if isinstance(first, dict):
                        p = first.get("price")
                        if p is not None and str(p).strip():
                            out["price"] = float(p)
            # Time in force
            tif = binance.get("timeInForce")
            if tif is not None and str(tif).strip():
                out["time_in_force"] = str(tif).strip()
            # Cumulative quote qty
            cq = binance.get("cumulativeQuoteQty")
            if cq is not None and str(cq).strip():
                try:
                    out["binance_cumulative_quote_qty"] = float(cq)
                except (TypeError, ValueError):
                    pass
        # Price fallback: payload.fill.price (e.g. when binance key absent)
        if "price" not in out:
            fill = payload.get("fill")
            if isinstance(fill, dict):
                p = fill.get("price")
                if p is not None and str(p).strip():
                    out["price"] = float(p)
    except (TypeError, ValueError):
        pass
    return out


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
        self._account_listener: Optional[Union[BinanceAccountListener, BinanceAccountListenerWsApi]] = None
        self._account_id: str = "default"

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
        quantity_raw = order.get("quantity", 0)
        price_raw = order.get("limit_price") or order.get("price")  # limit price for REST param
        time_in_force = order.get("time_in_force")

        if not symbol or not side or quantity_raw <= 0:
            return {
                "rejected": True,
                "order_id": order_id,
                "reject_reason": "Missing or invalid symbol, side, or quantity",
            }

        # Format quantity/price as decimal strings for Binance (no scientific notation, no excess precision)
        quantity = _decimal_str(quantity_raw)
        price = _decimal_str(price_raw) if price_raw is not None else None

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

    def start_fill_listener(
        self,
        callback: Callable[[Dict[str, Any]], None],
        *,
        store: Optional[Any] = None,
    ) -> None:
        """
        Start the Binance user data stream and invoke callback for each fill/reject.

        Callback receives unified event dict (event_type, order_id, broker_order_id,
        symbol, side, quantity, price, fee, executed_at, fill_id, reject_reason, etc.).
        When store is provided, fill events are enriched from the order payload when
        event has price 0/null, time_in_force empty, or binance_cumulative_quote_qty 0/null
        (Binance REST response: avgPrice/fills, timeInForce, cumulativeQuoteQty).
        """
        if store is not None:

            def enriched_cb(event: Dict[str, Any]) -> None:
                if (event.get("event_type") or "").strip().lower() != "fill":
                    callback(event)
                    return
                order_id = event.get("order_id") or ""
                broker_order_id = str(event.get("broker_order_id", ""))
                if not order_id and broker_order_id:
                    order_id = store.find_order_by_broker_order_id(broker_order_id) or ""
                order = store.get_order(order_id) or {} if order_id else {}
                enrichments = _enrichments_from_binance_payload(order)
                if not enrichments:
                    callback(event)
                    return
                updates: Dict[str, Any] = {}
                event_price = event.get("price")
                if (event_price is None or (event_price is not None and float(event_price) == 0)) and "price" in enrichments and (enrichments["price"] or 0) > 0:
                    updates["price"] = enrichments["price"]
                event_tif = event.get("time_in_force")
                if (event_tif is None or not str(event_tif or "").strip()) and "time_in_force" in enrichments:
                    updates["time_in_force"] = enrichments["time_in_force"]
                event_cq = event.get("binance_cumulative_quote_qty")
                if (event_cq is None or (event_cq is not None and float(event_cq) == 0)) and "binance_cumulative_quote_qty" in enrichments:
                    updates["binance_cumulative_quote_qty"] = enrichments["binance_cumulative_quote_qty"]
                if updates:
                    event = {**event, **updates}
                callback(event)

            cb = enriched_cb
        else:
            cb = callback
        listener = create_fills_listener(self._client, use_ws_api=self._use_ws_api)
        self._listener = listener
        listener.start_background(cb)

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

    def start_account_listener(
        self,
        callback: Callable[[AccountEvent], None],
    ) -> None:
        """
        Start listening for account/balance/position events from Binance user data stream.

        Callback receives unified AccountEvent dict (event_type, broker, account_id,
        balances[], positions[], updated_at, payload). Uses WebSocket API method by default
        (same as fills listener). Shares the same WebSocket connection with fill listener
        (multiplexed by event type).

        Args:
            callback: Function that receives unified account events.
        """
        # Use same WebSocket API setting as fills listener (default: WebSocket API)
        listener = create_account_listener(
            client=self._client,
            use_ws_api=self._use_ws_api,  # Use same setting as fills listener
            account_id=self._account_id,
            on_account_event=callback,
        )
        self._account_listener = listener
        listener.start_background()

    def get_account_snapshot(self, account_id: str = "default") -> AccountEvent:
        """
        Get current account snapshot via REST API (balances, positions).

        Used for periodic refresh and reconciliation. Returns the same unified
        structure as account listener events.

        Args:
            account_id: Account identifier (default: "default")

        Returns:
            AccountEvent dict with same structure as account listener callback:
            {
                "event_type": "account_position",
                "broker": "binance",
                "account_id": account_id,
                "balances": [...],
                "positions": [],
                "updated_at": "...",
                "payload": {...}
            }

        Raises:
            BinanceAPIError: On API error
        """
        return self._client.get_account_snapshot(account_id=account_id)

    def stop_account_listener(self) -> None:
        """
        Stop the account event listener.

        Closes WebSocket connection. Does not close listenKey if shared with
        fill listener (fill listener will close it).
        """
        if self._account_listener:
            self._account_listener.stop()
            self._account_listener = None
