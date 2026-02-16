"""
Binance fills listener (user data stream).

Supports two subscription styles:
- ListenKey (classic): REST POST for listenKey, then connect to wss://stream.../ws/<listenKey>.
- WebSocket API (method-based): Connect to wss://ws-api.../ws-api/v3, send
  userDataStream.subscribe.signature, receive events with subscriptionId.

Parses executionReport events and invokes a callback with unified fill/reject
events aligned with oms_fills schema.
"""

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional, Union

import websocket
from pydantic import ValidationError

from oms.log import logger

from .api_client import BinanceAPIClient, BinanceAPIError
from .schemas_pydantic import (
    BinanceExecutionReport,
    CancelledEvent,
    ExpiredEvent,
    FillEvent,
    RejectEvent,
)

# Classic stream: listenKey in URL path
STREAM_URL_TESTNET = "wss://stream.testnet.binance.vision/ws"
STREAM_URL_MAIN = "wss://stream.binance.com:9443/ws"

# WebSocket API (method-based): send userDataStream.subscribe.signature
WS_API_URL_TESTNET = "wss://ws-api.testnet.binance.vision/ws-api/v3"
WS_API_URL_MAIN = "wss://ws-api.binance.com:443/ws-api/v3"


def _stream_url_from_base_url(base_url: str) -> str:
    """Derive WebSocket stream base URL from REST base URL (classic listenKey)."""
    if "testnet" in base_url:
        return STREAM_URL_TESTNET
    return STREAM_URL_MAIN


def _ws_api_url_from_base_url(base_url: str) -> str:
    """Derive WebSocket API URL from REST base URL (method-based)."""
    if "testnet" in base_url:
        return WS_API_URL_TESTNET
    return WS_API_URL_MAIN


def create_fills_listener(
    client: BinanceAPIClient,
    use_ws_api: Optional[bool] = None,
    **kwargs: Any,
) -> Union["BinanceFillsListener", "BinanceFillsListenerWsApi"]:
    """
    Create the default fills listener for Binance user data stream.

    Default is WebSocket API (method-based); set BINANCE_FILLS_STREAM=listenkey
    to use the classic listenKey stream instead.

    Args:
        client: Binance API client.
        use_ws_api: True = WS API, False = listenKey. If None, uses env
                    BINANCE_FILLS_STREAM (wsapi | listenkey); default wsapi.
        **kwargs: Passed to the listener constructor (on_fill_or_reject, etc.).

    Returns:
        BinanceFillsListenerWsApi (default) or BinanceFillsListener.
    """
    if use_ws_api is None:
        stream = os.environ.get("BINANCE_FILLS_STREAM", "wsapi").strip().lower()
        use_ws_api = stream != "listenkey"
    if use_ws_api:
        return BinanceFillsListenerWsApi(client, **kwargs)
    return BinanceFillsListener(client, **kwargs)


def parse_execution_report(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a Binance executionReport event into a unified fill or reject event.

    Uses Pydantic validation for type safety and validation.

    Unified format (aligned with oms_fills):
    - event_type: 'fill' | 'reject' | 'cancelled' | 'expired'
    - order_id: internal (clientOrderId)
    - broker_order_id: Binance orderId
    - symbol, side, quantity, price, fee, fee_asset
    - executed_at: ISO8601
    - fill_id: broker trade id (for fills)
    - reject_reason: for rejections/cancellations/expirations

    Returns:
        Unified event dict, or None if the event is not a fill, reject, cancelled, or expired.
    """
    # Support wrapped payload: { "event": { ... } } or raw { "e": "executionReport", ... }
    event = payload.get("event") if "event" in payload else payload

    # Binance user data stream sends other event types (e.g. outboundAccountPosition).
    # Only process executionReport; skip others without logging.
    if (event or {}).get("e") != "executionReport":
        return None

    # Validate raw Binance event with Pydantic
    try:
        binance_event = BinanceExecutionReport(**event)
    except ValidationError as e:
        logger.warning("Invalid Binance executionReport event: {}", e)
        return None

    exec_type = binance_event.x  # NEW, CANCELED, TRADE, REJECTED, EXPIRED, etc.
    order_status = binance_event.X  # NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED

    # Fill: execution type TRADE (each fill is one TRADE event)
    if exec_type == "TRADE":
        # Validate quantity > 0 for fills
        qty_str = binance_event.l
        if not qty_str:
            return None
        try:
            qty = float(qty_str)
        except (TypeError, ValueError):
            return None
        if qty <= 0:
            return None

        # Parse other fields
        try:
            price = float(binance_event.L) if binance_event.L else 0.0
        except (TypeError, ValueError):
            price = 0.0
        try:
            fee = float(binance_event.n) if binance_event.n else 0.0
        except (TypeError, ValueError):
            fee = 0.0
        fee_asset = binance_event.N
        
        # Transaction time T in ms
        t_ms = binance_event.T
        if t_ms is not None:
            try:
                executed_at = datetime.fromtimestamp(int(t_ms) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            except (TypeError, ValueError):
                executed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        else:
            executed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        # Binance order status X: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED
        # z: cumulative executed quantity (for partial/full fill handling in OMS)
        try:
            executed_qty_cumulative = float(binance_event.z) if binance_event.z else None
        except (TypeError, ValueError):
            executed_qty_cumulative = None

        # Validate with Pydantic FillEvent model
        try:
            fill_event = FillEvent(
                event_type="fill",
                order_id=binance_event.c or "",
                broker_order_id=str(binance_event.i) if binance_event.i else "",
                symbol=binance_event.s or "",
                side=binance_event.S or "",
                quantity=qty,
                price=price,
                fee=fee,
                fee_asset=fee_asset,
                executed_at=executed_at,
                fill_id=str(binance_event.t) if binance_event.t else "",
                order_status=order_status,
                executed_qty_cumulative=executed_qty_cumulative,
            )
            return fill_event.model_dump_dict()
        except ValidationError as e:
            logger.warning("Invalid fill event after parsing: {}", e)
            return None

    # Reject: execution type REJECTED or order status REJECTED
    if exec_type == "REJECTED" or order_status == "REJECTED":
        try:
            qty = float(binance_event.q) if binance_event.q else 0.0
        except (TypeError, ValueError):
            qty = 0.0
        try:
            price = float(binance_event.p) if binance_event.p else 0.0
        except (TypeError, ValueError):
            price = 0.0
        
        try:
            reject_event = RejectEvent(
                event_type="reject",
                order_id=binance_event.c or "",
                broker_order_id=str(binance_event.i) if binance_event.i else "",
                symbol=binance_event.s or "",
                side=binance_event.S or "",
                quantity=qty,
                price=price,
                executed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                reject_reason=binance_event.r or "REJECTED",
            )
            return reject_event.model_dump_dict()
        except ValidationError as e:
            logger.warning("Invalid reject event after parsing: {}", e)
            return None

    # Cancelled: exec_type CANCELED or order status CANCELED
    if exec_type == "CANCELED" or order_status == "CANCELED":
        try:
            qty = float(binance_event.q) if binance_event.q else 0.0
        except (TypeError, ValueError):
            qty = 0.0
        try:
            price = float(binance_event.p) if binance_event.p else 0.0
        except (TypeError, ValueError):
            price = 0.0
        
        try:
            cancelled_event = CancelledEvent(
                event_type="cancelled",
                order_id=binance_event.c or "",
                broker_order_id=str(binance_event.i) if binance_event.i else "",
                symbol=binance_event.s or "",
                side=binance_event.S or "",
                quantity=qty,
                price=price,
                executed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                reject_reason=binance_event.r or "CANCELED",
            )
            return cancelled_event.model_dump_dict()
        except ValidationError as e:
            logger.warning("Invalid cancelled event after parsing: {}", e)
            return None

    # Expired: exec_type EXPIRED or order status EXPIRED
    if exec_type == "EXPIRED" or order_status == "EXPIRED":
        try:
            qty = float(binance_event.q) if binance_event.q else 0.0
        except (TypeError, ValueError):
            qty = 0.0
        try:
            price = float(binance_event.p) if binance_event.p else 0.0
        except (TypeError, ValueError):
            price = 0.0
        
        try:
            expired_event = ExpiredEvent(
                event_type="expired",
                order_id=binance_event.c or "",
                broker_order_id=str(binance_event.i) if binance_event.i else "",
                symbol=binance_event.s or "",
                side=binance_event.S or "",
                quantity=qty,
                price=price,
                executed_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                reject_reason=binance_event.r or "EXPIRED",
            )
            return expired_event.model_dump_dict()
        except ValidationError as e:
            logger.warning("Invalid expired event after parsing: {}", e)
            return None

    return None


class BinanceFillsListener:
    """
    Listens to Binance user data stream and invokes a callback for each fill or reject.

    Uses the REST client to create/keepalive/close the listen key and connects
    to the WebSocket to receive executionReport events.
    """

    _KEEPALIVE_INTERVAL_SEC = 30 * 60  # 30 minutes per Binance recommendation

    def __init__(
        self,
        client: BinanceAPIClient,
        stream_base_url: Optional[str] = None,
        on_fill_or_reject: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_stream_connected: Optional[Callable[[], None]] = None,
    ):
        """
        Args:
            client: Binance API client (used for listen key and optional keepalive).
            stream_base_url: WebSocket base URL (e.g. wss://stream.binance.com:9443/ws).
                            If None, derived from client's base_url.
            on_fill_or_reject: Callback invoked with unified event dict for each fill/reject.
            on_stream_connected: Optional callback invoked when WebSocket is opened (stream
                                is connected; account is the one that created the listen key).
        """
        self._client = client
        self._stream_base_url = stream_base_url or _stream_url_from_base_url(client.base_url)
        self._on_fill_or_reject = on_fill_or_reject
        self._on_stream_connected = on_stream_connected
        self._listen_key: Optional[str] = None
        self._ws: Any = None
        self._ws_connected = False
        self._thread: Optional[threading.Thread] = None
        self._keepalive_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    @property
    def stream_connected(self) -> bool:
        """True after WebSocket has opened; False before or after close."""
        return self._ws_connected

    def start(self, callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> None:
        """
        Start the user data stream and listen for execution reports.
        Blocks until stop() is called (runs the WebSocket loop in the current thread).
        For non-blocking use, run start() in a daemon thread.

        Args:
            callback: Override for on_fill_or_reject if not set in __init__.
        """
        cb = callback or self._on_fill_or_reject
        if not cb:
            raise ValueError("No callback provided (constructor or start())")

        self._listen_key = self._client.start_user_data_stream()
        ws_url = f"{self._stream_base_url.rstrip('/')}/{self._listen_key}"
        logger.info("Binance user data stream started, connecting to {}", ws_url[:60] + "...")

        def keepalive_loop() -> None:
            while not self._stop.wait(timeout=self._KEEPALIVE_INTERVAL_SEC):
                key = self._listen_key
                if not key:
                    break
                try:
                    self._client.keepalive_user_data_stream(key)
                    logger.debug("User data stream keepalive sent")
                except BinanceAPIError as e:
                    logger.warning("User data stream keepalive failed: {}", e)

        self._stop.clear()
        self._keepalive_thread = threading.Thread(target=keepalive_loop, daemon=True)
        self._keepalive_thread.start()

        def on_message(_ws: Any, raw: str) -> None:
            try:
                payload = json.loads(raw)
                unified = parse_execution_report(payload)
                if unified:
                    cb(unified)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON from stream: {}", e)
            except Exception as e:
                logger.exception("Error processing stream message: {}", e)

        def on_open(_ws: Any) -> None:
            self._ws_connected = True
            logger.info("Binance user data stream WebSocket connected (account bound to listen key)")
            if self._on_stream_connected:
                try:
                    self._on_stream_connected()
                except Exception as e:
                    logger.exception("on_stream_connected callback error: {}", e)

        def on_error(_ws: Any, err: Optional[Exception]) -> None:
            if err:
                logger.error("Binance stream error: {}", err)

        def on_close(_ws: Any, close_status: Optional[int], close_msg: Optional[str]) -> None:
            self._ws_connected = False
            logger.info("Binance stream closed: {} {}", close_status, close_msg)

        self._ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        self._ws.run_forever()

    def start_background(self, callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> None:
        """
        Start the listener in a daemon thread. Returns immediately.
        """
        cb = callback or self._on_fill_or_reject
        if not cb:
            raise ValueError("No callback provided (constructor or start_background())")
        logger.info("Binance fill listener (listenKey): starting background thread")
        self._thread = threading.Thread(target=self.start, args=(cb,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Close WebSocket and user data stream."""
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                logger.debug("Error closing WebSocket: %s", e)
            self._ws = None
        if self._listen_key:
            try:
                self._client.close_user_data_stream(self._listen_key)
            except BinanceAPIError as e:
                logger.warning("Error closing user data stream: {}", e)
            self._listen_key = None
        self._keepalive_thread = None


class BinanceFillsListenerWsApi:
    """
    Listens to Binance user data stream via the WebSocket API (method-based).

    Connects to wss://ws-api.../ws-api/v3, sends userDataStream.subscribe.signature
    with API key + HMAC signature, then receives executionReport events.
    Use this if the classic listenKey stream does not deliver events (e.g. on testnet).
    """

    def __init__(
        self,
        client: BinanceAPIClient,
        ws_api_url: Optional[str] = None,
        on_fill_or_reject: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_stream_connected: Optional[Callable[[], None]] = None,
    ):
        self._client = client
        self._ws_api_url = ws_api_url or _ws_api_url_from_base_url(client.base_url)
        self._on_fill_or_reject = on_fill_or_reject
        self._on_stream_connected = on_stream_connected
        self._ws: Any = None
        self._ws_connected = False
        self._subscription_id: Optional[int] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    @property
    def stream_connected(self) -> bool:
        """True after WebSocket has opened and subscription was confirmed."""
        return self._ws_connected and self._subscription_id is not None

    def start(self, callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> None:
        cb = callback or self._on_fill_or_reject
        if not cb:
            raise ValueError("No callback provided (constructor or start())")

        self._stop.clear()
        logger.info("Binance fill listener (WS API): thread started, syncing time...")
        # Pre-sync in this thread so on_open has a valid offset even if GET fails from WS thread
        self._client._ensure_time_sync()
        ws_url = self._ws_api_url
        logger.info("Binance WS API user data stream, connecting to {}", ws_url[:50] + "...")

        def on_message(_ws: Any, raw: str) -> None:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON from WS API: {}", e)
                return
            # Method response: {"id": "...", "status": 200, "result": {"subscriptionId": 0}}
            if "result" in payload:
                res = payload.get("result") or {}
                if "subscriptionId" in res:
                    self._subscription_id = res["subscriptionId"]
                    self._ws_connected = True
                    logger.info("User data stream subscribed (subscriptionId={})", self._subscription_id)
                    if self._on_stream_connected:
                        try:
                            self._on_stream_connected()
                        except Exception as e:
                            logger.exception("on_stream_connected callback error: {}", e)
                return
            if "error" in payload:
                err = payload.get("error", {})
                logger.error("WS API error: {} (code {})", err.get("msg"), err.get("code"))
                return
            # Event: {"subscriptionId": 0, "event": {"e": "executionReport", ...}}
            unified = parse_execution_report(payload)
            if unified:
                cb(unified)

        def on_open(_ws: Any) -> None:
            # Build and send userDataStream.subscribe.signature (HMAC)
            # Use server-time-synced timestamp so Binance does not reject with timestamp error
            self._client._ensure_time_sync()
            local_ms = int(time.time() * 1000)
            timestamp = local_ms + (self._client._time_offset_ms or 0)
            params = {
                "apiKey": self._client.api_key,
                "timestamp": timestamp,
                "recvWindow": 5000,
            }
            params["signature"] = self._client._sign_request(params)
            req = {
                "id": str(uuid.uuid4()),
                "method": "userDataStream.subscribe.signature",
                "params": params,
            }
            try:
                _ws.send(json.dumps(req))
            except Exception as e:
                logger.exception("Failed to send userDataStream.subscribe.signature: {}", e)

        def on_error(_ws: Any, err: Optional[Exception]) -> None:
            if err:
                logger.error("Binance WS API stream error: {}", err)

        def on_close(_ws: Any, close_status: Optional[int], close_msg: Optional[str]) -> None:
            self._ws_connected = False
            self._subscription_id = None
            logger.info("Binance WS API stream closed: {} {}", close_status, close_msg)

        self._ws = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )
        self._ws.run_forever()

    def start_background(self, callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> None:
        cb = callback or self._on_fill_or_reject
        if not cb:
            raise ValueError("No callback provided (constructor or start_background())")
        logger.info("Binance fill listener (WS API): starting background thread")
        self._thread = threading.Thread(target=self.start, args=(cb,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._ws and self._subscription_id is not None:
            try:
                req = {
                    "id": str(uuid.uuid4()),
                    "method": "userDataStream.unsubscribe",
                    "params": {"subscriptionId": self._subscription_id},
                }
                self._ws.send(json.dumps(req))
            except Exception as e:
                logger.debug("Error sending unsubscribe: %s", e)
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                logger.debug("Error closing WebSocket: %s", e)
            self._ws = None
        self._subscription_id = None
