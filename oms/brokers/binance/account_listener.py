"""
Binance account listener (user data stream).

Parses account/balance/position events from Binance user data stream:
- outboundAccountPosition: full account snapshot when balance changes
- balanceUpdate: single-asset balance delta during transfers/deposits/withdrawals

Supports two subscription styles:
- ListenKey (classic): REST POST for listenKey, then connect to wss://stream.../ws/<listenKey>.
- WebSocket API (method-based): Connect to wss://ws-api.../ws-api/v3, send
  userDataStream.subscribe.signature, receive events with subscriptionId.

Shares WebSocket connection with fills listener (multiplexed by event type).
Converts Binance events to unified AccountEvent format.
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
    AccountPositionEvent,
    BalanceUpdateEvent,
    BinanceBalanceUpdate,
    BinanceOutboundAccountPosition,
)

# Reuse same stream URLs as fills listener
STREAM_URL_TESTNET = "wss://stream.testnet.binance.vision/ws"
STREAM_URL_MAIN = "wss://stream.binance.com:9443/ws"

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


def parse_account_event(payload: Dict[str, Any], account_id: str = "default") -> Optional[Dict[str, Any]]:
    """
    Parse a Binance account event (outboundAccountPosition or balanceUpdate) into unified AccountEvent.

    Uses Pydantic validation for type safety.

    Unified format (aligned with AccountEvent from oms.brokers.base):
    - event_type: 'account_position' | 'balance_update'
    - broker: 'binance'
    - account_id: account identifier
    - balances: List[dict] with {'asset': str, 'available': str, 'locked': str}
    - positions: List[dict] (empty for spot, populated for futures)
    - updated_at: ISO8601 timestamp
    - payload: raw Binance event blob

    Args:
        payload: Raw Binance WebSocket message (may be wrapped in {"event": {...}})
        account_id: Account identifier (default: "default")

    Returns:
        Unified AccountEvent dict, or None if the event is not an account event.
    """
    # Support wrapped payload: { "event": { ... } } or raw { "e": "outboundAccountPosition", ... }
    event = payload.get("event") if "event" in payload else payload

    event_type_raw = (event or {}).get("e")
    if event_type_raw not in ("outboundAccountPosition", "balanceUpdate"):
        return None

    # Parse outboundAccountPosition
    if event_type_raw == "outboundAccountPosition":
        try:
            binance_event = BinanceOutboundAccountPosition(**event)
        except ValidationError as e:
            logger.warning("Invalid Binance outboundAccountPosition event: {}", e)
            return None

        # Convert balances to unified format
        balances = []
        for bal in binance_event.B:
            balances.append({
                "asset": bal.a,
                "available": bal.f,
                "locked": bal.l,
            })

        # Timestamp: use E (event time) or u (last account update) in ms
        timestamp_ms = binance_event.E or binance_event.u or int(time.time() * 1000)
        try:
            updated_at = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except (TypeError, ValueError):
            updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        try:
            account_event = AccountPositionEvent(
                event_type="account_position",
                broker="binance",
                account_id=account_id,
                balances=balances,
                positions=[],  # Spot accounts don't have positions
                updated_at=updated_at,
                payload=event,  # Store raw event
            )
            return account_event.model_dump_dict()
        except ValidationError as e:
            logger.warning("Invalid account position event after parsing: {}", e)
            return None

    # Parse balanceUpdate
    if event_type_raw == "balanceUpdate":
        try:
            binance_event = BinanceBalanceUpdate(**event)
        except ValidationError as e:
            logger.warning("Invalid Binance balanceUpdate event: {}", e)
            return None

        # Convert to unified format: single balance dict
        # Note: balanceUpdate only provides delta (d), not free/locked breakdown
        # We'll store delta in 'available' and set 'locked' to 0
        # The account store should merge this with existing balances
        balances = [{
            "asset": binance_event.a,
            "available": binance_event.d,  # Delta (positive for deposit, negative for withdrawal)
            "locked": "0.0",  # balanceUpdate doesn't provide locked amount
        }]

        # Timestamp: use E (event time) or T (clear time) in ms
        timestamp_ms = binance_event.E or binance_event.T or int(time.time() * 1000)
        try:
            updated_at = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
        except (TypeError, ValueError):
            updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        try:
            balance_event = BalanceUpdateEvent(
                event_type="balance_update",
                broker="binance",
                account_id=account_id,
                balances=balances,
                positions=[],  # balanceUpdate doesn't include positions
                updated_at=updated_at,
                payload=event,  # Store raw event
            )
            return balance_event.model_dump_dict()
        except ValidationError as e:
            logger.warning("Invalid balance update event after parsing: {}", e)
            return None

    return None


def create_account_listener(
    client: BinanceAPIClient,
    use_ws_api: Optional[bool] = None,
    **kwargs: Any,
) -> Union["BinanceAccountListener", "BinanceAccountListenerWsApi"]:
    """
    Create the default account listener for Binance user data stream.

    Default is WebSocket API (method-based); set BINANCE_ACCOUNT_STREAM=listenkey
    to use the classic listenKey stream instead.

    Args:
        client: Binance API client.
        use_ws_api: True = WS API, False = listenKey. If None, uses env
                    BINANCE_ACCOUNT_STREAM (wsapi | listenkey); default wsapi.
        **kwargs: Passed to the listener constructor (on_account_event, account_id, etc.).

    Returns:
        BinanceAccountListenerWsApi (default) or BinanceAccountListener.
    """
    if use_ws_api is None:
        stream = os.environ.get("BINANCE_ACCOUNT_STREAM", "wsapi").strip().lower()
        use_ws_api = stream != "listenkey"
    if use_ws_api:
        return BinanceAccountListenerWsApi(client, **kwargs)
    return BinanceAccountListener(client, **kwargs)


class BinanceAccountListener:
    """
    Listens to Binance user data stream for account/balance events.

    Uses the REST client to create/keepalive/close the listen key and connects
    to the WebSocket to receive outboundAccountPosition and balanceUpdate events.
    Shares the same listenKey with fills listener (same WebSocket connection).
    """

    _KEEPALIVE_INTERVAL_SEC = 30 * 60  # 30 minutes

    def __init__(
        self,
        client: BinanceAPIClient,
        account_id: str = "default",
        listen_key: Optional[str] = None,
        on_account_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_stream_connected: Optional[Callable[[], None]] = None,
    ):
        """
        Args:
            client: Binance API client.
            account_id: Account identifier (default: "default").
            listen_key: Optional listenKey to reuse (if sharing with fills listener).
                        If None, creates a new listenKey.
            on_account_event: Callback for each account event (unified AccountEvent dict).
            on_stream_connected: Optional callback when WebSocket connects.
        """
        self._client = client
        self._account_id = account_id
        self._listen_key = listen_key
        self._on_account_event = on_account_event
        self._on_stream_connected = on_stream_connected
        self._stream_base_url = _stream_url_from_base_url(client.base_url)
        self._ws: Any = None
        self._ws_connected = False
        self._keepalive_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def stream_connected(self) -> bool:
        """True after WebSocket has opened."""
        return self._ws_connected

    def start(self, callback: Optional[Callable[[Dict[str, Any]], None]] = None) -> None:
        """
        Start listening for account events. Blocks until WebSocket closes.

        Args:
            callback: Optional callback (overrides constructor callback).
        """
        cb = callback or self._on_account_event
        if not cb:
            raise ValueError("No callback provided (constructor or start())")

        # Create listenKey if not provided (sharing with fills listener)
        if not self._listen_key:
            self._listen_key = self._client.start_user_data_stream()
        ws_url = f"{self._stream_base_url.rstrip('/')}/{self._listen_key}"
        logger.info("Binance account listener started, connecting to {}", ws_url[:60] + "...")

        def keepalive_loop() -> None:
            while not self._stop.wait(timeout=self._KEEPALIVE_INTERVAL_SEC):
                key = self._listen_key
                if not key:
                    break
                try:
                    self._client.keepalive_user_data_stream(key)
                    logger.debug("Account listener keepalive sent")
                except BinanceAPIError as e:
                    logger.warning("Account listener keepalive failed: {}", e)

        self._stop.clear()
        self._keepalive_thread = threading.Thread(target=keepalive_loop, daemon=True)
        self._keepalive_thread.start()

        def on_message(_ws: Any, raw: str) -> None:
            try:
                payload = json.loads(raw)
                unified = parse_account_event(payload, account_id=self._account_id)
                if unified:
                    cb(unified)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON from account stream: {}", e)
            except Exception as e:
                logger.exception("Error processing account stream message: {}", e)

        def on_open(_ws: Any) -> None:
            self._ws_connected = True
            logger.info("Binance account listener WebSocket connected")
            if self._on_stream_connected:
                try:
                    self._on_stream_connected()
                except Exception as e:
                    logger.exception("on_stream_connected callback error: {}", e)

        def on_error(_ws: Any, err: Optional[Exception]) -> None:
            if err:
                logger.error("Binance account stream error: {}", err)

        def on_close(_ws: Any, close_status: Optional[int], close_msg: Optional[str]) -> None:
            self._ws_connected = False
            logger.info("Binance account stream closed: {} {}", close_status, close_msg)

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
        cb = callback or self._on_account_event
        if not cb:
            raise ValueError("No callback provided (constructor or start_background())")
        logger.info("Binance account listener: starting background thread")
        self._thread = threading.Thread(target=self.start, args=(cb,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Close WebSocket. Note: does not close listenKey if shared with fills listener."""
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                logger.debug("Error closing account WebSocket: %s", e)
            self._ws = None
        # Only close listenKey if we created it (not if shared)
        # For shared listenKey, fills listener will close it
        # self._listen_key = None
        self._keepalive_thread = None


class BinanceAccountListenerWsApi:
    """
    Listens to Binance user data stream for account/balance events via WebSocket API (method-based).

    Connects to wss://ws-api.../ws-api/v3, sends userDataStream.subscribe.signature
    with API key + HMAC signature, then receives outboundAccountPosition and balanceUpdate events.
    Use this if the classic listenKey stream does not deliver events (e.g. on testnet).
    """

    def __init__(
        self,
        client: BinanceAPIClient,
        account_id: str = "default",
        ws_api_url: Optional[str] = None,
        on_account_event: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_stream_connected: Optional[Callable[[], None]] = None,
    ):
        """
        Args:
            client: Binance API client.
            account_id: Account identifier (default: "default").
            ws_api_url: Optional WebSocket API URL (defaults based on client.base_url).
            on_account_event: Callback for each account event (unified AccountEvent dict).
            on_stream_connected: Optional callback when WebSocket connects.
        """
        self._client = client
        self._account_id = account_id
        self._ws_api_url = ws_api_url or _ws_api_url_from_base_url(client.base_url)
        self._on_account_event = on_account_event
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
        """
        Start listening for account events. Blocks until WebSocket closes.

        Args:
            callback: Optional callback (overrides constructor callback).
        """
        cb = callback or self._on_account_event
        if not cb:
            raise ValueError("No callback provided (constructor or start())")

        self._stop.clear()
        logger.info("Binance account listener (WS API): thread started, syncing time...")
        # Pre-sync in this thread so on_open has a valid offset even if GET fails from WS thread
        self._client._ensure_time_sync()
        ws_url = self._ws_api_url
        logger.info("Binance WS API account listener, connecting to {}", ws_url[:50] + "...")

        def on_message(_ws: Any, raw: str) -> None:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as e:
                logger.warning("Invalid JSON from WS API account stream: {}", e)
                return
            # Method response: {"id": "...", "status": 200, "result": {"subscriptionId": 0}}
            if "result" in payload:
                res = payload.get("result") or {}
                if "subscriptionId" in res:
                    self._subscription_id = res["subscriptionId"]
                    self._ws_connected = True
                    logger.info("Account listener subscribed (subscriptionId={})", self._subscription_id)
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
            # Event: {"subscriptionId": 0, "event": {"e": "outboundAccountPosition", ...}}
            unified = parse_account_event(payload, account_id=self._account_id)
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
                logger.error("Binance WS API account stream error: {}", err)

        def on_close(_ws: Any, close_status: Optional[int], close_msg: Optional[str]) -> None:
            self._ws_connected = False
            self._subscription_id = None
            logger.info("Binance WS API account stream closed: {} {}", close_status, close_msg)

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
        cb = callback or self._on_account_event
        if not cb:
            raise ValueError("No callback provided (constructor or start_background())")
        logger.info("Binance account listener (WS API): starting background thread")
        self._thread = threading.Thread(target=self.start, args=(cb,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Close WebSocket and unsubscribe."""
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
