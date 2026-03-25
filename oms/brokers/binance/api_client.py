"""
Binance API client (lowest level).

HTTP client for Binance REST API with HMAC-SHA256 signing.
Supports testnet and production URLs.
Syncs request timestamp with Binance server time to avoid -1021 (timestamp ahead/behind).
"""

import hashlib
import hmac
import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlencode

import requests


# Refresh server-time offset after this many seconds
TIME_OFFSET_REFRESH_INTERVAL = 300


def _decimal_str(value, max_decimals: int = 8) -> str:
    """Format number as decimal string for Binance (no scientific notation; no excess precision)."""
    if isinstance(value, str):
        return value
    v = float(value)
    v = round(v, max_decimals)
    s = format(v, f'.{max_decimals}f').rstrip('0').rstrip('.')
    return s if s else '0'


class BinanceAPIError(Exception):
    """Binance API error."""
    
    def __init__(self, message: str, error_data: Optional[Dict] = None):
        """
        Initialize Binance API error.
        
        Args:
            message: Error message string.
            error_data: Optional error response JSON from Binance (for payload storage).
        """
        super().__init__(message)
        self.message = message
        self.error_data = error_data


class BinanceAPIClient:
    """
    Low-level HTTP client for Binance REST API.
    
    Handles:
    - HMAC-SHA256 request signing
    - Testnet vs production URLs
    - Server time sync (avoids -1021 timestamp errors)
    - Place order, query order, cancel order endpoints
    """
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        testnet: bool = False
    ):
        """
        Initialize Binance API client.
        
        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            base_url: Base URL (e.g., https://api.binance.com or https://testnet.binance.vision)
            testnet: Whether using testnet (affects default base_url if not provided)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        
        if not base_url:
            if testnet:
                # Spot testnet
                self.base_url = "https://testnet.binance.vision"
            else:
                # Spot production
                self.base_url = "https://api.binance.com"
        else:
            self.base_url = base_url.rstrip('/')
        
        self.session = requests.Session()
        self.session.headers.update({
            'X-MBX-APIKEY': self.api_key
        })
        # Server time sync: offset in ms to add to local time to get Binance server time
        self._time_offset_ms: Optional[int] = None
        self._time_offset_fetched_at: Optional[float] = None

    def _fetch_time_offset(self) -> None:
        """Fetch Binance server time and set _time_offset_ms (server - local)."""
        endpoint = '/api/v3/time'
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            server_time_ms = int(data.get('serverTime', 0))
            local_ms = int(time.time() * 1000)
            self._time_offset_ms = server_time_ms - local_ms
            self._time_offset_fetched_at = time.time()
        except Exception:
            # On failure, preserve existing offset so REST/wsapi can keep using last good sync
            if self._time_offset_ms is None:
                self._time_offset_ms = 0
            # Do not update _time_offset_fetched_at so next _ensure_time_sync will retry

    def _ensure_time_sync(self) -> None:
        """Refresh server-time offset if not set or stale."""
        now = time.time()
        if (
            self._time_offset_ms is None
            or self._time_offset_fetched_at is None
            or (now - self._time_offset_fetched_at) > TIME_OFFSET_REFRESH_INTERVAL
        ):
            self._fetch_time_offset()

    def _sign_request(self, params: Dict) -> str:
        """
        Generate HMAC-SHA256 signature for request parameters.
        
        Args:
            params: Request parameters dict
            
        Returns:
            Signature string (hex)
        """
        query_string = urlencode(sorted(params.items()))
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _prepare_params(self, params: Dict) -> Dict:
        """
        Prepare parameters: add timestamp (synced with Binance server) and signature.
        
        Args:
            params: Request parameters
            
        Returns:
            Parameters with timestamp and signature
        """
        self._ensure_time_sync()
        local_ms = int(time.time() * 1000)
        params = params.copy()
        params['timestamp'] = local_ms + (self._time_offset_ms or 0)
        params['signature'] = self._sign_request(params)
        return params
    
    def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Union[float, str],
        price: Optional[Union[float, str]] = None,
        time_in_force: Optional[str] = None,
        client_order_id: Optional[str] = None
    ) -> Dict:
        """
        Place a new order.
        
        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            side: Order side ('BUY' or 'SELL')
            order_type: Order type ('MARKET', 'LIMIT', etc.)
            quantity: Order quantity
            price: Order price (required for LIMIT orders)
            time_in_force: Time in force (e.g., 'GTC', 'IOC', 'FOK') - required for LIMIT
            client_order_id: Client order ID (optional)
            
        Returns:
            Order response dict from Binance
            
        Raises:
            BinanceAPIError: On API error
        """
        endpoint = '/api/v3/order'
        url = f"{self.base_url}{endpoint}"
        
        # Binance expects quantity/price as strings (decimal form, no scientific notation)
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': _decimal_str(quantity),
        }
        if price is not None:
            params['price'] = _decimal_str(price)
        if time_in_force:
            params['timeInForce'] = time_in_force
        if client_order_id:
            params['newClientOrderId'] = client_order_id

        params = self._prepare_params(params)
        # Send params in same (sorted) order as used for signature so server can verify
        param_list = sorted(params.items())

        try:
            response = self.session.post(url, params=param_list, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    # Non-JSON error response; store text as error_data
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e

    def query_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None
    ) -> Dict:
        """
        Query order status.
        
        Args:
            symbol: Trading symbol
            order_id: Binance order ID
            client_order_id: Client order ID (alternative to order_id)
            
        Returns:
            Order status dict from Binance
            
        Raises:
            BinanceAPIError: On API error
        """
        endpoint = '/api/v3/order'
        url = f"{self.base_url}{endpoint}"
        
        params = {'symbol': symbol}
        
        if order_id:
            params['orderId'] = order_id
        elif client_order_id:
            params['origClientOrderId'] = client_order_id
        else:
            raise ValueError("Either order_id or client_order_id must be provided")
        
        params = self._prepare_params(params)
        param_list = sorted(params.items())

        try:
            response = self.session.get(url, params=param_list, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    # Non-JSON error response; store text as error_data
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List open orders (signed GET /api/v3/openOrders).

        If ``symbol`` is omitted, returns all open orders for the account (higher rate-limit weight).

        Returns:
            List of order dicts (Binance JSON shape).
        """
        endpoint = "/api/v3/openOrders"
        url = f"{self.base_url}{endpoint}"
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        params = self._prepare_params(params)
        param_list = sorted(params.items())
        try:
            response = self.session.get(url, params=param_list, timeout=30)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else []
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None
    ) -> Dict:
        """
        Cancel an order.
        
        Args:
            symbol: Trading symbol
            order_id: Binance order ID
            client_order_id: Client order ID (alternative to order_id)
            
        Returns:
            Cancel response dict from Binance
            
        Raises:
            BinanceAPIError: On API error
        """
        endpoint = '/api/v3/order'
        url = f"{self.base_url}{endpoint}"
        
        params = {'symbol': symbol}
        
        if order_id:
            params['orderId'] = order_id
        elif client_order_id:
            params['origClientOrderId'] = client_order_id
        else:
            raise ValueError("Either order_id or client_order_id must be provided")
        
        params = self._prepare_params(params)
        param_list = sorted(params.items())

        try:
            response = self.session.delete(url, params=param_list, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    # Non-JSON error response; store text as error_data
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e

    def get_account(self) -> Dict:
        """
        Get current account info (signed). Use to verify API key + HMAC auth.

        Returns:
            Account dict (balances, permissions, etc.)

        Raises:
            BinanceAPIError: On API error (e.g. invalid key or signature)
        """
        endpoint = '/api/v3/account'
        url = f"{self.base_url}{endpoint}"
        params = self._prepare_params({})
        param_list = sorted(params.items())
        try:
            response = self.session.get(url, params=param_list, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    # Non-JSON error response; store text as error_data
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e

    def get_account_snapshot(self, account_id: str = "default") -> Dict:
        """
        Get current account snapshot (balances) via REST API.

        Converts Binance REST account response to unified AccountEvent format
        for consistency with account listener events.

        Args:
            account_id: Account identifier (default: "default")

        Returns:
            Unified AccountEvent dict:
            {
                "event_type": "account_position",
                "broker": "binance",
                "account_id": account_id,
                "balances": [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}, ...],
                "positions": [],
                "updated_at": "2024-01-01T00:00:00Z",
                "payload": {...}  # Raw Binance REST response
            }

        Raises:
            BinanceAPIError: On API error
        """
        from datetime import datetime, timezone

        account_resp = self.get_account()
        
        # Convert Binance balances to unified format
        balances = []
        binance_balances = account_resp.get("balances", [])
        for bal in binance_balances:
            asset = bal.get("asset", "")
            free = bal.get("free", "0.0")
            locked = bal.get("locked", "0.0")
            if asset:  # Only include non-zero or non-empty assets
                balances.append({
                    "asset": asset,
                    "available": str(free) if free else "0.0",
                    "locked": str(locked) if locked else "0.0",
                })

        # Timestamp: use account updateTime if available, else current time
        update_time_ms = account_resp.get("updateTime")
        if update_time_ms:
            try:
                updated_at = datetime.fromtimestamp(int(update_time_ms) / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")
            except (TypeError, ValueError):
                updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        else:
            updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        return {
            "event_type": "account_position",
            "broker": "binance",
            "account_id": account_id,
            "balances": balances,
            "positions": [],  # Spot accounts don't have positions
            "updated_at": updated_at,
            "payload": account_resp,  # Store raw REST response
        }

    def start_user_data_stream(self) -> str:
        """
        Create a user data stream listen key (no signature required).
        Used to connect to the WebSocket for execution reports / fills.

        Returns:
            listenKey string

        Raises:
            BinanceAPIError: On API error
        """
        endpoint = '/api/v3/userDataStream'
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.post(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data['listenKey']
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    # Non-JSON error response; store text as error_data
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e

    def keepalive_user_data_stream(self, listen_key: str) -> None:
        """
        Keep user data stream alive (no signature required).
        Should be called periodically (e.g. every 30 min).

        Raises:
            BinanceAPIError: On API error
        """
        endpoint = '/api/v3/userDataStream'
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.put(url, params={'listenKey': listen_key}, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    # Non-JSON error response; store text as error_data
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e

    def close_user_data_stream(self, listen_key: str) -> None:
        """
        Close user data stream (no signature required).

        Raises:
            BinanceAPIError: On API error
        """
        endpoint = '/api/v3/userDataStream'
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.delete(url, params={'listenKey': listen_key}, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise BinanceAPIError(
                        f"Binance API error: {error_data.get('msg', 'Unknown error')} "
                        f"(code: {error_data.get('code', 'N/A')})",
                        error_data=error_data,
                    ) from e
                except ValueError:
                    # Non-JSON error response; store text as error_data
                    error_data = {"text": e.response.text}
                    raise BinanceAPIError(
                        f"Binance API error: {e.response.text}",
                        error_data=error_data,
                    ) from e
            raise BinanceAPIError(f"Request failed: {str(e)}", error_data=None) from e
