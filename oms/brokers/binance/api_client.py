"""
Binance API client (lowest level).

HTTP client for Binance REST API with HMAC-SHA256 signing.
Supports testnet and production URLs.
Syncs request timestamp with Binance server time to avoid -1021 (timestamp ahead/behind).
"""

import hashlib
import hmac
import time
from typing import Dict, Optional
from urllib.parse import urlencode

import requests


# Refresh server-time offset after this many seconds
TIME_OFFSET_REFRESH_INTERVAL = 300


class BinanceAPIError(Exception):
    """Binance API error."""
    pass


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
        quantity: float,
        price: Optional[float] = None,
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
        
        # Binance expects quantity/price as strings; use same format for signing and request
        params = {
            'symbol': symbol,
            'side': side,
            'type': order_type,
            'quantity': str(quantity) if not isinstance(quantity, str) else quantity,
        }
        if price is not None:
            params['price'] = str(price) if not isinstance(price, str) else price
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
                        f"(code: {error_data.get('code', 'N/A')})"
                    ) from e
                except ValueError:
                    raise BinanceAPIError(f"Binance API error: {e.response.text}") from e
            raise BinanceAPIError(f"Request failed: {str(e)}") from e

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
                        f"(code: {error_data.get('code', 'N/A')})"
                    ) from e
                except ValueError:
                    raise BinanceAPIError(f"Binance API error: {e.response.text}") from e
            raise BinanceAPIError(f"Request failed: {str(e)}") from e

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
                        f"(code: {error_data.get('code', 'N/A')})"
                    ) from e
                except ValueError:
                    raise BinanceAPIError(f"Binance API error: {e.response.text}") from e
            raise BinanceAPIError(f"Request failed: {str(e)}") from e

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
                        f"(code: {error_data.get('code', 'N/A')})"
                    ) from e
                except ValueError:
                    raise BinanceAPIError(f"Binance API error: {e.response.text}") from e
            raise BinanceAPIError(f"Request failed: {str(e)}") from e

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
                        f"(code: {error_data.get('code', 'N/A')})"
                    ) from e
                except ValueError:
                    raise BinanceAPIError(f"Binance API error: {e.response.text}") from e
            raise BinanceAPIError(f"Request failed: {str(e)}") from e

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
                        f"(code: {error_data.get('code', 'N/A')})"
                    ) from e
                except ValueError:
                    raise BinanceAPIError(f"Binance API error: {e.response.text}") from e
            raise BinanceAPIError(f"Request failed: {str(e)}") from e

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
                        f"(code: {error_data.get('code', 'N/A')})"
                    ) from e
                except ValueError:
                    raise BinanceAPIError(f"Binance API error: {e.response.text}") from e
            raise BinanceAPIError(f"Request failed: {str(e)}") from e
