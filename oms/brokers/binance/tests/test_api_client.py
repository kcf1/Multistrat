"""
Unit tests for Binance API client.

Tests use mocked HTTP responses to verify:
- Request signing (HMAC-SHA256)
- Request format (URL, params, headers)
- Error handling
"""

import hashlib
import hmac
import json
import time
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlparse

import pytest
import requests.exceptions
import responses

from oms.brokers.binance.api_client import BinanceAPIClient, BinanceAPIError


@pytest.fixture(autouse=True)
def disable_time_sync():
    """Disable server-time sync in unit tests so only the mocked API request is made."""
    def _no_sync(self):
        self._time_offset_ms = 0
        self._time_offset_fetched_at = time.time()
    with patch.object(BinanceAPIClient, '_ensure_time_sync', _no_sync):
        yield


@pytest.fixture
def api_client():
    """Create a test Binance API client."""
    return BinanceAPIClient(
        api_key='test_api_key',
        api_secret='test_api_secret',
        base_url='https://testnet.binance.vision',
        testnet=True
    )


@pytest.fixture
def api_client_prod():
    """Create a production Binance API client."""
    return BinanceAPIClient(
        api_key='test_api_key',
        api_secret='test_api_secret',
        base_url='https://api.binance.com',
        testnet=False
    )


class TestBinanceAPIClientInit:
    """Test client initialization."""
    
    def test_init_with_base_url(self):
        """Test initialization with explicit base URL."""
        client = BinanceAPIClient(
            api_key='key',
            api_secret='secret',
            base_url='https://api.binance.com'
        )
        assert client.base_url == 'https://api.binance.com'
        assert client.api_key == 'key'
        assert client.api_secret == 'secret'
        assert 'X-MBX-APIKEY' in client.session.headers
        assert client.session.headers['X-MBX-APIKEY'] == 'key'
    
    def test_init_testnet_default(self):
        """Test initialization with testnet=True uses testnet URL."""
        client = BinanceAPIClient(
            api_key='key',
            api_secret='secret',
            base_url='',
            testnet=True
        )
        assert client.base_url == 'https://testnet.binance.vision'
    
    def test_init_production_default(self):
        """Test initialization with testnet=False uses production URL."""
        client = BinanceAPIClient(
            api_key='key',
            api_secret='secret',
            base_url='',
            testnet=False
        )
        assert client.base_url == 'https://api.binance.com'


class TestRequestSigning:
    """Test HMAC-SHA256 request signing."""
    
    def test_sign_request(self, api_client):
        """Test signature generation."""
        params = {'symbol': 'BTCUSDT', 'side': 'BUY', 'quantity': '0.001'}
        signature = api_client._sign_request(params)
        
        # Verify signature is hex string
        assert isinstance(signature, str)
        assert len(signature) == 64  # SHA256 hex is 64 chars
        
        # Verify signature is deterministic
        signature2 = api_client._sign_request(params)
        assert signature == signature2
    
    def test_sign_request_verification(self, api_client):
        """Test signature matches expected HMAC-SHA256."""
        params = {'symbol': 'BTCUSDT', 'side': 'BUY'}
        signature = api_client._sign_request(params)
        
        # Manually compute expected signature
        query_string = 'side=BUY&symbol=BTCUSDT'
        expected = hmac.new(
            api_client.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        assert signature == expected
    
    def test_prepare_params_adds_timestamp_and_signature(self, api_client):
        """Test _prepare_params adds timestamp and signature."""
        params = {'symbol': 'BTCUSDT', 'side': 'BUY'}
        prepared = api_client._prepare_params(params)
        
        assert 'timestamp' in prepared
        assert 'signature' in prepared
        assert isinstance(prepared['timestamp'], int)
        assert len(prepared['signature']) == 64


class TestPlaceOrder:
    """Test place_order endpoint."""
    
    @responses.activate
    def test_place_order_market_success(self, api_client):
        """Test successful market order placement."""
        responses.add(
            responses.POST,
            'https://testnet.binance.vision/api/v3/order',
            json={
                'orderId': 12345,
                'symbol': 'BTCUSDT',
                'status': 'FILLED',
                'clientOrderId': 'test_order_123',
                'side': 'BUY',
                'type': 'MARKET',
                'executedQty': '0.001',
                'price': '50000.00'
            },
            status=200
        )
        
        result = api_client.place_order(
            symbol='BTCUSDT',
            side='BUY',
            order_type='MARKET',
            quantity=0.001,
            client_order_id='test_order_123'
        )
        
        assert result['orderId'] == 12345
        assert result['symbol'] == 'BTCUSDT'
        assert result['status'] == 'FILLED'
        
        # Verify request format
        assert len(responses.calls) == 1
        request = responses.calls[0].request
        assert request.method == 'POST'
        assert 'X-MBX-APIKEY' in request.headers
        assert request.headers['X-MBX-APIKEY'] == 'test_api_key'
        
        # Verify params include signature and timestamp
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        assert 'signature' in params
        assert 'timestamp' in params
    
    @responses.activate
    def test_place_order_limit_success(self, api_client):
        """Test successful limit order placement."""
        responses.add(
            responses.POST,
            'https://testnet.binance.vision/api/v3/order',
            json={
                'orderId': 12346,
                'symbol': 'BTCUSDT',
                'status': 'NEW',
                'side': 'SELL',
                'type': 'LIMIT',
                'price': '51000.00',
                'timeInForce': 'GTC'
            },
            status=200
        )
        
        result = api_client.place_order(
            symbol='BTCUSDT',
            side='SELL',
            order_type='LIMIT',
            quantity=0.001,
            price=51000.00,
            time_in_force='GTC'
        )
        
        assert result['orderId'] == 12346
        assert result['type'] == 'LIMIT'
        
        # Verify price and timeInForce in request
        request = responses.calls[0].request
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        assert params['price'][0] == '51000.0'
        assert params['timeInForce'][0] == 'GTC'
    
    @responses.activate
    def test_place_order_api_error(self, api_client):
        """Test API error handling."""
        responses.add(
            responses.POST,
            'https://testnet.binance.vision/api/v3/order',
            json={
                'code': -1013,
                'msg': 'Invalid quantity.'
            },
            status=400
        )
        
        with pytest.raises(BinanceAPIError) as exc_info:
            api_client.place_order(
                symbol='BTCUSDT',
                side='BUY',
                order_type='MARKET',
                quantity=-0.001  # Invalid negative quantity
            )
        
        assert 'Invalid quantity' in str(exc_info.value)
        assert '-1013' in str(exc_info.value)
    
    def test_place_order_network_error(self, api_client):
        """Test network error handling."""
        # Simulate network error by mocking requests to raise ConnectionError
        with patch.object(api_client.session, 'post') as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError('Connection timeout')
            
            with pytest.raises(BinanceAPIError) as exc_info:
                api_client.place_order(
                    symbol='BTCUSDT',
                    side='BUY',
                    order_type='MARKET',
                    quantity=0.001
                )
            
            assert 'Request failed' in str(exc_info.value)


class TestQueryOrder:
    """Test query_order endpoint."""
    
    @responses.activate
    def test_query_order_by_order_id(self, api_client):
        """Test query order by Binance order ID."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/order',
            json={
                'orderId': 12345,
                'symbol': 'BTCUSDT',
                'status': 'FILLED',
                'side': 'BUY',
                'type': 'MARKET',
                'executedQty': '0.001'
            },
            status=200
        )
        
        result = api_client.query_order(
            symbol='BTCUSDT',
            order_id=12345
        )
        
        assert result['orderId'] == 12345
        assert result['status'] == 'FILLED'
        
        # Verify request params
        request = responses.calls[0].request
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        assert params['symbol'][0] == 'BTCUSDT'
        assert params['orderId'][0] == '12345'
        assert 'signature' in params
    
    @responses.activate
    def test_query_order_by_client_order_id(self, api_client):
        """Test query order by client order ID."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/order',
            json={
                'orderId': 12345,
                'clientOrderId': 'test_order_123',
                'status': 'NEW'
            },
            status=200
        )
        
        result = api_client.query_order(
            symbol='BTCUSDT',
            client_order_id='test_order_123'
        )
        
        assert result['clientOrderId'] == 'test_order_123'
        
        # Verify request params
        request = responses.calls[0].request
        parsed_url = urlparse(request.url)
        params = parse_qs(parsed_url.query)
        assert params['origClientOrderId'][0] == 'test_order_123'
    
    def test_query_order_missing_ids(self, api_client):
        """Test query_order requires order_id or client_order_id."""
        with pytest.raises(ValueError, match='Either order_id or client_order_id'):
            api_client.query_order(symbol='BTCUSDT')


class TestCancelOrder:
    """Test cancel_order endpoint."""
    
    @responses.activate
    def test_cancel_order_success(self, api_client):
        """Test successful order cancellation."""
        responses.add(
            responses.DELETE,
            'https://testnet.binance.vision/api/v3/order',
            json={
                'orderId': 12345,
                'symbol': 'BTCUSDT',
                'status': 'CANCELED'
            },
            status=200
        )
        
        result = api_client.cancel_order(
            symbol='BTCUSDT',
            order_id=12345
        )
        
        assert result['status'] == 'CANCELED'
        
        # Verify DELETE method
        assert len(responses.calls) == 1
        request = responses.calls[0].request
        assert request.method == 'DELETE'
    
    @responses.activate
    def test_cancel_order_not_found(self, api_client):
        """Test cancel order that doesn't exist."""
        responses.add(
            responses.DELETE,
            'https://testnet.binance.vision/api/v3/order',
            json={
                'code': -2011,
                'msg': 'Unknown order sent.'
            },
            status=400
        )
        
        with pytest.raises(BinanceAPIError) as exc_info:
            api_client.cancel_order(
                symbol='BTCUSDT',
                order_id=99999
            )
        
        assert 'Unknown order' in str(exc_info.value)


class TestBinanceAPIClientGetAccount:
    """Test get_account (signed) for auth verification."""

    @responses.activate
    def test_get_account_success(self, api_client):
        """GET /api/v3/account returns account with timestamp + signature."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/account',
            json={'balances': [], 'makerCommission': 0, 'updateTime': 123456789},
            status=200
        )
        account = api_client.get_account()
        assert account.get('balances') is not None
        assert 'makerCommission' in account
        assert len(responses.calls) == 1
        req = responses.calls[0].request
        assert 'timestamp' in req.url
        assert 'signature' in req.url
        assert req.headers.get('X-MBX-APIKEY') == 'test_api_key'

    @responses.activate
    def test_get_account_unauthorized(self, api_client):
        """GET /api/v3/account raises on 401."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/account',
            json={'code': -2014, 'msg': 'API-key format invalid.'},
            status=401
        )
        with pytest.raises(BinanceAPIError) as exc_info:
            api_client.get_account()
        assert 'API-key' in str(exc_info.value) or '2014' in str(exc_info.value)


class TestBinanceAPIClientUserDataStream:
    """Test user data stream endpoints (no signature required)."""

    @responses.activate
    def test_start_user_data_stream_returns_listen_key(self, api_client):
        """POST /api/v3/userDataStream returns listenKey."""
        responses.add(
            responses.POST,
            'https://testnet.binance.vision/api/v3/userDataStream',
            json={'listenKey': 'pqia91ma19a5s61cv6a81va65b'},
            status=200
        )
        key = api_client.start_user_data_stream()
        assert key == 'pqia91ma19a5s61cv6a81va65b'
        assert len(responses.calls) == 1
        assert responses.calls[0].request.headers.get('X-MBX-APIKEY') == 'test_api_key'

    @responses.activate
    def test_keepalive_user_data_stream(self, api_client):
        """PUT /api/v3/userDataStream?listenKey=... succeeds."""
        responses.add(
            responses.PUT,
            'https://testnet.binance.vision/api/v3/userDataStream',
            json={},
            status=200
        )
        api_client.keepalive_user_data_stream('some-listen-key')
        assert len(responses.calls) == 1
        assert 'listenKey=some-listen-key' in responses.calls[0].request.url

    @responses.activate
    def test_close_user_data_stream(self, api_client):
        """DELETE /api/v3/userDataStream?listenKey=... succeeds."""
        responses.add(
            responses.DELETE,
            'https://testnet.binance.vision/api/v3/userDataStream',
            json={},
            status=200
        )
        api_client.close_user_data_stream('some-listen-key')
        assert len(responses.calls) == 1
        assert 'listenKey=some-listen-key' in responses.calls[0].request.url


class TestBinanceAPIClientGetAccountSnapshot:
    """Test get_account_snapshot (task 12.2.3)."""

    @responses.activate
    def test_get_account_snapshot_converts_to_unified_format(self, api_client):
        """get_account_snapshot converts Binance REST response to unified AccountEvent format."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/account',
            json={
                'balances': [
                    {'asset': 'USDT', 'free': '1000.500000', 'locked': '0.000000'},
                    {'asset': 'BTC', 'free': '0.100000', 'locked': '0.050000'},
                ],
                'updateTime': 1564034571105,
            },
            status=200
        )
        snapshot = api_client.get_account_snapshot(account_id='test-account')
        assert snapshot['event_type'] == 'account_position'
        assert snapshot['broker'] == 'binance'
        assert snapshot['account_id'] == 'test-account'
        assert len(snapshot['balances']) == 2
        assert snapshot['balances'][0]['asset'] == 'USDT'
        assert snapshot['balances'][0]['available'] == '1000.500000'
        assert snapshot['balances'][0]['locked'] == '0.000000'
        assert snapshot['balances'][1]['asset'] == 'BTC'
        assert snapshot['balances'][1]['available'] == '0.100000'
        assert snapshot['balances'][1]['locked'] == '0.050000'
        assert snapshot['positions'] == []
        assert 'updated_at' in snapshot
        assert 'payload' in snapshot
        assert snapshot['payload']['updateTime'] == 1564034571105

    @responses.activate
    def test_get_account_snapshot_default_account_id(self, api_client):
        """get_account_snapshot uses default account_id if not provided."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/account',
            json={'balances': [], 'updateTime': 123456789},
            status=200
        )
        snapshot = api_client.get_account_snapshot()
        assert snapshot['account_id'] == 'default'

    @responses.activate
    def test_get_account_snapshot_filters_empty_assets(self, api_client):
        """get_account_snapshot only includes assets with non-empty asset symbol."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/account',
            json={
                'balances': [
                    {'asset': 'USDT', 'free': '1000.0', 'locked': '0.0'},
                    {'asset': '', 'free': '0.0', 'locked': '0.0'},  # Empty asset
                    {'asset': 'BTC', 'free': '0.0', 'locked': '0.0'},
                ],
            },
            status=200
        )
        snapshot = api_client.get_account_snapshot()
        # Should include USDT and BTC, but not empty asset
        assert len(snapshot['balances']) == 2
        assert snapshot['balances'][0]['asset'] == 'USDT'
        assert snapshot['balances'][1]['asset'] == 'BTC'

    @responses.activate
    def test_get_account_snapshot_handles_missing_update_time(self, api_client):
        """get_account_snapshot uses current time if updateTime is missing."""
        responses.add(
            responses.GET,
            'https://testnet.binance.vision/api/v3/account',
            json={'balances': []},  # No updateTime
            status=200
        )
        snapshot = api_client.get_account_snapshot()
        assert 'updated_at' in snapshot
        # Should be recent timestamp
        assert snapshot['updated_at'] is not None
