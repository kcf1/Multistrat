"""
Quick test script to place an order and show all returned events.
"""
import os
import time
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

from oms.brokers.binance.api_client import BinanceAPIClient
from oms.brokers.binance.adapter import BinanceBrokerAdapter
from oms.brokers.binance.fills_listener import create_fills_listener
from oms.brokers.binance.account_listener import create_account_listener

def main():
    # Initialize client
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        print("Error: BINANCE_API_KEY and BINANCE_API_SECRET must be set in .env")
        return
    
    # Use testnet by default
    base_url = os.getenv("BINANCE_BASE_URL", "https://testnet.binance.vision")
    client = BinanceAPIClient(api_key=api_key, api_secret=api_secret, base_url=base_url)
    adapter = BinanceBrokerAdapter(client=client)
    
    # Collect events
    fill_events = []
    account_events = []
    
    # Start fill listener
    print("Starting fill listener...")
    fills_listener = create_fills_listener(client, on_fill_or_reject=fill_events.append)
    fills_listener.start_background()
    
    # Wait for connection
    for _ in range(50):
        time.sleep(0.2)
        if fills_listener.stream_connected:
            break
    if not fills_listener.stream_connected:
        print("Fill listener did not connect")
        fills_listener.stop()
        return
    print(f"[OK] Fill listener connected (subscriptionId={fills_listener._subscription_id})")
    
    # Start account listener
    print("Starting account listener...")
    account_listener = create_account_listener(client, on_account_event=account_events.append)
    account_listener.start_background()
    
    # Wait for connection
    for _ in range(50):
        time.sleep(0.2)
        if account_listener.stream_connected:
            break
    if not account_listener.stream_connected:
        print("Account listener did not connect")
        account_listener.stop()
        fills_listener.stop()
        return
    print(f"[OK] Account listener connected (subscriptionId={account_listener._subscription_id})")
    
    # Place order
    symbol = "BTCUSDT"
    side = "BUY"
    quantity = 0.0001
    order_id = f"test_{int(time.time() * 1000)}"
    
    print(f"\n--- Placing order ---")
    print(f"Symbol: {symbol}")
    print(f"Side: {side}")
    print(f"Quantity: {quantity}")
    print(f"Order ID: {order_id}")
    
    try:
        order = {
            "order_id": order_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "order_type": "MARKET",
        }
        result = adapter.place_order(order)
        
        print(f"\n--- Order Response ---")
        print(json.dumps(result, indent=2))
        
        broker_order_id = result.get("broker_order_id")
        print(f"\nBroker Order ID: {broker_order_id}")
        
    except Exception as e:
        print(f"\nError placing order: {e}")
        fills_listener.stop()
        account_listener.stop()
        return
    
    # Wait for events (up to 10 seconds)
    print(f"\n--- Waiting for events (up to 10s) ---")
    for i in range(100):
        time.sleep(0.1)
        if fill_events and account_events:
            break
    
    # Show fill events
    print(f"\n--- Fill Events ({len(fill_events)} received) ---")
    for i, event in enumerate(fill_events, 1):
        print(f"\nFill Event #{i}:")
        print(json.dumps(event, indent=2))
    
    # Show account events
    print(f"\n--- Account Events ({len(account_events)} received) ---")
    for i, event in enumerate(account_events, 1):
        print(f"\nAccount Event #{i}:")
        print(f"  Event Type: {event.get('event_type')}")
        print(f"  Broker: {event.get('broker')}")
        print(f"  Account ID: {event.get('account_id')}")
        print(f"  Updated At: {event.get('updated_at')}")
        print(f"  Balances ({len(event.get('balances', []))}):")
        for bal in event.get('balances', []):
            print(f"    - {bal.get('asset')}: available={bal.get('available')}, locked={bal.get('locked')}")
        print(f"  Positions: {event.get('positions', [])}")
        print(f"  Payload (raw):")
        print(json.dumps(event.get('payload', {}), indent=4))
    
    # Stop listeners
    print(f"\n--- Stopping listeners ---")
    fills_listener.stop()
    account_listener.stop()
    print("Done!")

if __name__ == "__main__":
    main()
