#!/usr/bin/env python3
"""
Standalone Binance **spot** user data stream sniffer: logs every WebSocket frame
to a text file (no Redis, no Postgres). Use beside OMS to verify the broker is
pushing events (fills, account snapshots, balanceUpdate, etc.).

Uses the same credentials and URL rules as OMS by default:
  BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_BASE_URL, BINANCE_TESTNET

Use **--live** to ignore those URL flags and connect to **production** spot
(``https://api.binance.com``). You must use **mainnet API keys**; testnet keys will fail.

Subscription mode (same defaults as fills listener):
  BINANCE_FILLS_STREAM=wsapi|listenkey   (default wsapi)

Output path:
  --out PATH   or env BINANCE_USER_STREAM_LOG (default: binance_user_stream.log in cwd)

Examples:
  python scripts/binance_user_stream_log_to_file.py
  python scripts/binance_user_stream_log_to_file.py --live
  python scripts/binance_user_stream_log_to_file.py --out C:\\temp\\binance_ws.log
  python scripts/binance_user_stream_log_to_file.py --mode listenkey
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

try:
    import dotenv

    dotenv.load_dotenv(os.path.join(repo_root, ".env"))
except ImportError:
    pass

import websocket

from oms.brokers.binance.api_client import BinanceAPIClient, BinanceAPIError
from oms.brokers.binance.fills_listener import (
    _stream_url_from_base_url,
    _ws_api_url_from_base_url,
)


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _client_from_env(*, live: bool = False, testnet_flag: bool = False) -> BinanceAPIClient:
    key = (os.environ.get("BINANCE_API_KEY") or "").strip()
    secret = (os.environ.get("BINANCE_API_SECRET") or "").strip()
    if live:
        base = "https://api.binance.com"
        testnet = False
    elif testnet_flag:
        base = "https://testnet.binance.vision"
        testnet = True
    else:
        base = (os.environ.get("BINANCE_BASE_URL") or "").strip()
        testnet_raw = (os.environ.get("BINANCE_TESTNET") or "").strip().lower()
        testnet = testnet_raw in ("1", "true", "yes")
        if not base:
            base = "https://testnet.binance.vision" if testnet else "https://api.binance.com"
    if not key or not secret:
        print("BINANCE_API_KEY and BINANCE_API_SECRET are required.", file=sys.stderr)
        sys.exit(1)
    return BinanceAPIClient(api_key=key, api_secret=secret, base_url=base, testnet=testnet)


def _append_log(path: str, line: str) -> None:
    line_one = line.replace("\r", " ").replace("\n", " ")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{_ts()} {line_one}\n")
        f.flush()


_KEEPALIVE_INTERVAL_SEC = 30 * 60


def _run_listenkey(client: BinanceAPIClient, out_path: str, ws_ref: dict[str, Any]) -> None:
    stream_base = _stream_url_from_base_url(client.base_url)
    listen_key = client.start_user_data_stream()
    ws_url = f"{stream_base.rstrip('/')}/{listen_key}"
    _append_log(out_path, f"[meta] listenKey mode URL prefix={ws_url[:56]}...")

    stop = threading.Event()

    def keepalive_loop() -> None:
        while not stop.wait(timeout=_KEEPALIVE_INTERVAL_SEC):
            try:
                client.keepalive_user_data_stream(listen_key)
                _append_log(out_path, "[meta] listenKey keepalive OK")
            except BinanceAPIError as e:
                _append_log(out_path, f"[meta] listenKey keepalive failed: {e}")

    ka = threading.Thread(target=keepalive_loop, daemon=True)
    ka.start()

    def on_message(_ws: Any, raw: str) -> None:
        _append_log(out_path, raw)

    def on_error(_ws: Any, err: Optional[BaseException]) -> None:
        if err:
            _append_log(out_path, f"[meta] ws error: {err!r}")

    def on_close(_ws: Any, close_status: Optional[int], close_msg: Optional[str]) -> None:
        stop.set()
        _append_log(out_path, f"[meta] ws closed status={close_status} msg={close_msg!r}")

    def on_open(_ws: Any) -> None:
        _append_log(out_path, "[meta] listenKey WebSocket open")

    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws_ref["ws"] = ws
    try:
        ws.run_forever(ping_interval=20, ping_timeout=10)
    finally:
        stop.set()
        ws_ref["ws"] = None
        try:
            client.close_user_data_stream(listen_key)
        except BinanceAPIError:
            pass


def _run_wsapi(client: BinanceAPIClient, out_path: str, ws_ref: dict[str, Any]) -> None:
    ws_url = _ws_api_url_from_base_url(client.base_url)
    _append_log(out_path, f"[meta] wsapi mode connecting {ws_url}")

    def on_message(_ws: Any, raw: str) -> None:
        _append_log(out_path, raw)

    def on_error(_ws: Any, err: Optional[BaseException]) -> None:
        if err:
            _append_log(out_path, f"[meta] ws error: {err!r}")

    def on_close(_ws: Any, close_status: Optional[int], close_msg: Optional[str]) -> None:
        _append_log(out_path, f"[meta] ws closed status={close_status} msg={close_msg!r}")

    def on_open(ws: Any) -> None:
        client._ensure_time_sync()
        local_ms = int(time.time() * 1000)
        timestamp = local_ms + (client._time_offset_ms or 0)
        params = {
            "apiKey": client.api_key,
            "timestamp": timestamp,
            "recvWindow": 5000,
        }
        params["signature"] = client._sign_request(params)
        req = {
            "id": str(uuid.uuid4()),
            "method": "userDataStream.subscribe.signature",
            "params": params,
        }
        try:
            ws.send(json.dumps(req))
            _append_log(out_path, "[meta] sent userDataStream.subscribe.signature")
        except Exception as e:
            _append_log(out_path, f"[meta] subscribe send failed: {e!r}")

    ws = websocket.WebSocketApp(
        ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws_ref["ws"] = ws
    client._ensure_time_sync()
    try:
        ws.run_forever(ping_interval=20, ping_timeout=10)
    finally:
        ws_ref["ws"] = None


def main() -> int:
    parser = argparse.ArgumentParser(description="Log Binance user data stream to a file.")
    parser.add_argument(
        "--out",
        default=os.environ.get("BINANCE_USER_STREAM_LOG", "binance_user_stream.log"),
        help="Log file path (default: ./binance_user_stream.log or BINANCE_USER_STREAM_LOG)",
    )
    parser.add_argument(
        "--mode",
        choices=("wsapi", "listenkey"),
        default=None,
        help="Override BINANCE_FILLS_STREAM (wsapi|listenkey); default follows env then wsapi",
    )
    net = parser.add_mutually_exclusive_group()
    net.add_argument(
        "--live",
        action="store_true",
        help="Production spot (api.binance.com); ignores BINANCE_BASE_URL / BINANCE_TESTNET",
    )
    net.add_argument(
        "--testnet",
        action="store_true",
        help="Spot testnet (testnet.binance.vision); ignores BINANCE_BASE_URL / BINANCE_TESTNET",
    )
    args = parser.parse_args()

    mode = args.mode
    if mode is None:
        stream = os.environ.get("BINANCE_FILLS_STREAM", "wsapi").strip().lower()
        mode = "listenkey" if stream == "listenkey" else "wsapi"

    out_path = os.path.abspath(args.out)
    client = _client_from_env(live=args.live, testnet_flag=args.testnet)
    print(
        f"Logging to {out_path} mode={mode} base_url={client.base_url} testnet={client.testnet} (Ctrl+C to stop)",
        file=sys.stderr,
    )

    ws_ref: dict[str, Any] = {"ws": None}

    def handle_sigint(_signum: int, _frame: Any) -> None:
        w = ws_ref.get("ws")
        if w is not None:
            try:
                w.close()
            except Exception:
                pass
        print("Stopping…", file=sys.stderr)

    signal.signal(signal.SIGINT, handle_sigint)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_sigint)

    try:
        if mode == "listenkey":
            _run_listenkey(client, out_path, ws_ref)
        else:
            _run_wsapi(client, out_path, ws_ref)
    except KeyboardInterrupt:
        handle_sigint(0, None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
