"""
OMS runnable entrypoint (tasks 12.1.11a, 12.1.11b).

Bootstrap: create store and registry, register Binance adapter, start fill listeners.
Main loop: process_one (risk_approved), process_one_cancel, trim_oms_streams periodically.
"""

import os
import signal
import sys
from typing import Callable, Optional

from redis import Redis

from oms.log import logger

from oms.cancel_consumer import ensure_cancel_requested_consumer_group
from oms.cleanup import trim_oms_streams
from oms.consumer import ensure_risk_approved_consumer_group
from oms.redis_flow import make_fill_callback, process_one, process_one_cancel
from oms.registry import AdapterRegistry
from oms.storage.redis_order_store import RedisOrderStore
from oms.sync import sync_one_order

DEFAULT_BLOCK_MS = 5000
DEFAULT_TRIM_EVERY_N = 12  # trim streams every N loop iterations (~60s if block_ms=5000)
CONSUMER_GROUP = "oms"
CONSUMER_NAME = "oms-1"


def _env(key: str, default: str = "") -> str:
    return (os.environ.get(key) or default).strip()


def get_redis() -> Redis:
    """Build Redis client from REDIS_URL."""
    url = _env("REDIS_URL", "redis://localhost:6379")
    return Redis.from_url(url, decode_responses=True)


def get_store(redis: Redis) -> RedisOrderStore:
    """Build Redis order store."""
    return RedisOrderStore(redis)


def get_registry() -> AdapterRegistry:
    """
    Build adapter registry and register Binance if BINANCE_API_KEY is set.
    """
    registry = AdapterRegistry()
    api_key = _env("BINANCE_API_KEY")
    if api_key:
        from oms.brokers.binance.api_client import BinanceAPIClient
        from oms.brokers.binance.adapter import BinanceBrokerAdapter
        base_url = _env("BINANCE_BASE_URL") or "https://testnet.binance.vision"
        client = BinanceAPIClient(
            api_key=api_key,
            api_secret=_env("BINANCE_API_SECRET"),
            base_url=base_url,
            testnet="testnet" in base_url.lower(),
        )
        registry.register("binance", BinanceBrokerAdapter(client=client))
    return registry


def start_fill_listeners(
    redis: Redis,
    store: RedisOrderStore,
    registry: AdapterRegistry,
    on_terminal_sync: Optional[Callable[[str], None]] = None,
    terminal_order_ttl_seconds: Optional[int] = None,
) -> None:
    """
    Start fill listener for each registered adapter (12.1.11a).
    One shared callback from make_fill_callback(redis, store) per adapter.
    """
    fill_cb = make_fill_callback(
        redis,
        store,
        terminal_order_ttl_seconds=terminal_order_ttl_seconds,
        on_terminal_sync=on_terminal_sync,
    )
    for broker_name in registry.broker_names():
        adapter = registry.get(broker_name)
        if adapter and hasattr(adapter, "start_fill_listener"):
            adapter.start_fill_listener(fill_cb)


def stop_fill_listeners(registry: AdapterRegistry) -> None:
    """Stop fill listener on each registered adapter."""
    for broker_name in registry.broker_names():
        adapter = registry.get(broker_name)
        if adapter and hasattr(adapter, "stop_fill_listener") and callable(getattr(adapter, "stop_fill_listener")):
            adapter.stop_fill_listener()


def run_oms_loop(
    redis: Redis,
    store: RedisOrderStore,
    registry: AdapterRegistry,
    *,
    block_ms: int = DEFAULT_BLOCK_MS,
    trim_every_n: int = DEFAULT_TRIM_EVERY_N,
    stop_after_n: Optional[int] = None,
    run_until: Optional[Callable[[], bool]] = None,
    consumer_group: str = CONSUMER_GROUP,
    consumer_name: str = CONSUMER_NAME,
    on_terminal_sync: Optional[Callable[[str], None]] = None,
) -> int:
    """
    Run OMS main loop (12.1.11b): process_one, process_one_cancel, trim periodically.

    Ensures consumer groups for risk_approved and cancel_requested, then loops until
    stop_after_n messages processed (if set), run_until() returns True, or interrupted.

    Returns number of messages processed (risk_approved + cancel_requested).
    """
    ensure_risk_approved_consumer_group(redis, consumer_group)
    ensure_cancel_requested_consumer_group(redis, consumer_group)

    processed = 0
    iteration = 0
    while True:
        if stop_after_n is not None and processed >= stop_after_n:
            break
        if run_until is not None and run_until():
            break

        result = process_one(
            redis,
            store,
            registry,
            block_ms=block_ms,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
            on_terminal_sync=on_terminal_sync,
        )
        if result is not None:
            processed += 1
            if stop_after_n is not None and processed >= stop_after_n:
                break

        cancel_result = process_one_cancel(
            redis,
            store,
            registry,
            block_ms=0,
            consumer_group=consumer_group,
            consumer_name=consumer_name,
        )
        if cancel_result is not None:
            processed += 1
            if stop_after_n is not None and processed >= stop_after_n:
                break

        iteration += 1
        if trim_every_n and iteration % trim_every_n == 0:
            removed = trim_oms_streams(redis)
            logger.debug("trim_oms_streams removed {} entries", removed)

        if run_until is not None and run_until():
            break

    return processed


def main() -> int:
    """
    Load REDIS_URL, create store and registry, register Binance, start fill listeners,
    then run process_one / process_one_cancel loop until shutdown.
    """
    redis = get_redis()
    store = get_store(redis)
    registry = get_registry()

    if not registry.broker_names():
        logger.warning("No adapters registered (set BINANCE_API_KEY for Binance)")
        return 1

    logger.info("OMS starting brokers={}", registry.broker_names())

    database_url = _env("DATABASE_URL")
    sync_ttl = 3600
    try:
        sync_ttl = int(os.environ.get("OMS_SYNC_TTL_AFTER_SECONDS", "3600"))
    except (TypeError, ValueError):
        pass

    def on_terminal_sync(order_id: str) -> None:
        if database_url:
            sync_one_order(redis, store, database_url, order_id, ttl_after_sync_seconds=sync_ttl)

    start_fill_listeners(redis, store, registry, on_terminal_sync=on_terminal_sync)
    logger.info("Fill listeners started for brokers={}", registry.broker_names())

    shutdown = [False]

    def on_signal(_signum: int, _frame: object) -> None:
        shutdown[0] = True

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        run_oms_loop(
            redis,
            store,
            registry,
            block_ms=DEFAULT_BLOCK_MS,
            trim_every_n=DEFAULT_TRIM_EVERY_N,
            run_until=lambda: shutdown[0],
            on_terminal_sync=on_terminal_sync if database_url else None,
        )
    except KeyboardInterrupt:
        pass
    finally:
        stop_fill_listeners(registry)
        logger.info("OMS shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
