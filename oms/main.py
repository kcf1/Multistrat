"""
OMS runnable entrypoint (tasks 12.1.11a, 12.1.11b).

Bootstrap: create store and registry, register Binance adapter, start fill listeners.
Main loop: process_one (risk_approved), process_one_cancel, trim_oms_streams periodically.
"""

import os
import signal
import sys
import time
from typing import Any, Callable, Optional, Union

from redis import Redis

from oms.log import logger

from oms.account_flow import make_account_callback
from oms.account_repair import run_all_account_repairs
from oms.account_sync import sync_accounts_to_postgres, write_balance_change
from oms.symbol_sync import sync_symbols_from_binance
from oms.cleanup import set_account_key_ttl
from oms.cancel_consumer import ensure_cancel_requested_consumer_group
from oms.cleanup import trim_oms_streams
from oms.consumer import ensure_risk_approved_consumer_group
from oms.redis_flow import make_fill_callback, process_many, process_many_cancel
from oms.registry import AdapterRegistry
from oms.storage.redis_account_store import RedisAccountStore
from oms.storage.redis_order_store import RedisOrderStore
from oms.repair import run_all_repairs
from oms.sync import sync_one_order, sync_terminal_orders, DEFAULT_SYNC_INTERVAL_SECONDS

# Use blocking Redis reads for event-driven behavior (wakes immediately on new messages)
# Block timeout allows periodic checks for cancels and trimming
DEFAULT_BLOCK_MS = 100  # block up to 100ms waiting for messages (event-driven, faster cancel processing)
DEFAULT_POLL_SLEEP_SECONDS = 0.0  # not used when blocking; kept for backward compatibility
DEFAULT_TRIM_EVERY_N = 200  # trim streams every N loop iterations (adjusted for faster wake-ups)
DEFAULT_BATCH_SIZE = 50  # process up to N orders per batch (bulk processing, higher throughput)
DEFAULT_ACCOUNT_REFRESH_INTERVAL_SECONDS = 60  # periodic REST get_account_snapshot
DEFAULT_ACCOUNT_SYNC_INTERVAL_SECONDS = 60  # periodic sync_accounts_to_postgres (when pg_connect set)
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


def get_account_store(redis: Redis) -> RedisAccountStore:
    """Build account store (12.2.9)."""
    return RedisAccountStore(redis)


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
            adapter.start_fill_listener(fill_cb, store=store)


def stop_fill_listeners(registry: AdapterRegistry) -> None:
    """Stop fill listener on each registered adapter."""
    for broker_name in registry.broker_names():
        adapter = registry.get(broker_name)
        if adapter and hasattr(adapter, "stop_fill_listener") and callable(getattr(adapter, "stop_fill_listener")):
            adapter.stop_fill_listener()


def start_account_listeners(
    redis: Redis,
    account_store: RedisAccountStore,
    registry: AdapterRegistry,
    on_account_updated: Optional[Callable[[str, str], None]] = None,
    on_balance_change: Optional[
        Callable[[str, str, str, Any, str, Any, Optional[dict]], None]
    ] = None,
) -> None:
    """
    Start account listener for each registered adapter that supports it (12.2.9).
    Callback updates Redis account store and optionally triggers sync.
    on_balance_change(broker, account_id, asset, delta, event_type, event_time, payload) is called
    for balanceUpdate events so caller can write to balance_changes and set TTL.
    """
    account_cb = make_account_callback(
        redis,
        account_store,
        on_account_updated=on_account_updated,
        on_balance_change=on_balance_change,
    )
    for broker_name in registry.broker_names():
        adapter = registry.get(broker_name)
        if adapter and hasattr(adapter, "start_account_listener") and callable(getattr(adapter, "start_account_listener")):
            adapter.start_account_listener(account_cb)


def stop_account_listeners(registry: AdapterRegistry) -> None:
    """Stop account listener on each registered adapter."""
    for broker_name in registry.broker_names():
        adapter = registry.get(broker_name)
        if adapter and hasattr(adapter, "stop_account_listener") and callable(getattr(adapter, "stop_account_listener")):
            adapter.stop_account_listener()


def wait_for_account_listeners_connected(
    registry: AdapterRegistry,
    timeout_seconds: int = 30,
    poll_interval_seconds: float = 0.2,
) -> bool:
    """
    Wait until every adapter with an account listener reports stream_connected.
    If no adapters have account listeners, returns True immediately.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        all_connected = True
        any_listener = False
        for broker_name in registry.broker_names():
            adapter = registry.get(broker_name)
            if not adapter or not hasattr(adapter, "_account_listener") or adapter._account_listener is None:
                continue
            any_listener = True
            listener = adapter._account_listener
            if hasattr(listener, "stream_connected") and not listener.stream_connected:
                all_connected = False
                break
        if not any_listener or all_connected:
            if any_listener:
                logger.info("All account listeners connected")
            return True
        time.sleep(poll_interval_seconds)
    logger.warning("Account listeners did not connect within {}s; proceeding anyway", timeout_seconds)
    return False


def wait_for_fill_listeners_connected(
    registry: AdapterRegistry,
    timeout_seconds: int = 30,
    poll_interval_seconds: float = 0.2,
) -> bool:
    """
    Wait until every adapter with a fill listener reports stream_connected.
    Avoids processing orders before the user data stream subscription is active.
    Returns True if all connected within timeout, False otherwise.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        all_connected = True
        for broker_name in registry.broker_names():
            adapter = registry.get(broker_name)
            if not adapter or not hasattr(adapter, "_listener") or adapter._listener is None:
                continue
            listener = adapter._listener
            if hasattr(listener, "stream_connected") and not listener.stream_connected:
                all_connected = False
                break
        if all_connected:
            logger.info("All fill listeners connected")
            return True
        time.sleep(poll_interval_seconds)
    logger.warning("Fill listeners did not connect within {}s; proceeding anyway", timeout_seconds)
    return False


def run_oms_loop(
    redis: Redis,
    store: RedisOrderStore,
    registry: AdapterRegistry,
    *,
    account_store: Optional[RedisAccountStore] = None,
    block_ms: Optional[int] = None,
    poll_sleep_seconds: float = DEFAULT_POLL_SLEEP_SECONDS,
    trim_every_n: int = DEFAULT_TRIM_EVERY_N,
    batch_size: int = DEFAULT_BATCH_SIZE,
    stop_after_n: Optional[int] = None,
    run_until: Optional[Callable[[], bool]] = None,
    consumer_group: str = CONSUMER_GROUP,
    consumer_name: str = CONSUMER_NAME,
    on_terminal_sync: Optional[Callable[[str], None]] = None,
    pg_connect: Optional[Union[str, Callable[[], Any]]] = None,
    sync_interval_seconds: int = 0,
    sync_ttl_after_seconds: Optional[int] = None,
    account_refresh_interval_seconds: int = 0,
    account_sync_interval_seconds: int = 0,
    account_sync_ttl_seconds: Optional[int] = None,
) -> int:
    """
    Run OMS main loop (12.1.11b): process_many (bulk), process_one_cancel, trim periodically.

    Ensures consumer groups for risk_approved and cancel_requested, then loops until
    stop_after_n messages processed (if set), run_until() returns True, or interrupted.

    Processes orders in batches (batch_size) for improved throughput when multiple orders
    are queued. Reads up to batch_size messages per iteration.

    When block_ms > 0: Uses blocking Redis Stream reads (XREADGROUP BLOCK) for event-driven
    behavior - wakes up immediately when a message arrives. The block timeout allows periodic
    checks for cancel requests and stream trimming.

    When block_ms = 0: Uses non-blocking reads with polling (poll_sleep_seconds) - less efficient
    but may be needed if blocking reads cause issues in some environments.

    Returns number of messages processed (risk_approved + cancel_requested).
    """
    if block_ms is None:
        block_ms = DEFAULT_BLOCK_MS
    ensure_risk_approved_consumer_group(redis, consumer_group)
    ensure_cancel_requested_consumer_group(redis, consumer_group)

    processed = 0
    iteration = 0
    last_sync_time = 0.0
    last_account_refresh_time = 0.0
    last_account_sync_time = 0.0
    while True:
        if stop_after_n is not None and processed >= stop_after_n:
            break
        if run_until is not None and run_until():
            break

        iteration += 1
        logger.debug("OMS loop iteration {}", iteration)

        results = []
        cancel_results = []
        try:
            logger.debug("OMS loop iter {}: process_many start (batch_size={})", iteration, batch_size)
            results = process_many(
                redis,
                store,
                registry,
                count=batch_size,
                block_ms=block_ms,
                consumer_group=consumer_group,
                consumer_name=consumer_name,
                on_terminal_sync=on_terminal_sync,
            )
            logger.debug("OMS loop iter {}: process_many done count={}", iteration, len(results))
            if results:
                processed += len(results)
                if stop_after_n is not None and processed >= stop_after_n:
                    break

            logger.debug("OMS loop iter {}: process_many_cancel start (batch_size={})", iteration, batch_size)
            cancel_results = process_many_cancel(
                redis,
                store,
                registry,
                count=batch_size,
                block_ms=0,  # Non-blocking for cancels (checked after orders)
                consumer_group=consumer_group,
                consumer_name=consumer_name,
            )
            logger.debug("OMS loop iter {}: process_many_cancel done count={}", iteration, len(cancel_results))
            if cancel_results:
                processed += len(cancel_results)
                if stop_after_n is not None and processed >= stop_after_n:
                    break
        except Exception as e:
            logger.exception("OMS loop iteration error (continuing): {}", e)

        # With blocking reads: no sleep needed - block_ms handles waiting
        # If both returned empty, we either timed out (block_ms) or no messages available
        # The blocking read will wake up immediately when a message arrives (event-driven)
        if not results and not cancel_results and block_ms == 0:
            # Only sleep if using non-blocking mode (backward compatibility)
            logger.debug("OMS loop iter {}: idle, sleep {}s", iteration, poll_sleep_seconds)
            if iteration % 20 == 1 and iteration > 1:
                logger.debug("OMS loop: waiting for messages (iteration {})", iteration)
            time.sleep(poll_sleep_seconds)
        if trim_every_n and iteration % trim_every_n == 0:
            logger.debug("OMS loop iter {}: trim start", iteration)
            try:
                removed = trim_oms_streams(redis)
                logger.debug("OMS loop iter {}: trim done removed={}", iteration, removed)
            except Exception as e:
                logger.warning("trim_oms_streams failed: {}", e)
        if pg_connect and sync_interval_seconds > 0 and (time.time() - last_sync_time) >= sync_interval_seconds:
            try:
                count = sync_terminal_orders(
                    redis,
                    store,
                    pg_connect,
                    ttl_after_sync_seconds=sync_ttl_after_seconds,
                )
                logger.debug("OMS periodic sync: synced {} terminal order(s) to Postgres", count)
            except Exception as e:
                logger.warning("sync_terminal_orders failed: {}", e)
            try:
                repaired = run_all_repairs(pg_connect)
                if repaired:
                    logger.debug("OMS periodic repair: updated {} order row(s)", repaired)
            except Exception as e:
                logger.warning("run_all_repairs failed: {}", e)
            last_sync_time = time.time()

        # Periodic account refresh: get_account_snapshot from each adapter and apply to Redis (12.2.9)
        now = time.time()
        if account_store and account_refresh_interval_seconds > 0 and (now - last_account_refresh_time) >= account_refresh_interval_seconds:
            try:
                for broker_name in registry.broker_names():
                    adapter = registry.get(broker_name)
                    if not adapter or not hasattr(adapter, "get_account_snapshot"):
                        continue
                    account_id = getattr(adapter, "_account_id", "default")
                    try:
                        snapshot = adapter.get_account_snapshot(account_id=account_id)
                    except Exception as e:
                        logger.warning("get_account_snapshot failed broker={} account_id={}: {}", broker_name, account_id, e)
                        continue
                    if not snapshot:
                        continue
                    account_store.apply_account_position(
                        broker=snapshot.get("broker", broker_name),
                        account_id=snapshot.get("account_id", account_id),
                        balances=snapshot.get("balances", []),
                        positions=snapshot.get("positions", []),
                        updated_at=snapshot.get("updated_at"),
                        payload=snapshot.get("payload"),
                    )
                logger.debug("OMS periodic account refresh done")
            except Exception as e:
                logger.warning("account refresh failed: {}", e)
            last_account_refresh_time = now

        # Periodic account sync to Postgres and account repairs (12.2.9)
        if pg_connect and account_store and account_sync_interval_seconds > 0 and (now - last_account_sync_time) >= account_sync_interval_seconds:
            try:
                count = sync_accounts_to_postgres(
                    redis,
                    account_store,
                    pg_connect,
                    ttl_after_sync_seconds=account_sync_ttl_seconds,
                    sync_balances=False,  # OMS loop: sync accounts only; balance_changes via on_balance_change
                )
                logger.debug("OMS periodic account sync: synced {} account(s) to Postgres", count)
            except Exception as e:
                logger.warning("sync_accounts_to_postgres failed: {}", e)
            try:
                repaired = run_all_account_repairs(pg_connect)
                if repaired:
                    logger.debug("OMS periodic account repair: updated {} row(s)", repaired)
            except Exception as e:
                logger.warning("run_all_account_repairs failed: {}", e)
            last_account_sync_time = now

        logger.debug("OMS loop iter {}: run_until check", iteration)
        if run_until is not None and run_until():
            logger.debug("OMS loop iter {}: shutdown requested, exiting", iteration)
            break
        logger.debug("OMS loop iter {}: next iteration", iteration)

    return processed


def main() -> int:
    """
    Load REDIS_URL, create store and registry, register Binance, start fill listeners,
    then run process_many / process_many_cancel loop until shutdown.
    """
    redis = get_redis()
    store = get_store(redis)
    account_store = get_account_store(redis)
    registry = get_registry()

    if not registry.broker_names():
        logger.warning("No adapters registered (set BINANCE_API_KEY for Binance)")
        return 1

    logger.info("OMS starting brokers={}", registry.broker_names())

    database_url = _env("DATABASE_URL")
    sync_ttl = 300
    try:
        sync_ttl = int(os.environ.get("OMS_SYNC_TTL_AFTER_SECONDS", "300"))
    except (TypeError, ValueError):
        pass

    def on_terminal_sync(order_id: str) -> None:
        if database_url:
            sync_one_order(redis, store, database_url, order_id, ttl_after_sync_seconds=sync_ttl)

    start_fill_listeners(redis, store, registry, on_terminal_sync=on_terminal_sync)
    logger.info("Fill listeners started for brokers={}", registry.broker_names())
    wait_for_fill_listeners_connected(registry)

    # Wire on_balance_change when DATABASE_URL set: write balance_changes row and set TTL on account keys (only for balance change events)
    account_ttl_after_balance_change = sync_ttl
    try:
        env_acc_ttl = os.environ.get("OMS_ACCOUNT_TTL_AFTER_BALANCE_CHANGE_SECONDS")
        if env_acc_ttl is not None:
            account_ttl_after_balance_change = int(env_acc_ttl)
    except (TypeError, ValueError):
        pass

    def _on_balance_change(
        broker: str,
        account_id: str,
        asset: str,
        delta: Any,
        event_type: str,
        event_time: Any,
        payload: Optional[dict],
    ) -> None:
        if not database_url:
            return
        try:
            delta_val = float(delta) if delta is not None else 0.0
            if delta_val > 0:
                change_type = "deposit"
            elif delta_val < 0:
                change_type = "withdrawal"
            else:
                change_type = "adjustment"
            write_balance_change(
                database_url,
                account_id,
                asset,
                change_type,
                delta or 0,
                event_type or "balanceUpdate",
                event_time,
                payload=payload,
                book="default",
                broker=broker,
            )
        except Exception as e:
            logger.exception("write_balance_change failed: {}", e)
            return
        try:
            set_account_key_ttl(redis, broker, account_id, account_ttl_after_balance_change)
        except Exception as e:
            logger.warning("set_account_key_ttl after balance change failed: {}", e)

    start_account_listeners(
        redis,
        account_store,
        registry,
        on_balance_change=_on_balance_change if database_url else None,
    )
    logger.info("Account listeners started for brokers={}", registry.broker_names())
    wait_for_account_listeners_connected(registry)

    # One-time symbol sync at startup (symbols are mostly static)
    if database_url and "binance" in registry.broker_names():
        binance_base_url = _env("BINANCE_BASE_URL") or "https://testnet.binance.vision"
        try:
            n = sync_symbols_from_binance(binance_base_url, database_url, broker="binance")
            logger.info("Symbol sync from Binance: upserted {} symbol(s)", n)
        except Exception as e:
            logger.warning("Symbol sync at startup failed (continuing): %s", e)

    shutdown = [False]

    def on_signal(_signum: int, _frame: object) -> None:
        shutdown[0] = True

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    # Allow override via environment variables
    block_ms = DEFAULT_BLOCK_MS
    try:
        env_block = os.environ.get("OMS_BLOCK_MS")
        if env_block:
            block_ms = int(env_block)
    except (TypeError, ValueError):
        pass
    
    poll_sleep = DEFAULT_POLL_SLEEP_SECONDS
    try:
        env_poll = os.environ.get("OMS_POLL_SLEEP_SECONDS")
        if env_poll:
            poll_sleep = float(env_poll)
    except (TypeError, ValueError):
        pass
    
    batch_size = DEFAULT_BATCH_SIZE
    try:
        env_batch = os.environ.get("OMS_BATCH_SIZE")
        if env_batch:
            batch_size = int(env_batch)
    except (TypeError, ValueError):
        pass

    try:
        run_oms_loop(
            redis,
            store,
            registry,
            account_store=account_store,
            block_ms=block_ms,
            poll_sleep_seconds=poll_sleep,
            batch_size=batch_size,
            trim_every_n=DEFAULT_TRIM_EVERY_N,
            run_until=lambda: shutdown[0],
            on_terminal_sync=on_terminal_sync if database_url else None,
            pg_connect=database_url if database_url else None,
            sync_interval_seconds=DEFAULT_SYNC_INTERVAL_SECONDS if database_url else 0,
            sync_ttl_after_seconds=sync_ttl,
            account_refresh_interval_seconds=DEFAULT_ACCOUNT_REFRESH_INTERVAL_SECONDS,
            account_sync_interval_seconds=DEFAULT_ACCOUNT_SYNC_INTERVAL_SECONDS if database_url else 0,
            account_sync_ttl_seconds=None,  # No default TTL for account keys; optional env override could be added
        )
    except KeyboardInterrupt:
        pass
    finally:
        stop_account_listeners(registry)
        stop_fill_listeners(registry)
        logger.info("OMS shutdown complete")

    return 0


if __name__ == "__main__":
    sys.exit(main())
