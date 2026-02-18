"""
Account event callback handler (task 12.2.5).

Provides make_account_callback factory function that creates a callback suitable
for adapter.start_account_listener(). Validates account events, updates Redis
account store, and optionally triggers sync.
"""

from typing import Any, Callable, Dict, Optional

from pydantic import ValidationError
from redis import Redis

from oms.brokers.binance.schemas_pydantic import AccountPositionEvent, BalanceUpdateEvent
from oms.log import logger
from oms.storage.redis_account_store import RedisAccountStore


def make_account_callback(
    redis: Redis,
    account_store: RedisAccountStore,
    on_account_updated: Optional[Callable[[str, str], None]] = None,
    on_balance_change: Optional[
        Callable[[str, str, str, Any, str, Any, Optional[Dict[str, Any]]], None]
    ] = None,
) -> Callable[[Dict[str, Any]], None]:
    """
    Return a callback suitable for adapter.start_account_listener(callback).

    On each account event (account_position or balance_update):
    - Validates event structure using Pydantic models
    - Updates Redis account store (apply_account_position or apply_balance_update)
    - Uses updated_at timestamp for idempotency (avoids overwriting newer data with older snapshots)
    - Optionally calls on_account_updated(broker, account_id) to trigger sync
    - For balance_update: optionally calls on_balance_change(broker, account_id, asset, delta,
      event_type, event_time, payload) so caller can write to balance_changes (delta from payload.d)

    Args:
        redis: Redis client (for future use if needed)
        account_store: RedisAccountStore instance
        on_account_updated: Optional callback(broker, account_id) to trigger sync
        on_balance_change: Optional callback(broker, account_id, asset, delta, event_type,
            event_time, payload) to write balance_changes row (e.g. via account_sync.write_balance_change)

    Returns:
        Callback function that accepts AccountEvent dict
    """
    def on_account_event(event: Dict[str, Any]) -> None:
        # Validate event with Pydantic models before processing
        event_type = (event.get("event_type") or "").strip().lower()
        
        try:
            if event_type == "account_position":
                validated_event = AccountPositionEvent(**event)
            elif event_type == "balance_update":
                validated_event = BalanceUpdateEvent(**event)
            else:
                logger.warning(
                    "Account callback: unknown event_type={}, skipping",
                    event_type,
                )
                return
            
            # Use validated event dict
            event = validated_event.model_dump_dict()
        except ValidationError as e:
            # Log validation error and skip processing
            errors = []
            for error in e.errors():
                field = ".".join(str(loc) for loc in error["loc"])
                msg = error["msg"]
                errors.append(f"{field}: {msg}")
            error_msg = "; ".join(errors) if errors else str(e)
            logger.error(
                "Account callback: invalid event structure (event_type={}): {}, skipping",
                event_type,
                error_msg,
            )
            return

        broker = event.get("broker", "")
        account_id = event.get("account_id", "")
        updated_at = event.get("updated_at", "")
        payload = event.get("payload", {})

        if not broker or not account_id:
            logger.warning(
                "Account callback: missing broker or account_id, skipping"
            )
            return

        # Check idempotency: compare updated_at with existing account data
        # Avoid overwriting newer data with older periodic snapshot
        existing_account = account_store.get_account(broker, account_id)
        if existing_account:
            existing_updated_at = existing_account.get("updated_at", "")
            if existing_updated_at and updated_at:
                # Compare timestamps (ISO format, lexicographically sortable)
                if updated_at < existing_updated_at:
                    logger.debug(
                        "Account callback: skipping older event (existing={}, event={})",
                        existing_updated_at, updated_at,
                    )
                    return

        # Update Redis account store based on event type
        if event_type == "account_position":
            balances = event.get("balances", [])
            positions = event.get("positions", [])
            account_store.apply_account_position(
                broker=broker,
                account_id=account_id,
                balances=balances,
                positions=positions,
                updated_at=updated_at,
                payload=payload,
            )
            logger.info(
                "Account callback: account_position broker={} account_id={} balances={} positions={}",
                broker, account_id, len(balances), len(positions),
            )
        elif event_type == "balance_update":
            balances = event.get("balances", [])
            if not balances:
                logger.warning(
                    "Account callback: balance_update event has no balances, skipping"
                )
                return
            # balance_update events have a single balance dict
            balance = balances[0]
            account_store.apply_balance_update(
                broker=broker,
                account_id=account_id,
                balance=balance,
                updated_at=updated_at,
                payload=payload,
            )
            logger.info(
                "Account callback: balance_update broker={} account_id={} asset={}",
                broker, account_id, balance.get("asset", ""),
            )
            # Optionally write balance_changes (delta from Binance payload.d, event time from payload.T)
            if on_balance_change and isinstance(payload, dict):
                asset = (balance.get("asset") or "").strip()
                delta = payload.get("d")  # Binance balanceUpdate delta
                event_time = payload.get("T")  # Binance clear time (ms)
                if asset is not None:
                    change_type = "deposit" if (delta is not None and float(delta) > 0) else "withdrawal"
                    if delta is not None and float(delta) == 0:
                        change_type = "adjustment"
                    try:
                        on_balance_change(
                            broker, account_id, asset, delta,
                            "balanceUpdate", event_time, payload,
                        )
                    except Exception as e:
                        logger.exception(
                            "Account callback: on_balance_change error broker={} account_id={}: {}",
                            broker, account_id, e,
                        )

        # Optionally trigger sync callback
        if on_account_updated:
            try:
                on_account_updated(broker, account_id)
            except Exception as e:
                logger.exception(
                    "Account callback: on_account_updated error for broker={} account_id={}: {}",
                    broker, account_id, e,
                )

    return on_account_event
