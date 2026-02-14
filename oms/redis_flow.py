"""
OMS flow: consumer → order store → adapter registry → place_order → fill callback → producer.

Used by Redis-through-testnet integration test and OMS main loop (task 12.1.9).
"""

import uuid
from typing import Any, Callable, Dict, Optional, Union

from redis import Redis

from oms.cancel_consumer import ack_cancel_requested, read_one_cancel_request, read_one_cancel_request_cg
from oms.cleanup import set_order_key_ttl
from oms.consumer import ack_risk_approved, read_one_risk_approved, read_one_risk_approved_cg, read_one_risk_approved_pending
from oms.producer import produce_oms_fill
from oms.registry import AdapterRegistry
from oms.schemas import RISK_APPROVED_STREAM
from oms.storage.redis_order_store import RedisOrderStore
from oms.streams import get_pending_delivery_count

TERMINAL_STATUSES = ("filled", "rejected", "cancelled", "expired")
OMS_PLACE_ORDER_MAX_RETRIES = 3  # 12.1.9d: after this many deliveries, reject and XACK


def make_fill_callback(
    redis: Redis,
    store: RedisOrderStore,
    terminal_order_ttl_seconds: Optional[int] = None,
    on_terminal_sync: Optional[Callable[[str], None]] = None,
) -> Callable[[Dict[str, Any]], None]:
    """
    Return a callback suitable for adapter.start_fill_listener(callback).

    On each fill/reject/cancelled/expired: updates order store and produces to oms_fills (12.1.8).
    When status is terminal (filled, rejected, cancelled, expired): if on_terminal_sync is set,
    calls it (sync to Postgres + TTL); else optionally sets TTL only (12.1.9b, 12.1.10).
    """

    def on_fill_or_reject(event: Dict[str, Any]) -> None:
        order_id = event.get("order_id") or ""
        broker_order_id = str(event.get("broker_order_id", ""))
        if not order_id and broker_order_id:
            order_id = store.find_order_by_broker_order_id(broker_order_id) or ""
        if not order_id:
            return

        event_type = (event.get("event_type") or "fill").strip().lower()
        if event_type == "fill":
            pass  # status set below from order_status / cumulative
        elif event_type == "cancelled":
            status = "cancelled"
            executed_qty = None
        elif event_type == "expired":
            status = "expired"
            executed_qty = None
        else:
            status = "rejected"
            executed_qty = None

        if event_type == "fill":
            # Map Binance order_status (X) to Redis status; use cumulative qty or accumulate
            binance_order_status = (event.get("order_status") or "").strip().upper()
            if binance_order_status == "FILLED":
                status = "filled"
            elif binance_order_status == "PARTIALLY_FILLED":
                status = "partially_filled"
            else:
                status = "partially_filled"  # NEW or unknown: treat as partial

            cumulative = event.get("executed_qty_cumulative")
            if cumulative is not None:
                executed_qty = float(cumulative)
            else:
                order = store.get_order(order_id) or {}
                current = order.get("executed_qty")
                try:
                    prev = float(current) if current is not None else 0.0
                except (TypeError, ValueError):
                    prev = 0.0
                incremental = event.get("quantity") or 0
                try:
                    inc = float(incremental) if incremental is not None else 0.0
                except (TypeError, ValueError):
                    inc = 0.0
                executed_qty = prev + inc

        fill_price = event.get("price")
        store.update_fill_status(
            order_id, status, executed_qty=executed_qty,
            **({"price": fill_price} if fill_price is not None else {}),
        )

        order = store.get_order(order_id) or {}
        payload: Dict[str, Any] = {
            "event_type": event.get("event_type", "fill"),
            "order_id": order_id,
            "broker_order_id": broker_order_id,
            "symbol": event.get("symbol", ""),
            "side": event.get("side", ""),
            "quantity": event.get("quantity"),
            "price": event.get("price"),
            "fee": event.get("fee"),
            "fee_asset": event.get("fee_asset"),
            "executed_at": event.get("executed_at", ""),
            "fill_id": event.get("fill_id", ""),
            "reject_reason": event.get("reject_reason", ""),
            "book": order.get("book", ""),
            "comment": order.get("comment", ""),
        }
        produce_oms_fill(redis, payload)

        if status in TERMINAL_STATUSES:
            if on_terminal_sync is not None:
                on_terminal_sync(order_id)
            elif terminal_order_ttl_seconds is not None and terminal_order_ttl_seconds > 0:
                set_order_key_ttl(redis, order_id, terminal_order_ttl_seconds)

    return on_fill_or_reject


def _reject_event(order_id: str, order: Dict[str, Any], broker_order_id: str = "", reject_reason: str = "") -> Dict[str, Any]:
    return {
        "event_type": "reject",
        "order_id": order_id,
        "broker_order_id": broker_order_id,
        "symbol": order.get("symbol", ""),
        "side": order.get("side", ""),
        "quantity": order.get("quantity"),
        "price": order.get("price"),
        "fee": "",
        "fee_asset": "",
        "executed_at": "",
        "fill_id": "",
        "reject_reason": reject_reason,
        "book": order.get("book", ""),
        "comment": order.get("comment", ""),
    }


def process_one(
    redis: Redis,
    store: RedisOrderStore,
    registry: AdapterRegistry,
    start_id: str = "0",
    block_ms: Optional[int] = None,
    consumer_group: Optional[str] = "oms",
    consumer_name: Optional[str] = "oms-1",
    on_terminal_sync: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    """
    One iteration of OMS loop (12.1.9): consumer → store → registry → place_order → producer.

    Reads one risk_approved message (12.1.6), stages in store, routes by registry (12.1.7),
    places order, updates store; on reject produces to oms_fills (12.1.8).
    When consumer_group is set, uses XREADGROUP + XACK so message is not redelivered.
    When order is rejected, if on_terminal_sync is set, calls it (sync to Postgres + TTL, 12.1.10).
    Does not start the fill listener; caller must run the listener with make_fill_callback.

    Returns:
        Dict with order_id, broker_order_id, rejected, reject_reason; or None if no message.
    """
    if consumer_group and consumer_name:
        out = read_one_risk_approved_cg(redis, consumer_group, consumer_name, block_ms=block_ms)
        if not out:
            out = read_one_risk_approved_pending(redis, consumer_group, consumer_name)
    else:
        out = read_one_risk_approved(redis, start_id=start_id, block_ms=block_ms)
    if not out:
        return None
    entry_id, order = out
    order_id = order.get("order_id") or str(uuid.uuid4())
    order["order_id"] = order_id

    if not store.get_order(order_id):
        store.stage_order(order_id, order)
    broker = order.get("broker", "") or "binance"
    adapter = registry.get(broker)
    if not adapter:
        store.update_fill_status(order_id, "rejected")
        produce_oms_fill(redis, _reject_event(order_id, order, reject_reason="No adapter for broker"))
        if consumer_group and consumer_name:
            ack_risk_approved(redis, consumer_group, entry_id)
        if on_terminal_sync is not None:
            on_terminal_sync(order_id)
        return {"order_id": order_id, "rejected": True, "reject_reason": "No adapter for broker"}

    try:
        response = adapter.place_order(order)
    except Exception as e:
        retry_key = f"oms:retry:risk_approved:{entry_id}"
        retry_count = redis.incr(retry_key)
        if consumer_group and consumer_name:
            delivery_count = get_pending_delivery_count(redis, RISK_APPROVED_STREAM, consumer_group, entry_id)
            attempts = max(retry_count, delivery_count)
        else:
            attempts = retry_count
        if attempts >= OMS_PLACE_ORDER_MAX_RETRIES:
            store.update_fill_status(order_id, "rejected")
            produce_oms_fill(
                redis,
                _reject_event(
                    order_id,
                    order,
                    reject_reason=f"place_order failed after {OMS_PLACE_ORDER_MAX_RETRIES} retries: {e!s}",
                ),
            )
            if consumer_group and consumer_name:
                ack_risk_approved(redis, consumer_group, entry_id)
            redis.delete(retry_key)
            if on_terminal_sync is not None:
                on_terminal_sync(order_id)
            return {
                "order_id": order_id,
                "rejected": True,
                "reject_reason": f"place_order failed after {OMS_PLACE_ORDER_MAX_RETRIES} retries: {e!s}",
            }
        return {"order_id": order_id, "rejected": False, "retry_later": True}

    if response.get("rejected"):
        store.update_fill_status(order_id, "rejected")
        produce_oms_fill(
            redis,
            _reject_event(
                order_id,
                order,
                broker_order_id=str(response.get("broker_order_id", "")),
                reject_reason=response.get("reject_reason", "rejected"),
            ),
        )
        if consumer_group and consumer_name:
            ack_risk_approved(redis, consumer_group, entry_id)
        if on_terminal_sync is not None:
            on_terminal_sync(order_id)
        return {
            "order_id": order_id,
            "rejected": True,
            "reject_reason": response.get("reject_reason", "rejected"),
        }

    store.update_status(
        order_id,
        "sent",
        "pending",
        extra_fields={
            "broker_order_id": response.get("broker_order_id"),
            "executed_qty": response.get("executed_qty"),
            "price": response.get("price"),
            "limit_price": response.get("limit_price"),
            "binance_transact_time": response.get("binance_transact_time"),
            "binance_cumulative_quote_qty": response.get("binance_cumulative_quote_qty"),
        },
    )
    if consumer_group and consumer_name:
        ack_risk_approved(redis, consumer_group, entry_id)
    return {
        "order_id": order_id,
        "broker_order_id": response.get("broker_order_id"),
        "rejected": False,
    }


def process_one_risk_approved(
    redis: Redis,
    store: RedisOrderStore,
    get_adapter: Union[Callable[[str], Any], AdapterRegistry],
    start_id: str = "0",
    block_ms: Optional[int] = None,
    on_terminal_sync: Optional[Callable[[str], None]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Read one risk_approved, stage, place via adapter, update store.

    get_adapter: either a callable (broker_name -> adapter) or an AdapterRegistry.
    Does not start the fill listener; use make_fill_callback for that.
    When order is terminal (reject path), on_terminal_sync(order_id) is called if set (12.1.10).
    """
    if isinstance(get_adapter, AdapterRegistry):
        return process_one(
            redis, store, get_adapter,
            start_id=start_id, block_ms=block_ms, on_terminal_sync=on_terminal_sync,
        )
    # Legacy: wrap callable as a one-off registry
    class _Registry(AdapterRegistry):
        def __init__(self, fn: Callable[[str], Any]) -> None:
            super().__init__()
            self._fn = fn
        def get(self, broker_name: str) -> Optional[Any]:
            return self._fn(broker_name)
    return process_one(
        redis, store, _Registry(get_adapter),
        start_id=start_id, block_ms=block_ms, on_terminal_sync=on_terminal_sync,
    )


def process_one_cancel(
    redis: Redis,
    store: RedisOrderStore,
    registry: AdapterRegistry,
    start_id: str = "0",
    block_ms: Optional[int] = None,
    publish_to_oms_fills: bool = True,
    consumer_group: Optional[str] = "oms",
    consumer_name: Optional[str] = "oms-1",
) -> Optional[Dict[str, Any]]:
    """
    Read one cancel_requested message, resolve order, call adapter.cancel_order, update store (12.1.9f).

    When consumer_group and consumer_name are set, uses XREADGROUP + XACK so each message
    is delivered once (same pattern as process_one for risk_approved).

    Resolves order by order_id (from message) or by broker_order_id (lookup in store).
    Updates store to cancelled and optionally publishes cancelled event to oms_fills.

    Returns:
        Dict with order_id, broker_order_id, cancelled, reject_reason; or None if no message.
    """
    if consumer_group and consumer_name:
        out = read_one_cancel_request_cg(redis, consumer_group, consumer_name, block_ms=block_ms)
    else:
        out = read_one_cancel_request(redis, start_id=start_id, block_ms=block_ms)
    if not out:
        return None
    entry_id, req = out
    order_id = req.get("order_id")
    broker_order_id = req.get("broker_order_id") or ""
    symbol = (req.get("symbol") or "").strip()
    broker = (req.get("broker") or "binance").strip()

    if order_id:
        order = store.get_order(order_id)
        if not order:
            if consumer_group and consumer_name:
                ack_cancel_requested(redis, consumer_group, entry_id)
            return {"order_id": order_id, "cancelled": False, "reject_reason": "Order not found in store"}
        broker_order_id = (order.get("broker_order_id") or "").strip()
        symbol = (order.get("symbol") or "").strip()
        if not broker_order_id or not symbol:
            if consumer_group and consumer_name:
                ack_cancel_requested(redis, consumer_group, entry_id)
            return {"order_id": order_id, "cancelled": False, "reject_reason": "Order has no broker_order_id/symbol"}
    else:
        if not broker_order_id or not symbol:
            if consumer_group and consumer_name:
                ack_cancel_requested(redis, consumer_group, entry_id)
            return {"cancelled": False, "reject_reason": "Missing broker_order_id or symbol"}
        order_id = store.find_order_by_broker_order_id(broker_order_id)
        order = store.get_order(order_id) if order_id else None

    adapter = registry.get(broker)
    if not adapter:
        if consumer_group and consumer_name:
            ack_cancel_requested(redis, consumer_group, entry_id)
        return {"order_id": order_id, "cancelled": False, "reject_reason": "No adapter for broker"}

    response = adapter.cancel_order(broker_order_id=broker_order_id, symbol=symbol)
    if response.get("rejected"):
        if consumer_group and consumer_name:
            ack_cancel_requested(redis, consumer_group, entry_id)
        return {"order_id": order_id, "cancelled": False, "reject_reason": response.get("reject_reason", "rejected")}

    if order_id:
        store.update_fill_status(order_id, "cancelled")
        if publish_to_oms_fills:
            order = order or store.get_order(order_id) or {}
            produce_oms_fill(redis, {
                "event_type": "cancelled",
                "order_id": order_id,
                "broker_order_id": broker_order_id,
                "symbol": order.get("symbol", symbol),
                "side": order.get("side", ""),
                "quantity": order.get("quantity", ""),
                "price": order.get("price", ""),
                "fee": "", "fee_asset": "", "executed_at": "", "fill_id": "",
                "reject_reason": response.get("reject_reason", "CANCELED"),
                "book": order.get("book", ""),
                "comment": order.get("comment", ""),
            })
    if consumer_group and consumer_name:
        ack_cancel_requested(redis, consumer_group, entry_id)
    return {
        "order_id": order_id,
        "broker_order_id": broker_order_id,
        "cancelled": True,
        "status": response.get("status", "CANCELED"),
    }
