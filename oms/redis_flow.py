"""
OMS flow: consumer → order store → adapter registry → place_order → fill callback → producer.

Used by Redis-through-testnet integration test and OMS main loop (task 12.1.9).
"""

import uuid
from typing import Any, Callable, Dict, Optional, Union

from redis import Redis

from oms.consumer import read_one_risk_approved
from oms.producer import produce_oms_fill
from oms.registry import AdapterRegistry
from oms.storage.redis_order_store import RedisOrderStore


def make_fill_callback(
    redis: Redis,
    store: RedisOrderStore,
) -> Callable[[Dict[str, Any]], None]:
    """
    Return a callback suitable for adapter.start_fill_listener(callback).

    On each fill/reject: updates order store and produces to oms_fills (12.1.8).
    """

    def on_fill_or_reject(event: Dict[str, Any]) -> None:
        order_id = event.get("order_id") or ""
        broker_order_id = str(event.get("broker_order_id", ""))
        if not order_id and broker_order_id:
            order_id = store.find_order_by_broker_order_id(broker_order_id) or ""
        if not order_id:
            return

        status = "filled" if event.get("event_type") == "fill" else "rejected"
        executed_qty = event.get("quantity") if status == "filled" else None
        store.update_fill_status(order_id, status, executed_qty=executed_qty)

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
) -> Optional[Dict[str, Any]]:
    """
    One iteration of OMS loop (12.1.9): consumer → store → registry → place_order → producer.

    Reads one risk_approved message (12.1.6), stages in store, routes by registry (12.1.7),
    places order, updates store; on reject produces to oms_fills (12.1.8).
    Does not start the fill listener; caller must run the listener with make_fill_callback.

    Returns:
        Dict with order_id, broker_order_id, rejected, reject_reason; or None if no message.
    """
    out = read_one_risk_approved(redis, start_id=start_id, block_ms=block_ms)
    if not out:
        return None
    _entry_id, order = out
    order_id = order.get("order_id") or str(uuid.uuid4())
    order["order_id"] = order_id

    store.stage_order(order_id, order)
    broker = order.get("broker", "") or "binance"
    adapter = registry.get(broker)
    if not adapter:
        store.update_fill_status(order_id, "rejected")
        produce_oms_fill(redis, _reject_event(order_id, order, reject_reason="No adapter for broker"))
        return {"order_id": order_id, "rejected": True, "reject_reason": "No adapter for broker"}

    response = adapter.place_order(order)
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
            "binance_transact_time": response.get("binance_transact_time"),
            "binance_cumulative_quote_qty": response.get("binance_cumulative_quote_qty"),
        },
    )
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
) -> Optional[Dict[str, Any]]:
    """
    Read one risk_approved, stage, place via adapter, update store.

    get_adapter: either a callable (broker_name -> adapter) or an AdapterRegistry.
    Does not start the fill listener; use make_fill_callback for that.
    """
    if isinstance(get_adapter, AdapterRegistry):
        return process_one(redis, store, get_adapter, start_id=start_id, block_ms=block_ms)
    # Legacy: wrap callable as a one-off registry
    class _Registry(AdapterRegistry):
        def __init__(self, fn: Callable[[str], Any]) -> None:
            super().__init__()
            self._fn = fn
        def get(self, broker_name: str) -> Optional[Any]:
            return self._fn(broker_name)
    return process_one(redis, store, _Registry(get_adapter), start_id=start_id, block_ms=block_ms)
