"""
Minimal OMS flow: read one risk_approved message, stage, place via adapter, publish fills to oms_fills.

Used by Redis-through-testnet integration test and future OMS main loop.
"""

import uuid
from typing import Any, Callable, Dict, Optional

from redis import Redis

from oms.schemas import OMS_FILLS_STREAM, RISK_APPROVED_STREAM
from oms.storage.redis_order_store import RedisOrderStore
from oms.streams import add_message, read_messages


def _order_from_stream_fields(fields: Dict[str, str]) -> Dict[str, Any]:
    """Build risk_approved-style order dict from stream entry (all strings)."""
    order: Dict[str, Any] = {
        "broker": fields.get("broker", ""),
        "account_id": fields.get("account_id", ""),
        "symbol": fields.get("symbol", ""),
        "side": fields.get("side", ""),
        "quantity": float(fields["quantity"]) if fields.get("quantity") else 0,
        "order_type": fields.get("order_type", "MARKET"),
        "book": fields.get("book", ""),
        "comment": fields.get("comment", ""),
    }
    if fields.get("price"):
        try:
            order["price"] = float(fields["price"])
        except (TypeError, ValueError):
            order["price"] = None
    else:
        order["price"] = None
    if fields.get("time_in_force"):
        order["time_in_force"] = fields["time_in_force"]
    if fields.get("order_id"):
        order["order_id"] = fields["order_id"]
    return order


def make_fill_callback(
    redis: Redis,
    store: RedisOrderStore,
) -> Callable[[Dict[str, Any]], None]:
    """
    Return a callback suitable for adapter.start_fill_listener(callback).

    On each fill/reject: updates order store and XADDs to oms_fills (with book/comment from order).
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
        book = order.get("book", "")
        comment = order.get("comment", "")

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
            "book": book,
            "comment": comment,
        }
        add_message(redis, OMS_FILLS_STREAM, payload)

    return on_fill_or_reject


def process_one_risk_approved(
    redis: Redis,
    store: RedisOrderStore,
    get_adapter: Callable[[str], Any],
) -> Optional[Dict[str, Any]]:
    """
    Read one message from risk_approved, stage order, place via adapter, update store.

    Does not start the fill listener; caller must run the listener and use make_fill_callback
    so fills are written to oms_fills.

    Returns:
        Dict with "order_id", "broker_order_id", "rejected", "reject_reason" if rejected;
        or None if no message was available.
    """
    messages = read_messages(redis, RISK_APPROVED_STREAM, start_id="0", count=1)
    if not messages:
        return None

    _entry_id, fields = messages[0]
    order = _order_from_stream_fields(fields)
    order_id = order.get("order_id") or str(uuid.uuid4())
    order["order_id"] = order_id

    store.stage_order(order_id, order)
    broker = order.get("broker", "") or "binance"
    adapter = get_adapter(broker)
    if not adapter:
        store.update_fill_status(order_id, "rejected")
        add_message(
            redis,
            OMS_FILLS_STREAM,
            {
                "event_type": "reject",
                "order_id": order_id,
                "broker_order_id": "",
                "symbol": order.get("symbol", ""),
                "side": order.get("side", ""),
                "quantity": order.get("quantity"),
                "price": order.get("price"),
                "fee": "",
                "fee_asset": "",
                "executed_at": "",
                "fill_id": "",
                "reject_reason": "No adapter for broker",
                "book": order.get("book", ""),
                "comment": order.get("comment", ""),
            },
        )
        return {"order_id": order_id, "rejected": True, "reject_reason": "No adapter for broker"}

    response = adapter.place_order(order)
    if response.get("rejected"):
        store.update_fill_status(order_id, "rejected")
        add_message(
            redis,
            OMS_FILLS_STREAM,
            {
                "event_type": "reject",
                "order_id": order_id,
                "broker_order_id": response.get("broker_order_id", ""),
                "symbol": order.get("symbol", ""),
                "side": order.get("side", ""),
                "quantity": order.get("quantity"),
                "price": order.get("price"),
                "fee": "",
                "fee_asset": "",
                "executed_at": "",
                "fill_id": "",
                "reject_reason": response.get("reject_reason", "rejected"),
                "book": order.get("book", ""),
                "comment": order.get("comment", ""),
            },
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
