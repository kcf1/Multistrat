"""
OMS Redis order store (task 12.1.4).

Key layout:
- orders:{order_id} — Hash (internal_id, broker, account_id, broker_order_id, symbol, side, order_type, quantity, price (executed), limit_price, time_in_force, status, book, comment, created_at, updated_at, executed_qty, binance_*, payload)
- orders:by_status:{status} — Set of order_id
- orders:by_book:{book} — Set of order_id
- orders:by_broker_order_id:{broker_order_id} — String = order_id

Uses pipelines for atomic multi-key updates.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from redis import Redis


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _order_key(order_id: str) -> str:
    return f"orders:{order_id}"


def _status_set_key(status: str) -> str:
    return f"orders:by_status:{status}"


def _book_set_key(book: str) -> str:
    return f"orders:by_book:{book}"


def _broker_order_id_key(broker_order_id: str) -> str:
    return f"orders:by_broker_order_id:{broker_order_id}"


def _flatten_order(order: Dict[str, Any]) -> Dict[str, str]:
    """Convert order dict to string values for Redis hash."""
    out: Dict[str, str] = {}
    for k, v in order.items():
        if v is None:
            continue
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v)
        else:
            out[k] = str(v)
    return out


def _unflatten_order(raw: Dict[Any, Any]) -> Dict[str, Any]:
    """Convert Redis hash reply to order dict (handles bytes or str keys/values)."""
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        key = k.decode() if isinstance(k, bytes) else k
        val = v.decode() if isinstance(v, bytes) else v
        if key == "payload":
            try:
                out[key] = json.loads(val) if val else None
            except (json.JSONDecodeError, TypeError):
                out[key] = val
        elif key in ("quantity", "price", "limit_price", "executed_qty", "binance_cumulative_quote_qty"):
            try:
                out[key] = float(val) if val else None
            except (TypeError, ValueError):
                out[key] = val
        elif key == "binance_transact_time":
            try:
                out[key] = int(val) if val else None
            except (TypeError, ValueError):
                out[key] = val
        else:
            out[key] = val
    return out


class RedisOrderStore:
    """
    Redis-backed order store with status/book/broker_order_id indexes.
    """

    def __init__(self, redis_client: Redis):
        self._redis = redis_client

    def stage_order(self, order_id: str, order_data: Dict[str, Any]) -> None:
        """
        Create order hash and add to indexes (by_status=pending, by_book if present).
        Atomic via pipeline.
        """
        now = _now_iso()
        order = {
            "internal_id": order_id,
            "broker": order_data.get("broker", ""),
            "account_id": order_data.get("account_id", ""),
            "broker_order_id": order_data.get("broker_order_id", ""),
            "symbol": order_data.get("symbol", ""),
            "side": order_data.get("side", ""),
            "order_type": order_data.get("order_type", ""),
            "quantity": order_data.get("quantity", ""),
            "price": order_data.get("price", ""),  # executed (from broker/fills); may be None at stage
            "limit_price": order_data.get("limit_price", ""),
            "time_in_force": order_data.get("time_in_force", ""),
            "status": "pending",
            "book": order_data.get("book", ""),
            "comment": order_data.get("comment", ""),
            "created_at": now,
            "updated_at": now,
        }
        # Optional numeric / broker-specific (pass through if present)
        for key in ("executed_qty", "binance_cumulative_quote_qty", "binance_transact_time"):
            if key in order_data and order_data[key] is not None:
                order[key] = order_data[key]
        if "payload" in order_data and order_data["payload"] is not None:
            order["payload"] = json.dumps(order_data["payload"]) if not isinstance(order_data["payload"], str) else order_data["payload"]

        pipe = self._redis.pipeline()
        pipe.hset(_order_key(order_id), mapping=_flatten_order(order))
        pipe.sadd(_status_set_key("pending"), order_id)
        book = order_data.get("book") or ""
        if book:
            pipe.sadd(_book_set_key(book), order_id)
        pipe.execute()

    def update_status(
        self,
        order_id: str,
        new_status: str,
        old_status: str,
        extra_fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Update order status and indexes. Move order from old_status set to new_status set.
        If extra_fields contains broker_order_id, set orders:by_broker_order_id.
        Atomic via pipeline.
        """
        extra_fields = extra_fields or {}
        now = _now_iso()
        updates = {"status": new_status, "updated_at": now, **extra_fields}
        pipe = self._redis.pipeline()
        pipe.hset(_order_key(order_id), mapping=_flatten_order(updates))
        if old_status != new_status:
            pipe.srem(_status_set_key(old_status), order_id)
        pipe.sadd(_status_set_key(new_status), order_id)
        broker_order_id = extra_fields.get("broker_order_id")
        if broker_order_id:
            pipe.set(_broker_order_id_key(str(broker_order_id)), order_id)
        pipe.execute()

    def update_fill_status(
        self,
        order_id: str,
        status: str,
        executed_qty: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        """
        Update order after fill/reject: set status (filled/rejected) and optional executed_qty.
        Requires current status for index move; we fetch it if not in kwargs.
        """
        old_status = kwargs.pop("old_status", None)
        if old_status is None:
            order = self.get_order(order_id)
            old_status = (order or {}).get("status", "pending")
        extra = {"executed_qty": executed_qty} if executed_qty is not None else {}
        extra.update(kwargs)
        self.update_status(order_id, status, old_status, extra_fields=extra)

    def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Return order hash as dict, or None if not found."""
        raw = self._redis.hgetall(_order_key(order_id))
        if not raw:
            return None
        return _unflatten_order(raw)

    def find_order_by_broker_order_id(self, broker_order_id: str) -> Optional[str]:
        """Return internal order_id for the given broker_order_id, or None."""
        order_id = self._redis.get(_broker_order_id_key(broker_order_id))
        if order_id is None:
            return None
        return order_id.decode() if isinstance(order_id, bytes) else order_id

    def get_order_ids_in_status(self, status: str) -> List[str]:
        """Return list of order_id in the given status set (e.g. for sync by status)."""
        key = _status_set_key(status)
        members = self._redis.smembers(key)
        if not members:
            return []
        return [m.decode() if isinstance(m, bytes) else m for m in members]
