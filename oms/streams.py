"""
Redis stream helpers for OMS (risk_approved consumer, oms_fills producer).

Used by integration tests and future OMS main loop (12.1.6, 12.1.8).
"""

from typing import Any, Dict, List, Optional, Tuple

from redis import Redis


def _flatten_fields(fields: Dict[str, Any]) -> Dict[str, str]:
    """Convert dict to string values for Redis stream XADD."""
    out: Dict[str, str] = {}
    for k, v in fields.items():
        if v is None:
            continue
        out[k] = str(v)
    return out


def add_message(redis: Redis, stream: str, fields: Dict[str, Any]) -> str:
    """
    XADD one message to a stream. Fields are stringified.
    Returns the stream entry id (e.g. '1234567890123-0').
    """
    flat = _flatten_fields(fields)
    if not flat:
        raise ValueError("At least one field required")
    return redis.xadd(stream, flat)  # type: ignore[return-value]


def read_messages(
    redis: Redis,
    stream: str,
    start_id: str = "0",
    count: Optional[int] = 1,
    block_ms: Optional[int] = None,
) -> List[Tuple[str, Dict[str, str]]]:
    """
    XREAD from stream. Returns list of (entry_id, fields_dict).
    start_id: "0" = from start, "$" = only new.
    """
    kwargs: Dict[str, Any] = {"streams": {stream: start_id}}
    if count is not None:
        kwargs["count"] = count
    if block_ms is not None:
        kwargs["block"] = block_ms

    reply = redis.xread(**kwargs)
    if not reply:
        return []
    # reply is [(stream_name_bytes, [(id, {k:v}), ...]), ...]
    result: List[Tuple[str, Dict[str, str]]] = []
    for stream_name, entries in reply:
        for eid, flds in entries:
            eid_str = eid.decode() if isinstance(eid, bytes) else eid
            decoded: Dict[str, str] = {}
            for k, v in (flds or {}).items():
                key = k.decode() if isinstance(k, bytes) else k
                val = v.decode() if isinstance(v, bytes) else v
                decoded[key] = val
            result.append((eid_str, decoded))
    return result


def read_messages_group(
    redis: Redis,
    stream: str,
    group: str,
    consumer: str,
    id: str = ">",
    count: Optional[int] = 1,
    block_ms: Optional[int] = None,
) -> List[Tuple[str, Dict[str, str]]]:
    """
    XREADGROUP: read from stream as consumer in group. id=">" means new messages only.
    Returns list of (entry_id, fields_dict).
    """
    kwargs: Dict[str, Any] = {}
    if count is not None:
        kwargs["count"] = count
    if block_ms is not None:
        kwargs["block"] = block_ms
    reply = redis.xreadgroup(group, consumer, {stream: id}, **kwargs)
    if not reply:
        return []
    result: List[Tuple[str, Dict[str, str]]] = []
    for stream_name, entries in reply:
        for eid, flds in entries:
            eid_str = eid.decode() if isinstance(eid, bytes) else eid
            decoded: Dict[str, str] = {}
            for k, v in (flds or {}).items():
                key = k.decode() if isinstance(k, bytes) else k
                val = v.decode() if isinstance(v, bytes) else v
                decoded[key] = val
            result.append((eid_str, decoded))
    return result


def ack_message(redis: Redis, stream: str, group: str, *entry_ids: str) -> int:
    """XACK: acknowledge message(s) so they are not redelivered. Returns count acked."""
    if not entry_ids:
        return 0
    return redis.xack(stream, group, *entry_ids)  # type: ignore[return-value]


def ensure_consumer_group(redis: Redis, stream: str, group: str, start_id: str = "0") -> None:
    """XGROUP CREATE stream group start_id MKSTREAM. Idempotent if group exists."""
    try:
        redis.xgroup_create(stream, group, id=start_id, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" in str(e) or "already exists" in str(e).lower():
            return
        raise


def read_latest(redis: Redis, stream: str, count: int = 10) -> List[Tuple[str, Dict[str, str]]]:
    """XRANGE stream - + COUNT count. Returns last `count` entries (oldest first in slice)."""
    raw = redis.xrange(stream, min="-", max="+", count=count)
    out: List[Tuple[str, Dict[str, str]]] = []
    for eid, flds in raw:
        eid_str = eid.decode() if isinstance(eid, bytes) else eid
        decoded: Dict[str, str] = {}
        for k, v in (flds or {}).items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            decoded[key] = val
        out.append((eid_str, decoded))
    return out
