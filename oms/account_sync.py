"""
OMS → Postgres account sync (task 12.2.7).

Sync accounts and balances from Redis account store to Postgres
(accounts, balances). No positions table (dropped for PMS). Optionally write
balance_changes when processing balanceUpdate events. TTL on account keys is
set only from balance-change callbacks (main), not from periodic sync.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Union

from redis import Redis

from oms.log import logger
from oms.storage.redis_account_store import RedisAccountStore

# Key pattern to discover brokers (accounts:by_broker:{broker})
ACCOUNTS_BY_BROKER_PREFIX = "accounts:by_broker:"


def _account_to_row(account: Dict[str, Any], broker: str, account_id: str) -> Dict[str, Any]:
    """Map Redis account metadata to Postgres accounts row (column names).
    name: from account metadata if set, else derived as broker:account_id (not user-set in current flow).
    """
    name = account.get("name")
    if name is None or name == "":
        name = f"{broker}:{account_id}"
    return {
        "account_id": account_id,
        "name": name[:255] if name else None,
        "broker": (broker or "")[:255],
        "created_at": account.get("created_at") or account.get("updated_at"),
        "updated_at": account.get("updated_at"),
        "config": account.get("payload"),
    }


def _balance_to_row(balance: Dict[str, Any], account_pk: int, updated_at: Optional[str] = None) -> Dict[str, Any]:
    """Map Redis balance dict to Postgres balances row."""
    def _num(v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        if isinstance(v, (int, float, Decimal)):
            return Decimal(str(v))
        try:
            return Decimal(str(v))
        except (TypeError, ValueError):
            return None

    return {
        "account_id": account_pk,
        "asset": (balance.get("asset") or "")[:64],
        "available": _num(balance.get("available")) or Decimal("0"),
        "locked": _num(balance.get("locked")) or Decimal("0"),
        "updated_at": updated_at,
    }


def _pg_conn(pg_connect: Union[str, Callable[[], Any]]):
    """Return an open connection; caller must close if we_opened."""
    if callable(pg_connect):
        return pg_connect(), False
    import psycopg2
    return psycopg2.connect(pg_connect), True


def _pg_params(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert row to psycopg2 params; config/payload as JSONB."""
    try:
        from psycopg2.extras import Json
    except ImportError:
        Json = lambda x: json.dumps(x) if isinstance(x, dict) else x  # noqa: E731
    out = dict(row)
    for key in ("config", "payload"):
        if out.get(key) is not None and isinstance(out[key], dict):
            out[key] = Json(out[key])
        elif out.get(key) is not None and isinstance(out[key], str):
            try:
                out[key] = Json(json.loads(out[key]))
            except (TypeError, ValueError):
                out[key] = Json({})
    return out


def _discover_brokers(redis: Redis) -> List[str]:
    """Return list of brokers that have at least one account in Redis."""
    keys = redis.keys(ACCOUNTS_BY_BROKER_PREFIX + "*")
    brokers: List[str] = []
    seen: set = set()
    for k in keys or []:
        key = k.decode() if isinstance(k, bytes) else k
        if ":" in key:
            broker = key.split(":")[-1]
            if broker and broker not in seen:
                seen.add(broker)
                brokers.append(broker)
    return sorted(brokers)


def sync_accounts_to_postgres(
    redis: Redis,
    account_store: RedisAccountStore,
    pg_connect: Union[str, Callable[[], Any]],
    ttl_after_sync_seconds: Optional[int] = None,
    sync_balances: bool = True,
) -> int:
    """
    Read accounts (and optionally balances) from Redis and UPSERT to Postgres (accounts, balances).
    When sync_balances is True: for each account, UPSERT balances and delete any balances in
    Postgres that are not in the current Redis set (cleanup for flattened/removed assets).
    Optionally set TTL on account Redis keys after sync.
    Returns number of accounts synced.
    """
    brokers = _discover_brokers(redis)
    if not brokers:
        logger.debug("sync_accounts_to_postgres: no brokers found in Redis")
        return 0

    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        count = 0
        for broker in brokers:
            for account_id in account_store.get_account_ids_by_broker(broker):
                account = account_store.get_account(broker, account_id)
                if not account:
                    continue
                row = _account_to_row(account, broker, account_id)
                cur.execute(
                    """
                    INSERT INTO accounts (account_id, name, broker, created_at, config)
                    VALUES (%(account_id)s, %(name)s, %(broker)s, %(created_at)s, %(config)s)
                    ON CONFLICT (broker, account_id) DO UPDATE SET
                        name = COALESCE(EXCLUDED.name, accounts.name),
                        config = COALESCE(EXCLUDED.config, accounts.config)
                    RETURNING id
                    """,
                    _pg_params({k: v for k, v in row.items() if k not in ("updated_at", "env")}),
                )
                account_pk = cur.fetchone()[0]
                updated_at = row.get("updated_at")

                if sync_balances:
                    balances = account_store.get_balances(broker, account_id)
                    current_assets = set()
                    for bal in balances:
                        asset = (bal.get("asset") or "").strip()
                        if not asset:
                            continue
                        current_assets.add(asset)
                        bal_row = _balance_to_row(bal, account_pk, updated_at)
                        cur.execute(
                            """
                            INSERT INTO balances (account_id, asset, available, locked, updated_at)
                            VALUES (%(account_id)s, %(asset)s, %(available)s, %(locked)s, %(updated_at)s)
                            ON CONFLICT (account_id, asset) DO UPDATE SET
                                available = EXCLUDED.available,
                                locked = EXCLUDED.locked,
                                updated_at = EXCLUDED.updated_at
                            """,
                            _pg_params(bal_row),
                        )

                    # Delete balances for this account that are no longer in Redis (flattened)
                    if current_assets:
                        cur.execute(
                            "DELETE FROM balances WHERE account_id = %s AND asset != ALL(%s)",
                            (account_pk, list(current_assets)),
                        )
                    else:
                        cur.execute("DELETE FROM balances WHERE account_id = %s", (account_pk,))

                count += 1

        conn.commit()
        if count:
            logger.info("sync_accounts_to_postgres synced {} account(s)", count)
        return count
    finally:
        if we_opened:
            conn.close()


def _normalize_event_time(event_time: Any):
    """Convert event_time to datetime (timezone-aware). Accepts ms (int/float) or datetime/str."""
    if event_time is None:
        return datetime.now(timezone.utc)
    if isinstance(event_time, datetime):
        return event_time if event_time.tzinfo else event_time.replace(tzinfo=timezone.utc)
    if isinstance(event_time, (int, float)):
        if event_time > 1e12:
            event_time = event_time / 1000.0
        return datetime.fromtimestamp(event_time, tz=timezone.utc)
    if isinstance(event_time, str):
        try:
            return datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
    return datetime.now(timezone.utc)


DEFAULT_CASH_BOOK = "default"


def write_balance_change(
    pg_connect: Union[str, Callable[[], Any]],
    account_id_pk: int,
    asset: str,
    change_type: str,
    delta: Union[Decimal, float, str],
    event_type: str,
    event_time: Any,
    balance_before: Optional[Union[Decimal, float]] = None,
    balance_after: Optional[Union[Decimal, float]] = None,
    broker_event_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    book: Optional[str] = None,
) -> None:
    """
    Insert one row into balance_changes. Call from account callback when
    processing balanceUpdate events (delta from payload.d or balance dict).
    book: default cash book for broker-fed records; use constant DEFAULT_CASH_BOOK if None.
    """
    def _num(v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return v
        try:
            return Decimal(str(v))
        except (TypeError, ValueError):
            return None

    event_time_dt = _normalize_event_time(event_time)
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        book_val = (book or DEFAULT_CASH_BOOK)[:255] if (book or DEFAULT_CASH_BOOK) else DEFAULT_CASH_BOOK
        cur.execute(
            """
            INSERT INTO balance_changes (
                account_id, asset, book, change_type, delta,
                balance_before, balance_after, event_type, broker_event_id,
                event_time, created_at, payload
            ) VALUES (
                %(account_id)s, %(asset)s, %(book)s, %(change_type)s, %(delta)s,
                %(balance_before)s, %(balance_after)s, %(event_type)s, %(broker_event_id)s,
                %(event_time)s, NOW(), %(payload)s
            )
            """,
            _pg_params({
                "account_id": account_id_pk,
                "asset": asset[:255] if asset else "",
                "book": book_val,
                "change_type": change_type[:64] if change_type else "adjustment",
                "delta": _num(delta) or Decimal("0"),
                "balance_before": _num(balance_before),
                "balance_after": _num(balance_after),
                "event_type": event_type[:64] if event_type else "balanceUpdate",
                "broker_event_id": broker_event_id,
                "event_time": event_time_dt,
                "payload": payload,
            }),
        )
        conn.commit()
    finally:
        if we_opened:
            conn.close()


def get_account_pk_by_broker_and_id(
    pg_connect: Union[str, Callable[[], Any]],
    broker: str,
    account_id: str,
) -> Optional[int]:
    """Resolve Postgres accounts.id from (broker, account_id). For use by balance_changes writer."""
    conn, we_opened = _pg_conn(pg_connect)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM accounts WHERE broker = %s AND account_id = %s",
            (broker, account_id),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        if we_opened:
            conn.close()
