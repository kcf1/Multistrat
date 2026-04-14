"""
Postgres session defaults for app schemas (Phase 0 — see docs/POSTGRES_SCHEMA_GROUPING_PLAN.md §7.8).

- Each service sets ``search_path`` so **home**-schema SQL can stay unqualified.
- **Cross-schema** SQL must still use ``schema.table`` (not handled here).

``SET search_path`` runs only on real :class:`psycopg2.extensions.connection` instances so
unit tests that pass mock connections are unchanged.
"""

from __future__ import annotations

import enum
from typing import Any

from psycopg2 import sql
from psycopg2.extensions import connection as PsycopgConnection

__all__ = [
    "PostgresService",
    "SCHEMA_MARKET_DATA",
    "SCHEMA_OMS",
    "SCHEMA_PMS",
    "SCHEMA_SCHEDULER",
    "configure_for_market_data",
    "configure_for_oms",
    "configure_for_pms",
    "configure_for_scheduler",
    "configure_search_path",
    "connect",
]

# Literal schema names for cross-module SQL (§7.8 rule 2).
SCHEMA_OMS = "oms"
SCHEMA_PMS = "pms"
SCHEMA_MARKET_DATA = "market_data"
SCHEMA_SCHEDULER = "scheduler"


class PostgresService(str, enum.Enum):
    OMS = SCHEMA_OMS
    PMS = SCHEMA_PMS
    MARKET_DATA = SCHEMA_MARKET_DATA
    SCHEDULER = SCHEMA_SCHEDULER


def _path_parts(service: PostgresService) -> tuple[str, str]:
    if service == PostgresService.OMS:
        return (SCHEMA_OMS, "public")
    if service == PostgresService.PMS:
        return (SCHEMA_PMS, "public")
    if service == PostgresService.MARKET_DATA:
        return (SCHEMA_MARKET_DATA, "public")
    if service == PostgresService.SCHEDULER:
        return (SCHEMA_SCHEDULER, "public")
    raise ValueError(f"unknown service: {service!r}")


def configure_search_path(conn: Any, service: PostgresService) -> None:
    """Run ``SET search_path`` for *service* if *conn* is a real psycopg2 connection."""
    if not isinstance(conn, PsycopgConnection):
        return
    parts = _path_parts(service)
    stmt = sql.SQL("SET search_path TO {}").format(
        sql.SQL(", ").join(sql.Identifier(p) for p in parts)
    )
    with conn.cursor() as cur:
        cur.execute(stmt)
    conn.commit()


def configure_for_oms(conn: Any) -> None:
    configure_search_path(conn, PostgresService.OMS)


def configure_for_pms(conn: Any) -> None:
    configure_search_path(conn, PostgresService.PMS)


def configure_for_market_data(conn: Any) -> None:
    configure_search_path(conn, PostgresService.MARKET_DATA)


def configure_for_scheduler(conn: Any) -> None:
    configure_search_path(conn, PostgresService.SCHEDULER)


def connect(dsn: str, service: PostgresService, **kwargs: Any) -> PsycopgConnection:
    """``psycopg2.connect`` then apply :func:`configure_search_path`."""
    import psycopg2

    conn = psycopg2.connect(dsn, **kwargs)
    configure_search_path(conn, service)
    return conn
