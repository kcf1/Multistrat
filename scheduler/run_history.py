"""
Persist scheduler invocations to ``scheduler_runs`` (Phase 5 §4.4).

Safe to call when ``DATABASE_URL`` is unset — callers skip writes.
"""

from __future__ import annotations

from typing import Any, Literal, cast

import psycopg2
from psycopg2.extras import Json

from pgconn import configure_for_scheduler

RunStatus = Literal["ok", "error"]


def record_run_start(database_url: str, job_id: str) -> int:
    """
    Insert a row with ``finished_at`` / ``status`` NULL (in progress).

    Returns the new ``scheduler_runs.id``.
    """
    conn = psycopg2.connect(database_url)
    configure_for_scheduler(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scheduler_runs (job_id) VALUES (%s) RETURNING id",
                (job_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise RuntimeError("RETURNING id produced no row")
            run_id = cast(int, row[0])
        conn.commit()
        return run_id
    finally:
        conn.close()


def record_run_end(
    database_url: str,
    run_id: int,
    status: RunStatus,
    error: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Set ``finished_at``, ``status``, and optional ``error`` / ``payload``."""
    conn = psycopg2.connect(database_url)
    configure_for_scheduler(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scheduler_runs SET finished_at = NOW(), status = %s, error = %s, payload = %s "
                "WHERE id = %s",
                (status, error, Json(payload) if payload is not None else None, run_id),
            )
        conn.commit()
    finally:
        conn.close()
