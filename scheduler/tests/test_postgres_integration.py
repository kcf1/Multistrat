"""
Postgres integration tests: ``scheduler_runs`` via ``run_history`` and key table
placement across schemas (Phase 5 §5.10.1, docs/POSTGRES_SCHEMA_GROUPING_PLAN.md §8).

Skipped when ``DATABASE_URL`` is unset or Postgres is unreachable.
"""

from __future__ import annotations

import os
import uuid

import pytest

from pgconn import configure_for_scheduler
from scheduler.run_history import record_run_end, record_run_start


def _pg_available() -> bool:
    url = os.environ.get("DATABASE_URL")
    if not url:
        return False
    try:
        import psycopg2

        conn = psycopg2.connect(url)
        conn.close()
        return True
    except Exception:
        return False


REQUIRED_TABLE_PLACEMENT: tuple[tuple[str, str], ...] = (
    ("oms", "orders"),
    ("oms", "accounts"),
    ("pms", "positions"),
    ("market_data", "ohlcv"),
    ("scheduler", "scheduler_runs"),
    ("public", "alembic_version"),
)


@pytest.mark.skipif(not _pg_available(), reason="DATABASE_URL not set or Postgres not reachable")
def test_run_history_start_end_persists_in_scheduler_schema() -> None:
    """``record_run_start`` / ``record_run_end`` write to ``scheduler.scheduler_runs``."""
    import psycopg2

    url = os.environ["DATABASE_URL"]
    job_id = f"pytest-scheduler-run-history-{uuid.uuid4().hex}"
    run_id = record_run_start(url, job_id)
    try:
        record_run_end(url, run_id, "ok", error=None, payload={"test": True})

        conn = psycopg2.connect(url)
        configure_for_scheduler(conn)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT job_id, status, finished_at IS NOT NULL, payload->>'test' "
                    "FROM scheduler_runs WHERE id = %s",
                    (run_id,),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row[0] == job_id
        assert row[1] == "ok"
        assert row[2] is True
        assert row[3] == "true"
    finally:
        conn = psycopg2.connect(url)
        configure_for_scheduler(conn)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM scheduler_runs WHERE id = %s", (run_id,))
            conn.commit()
        finally:
            conn.close()


@pytest.mark.skipif(not _pg_available(), reason="DATABASE_URL not set or Postgres not reachable")
def test_key_application_tables_in_expected_schemas() -> None:
    """Inventory check: canonical tables live in the grouped schemas (not ``public``)."""
    import psycopg2

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT schemaname, tablename FROM pg_tables
                WHERE tablename = ANY(%s)
                """,
                ([t for _, t in REQUIRED_TABLE_PLACEMENT],),
            )
            found = {(r[0], r[1]) for r in cur.fetchall()}
    finally:
        conn.close()

    missing = [pair for pair in REQUIRED_TABLE_PLACEMENT if pair not in found]
    assert not missing, (
        "Expected (schema, table) pairs missing from pg_tables "
        f"(run alembic upgrade head): {missing}; found={sorted(found)}"
    )
