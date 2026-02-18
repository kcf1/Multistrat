"""
Unit tests for account Postgres schema (task 12.2.6).

Verifies schema creation and indexes for accounts, balances,
balance_changes (positions and margin_snapshots removed). Runs Alembic upgrade/downgrade when
DATABASE_URL is set and Postgres is reachable; otherwise skipped.
"""

import os
from pathlib import Path

import pytest


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


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


@pytest.fixture(scope="module")
def db_conn():
    """Connection to Postgres; skip if DATABASE_URL not set or unreachable."""
    if not _pg_available():
        pytest.skip("DATABASE_URL not set or Postgres not reachable")
    import psycopg2
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        yield conn
    finally:
        conn.close()


def _run_alembic(command: str, revision: str = "head") -> None:
    from alembic import command
    from alembic.config import Config
    root = _project_root()
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    url = os.environ.get("DATABASE_URL")
    if url:
        config.set_main_option("sqlalchemy.url", url)
    if command == "upgrade":
        command.upgrade(config, revision)
    elif command == "downgrade":
        command.downgrade(config, revision)
    else:
        raise ValueError(f"Unknown command: {command}")


def _current_revision(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT version_num FROM alembic_version")
        row = cur.fetchone()
        return row[0] if row else ""


def _tables_exist(conn, names):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename = ANY(%s)
            """,
            (list(names),),
        )
        found = {r[0] for r in cur.fetchall()}
    return found >= set(names)


def _indexes_exist(conn, expected_index_names):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = 'public' AND indexname = ANY(%s)
            """,
            (list(expected_index_names),),
        )
        found = {r[0] for r in cur.fetchall()}
    return found >= set(expected_index_names)


ACCOUNT_TABLES = (
    "accounts",
    "balances",
    "balance_changes",
)

ACCOUNT_INDEXES = (
    "ix_accounts_broker",
    "uq_accounts_broker_account_id",
    "ix_balances_account_id",
    "uq_balances_account_id_asset",
    "ix_balance_changes_account_id_asset_event_time",
    "ix_balance_changes_account_id_change_type",
    "ix_balance_changes_event_time",
)


@pytest.mark.skipif(not _pg_available(), reason="DATABASE_URL not set or Postgres not reachable")
class TestAccountSchemaCreation:
    """Verify account tables and indexes after migration."""

    def test_account_tables_exist_after_upgrade(self, db_conn):
        """After upgrading to head, account tables exist."""
        _run_alembic("upgrade", "head")
        db_conn.commit()
        assert _tables_exist(db_conn, ACCOUNT_TABLES), (
            "Expected account tables missing. Run: alembic upgrade head"
        )

    def test_account_indexes_exist_after_upgrade(self, db_conn):
        """After upgrading to head, expected indexes exist on account tables."""
        _run_alembic("upgrade", "head")
        db_conn.commit()
        assert _indexes_exist(db_conn, ACCOUNT_INDEXES), (
            "Expected account indexes missing. Run: alembic upgrade head"
        )

    def test_upgrade_downgrade_account_revision(self, db_conn):
        """Upgrade to account revision creates tables/indexes; downgrade removes them."""
        # Ensure we're at revision before account tables
        _run_alembic("downgrade", "b2c3d4e5f6a7")
        db_conn.commit()

        rev_before = _current_revision(db_conn)
        assert _tables_exist(db_conn, ACCOUNT_TABLES) is False

        # Upgrade to account revision
        _run_alembic("upgrade", "c3d4e5f6a7b8")
        db_conn.commit()
        assert _tables_exist(db_conn, ACCOUNT_TABLES), "Account tables should exist after upgrade"
        assert _indexes_exist(db_conn, ACCOUNT_INDEXES), "Account indexes should exist after upgrade"

        # Downgrade
        _run_alembic("downgrade", "b2c3d4e5f6a7")
        db_conn.commit()
        assert _tables_exist(db_conn, ACCOUNT_TABLES) is False, (
            "Account tables should be dropped after downgrade"
        )

        # Restore head for other tests / CI
        _run_alembic("upgrade", "head")
        db_conn.commit()
