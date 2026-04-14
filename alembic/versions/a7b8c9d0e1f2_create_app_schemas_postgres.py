"""Phase 0: create application Postgres schemas (empty).

Revision ID: a7b8c9d0e1f2
Revises: w6x7y8z9a0b1
Create Date: 2026-04-14

Creates ``oms``, ``pms``, ``market_data``, ``scheduler`` and grants ``USAGE`` (and
``CREATE`` for migrations) to ``CURRENT_USER``. Tables stay in ``public`` until
later phased ``ALTER TABLE … SET SCHEMA`` revisions.

See docs/POSTGRES_SCHEMA_GROUPING_PLAN.md §6.1 Phase 0.
"""

from typing import Sequence, Union

from alembic import op


revision: str = "a7b8c9d0e1f2"
down_revision: Union[str, Sequence[str], None] = "w6x7y8z9a0b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for name in ("oms", "pms", "market_data", "scheduler"):
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {name}")
    # Migration runner is typically the DB owner; still grant explicitly for stricter roles.
    for name in ("oms", "pms", "market_data", "scheduler"):
        op.execute(f"GRANT USAGE ON SCHEMA {name} TO CURRENT_USER")
        op.execute(f"GRANT CREATE ON SCHEMA {name} TO CURRENT_USER")


def downgrade() -> None:
    for name in ("scheduler", "market_data", "pms", "oms"):
        op.execute(f"DROP SCHEMA IF EXISTS {name} CASCADE")
