"""Phase 4: move scheduler_runs from public to scheduler schema.

Revision ID: f2a3b4c5d6e7
Revises: e1f2a3b4c5d6
Create Date: 2026-04-14

Moves: scheduler_runs (audit table for scheduler/run_history.py).

See docs/POSTGRES_SCHEMA_GROUPING_PLAN.md §6.1 Phase 4.
"""

from typing import Sequence, Union

from alembic import op


revision: str = "f2a3b4c5d6e7"
down_revision: Union[str, Sequence[str], None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE public.scheduler_runs SET SCHEMA scheduler")


def downgrade() -> None:
    op.execute("ALTER TABLE scheduler.scheduler_runs SET SCHEMA public")
