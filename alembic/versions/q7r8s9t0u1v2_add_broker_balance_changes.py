"""add broker to balance_changes (E.2 / 7.5)

Revision ID: q7r8s9t0u1v2
Revises: p6q7r8s9t0u1
Create Date: 2026-03-09

Adds broker column to balance_changes for grain (broker, account_id, book, asset).
See docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md §7.5.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "q7r8s9t0u1v2"
down_revision: Union[str, Sequence[str], None] = "p6q7r8s9t0u1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "balance_changes",
        sa.Column("broker", sa.Text(), nullable=False, server_default=""),
    )


def downgrade() -> None:
    op.drop_column("balance_changes", "broker")
