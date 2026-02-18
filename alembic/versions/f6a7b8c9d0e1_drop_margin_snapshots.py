"""drop margin_snapshots table

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-02-19

Drops margin_snapshots table and its indexes (unused).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f6a7b8c9d0e1"
down_revision: Union[str, Sequence[str], None] = "e5f6a7b8c9d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("ix_margin_snapshots_timestamp", table_name="margin_snapshots")
    op.drop_index("ix_margin_snapshots_account_id", table_name="margin_snapshots")
    op.drop_table("margin_snapshots")


def downgrade() -> None:
    op.create_table(
        "margin_snapshots",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("total_margin", sa.Numeric(36, 18), nullable=True),
        sa.Column("available_balance", sa.Numeric(36, 18), nullable=True),
        sa.Column("timestamp", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_margin_snapshots_account_id", "margin_snapshots", ["account_id"])
    op.create_index("ix_margin_snapshots_timestamp", "margin_snapshots", ["timestamp"])
