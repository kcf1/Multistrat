"""add broker to positions; unique on (broker, account_id, book, asset) (E.2 / 7.5)

Revision ID: r8s9t0u1v2w3
Revises: q7r8s9t0u1v2
Create Date: 2026-03-09

Adds broker column to positions and changes unique constraint to (broker, account_id, book, asset).
See docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md §7.5.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "r8s9t0u1v2w3"
down_revision: Union[str, Sequence[str], None] = "q7r8s9t0u1v2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "positions",
        sa.Column("broker", sa.Text(), nullable=False, server_default=""),
    )
    op.execute("DROP INDEX IF EXISTS uq_positions_account_book_asset")
    op.create_index(
        "uq_positions_broker_account_book_asset",
        "positions",
        ["broker", "account_id", "book", "asset"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_positions_broker_account_book_asset", table_name="positions")
    op.create_index(
        "uq_positions_account_book_asset",
        "positions",
        ["account_id", "book", "asset"],
        unique=True,
    )
    op.drop_column("positions", "broker")
