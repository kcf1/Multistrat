"""positions symbol to asset (refactor 3.1)

Revision ID: m3n4o5p6q7r8
Revises: l2m3n4o5p6q7
Create Date: 2026-03-09

Renames positions.symbol -> asset and related indexes for asset-level position grain.
See docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md §3.1.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "m3n4o5p6q7r8"
down_revision: Union[str, Sequence[str], None] = "l2m3n4o5p6q7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "positions",
        "symbol",
        new_column_name="asset",
    )
    op.execute("ALTER INDEX uq_positions_account_book_symbol RENAME TO uq_positions_account_book_asset")
    op.execute("ALTER INDEX ix_positions_symbol RENAME TO ix_positions_asset")


def downgrade() -> None:
    op.execute("ALTER INDEX ix_positions_asset RENAME TO ix_positions_symbol")
    op.execute("ALTER INDEX uq_positions_account_book_asset RENAME TO uq_positions_account_book_symbol")
    op.alter_column(
        "positions",
        "asset",
        new_column_name="symbol",
    )
