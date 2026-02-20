"""rename pms_positions to positions

Revision ID: j0k1l2m3n4o5
Revises: i9j0k1l2m3n4
Create Date: 2026-02-20

Table renamed to positions; indexes renamed for consistency.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "j0k1l2m3n4o5"
down_revision: Union[str, Sequence[str], None] = "i9j0k1l2m3n4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.rename_table("pms_positions", "positions")
    op.execute("ALTER INDEX uq_pms_positions_account_book_symbol RENAME TO uq_positions_account_book_symbol")
    op.execute("ALTER INDEX ix_pms_positions_account_id RENAME TO ix_positions_account_id")
    op.execute("ALTER INDEX ix_pms_positions_symbol RENAME TO ix_positions_symbol")


def downgrade() -> None:
    op.execute("ALTER INDEX ix_positions_symbol RENAME TO ix_pms_positions_symbol")
    op.execute("ALTER INDEX ix_positions_account_id RENAME TO ix_pms_positions_account_id")
    op.execute("ALTER INDEX uq_positions_account_book_symbol RENAME TO uq_pms_positions_account_book_symbol")
    op.rename_table("positions", "pms_positions")
