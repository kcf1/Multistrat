"""add pms_positions table (12.3.4 / 12.3.6)

Revision ID: g7h8i9j0k1l2
Revises: f6a7b8c9d0e1
Create Date: 2026-02-20

PMS granular store: one row per (account_id, book, symbol) with open_qty (signed),
position_side, entry_avg, realized_pnl, unrealized_pnl. account_id = broker account id (text, matches orders).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "g7h8i9j0k1l2"
down_revision: Union[str, Sequence[str], None] = "f6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "pms_positions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("book", sa.Text(), nullable=False, server_default=""),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("open_qty", sa.Numeric(36, 18), nullable=False, server_default="0"),
        sa.Column("position_side", sa.Text(), nullable=False, server_default="flat"),
        sa.Column("entry_avg", sa.Numeric(36, 18), nullable=True),
        sa.Column("realized_pnl", sa.Numeric(36, 18), nullable=False, server_default="0"),
        sa.Column("unrealized_pnl", sa.Numeric(36, 18), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_pms_positions_account_book_symbol",
        "pms_positions",
        ["account_id", "book", "symbol"],
        unique=True,
    )
    op.create_index("ix_pms_positions_account_id", "pms_positions", ["account_id"])
    op.create_index("ix_pms_positions_symbol", "pms_positions", ["symbol"])


def downgrade() -> None:
    op.drop_index("ix_pms_positions_symbol", table_name="pms_positions")
    op.drop_index("ix_pms_positions_account_id", table_name="pms_positions")
    op.drop_index("uq_pms_positions_account_book_symbol", table_name="pms_positions")
    op.drop_table("pms_positions")
