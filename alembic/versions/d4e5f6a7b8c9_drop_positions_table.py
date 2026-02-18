"""drop positions table (free name for PMS)

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-02-19

Drops positions table so the name can be used by PMS (Position Management System).
OMS still stores position data in Redis (account:{broker}:{account_id}:positions);
no Postgres positions table in OMS schema.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, Sequence[str], None] = "c3d4e5f6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_index("uq_positions_account_id_symbol_side", table_name="positions")
    op.drop_index("ix_positions_account_id", table_name="positions")
    op.drop_table("positions")


def downgrade() -> None:
    op.create_table(
        "positions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Numeric(36, 18), nullable=False, server_default="0"),
        sa.Column("entry_price_avg", sa.Numeric(36, 18), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_positions_account_id", "positions", ["account_id"])
    op.create_index(
        "uq_positions_account_id_symbol_side",
        "positions",
        ["account_id", "symbol", "side"],
        unique=True,
    )
