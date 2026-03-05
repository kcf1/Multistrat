"""add symbols table (12.2.14)

Revision ID: l2m3n4o5p6q7
Revises: k1l2m3n4o5p6
Create Date: 2026-02-26

Creates symbols reference table (symbol, base_asset, quote_asset, etc.).
OMS or reference sync populates from broker exchangeInfo; PMS reads for base/quote when building cash.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "l2m3n4o5p6q7"
down_revision: Union[str, Sequence[str], None] = "k1l2m3n4o5p6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "symbols",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("base_asset", sa.Text(), nullable=False),
        sa.Column("quote_asset", sa.Text(), nullable=False),
        sa.Column("tick_size", sa.Numeric(36, 18), nullable=True),
        sa.Column("lot_size", sa.Numeric(36, 18), nullable=True),
        sa.Column("min_qty", sa.Numeric(36, 18), nullable=True),
        sa.Column("product_type", sa.Text(), nullable=True),
        sa.Column("broker", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("symbol"),
    )
    op.create_index("ix_symbols_broker", "symbols", ["broker"])


def downgrade() -> None:
    op.drop_index("ix_symbols_broker", table_name="symbols")
    op.drop_table("symbols")
