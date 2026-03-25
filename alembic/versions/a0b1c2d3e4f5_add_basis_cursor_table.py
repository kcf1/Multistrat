"""add basis_cursor table (market data basis)

Revision ID: a0b1c2d3e4f5
Revises: z6a7b8c9d0e1
Create Date: 2026-03-24

Explicit high-water cursor per (pair, contract_type, period) for basis ingestion.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a0b1c2d3e4f5"
down_revision: Union[str, Sequence[str], None] = "z6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "basis_cursor",
        sa.Column("pair", sa.Text(), nullable=False),
        sa.Column("contract_type", sa.Text(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("last_sample_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "pair",
            "contract_type",
            "period",
            name="pk_basis_cursor",
        ),
    )


def downgrade() -> None:
    op.drop_table("basis_cursor")
