"""drop annualized_basis_rate from basis_rate

Revision ID: b1c2d3e4f5a6
Revises: a0b1c2d3e4f5
Create Date: 2026-03-24
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b1c2d3e4f5a6"
down_revision: Union[str, Sequence[str], None] = "a0b1c2d3e4f5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("basis_rate", "annualized_basis_rate")


def downgrade() -> None:
    op.add_column(
        "basis_rate",
        sa.Column("annualized_basis_rate", sa.Numeric(36, 18), nullable=True),
    )
