"""add mark_price to pms_positions

Revision ID: h8i9j0k1l2m3
Revises: g7h8i9j0k1l2
Create Date: 2026-02-20

Stores the mark price used to compute unrealized_pnl for audit and display.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "h8i9j0k1l2m3"
down_revision: Union[str, Sequence[str], None] = "g7h8i9j0k1l2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("pms_positions", sa.Column("mark_price", sa.Numeric(36, 18), nullable=True))


def downgrade() -> None:
    op.drop_column("pms_positions", "mark_price")
