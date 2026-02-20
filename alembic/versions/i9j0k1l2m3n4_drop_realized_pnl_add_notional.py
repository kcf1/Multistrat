"""drop realized_pnl, add notional to pms_positions

Revision ID: i9j0k1l2m3n4
Revises: h8i9j0k1l2m3
Create Date: 2026-02-20

Drops realized_pnl; adds notional (position value, e.g. open_qty * mark_price).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "i9j0k1l2m3n4"
down_revision: Union[str, Sequence[str], None] = "h8i9j0k1l2m3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("pms_positions", "realized_pnl")
    op.add_column("pms_positions", sa.Column("notional", sa.Numeric(36, 18), nullable=True))


def downgrade() -> None:
    op.drop_column("pms_positions", "notional")
    op.add_column(
        "pms_positions",
        sa.Column("realized_pnl", sa.Numeric(36, 18), nullable=False, server_default="0"),
    )
