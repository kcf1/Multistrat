"""rename positions.usd_value -> usd_price; consistent with assets.usd_price

Revision ID: t0u1v2w3x4y5
Revises: s9t0u1v2w3x4
Create Date: 2026-03-09

Naming: usd_price = per-unit USD price (align with assets.usd_price).
Derived: usd_notional = usd_price * open_qty (in application).
"""
from typing import Sequence, Union

from alembic import op


revision: str = "t0u1v2w3x4y5"
down_revision: Union[str, Sequence[str], None] = "s9t0u1v2w3x4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "positions",
        "usd_value",
        new_column_name="usd_price",
    )


def downgrade() -> None:
    op.alter_column(
        "positions",
        "usd_price",
        new_column_name="usd_value",
    )
