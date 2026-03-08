"""positions: add usd_notional as generated column (open_qty * usd_price)

Revision ID: u1v2w3x4y5z6
Revises: t0u1v2w3x4y5
Create Date: 2026-03-09

usd_notional = open_qty * usd_price, maintained by PostgreSQL.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "u1v2w3x4y5z6"
down_revision: Union[str, Sequence[str], None] = "t0u1v2w3x4y5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE positions ADD COLUMN usd_notional NUMERIC(36, 18) "
        "GENERATED ALWAYS AS (open_qty * usd_price) STORED"
    )


def downgrade() -> None:
    op.drop_column("positions", "usd_notional")
