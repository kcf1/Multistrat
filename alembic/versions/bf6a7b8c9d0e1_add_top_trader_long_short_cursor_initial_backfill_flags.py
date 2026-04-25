"""Add initial_backfill flags to market_data.top_trader_long_short_cursor.

Revision ID: bf6a7b8c9d0e1
Revises: bf5a6b7c8d9e0
Create Date: 2026-04-26
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "bf6a7b8c9d0e1"
down_revision: Union[str, Sequence[str], None] = "bf5a6b7c8d9e0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "top_trader_long_short_cursor",
        sa.Column(
            "initial_backfill_done",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema="market_data",
    )
    op.add_column(
        "top_trader_long_short_cursor",
        sa.Column("initial_backfill_at", sa.DateTime(timezone=True), nullable=True),
        schema="market_data",
    )
    op.execute(
        "UPDATE market_data.top_trader_long_short_cursor "
        "SET initial_backfill_done = true, initial_backfill_at = now()"
    )


def downgrade() -> None:
    op.drop_column(
        "top_trader_long_short_cursor", "initial_backfill_at", schema="market_data"
    )
    op.drop_column(
        "top_trader_long_short_cursor", "initial_backfill_done", schema="market_data"
    )
