"""Add initial_backfill flags to market_data.taker_buy_sell_volume_cursor.

Revision ID: bf5a6b7c8d9e0
Revises: bf4a5b6c7d8e9
Create Date: 2026-04-26
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "bf5a6b7c8d9e0"
down_revision: Union[str, Sequence[str], None] = "bf4a5b6c7d8e9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "taker_buy_sell_volume_cursor",
        sa.Column(
            "initial_backfill_done",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema="market_data",
    )
    op.add_column(
        "taker_buy_sell_volume_cursor",
        sa.Column("initial_backfill_at", sa.DateTime(timezone=True), nullable=True),
        schema="market_data",
    )
    op.execute(
        "UPDATE market_data.taker_buy_sell_volume_cursor "
        "SET initial_backfill_done = true, initial_backfill_at = now()"
    )


def downgrade() -> None:
    op.drop_column(
        "taker_buy_sell_volume_cursor", "initial_backfill_at", schema="market_data"
    )
    op.drop_column(
        "taker_buy_sell_volume_cursor", "initial_backfill_done", schema="market_data"
    )
