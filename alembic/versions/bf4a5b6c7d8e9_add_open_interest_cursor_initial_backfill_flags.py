"""Add initial_backfill flags to market_data.open_interest_cursor.

Revision ID: bf4a5b6c7d8e9
Revises: bf3a4b5c6d7e8
Create Date: 2026-04-26
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "bf4a5b6c7d8e9"
down_revision: Union[str, Sequence[str], None] = "bf3a4b5c6d7e8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "open_interest_cursor",
        sa.Column(
            "initial_backfill_done",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema="market_data",
    )
    op.add_column(
        "open_interest_cursor",
        sa.Column("initial_backfill_at", sa.DateTime(timezone=True), nullable=True),
        schema="market_data",
    )
    op.execute(
        "UPDATE market_data.open_interest_cursor "
        "SET initial_backfill_done = true, initial_backfill_at = now()"
    )


def downgrade() -> None:
    op.drop_column("open_interest_cursor", "initial_backfill_at", schema="market_data")
    op.drop_column("open_interest_cursor", "initial_backfill_done", schema="market_data")
