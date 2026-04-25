"""Add initial_backfill flags to market_data.ingestion_cursor.

Revision ID: bf2a3b4c5d6e8
Revises: p7q8r9s0t1u2
Create Date: 2026-04-26

Per-dataset idempotency for universe-driven initial backfill (OHLCV).
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "bf2a3b4c5d6e8"
down_revision: Union[str, Sequence[str], None] = "p7q8r9s0t1u2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ingestion_cursor",
        sa.Column(
            "initial_backfill_done",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        schema="market_data",
    )
    op.add_column(
        "ingestion_cursor",
        sa.Column("initial_backfill_at", sa.DateTime(timezone=True), nullable=True),
        schema="market_data",
    )
    op.execute(
        "UPDATE market_data.ingestion_cursor "
        "SET initial_backfill_done = true, initial_backfill_at = now()"
    )


def downgrade() -> None:
    op.drop_column("ingestion_cursor", "initial_backfill_at", schema="market_data")
    op.drop_column("ingestion_cursor", "initial_backfill_done", schema="market_data")
