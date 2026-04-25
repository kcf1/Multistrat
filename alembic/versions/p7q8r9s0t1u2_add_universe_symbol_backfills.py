"""Phase 4: add market_data universe_symbol_backfills table.

Revision ID: p7q8r9s0t1u2
Revises: p1q2r3s4t5u6
Create Date: 2026-04-26

Tracks one-time "all dataset backfills" per newly eligible tradable symbol.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "p7q8r9s0t1u2"
down_revision: Union[str, Sequence[str], None] = "p1q2r3s4t5u6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "universe_symbol_backfills",
        sa.Column("symbol", sa.Text(), primary_key=True, nullable=False),
        sa.Column(
            "backfilled_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="market_data",
    )


def downgrade() -> None:
    op.drop_table("universe_symbol_backfills", schema="market_data")

