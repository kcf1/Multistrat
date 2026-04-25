"""Drop market_data.universe_symbol_backfills (replaced by per-dataset cursor flags).

Revision ID: bf7a8b9c0d1e2
Revises: bf6a7b8c9d0e1
Create Date: 2026-04-26
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "bf7a8b9c0d1e2"
down_revision: Union[str, Sequence[str], None] = "bf6a7b8c9d0e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("universe_symbol_backfills", schema="market_data")


def downgrade() -> None:
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
