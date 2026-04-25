"""Phase 4: add market_data_universe_assets table.

Revision ID: p1q2r3s4t5u6
Revises: f2a3b4c5d6e7
Create Date: 2026-04-26

Universe is base-asset only (no venue/pair fields); tradability is resolved elsewhere.
See docs/market_data/TOP100_UNIVERSE_DAILY_EXPANSION_PLAN.md.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "p1q2r3s4t5u6"
down_revision: Union[str, Sequence[str], None] = "f2a3b4c5d6e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "universe_assets",
        sa.Column("base_asset", sa.Text(), primary_key=True, nullable=False),
        sa.Column("source", sa.Text(), nullable=False, server_default=sa.text("'cmc_top100'")),
        sa.Column("first_seen_date", sa.Date(), nullable=True),
        sa.Column("last_seen_date", sa.Date(), nullable=True),
        sa.Column("current_rank", sa.Integer(), nullable=True),
        sa.Column(
            "is_current_top100",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "was_ever_top100",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="market_data",
    )

    op.create_index(
        "ix_market_data_universe_assets_is_current_top100",
        "universe_assets",
        ["is_current_top100"],
        schema="market_data",
    )
    op.create_index(
        "ix_market_data_universe_assets_was_ever_top100",
        "universe_assets",
        ["was_ever_top100"],
        schema="market_data",
    )
    op.create_index(
        "ix_market_data_universe_assets_last_seen_date",
        "universe_assets",
        ["last_seen_date"],
        schema="market_data",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_market_data_universe_assets_last_seen_date",
        table_name="universe_assets",
        schema="market_data",
    )
    op.drop_index(
        "ix_market_data_universe_assets_was_ever_top100",
        table_name="universe_assets",
        schema="market_data",
    )
    op.drop_index(
        "ix_market_data_universe_assets_is_current_top100",
        table_name="universe_assets",
        schema="market_data",
    )
    op.drop_table("universe_assets", schema="market_data")

