"""add top trader long/short tables (market data top trader long/short)

Revision ID: m5n6b7v8c9d0
Revises: h0i1j2k3l4m5
Create Date: 2026-03-25

Binance USD-M top trader long/short position ratio statistics keyed by (symbol, period, sample_time).
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "m5n6b7v8c9d0"
down_revision: Union[str, Sequence[str], None] = "h0i1j2k3l4m5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "top_trader_long_short",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("sample_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("long_short_position_ratio", sa.Numeric(18, 10), nullable=False),
        sa.Column("long_account_ratio", sa.Numeric(18, 10), nullable=False),
        sa.Column("short_account_ratio", sa.Numeric(18, 10), nullable=False),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "symbol",
            "period",
            "sample_time",
            name="pk_top_trader_long_short",
        ),
    )
    op.create_index(
        "ix_top_trader_long_short_symbol_period_sample_time",
        "top_trader_long_short",
        ["symbol", "period", "sample_time"],
    )

    op.create_table(
        "top_trader_long_short_cursor",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("last_sample_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "symbol",
            "period",
            name="pk_top_trader_long_short_cursor",
        ),
    )


def downgrade() -> None:
    op.drop_table("top_trader_long_short_cursor")
    op.drop_index(
        "ix_top_trader_long_short_symbol_period_sample_time",
        table_name="top_trader_long_short",
    )
    op.drop_table("top_trader_long_short")

