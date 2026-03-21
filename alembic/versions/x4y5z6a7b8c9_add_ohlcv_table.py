"""add ohlcv table (Phase 4 market data)

Revision ID: x4y5z6a7b8c9
Revises: v2w3x4y5z6a7
Create Date: 2026-03-22

Binance klines and other OHLCV sources: one row per (symbol, interval, open_time).
See docs/PHASE4_DETAILED_PLAN.md §4.1.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "x4y5z6a7b8c9"
down_revision: Union[str, Sequence[str], None] = "v2w3x4y5z6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ohlcv",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("interval", sa.Text(), nullable=False),
        sa.Column("open_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(36, 18), nullable=False),
        sa.Column("high", sa.Numeric(36, 18), nullable=False),
        sa.Column("low", sa.Numeric(36, 18), nullable=False),
        sa.Column("close", sa.Numeric(36, 18), nullable=False),
        sa.Column("volume", sa.Numeric(36, 18), nullable=False),
        sa.Column("quote_volume", sa.Numeric(36, 18), nullable=True),
        sa.Column("trades", sa.Integer(), nullable=True),
        sa.Column("close_time", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("symbol", "interval", "open_time", name="pk_ohlcv"),
    )
    op.create_index(
        "ix_ohlcv_symbol_interval_open_time",
        "ohlcv",
        ["symbol", "interval", "open_time"],
    )


def downgrade() -> None:
    op.drop_index("ix_ohlcv_symbol_interval_open_time", table_name="ohlcv")
    op.drop_table("ohlcv")
