"""add taker buy/sell volume tables (market data taker buy/sell volume)

Revision ID: h0i1j2k3l4m5
Revises: d3e4f5a6b7c8
Create Date: 2026-03-25

Binance USD-M taker buy/sell volume statistics keyed by (symbol, period, sample_time).
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "h0i1j2k3l4m5"
down_revision: Union[str, Sequence[str], None] = "d3e4f5a6b7c8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "taker_buy_sell_volume",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("sample_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("buy_sell_ratio", sa.Numeric(18, 10), nullable=False),
        sa.Column("buy_vol", sa.Numeric(30, 14), nullable=False),
        sa.Column("sell_vol", sa.Numeric(30, 14), nullable=False),
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
            name="pk_taker_buy_sell_volume",
        ),
    )
    op.create_index(
        "ix_taker_buy_sell_volume_symbol_period_sample_time",
        "taker_buy_sell_volume",
        ["symbol", "period", "sample_time"],
    )

    op.create_table(
        "taker_buy_sell_volume_cursor",
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
            name="pk_taker_buy_sell_volume_cursor",
        ),
    )


def downgrade() -> None:
    op.drop_table("taker_buy_sell_volume_cursor")
    op.drop_index(
        "ix_taker_buy_sell_volume_symbol_period_sample_time",
        table_name="taker_buy_sell_volume",
    )
    op.drop_table("taker_buy_sell_volume")

