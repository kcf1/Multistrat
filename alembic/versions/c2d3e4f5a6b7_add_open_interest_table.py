"""add open_interest table (market data open interest)

Revision ID: c2d3e4f5a6b7
Revises: b1c2d3e4f5a6
Create Date: 2026-03-24

Binance open interest statistics snapshots keyed by
(symbol, contract_type, period, sample_time).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c2d3e4f5a6b7"
down_revision: Union[str, Sequence[str], None] = "b1c2d3e4f5a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "open_interest",
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("contract_type", sa.Text(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("sample_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("sum_open_interest", sa.Numeric(36, 18), nullable=False),
        sa.Column("sum_open_interest_value", sa.Numeric(36, 18), nullable=False),
        sa.Column("cmc_circulating_supply", sa.Numeric(36, 18), nullable=True),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "symbol",
            "contract_type",
            "period",
            "sample_time",
            name="pk_open_interest",
        ),
    )
    op.create_index(
        "ix_open_interest_symbol_contract_type_period_sample_time",
        "open_interest",
        ["symbol", "contract_type", "period", "sample_time"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_open_interest_symbol_contract_type_period_sample_time",
        table_name="open_interest",
    )
    op.drop_table("open_interest")
