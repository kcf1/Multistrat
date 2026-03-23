"""add basis_rate table (market data basis)

Revision ID: z6a7b8c9d0e1
Revises: y5z6a7b8c9d0
Create Date: 2026-03-24

Binance basis snapshots keyed by (pair, contract_type, period, sample_time).
See docs/market_data/BASIS_IMPLEMENTATION_PLAN.md.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "z6a7b8c9d0e1"
down_revision: Union[str, Sequence[str], None] = "y5z6a7b8c9d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "basis_rate",
        sa.Column("pair", sa.Text(), nullable=False),
        sa.Column("contract_type", sa.Text(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("sample_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("basis", sa.Numeric(36, 18), nullable=False),
        sa.Column("basis_rate", sa.Numeric(36, 18), nullable=False),
        sa.Column("annualized_basis_rate", sa.Numeric(36, 18), nullable=False),
        sa.Column("futures_price", sa.Numeric(36, 18), nullable=False),
        sa.Column("index_price", sa.Numeric(36, 18), nullable=False),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint(
            "pair",
            "contract_type",
            "period",
            "sample_time",
            name="pk_basis_rate",
        ),
    )
    op.create_index(
        "ix_basis_rate_pair_contract_type_period_sample_time",
        "basis_rate",
        ["pair", "contract_type", "period", "sample_time"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_basis_rate_pair_contract_type_period_sample_time",
        table_name="basis_rate",
    )
    op.drop_table("basis_rate")
