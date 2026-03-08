"""positions: add usd_value; drop entry_avg, mark_price, notional, unrealized_pnl

Revision ID: s9t0u1v2w3x4
Revises: r8s9t0u1v2w3
Create Date: 2026-03-09

Position valuation restructure: single numeraire USD.
See docs/pms/POSITION_VALUATION_SCHEMA_PLAN.md.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "s9t0u1v2w3x4"
down_revision: Union[str, Sequence[str], None] = "r8s9t0u1v2w3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "positions",
        sa.Column("usd_value", sa.Numeric(36, 18), nullable=True),
    )
    op.drop_column("positions", "unrealized_pnl")
    op.drop_column("positions", "notional")
    op.drop_column("positions", "mark_price")
    op.drop_column("positions", "entry_avg")


def downgrade() -> None:
    op.add_column(
        "positions",
        sa.Column("entry_avg", sa.Numeric(36, 18), nullable=True),
    )
    op.add_column(
        "positions",
        sa.Column("mark_price", sa.Numeric(36, 18), nullable=True),
    )
    op.add_column(
        "positions",
        sa.Column("notional", sa.Numeric(36, 18), nullable=True),
    )
    op.add_column(
        "positions",
        sa.Column("unrealized_pnl", sa.Numeric(36, 18), nullable=False, server_default="0"),
    )
    op.drop_column("positions", "usd_value")
