"""add assets table (refactor 3.2)

Revision ID: n4o5p6q7r8s9
Revises: m3n4o5p6q7r8
Create Date: 2026-03-09

Creates assets table for per-asset USD valuation (usd_symbol, usd_price).
Population: derive from symbols table + known stables; see docs/pms/REFACTORING_PLAN_POSITIONS_AS_ASSETS.md §3.2.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "n4o5p6q7r8s9"
down_revision: Union[str, Sequence[str], None] = "m3n4o5p6q7r8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "assets",
        sa.Column("asset", sa.Text(), nullable=False),
        sa.Column("usd_symbol", sa.Text(), nullable=True),
        sa.Column("usd_price", sa.Numeric(36, 18), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("asset"),
    )
    op.create_index("ix_assets_usd_symbol", "assets", ["usd_symbol"])


def downgrade() -> None:
    op.drop_index("ix_assets_usd_symbol", table_name="assets")
    op.drop_table("assets")
