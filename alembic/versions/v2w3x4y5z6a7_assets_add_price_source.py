"""assets: add price_source (Option A, asset price feed)

Revision ID: v2w3x4y5z6a7
Revises: u1v2w3x4y5z6
Create Date: 2026-03-09

For audit: which source last updated usd_price (e.g. binance, manual, coingecko).
See docs/pms/ASSET_PRICE_FEED_PLAN.md §3.1.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "v2w3x4y5z6a7"
down_revision: Union[str, Sequence[str], None] = "u1v2w3x4y5z6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "assets",
        sa.Column("price_source", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("assets", "price_source")
