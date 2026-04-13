"""add ohlcv taker buy volumes

Revision ID: w6x7y8z9a0b1
Revises: n5o6p7q8r9s0
Create Date: 2026-04-13
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "w6x7y8z9a0b1"
down_revision: Union[str, Sequence[str], None] = "n5o6p7q8r9s0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("ohlcv", sa.Column("taker_buy_base_volume", sa.Numeric(36, 18), nullable=True))
    op.add_column("ohlcv", sa.Column("taker_buy_quote_volume", sa.Numeric(36, 18), nullable=True))


def downgrade() -> None:
    op.drop_column("ohlcv", "taker_buy_quote_volume")
    op.drop_column("ohlcv", "taker_buy_base_volume")
