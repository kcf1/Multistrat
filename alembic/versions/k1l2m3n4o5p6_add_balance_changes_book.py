"""add balance_changes.book (12.2.11)

Revision ID: k1l2m3n4o5p6
Revises: j0k1l2m3n4o5
Create Date: 2026-02-26

Adds column book (TEXT, default 'default') to balance_changes for build-from-order cash:
broker-fed rows use default cash book; book change records move cash to strategy books.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "k1l2m3n4o5p6"
down_revision: Union[str, Sequence[str], None] = "j0k1l2m3n4o5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "balance_changes",
        sa.Column("book", sa.Text(), nullable=False, server_default="default"),
    )


def downgrade() -> None:
    op.drop_column("balance_changes", "book")
