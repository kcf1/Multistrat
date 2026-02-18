"""drop accounts.env column

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-02-19

Removes env from accounts; name remains (derived broker:account_id or user-set later).
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("accounts", "env")


def downgrade() -> None:
    op.add_column("accounts", sa.Column("env", sa.Text(), nullable=True))
