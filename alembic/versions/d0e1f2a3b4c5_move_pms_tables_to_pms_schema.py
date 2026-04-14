"""Phase 2: move PMS tables from public to pms schema.

Revision ID: d0e1f2a3b4c5
Revises: c8d9e0f1a2b3
Create Date: 2026-04-14

Moves: assets, positions (no FK between them; order arbitrary).

See docs/POSTGRES_SCHEMA_GROUPING_PLAN.md §6.1 Phase 2.
"""

from typing import Sequence, Union

from alembic import op


revision: str = "d0e1f2a3b4c5"
down_revision: Union[str, Sequence[str], None] = "c8d9e0f1a2b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE public.assets SET SCHEMA pms")
    op.execute("ALTER TABLE public.positions SET SCHEMA pms")


def downgrade() -> None:
    op.execute("ALTER TABLE pms.positions SET SCHEMA public")
    op.execute("ALTER TABLE pms.assets SET SCHEMA public")
