"""Phase 1: move OMS tables from public to oms schema.

Revision ID: c8d9e0f1a2b3
Revises: a7b8c9d0e1f2
Create Date: 2026-04-14

Moves: orders, symbols, accounts, balances, balance_changes (FK balances -> accounts
remains valid across schemas; both end in oms).

See docs/POSTGRES_SCHEMA_GROUPING_PLAN.md §6.1 Phase 1.
"""

from typing import Sequence, Union

from alembic import op


revision: str = "c8d9e0f1a2b3"
down_revision: Union[str, Sequence[str], None] = "a7b8c9d0e1f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # No FK from orders/symbols to accounts; balances references accounts.id — move accounts before balances.
    op.execute("ALTER TABLE public.orders SET SCHEMA oms")
    op.execute("ALTER TABLE public.symbols SET SCHEMA oms")
    op.execute("ALTER TABLE public.accounts SET SCHEMA oms")
    op.execute("ALTER TABLE public.balances SET SCHEMA oms")
    op.execute("ALTER TABLE public.balance_changes SET SCHEMA oms")


def downgrade() -> None:
    op.execute("ALTER TABLE oms.balance_changes SET SCHEMA public")
    op.execute("ALTER TABLE oms.balances SET SCHEMA public")
    op.execute("ALTER TABLE oms.accounts SET SCHEMA public")
    op.execute("ALTER TABLE oms.orders SET SCHEMA public")
    op.execute("ALTER TABLE oms.symbols SET SCHEMA public")
