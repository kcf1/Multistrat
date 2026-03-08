"""balance_changes account_id to broker id (TEXT, same as orders)

Revision ID: o5p6q7r8s9t0
Revises: n4o5p6q7r8s9
Create Date: 2026-03-09

Change balance_changes.account_id from BIGINT (FK to accounts.id) to TEXT
(broker account id, same as orders.account_id). Enables aggregating by
(account_id, book, asset) without joining to accounts. Existing rows
backfilled from accounts.account_id where accounts.id = balance_changes.account_id.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "o5p6q7r8s9t0"
down_revision: Union[str, Sequence[str], None] = "n4o5p6q7r8s9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop FK so we can change the column
    op.drop_constraint(
        "balance_changes_account_id_fkey",
        "balance_changes",
        type_="foreignkey",
    )
    # Add new TEXT column and backfill from accounts
    op.add_column(
        "balance_changes",
        sa.Column("account_id_text", sa.Text(), nullable=True),
    )
    op.execute(
        """
        UPDATE balance_changes bc
        SET account_id_text = a.account_id
        FROM accounts a
        WHERE a.id = bc.account_id
        """
    )
    # Delete orphans (no matching account) then drop old column and rename
    op.execute(
        "DELETE FROM balance_changes WHERE account_id_text IS NULL"
    )
    op.drop_column("balance_changes", "account_id")
    op.alter_column(
        "balance_changes",
        "account_id_text",
        new_column_name="account_id",
        nullable=False,
    )


def downgrade() -> None:
    # Add BIGINT column and backfill from accounts
    op.add_column(
        "balance_changes",
        sa.Column("account_id_bigint", sa.BigInteger(), nullable=True),
    )
    op.execute(
        """
        UPDATE balance_changes bc
        SET account_id_bigint = a.id
        FROM accounts a
        WHERE a.account_id = bc.account_id
        """
    )
    op.execute("DELETE FROM balance_changes WHERE account_id_bigint IS NULL")
    op.drop_column("balance_changes", "account_id")
    op.alter_column(
        "balance_changes",
        "account_id_bigint",
        new_column_name="account_id",
        nullable=False,
        existing_nullable=True,
    )
    op.create_foreign_key(
        "balance_changes_account_id_fkey",
        "balance_changes",
        "accounts",
        ["account_id"],
        ["id"],
        ondelete="CASCADE",
    )
