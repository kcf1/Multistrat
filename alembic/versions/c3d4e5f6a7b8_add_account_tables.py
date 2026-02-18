"""add account tables for OMS account sync (12.2.6)

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-02-19

Tables: accounts, balances, positions, balance_changes, margin_snapshots.
Schema per PHASE2_DETAILED_PLAN §3.1 and §3.3.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ----- accounts ----------------------------------------------------------
    # One row per broker account. Maps to Redis account:{broker}:{account_id}.
    # account_id identifies the broker's account; unique (broker, account_id) for sync UPSERT.
    op.create_table(
        "accounts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("broker", sa.Text(), nullable=False),
        sa.Column("env", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("config", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_accounts_broker", "accounts", ["broker"])
    op.create_index(
        "uq_accounts_broker_account_id",
        "accounts",
        ["broker", "account_id"],
        unique=True,
    )

    # ----- balances ----------------------------------------------------------
    # Current state per asset. OMS account sync from Redis balances hash.
    op.create_table(
        "balances",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("asset", sa.Text(), nullable=False),
        sa.Column("available", sa.Numeric(36, 18), nullable=False, server_default="0"),
        sa.Column("locked", sa.Numeric(36, 18), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_balances_account_id", "balances", ["account_id"])
    op.create_index(
        "uq_balances_account_id_asset",
        "balances",
        ["account_id", "asset"],
        unique=True,
    )

    # ----- positions ----------------------------------------------------------
    # One row per (account, symbol, side) for futures; OMS sync from Redis positions.
    op.create_table(
        "positions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("side", sa.Text(), nullable=False),
        sa.Column("quantity", sa.Numeric(36, 18), nullable=False, server_default="0"),
        sa.Column("entry_price_avg", sa.Numeric(36, 18), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_positions_account_id", "positions", ["account_id"])
    op.create_index(
        "uq_positions_account_id_symbol_side",
        "positions",
        ["account_id", "symbol", "side"],
        unique=True,
    )

    # ----- balance_changes ----------------------------------------------------
    # Historical deposits, withdrawals, transfers; §3.3.
    op.create_table(
        "balance_changes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("asset", sa.Text(), nullable=False),
        sa.Column("change_type", sa.Text(), nullable=False),
        sa.Column("delta", sa.Numeric(36, 18), nullable=False),
        sa.Column("balance_before", sa.Numeric(36, 18), nullable=True),
        sa.Column("balance_after", sa.Numeric(36, 18), nullable=True),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("broker_event_id", sa.Text(), nullable=True),
        sa.Column("event_time", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("payload", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_balance_changes_account_id_asset_event_time",
        "balance_changes",
        ["account_id", "asset", "event_time"],
    )
    op.create_index(
        "ix_balance_changes_account_id_change_type",
        "balance_changes",
        ["account_id", "change_type"],
    )
    op.create_index("ix_balance_changes_event_time", "balance_changes", ["event_time"])

    # ----- margin_snapshots (optional; useful for futures) --------------------
    op.create_table(
        "margin_snapshots",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("account_id", sa.BigInteger(), nullable=False),
        sa.Column("total_margin", sa.Numeric(36, 18), nullable=True),
        sa.Column("available_balance", sa.Numeric(36, 18), nullable=True),
        sa.Column("timestamp", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["account_id"], ["accounts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_margin_snapshots_account_id", "margin_snapshots", ["account_id"])
    op.create_index("ix_margin_snapshots_timestamp", "margin_snapshots", ["timestamp"])


def downgrade() -> None:
    op.drop_index("ix_margin_snapshots_timestamp", table_name="margin_snapshots")
    op.drop_index("ix_margin_snapshots_account_id", table_name="margin_snapshots")
    op.drop_table("margin_snapshots")

    op.drop_index("ix_balance_changes_event_time", table_name="balance_changes")
    op.drop_index("ix_balance_changes_account_id_change_type", table_name="balance_changes")
    op.drop_index(
        "ix_balance_changes_account_id_asset_event_time",
        table_name="balance_changes",
    )
    op.drop_table("balance_changes")

    op.drop_index("uq_positions_account_id_symbol_side", table_name="positions")
    op.drop_index("ix_positions_account_id", table_name="positions")
    op.drop_table("positions")

    op.drop_index("uq_balances_account_id_asset", table_name="balances")
    op.drop_index("ix_balances_account_id", table_name="balances")
    op.drop_table("balances")

    op.drop_index("uq_accounts_broker_account_id", table_name="accounts")
    op.drop_index("ix_accounts_broker", table_name="accounts")
    op.drop_table("accounts")
