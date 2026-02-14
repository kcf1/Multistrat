"""add orders table for OMS sync (12.1.10)

Revision ID: a1b2c3d4e5f6
Revises: 7351930e8f1d
Create Date: 2026-02-15

Orders table: OMS syncs from Redis for audit/recovery; UPSERT by internal_id.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "7351930e8f1d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column("internal_id", sa.Text(), primary_key=True),
        sa.Column("broker", sa.Text(), nullable=False, server_default=""),
        sa.Column("account_id", sa.Text(), nullable=False, server_default=""),
        sa.Column("broker_order_id", sa.Text(), nullable=False, server_default=""),
        sa.Column("symbol", sa.Text(), nullable=False, server_default=""),
        sa.Column("side", sa.Text(), nullable=False, server_default=""),
        sa.Column("order_type", sa.Text(), nullable=False, server_default=""),
        sa.Column("quantity", sa.Numeric(36, 18), nullable=True),
        sa.Column("price", sa.Numeric(36, 18), nullable=True),
        sa.Column("time_in_force", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("executed_qty", sa.Numeric(36, 18), nullable=True),
        sa.Column("book", sa.Text(), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("binance_cumulative_quote_qty", sa.Numeric(36, 18), nullable=True),
        sa.Column("binance_transact_time", sa.BigInteger(), nullable=True),
        sa.Column("payload", sa.dialects.postgresql.JSONB(), nullable=True),
    )
    op.create_index("ix_orders_broker_order_id", "orders", ["broker_order_id"])
    op.create_index("ix_orders_status", "orders", ["status"])
    op.create_index("ix_orders_created_at", "orders", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_orders_created_at", table_name="orders")
    op.drop_index("ix_orders_status", table_name="orders")
    op.drop_index("ix_orders_broker_order_id", table_name="orders")
    op.drop_table("orders")
