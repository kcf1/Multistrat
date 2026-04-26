"""Add market_data.dataset_status table (dataset readiness marker).

Revision ID: s1t2u3v4w5x6
Revises: e1f2a3b4c5d6
Create Date: 2026-04-27

Stores one aggregated status row per (dataset, period) updated after a successful
dataset job cycle. Intended for downstream readiness gates (e.g. strategies).
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "s1t2u3v4w5x6"
down_revision: Union[str, Sequence[str], None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "market_data"


def upgrade() -> None:
    op.create_table(
        "dataset_status",
        sa.Column("dataset", sa.Text(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("last_complete_time", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'ok'")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.PrimaryKeyConstraint("dataset", "period", name="pk_dataset_status"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_dataset_status_dataset",
        "dataset_status",
        ["dataset"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index("ix_dataset_status_dataset", table_name="dataset_status", schema=SCHEMA)
    op.drop_table("dataset_status", schema=SCHEMA)

