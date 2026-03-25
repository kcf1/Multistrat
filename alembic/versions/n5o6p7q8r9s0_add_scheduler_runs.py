"""add scheduler_runs table (Phase 5 §4.4)

Revision ID: n5o6p7q8r9s0
Revises: m5n6b7v8c9d0
Create Date: 2026-03-26

Audit trail for batch scheduler job invocations: start/end, status, optional payload JSON.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "n5o6p7q8r9s0"
down_revision: Union[str, Sequence[str], None] = "m5n6b7v8c9d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scheduler_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("job_id", sa.Text(), nullable=False),
        sa.Column(
            "started_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("payload", sa.dialects.postgresql.JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_scheduler_runs"),
        sa.CheckConstraint(
            "(finished_at IS NULL AND status IS NULL) OR "
            "(finished_at IS NOT NULL AND status IN ('ok', 'error'))",
            name="ck_scheduler_runs_status_finished",
        ),
    )
    op.create_index(
        "ix_scheduler_runs_job_id_started_at",
        "scheduler_runs",
        ["job_id", "started_at"],
        postgresql_ops={"started_at": "DESC"},
    )


def downgrade() -> None:
    op.drop_index("ix_scheduler_runs_job_id_started_at", table_name="scheduler_runs")
    op.drop_table("scheduler_runs")
