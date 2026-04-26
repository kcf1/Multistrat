"""Add strategies_daily schema and Phase 1 double_sort_daily tables.

Revision ID: h3i4j5k6l7m8
Revises: f2a3b4c5d6e7
Create Date: 2026-04-26

Creates schema ``strategies_daily`` and tables ``l1feats_daily``, ``signals_daily``,
``factors_daily``, ``xsecs_daily``, ``labels_daily`` per docs/strategies/PHASE1_DETAILED_PLAN.md.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "h3i4j5k6l7m8"
down_revision: Union[str, Sequence[str], None] = "f2a3b4c5d6e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "strategies_daily"

_META = (
    sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
    sa.Column("pipeline_version", sa.Text(), nullable=False),
    sa.Column("source", sa.Text(), nullable=True),
)


def _precombined_columns() -> list:
    specs = [
        ("mom", (10, 20, 40)),
        ("trend", (5, 10, 20)),
        ("breakout", (10, 20, 40)),
        ("vwaprev", (5, 10, 20)),
        ("resrev", (5, 10, 20)),
        ("skew", (10, 20, 40)),
        ("vol", (10, 20, 40)),
        ("betasq", (10, 20, 40)),
        ("maxret", (10, 20, 40)),
        ("takerratio", (10, 20, 40)),
        ("vlm", (10, 20, 40)),
        ("quotevlm", (10, 20, 40)),
        ("retvlmcor", (10, 20, 40)),
    ]
    cols = []
    for abbr, ns in specs:
        for n in ns:
            cols.append(sa.Column(f"{abbr}_{n}", sa.Float(), nullable=True))
    return cols


def _label_horizon_columns() -> list:
    """Wide labels for horizons 1, 5, 10 (must match validators.DEFAULT_LABEL_HORIZONS)."""
    horizons = (1, 5, 10)
    cols = []
    for h in horizons:
        cols.extend(
            [
                sa.Column(f"logret_fwd_{h}", sa.Float(), nullable=True),
                sa.Column(f"simpret_fwd_{h}", sa.Float(), nullable=True),
                sa.Column(f"normret_fwd_{h}", sa.Float(), nullable=True),
                sa.Column(f"vol_weight_{h}", sa.Float(), nullable=True),
                sa.Column(f"vol_weighted_return_{h}", sa.Float(), nullable=True),
            ]
        )
    return cols


def upgrade() -> None:
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    op.create_table(
        "l1feats_daily",
        sa.Column("bar_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("quote_volume", sa.Float(), nullable=False),
        sa.Column("taker_buy_base_volume", sa.Float(), nullable=False),
        sa.Column("log_close", sa.Float(), nullable=False),
        sa.Column("log_return", sa.Float(), nullable=True),
        sa.Column("ewvol_20", sa.Float(), nullable=True),
        sa.Column("norm_return", sa.Float(), nullable=True),
        sa.Column("norm_close", sa.Float(), nullable=True),
        sa.Column("market_return", sa.Float(), nullable=True),
        sa.Column("market_ewvol_20", sa.Float(), nullable=True),
        sa.Column("beta_250", sa.Float(), nullable=True),
        sa.Column("resid_return", sa.Float(), nullable=True),
        sa.Column("log_volume", sa.Float(), nullable=True),
        sa.Column("log_quote_volume", sa.Float(), nullable=True),
        sa.Column("vwap_250", sa.Float(), nullable=True),
        *_META,
        sa.PrimaryKeyConstraint("bar_ts", "symbol"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_l1feats_daily_bar_ts", "l1feats_daily", ["bar_ts"], unique=False, schema=SCHEMA
    )
    op.create_index(
        "ix_l1feats_daily_symbol_bar_ts",
        "l1feats_daily",
        ["symbol", "bar_ts"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "signals_daily",
        sa.Column("bar_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        *_precombined_columns(),
        sa.Column("n_nonfinite", sa.Integer(), nullable=True),
        *_META,
        sa.PrimaryKeyConstraint("bar_ts", "symbol"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_signals_daily_bar_ts", "signals_daily", ["bar_ts"], unique=False, schema=SCHEMA
    )
    op.create_index(
        "ix_signals_daily_symbol_bar_ts",
        "signals_daily",
        ["symbol", "bar_ts"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "factors_daily",
        sa.Column("bar_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("mom_score", sa.Float(), nullable=True),
        sa.Column("trend_score", sa.Float(), nullable=True),
        sa.Column("breakout_score", sa.Float(), nullable=True),
        sa.Column("vwaprev_score", sa.Float(), nullable=True),
        sa.Column("resrev_score", sa.Float(), nullable=True),
        sa.Column("skew_score", sa.Float(), nullable=True),
        sa.Column("vol_score", sa.Float(), nullable=True),
        sa.Column("betasq_score", sa.Float(), nullable=True),
        sa.Column("maxret_score", sa.Float(), nullable=True),
        sa.Column("takerratio_score", sa.Float(), nullable=True),
        sa.Column("vlm_score", sa.Float(), nullable=True),
        sa.Column("quotevlm_score", sa.Float(), nullable=True),
        sa.Column("retvlmcor_score", sa.Float(), nullable=True),
        sa.Column("n_nonfinite", sa.Integer(), nullable=True),
        *_META,
        sa.PrimaryKeyConstraint("bar_ts", "symbol"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_factors_daily_bar_ts", "factors_daily", ["bar_ts"], unique=False, schema=SCHEMA
    )
    op.create_index(
        "ix_factors_daily_symbol_bar_ts",
        "factors_daily",
        ["symbol", "bar_ts"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "xsecs_daily",
        sa.Column("bar_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("mom_rank", sa.Float(), nullable=True),
        sa.Column("trend_rank", sa.Float(), nullable=True),
        sa.Column("breakout_rank", sa.Float(), nullable=True),
        sa.Column("vwaprev_rank", sa.Float(), nullable=True),
        sa.Column("resrev_rank", sa.Float(), nullable=True),
        sa.Column("skew_rank", sa.Float(), nullable=True),
        sa.Column("vol_rank", sa.Float(), nullable=True),
        sa.Column("betasq_rank", sa.Float(), nullable=True),
        sa.Column("maxret_rank", sa.Float(), nullable=True),
        sa.Column("takerratio_rank", sa.Float(), nullable=True),
        sa.Column("vlm_rank", sa.Float(), nullable=True),
        sa.Column("quotevlm_rank", sa.Float(), nullable=True),
        sa.Column("retvlmcor_rank", sa.Float(), nullable=True),
        sa.Column("n_symbols_xs", sa.Integer(), nullable=True),
        *_META,
        sa.PrimaryKeyConstraint("bar_ts", "symbol"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_xsecs_daily_bar_ts", "xsecs_daily", ["bar_ts"], unique=False, schema=SCHEMA
    )
    op.create_index(
        "ix_xsecs_daily_symbol_bar_ts",
        "xsecs_daily",
        ["symbol", "bar_ts"],
        unique=False,
        schema=SCHEMA,
    )

    op.create_table(
        "labels_daily",
        sa.Column("bar_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("label_asof_ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("bar_ts_venue", sa.Text(), nullable=True),
        *_label_horizon_columns(),
        *_META,
        sa.PrimaryKeyConstraint("bar_ts", "symbol"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_labels_daily_bar_ts", "labels_daily", ["bar_ts"], unique=False, schema=SCHEMA
    )
    op.create_index(
        "ix_labels_daily_symbol_bar_ts",
        "labels_daily",
        ["symbol", "bar_ts"],
        unique=False,
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("labels_daily", schema=SCHEMA)
    op.drop_table("xsecs_daily", schema=SCHEMA)
    op.drop_table("factors_daily", schema=SCHEMA)
    op.drop_table("signals_daily", schema=SCHEMA)
    op.drop_table("l1feats_daily", schema=SCHEMA)
    op.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
