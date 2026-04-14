"""Phase 3: move market_data tables from public to market_data schema.

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2026-04-14

Moves: ohlcv, ingestion_cursor, basis_rate, basis_cursor, open_interest,
open_interest_cursor, taker_buy_sell_volume, taker_buy_sell_volume_cursor,
top_trader_long_short, top_trader_long_short_cursor (no cross-table FKs).

See docs/POSTGRES_SCHEMA_GROUPING_PLAN.md §6.1 Phase 3.
"""

from typing import Sequence, Union

from alembic import op


revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, Sequence[str], None] = "d0e1f2a3b4c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for table in (
        "basis_cursor",
        "basis_rate",
        "ingestion_cursor",
        "ohlcv",
        "open_interest",
        "open_interest_cursor",
        "taker_buy_sell_volume",
        "taker_buy_sell_volume_cursor",
        "top_trader_long_short",
        "top_trader_long_short_cursor",
    ):
        op.execute(f"ALTER TABLE public.{table} SET SCHEMA market_data")


def downgrade() -> None:
    for table in (
        "top_trader_long_short_cursor",
        "top_trader_long_short",
        "taker_buy_sell_volume_cursor",
        "taker_buy_sell_volume",
        "open_interest_cursor",
        "open_interest",
        "ohlcv",
        "ingestion_cursor",
        "basis_rate",
        "basis_cursor",
    ):
        op.execute(f"ALTER TABLE market_data.{table} SET SCHEMA public")
