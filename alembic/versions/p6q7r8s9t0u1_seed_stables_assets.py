"""seed stables in assets table (refactor 5.1 stables-first)

Revision ID: p6q7r8s9t0u1
Revises: o5p6q7r8s9t0
Create Date: 2026-03-09

Inserts known stablecoins (USDT, USDC, BUSD, DAI, TUSD, USDP) with usd_price=1
for stables-first USD valuation. ON CONFLICT DO NOTHING so manual overrides are preserved.
"""
from typing import Sequence, Union

from alembic import op


revision: str = "p6q7r8s9t0u1"
down_revision: Union[str, Sequence[str], None] = "o5p6q7r8s9t0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

STABLES = ("USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP")


def upgrade() -> None:
    # Single INSERT; ON CONFLICT DO NOTHING preserves manual overrides
    values = ", ".join(f"('{a}', 1)" for a in STABLES)
    op.execute(
        f"""
        INSERT INTO assets (asset, usd_price)
        VALUES {values}
        ON CONFLICT (asset) DO NOTHING
        """
    )


def downgrade() -> None:
    in_list = ", ".join(f"'{a}'" for a in STABLES)
    op.execute(f"DELETE FROM assets WHERE asset IN ({in_list})")
