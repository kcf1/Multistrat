"""
PMS integration loop (12.3.8): read → derive → calculate → write.

Skips Redis; writes granular positions table only.
"""

import logging
from typing import Any, Callable, List, Optional, Union

from pms.granular_store import write_pms_positions
from pms.mark_price import MarkPriceProvider
from pms.reads import derive_positions_from_orders, query_orders_for_positions
from pms.schemas_pydantic import DerivedPosition

logger = logging.getLogger(__name__)


def enrich_positions_with_mark_prices(
    positions: List[DerivedPosition],
    mark_provider: MarkPriceProvider,
) -> List[DerivedPosition]:
    """
    Fetch mark prices for position symbols and set mark_price, notional, unrealized_pnl.

    When open_qty != 0: notional = open_qty * mark_price, unrealized_pnl = (mark_price - entry_avg) * open_qty.
    Flattened positions (open_qty == 0) keep unrealized_pnl 0.
    """
    if not positions:
        return []
    symbols = list({p.symbol for p in positions if p.symbol})
    result = mark_provider.get_mark_prices(symbols)
    prices = result.prices
    out: List[DerivedPosition] = []
    for p in positions:
        mark = prices.get(p.symbol) if prices else None
        mark_f = float(mark) if mark is not None else None
        notional: Optional[float] = None
        unrealized: float = 0.0
        if mark_f is not None and abs(p.open_qty) > 1e-15:
            notional = p.open_qty * mark_f
            if p.entry_avg is not None:
                unrealized = (mark_f - p.entry_avg) * p.open_qty
        out.append(
            p.model_copy(
                update={
                    "mark_price": mark_f,
                    "notional": notional,
                    "unrealized_pnl": unrealized,
                }
            )
        )
    return out


def run_one_tick(
    pg_connect: Union[str, Callable[[], Any]],
    mark_provider: MarkPriceProvider,
) -> int:
    """
    One loop iteration: read orders → derive positions → enrich with mark prices → write.

    Returns number of positions written to the granular table. Skips Redis.
    """
    order_rows = query_orders_for_positions(pg_connect)
    positions = derive_positions_from_orders(order_rows)
    positions = enrich_positions_with_mark_prices(positions, mark_provider)
    return write_pms_positions(pg_connect, positions)


def run_pms_loop(
    pg_connect: Union[str, Callable[[], Any]],
    mark_provider: MarkPriceProvider,
    tick_interval_seconds: float = 10.0,
    *,
    stop_event: Optional[Any] = None,
) -> None:
    """
    Run the PMS loop: every tick_interval_seconds, run_one_tick (read → derive → write).

    Skips Redis. Runs until stop_event is set (if provided) or forever.
    stop_event: optional object with .is_set() -> bool (e.g. threading.Event).
    """
    import time
    while True:
        try:
            n = run_one_tick(pg_connect, mark_provider)
            logger.debug("PMS tick wrote %s positions", n)
        except Exception as e:
            logger.exception("PMS tick failed: %s", e)
        if stop_event is not None and stop_event.is_set():
            break
        time.sleep(tick_interval_seconds)
