"""
PMS integration loop (12.3.8): read → derive → calculate → write.

Skips Redis; writes granular positions table only.
Uses asset-grain derivation (orders + balance_changes) and stables-first USD from assets table.
"""

from typing import Any, Callable, List, Optional, Union

from pms.granular_store import write_pms_positions
from pms.log import logger
from pms.mark_price import MarkPriceProvider
from pms.reads import (
    derive_positions_from_orders_and_balance_changes,
    query_assets_usd_config,
)
from pms.schemas_pydantic import DerivedPosition


def enrich_positions_with_usd_from_assets(
    pg_connect: Union[str, Callable[[], Any]],
    positions: List[DerivedPosition],
) -> List[DerivedPosition]:
    """
    Set mark_price, notional, unrealized_pnl for positions whose asset has usd_price in assets table (stables-first).
    Assets with only usd_symbol (non-stables) are left with mark_price/notional None; follow-up can use mark provider.
    """
    if not positions:
        return []
    config = query_assets_usd_config(pg_connect)
    out: List[DerivedPosition] = []
    for p in positions:
        asset = (p.asset or "").strip()
        cfg = config.get(asset) if asset else None
        usd_price = cfg.get("usd_price") if cfg else None
        mark_f: Optional[float] = None
        notional: Optional[float] = None
        unrealized: float = 0.0
        if usd_price is not None and abs(p.open_qty) > 1e-15:
            try:
                mark_f = float(usd_price)
                notional = p.open_qty * mark_f
            except (TypeError, ValueError):
                pass
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
    symbols = list({p.asset for p in positions if p.asset})
    result = mark_provider.get_mark_prices(symbols)
    prices = result.prices
    out: List[DerivedPosition] = []
    for p in positions:
        mark = prices.get(p.asset) if prices else None
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
    One loop iteration: derive positions (orders + balance_changes) → enrich with USD from assets (stables) → write.

    Returns number of positions written to the granular table. Skips Redis.
    Mark provider is reserved for future non-stable usd_symbol fetch; stables use assets.usd_price only.
    """
    positions = derive_positions_from_orders_and_balance_changes(pg_connect)
    positions = enrich_positions_with_usd_from_assets(pg_connect, positions)
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
            logger.info("PMS tick wrote {} positions", n)
        except Exception as e:
            logger.exception("PMS tick failed: {}", e)
        if stop_event is not None and stop_event.is_set():
            break
        time.sleep(tick_interval_seconds)
