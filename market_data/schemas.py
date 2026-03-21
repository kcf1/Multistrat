"""
OHLCV row model and Binance spot kline array parser (Phase 4 §9.2).

Binance GET /api/v3/klines returns an array of arrays:
0 open time (ms), 1-4 OHLC strings, 5 volume, 6 close time (ms), 7 quote volume,
8 trades, 9 taker buy base, 10 taker buy quote, 11 ignore.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, List, Union

from pydantic import BaseModel, Field, field_validator


def _ms_to_utc_aware(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


class OhlcvBar(BaseModel):
    """One persisted OHLCV row (matches ``ohlcv`` table, excluding ingested_at)."""

    model_config = {"frozen": True}

    symbol: str = Field(..., min_length=1)
    interval: str = Field(..., min_length=1)
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None
    trades: int | None = None
    close_time: datetime | None = None

    @field_validator("symbol")
    @classmethod
    def upper_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("interval")
    @classmethod
    def strip_interval(cls, v: str) -> str:
        return v.strip()


def parse_binance_kline(
    row: List[Union[str, int, float]],
    *,
    symbol: str,
    interval: str,
) -> OhlcvBar:
    """
    Parse one Binance kline array into ``OhlcvBar``.

    Requires at least indices **0–6** (through close time ms). Quote volume / trades
    (7–8) and taker fields (9–10) are optional when absent.
    """
    if len(row) < 7:
        raise ValueError(f"Binance kline row too short: need >= 7 elements, got {len(row)}")

    try:
        open_ms = int(row[0])
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid open time: {row[0]!r}") from e

    def _dec(i: int) -> Decimal:
        try:
            return Decimal(str(row[i]))
        except (InvalidOperation, TypeError) as e:
            raise ValueError(f"Invalid decimal at index {i}: {row[i]!r}") from e

    open_time = _ms_to_utc_aware(open_ms)
    close_time: datetime | None = None
    try:
        close_ms = int(row[6])
        close_time = _ms_to_utc_aware(close_ms)
    except (TypeError, ValueError, IndexError):
        pass

    trades: int | None = None
    if len(row) > 8 and row[8] is not None and row[8] != "":
        try:
            trades = int(row[8])
        except (TypeError, ValueError):
            trades = None

    quote_vol: Decimal | None = None
    if len(row) > 7 and row[7] is not None and str(row[7]).strip() != "":
        try:
            quote_vol = _dec(7)
        except ValueError:
            quote_vol = None

    return OhlcvBar(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        open=_dec(1),
        high=_dec(2),
        low=_dec(3),
        close=_dec(4),
        volume=_dec(5),
        quote_volume=quote_vol,
        trades=trades,
        close_time=close_time,
    )


def parse_binance_klines(
    rows: list[list[Any]],
    *,
    symbol: str,
    interval: str,
) -> list[OhlcvBar]:
    """Parse a list of kline arrays (e.g. full API response)."""
    return [parse_binance_kline(r, symbol=symbol, interval=interval) for r in rows]
