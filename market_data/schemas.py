"""
OHLCV row model and Binance spot kline array parser (Phase 4 §9.2).

Binance GET /api/v3/klines returns an array of arrays:
0 open time (ms), 1-4 OHLC strings, 5 volume, 6 close time (ms), 7 quote volume,
8 trades, 9 taker buy base, 10 taker buy quote, 11 ignore.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, List, Mapping, Union

from pydantic import BaseModel, Field, field_validator, model_validator


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

    @model_validator(mode="after")
    def ohlcv_sanity(self) -> OhlcvBar:
        if self.high < self.low:
            raise ValueError("high must be >= low")
        if self.high < self.open or self.high < self.close:
            raise ValueError("high must be >= open and close")
        if self.low > self.open or self.low > self.close:
            raise ValueError("low must be <= open and close")
        if self.volume < 0:
            raise ValueError("volume must be >= 0")
        if self.quote_volume is not None and self.quote_volume < 0:
            raise ValueError("quote_volume must be >= 0 when set")
        if self.trades is not None and self.trades < 0:
            raise ValueError("trades must be >= 0 when set")
        if self.close_time is not None and self.close_time < self.open_time:
            raise ValueError("close_time must be >= open_time when set")
        return self


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

    for idx in (1, 2, 3, 4, 5):
        if idx >= len(row):
            raise ValueError(f"Binance kline row missing field at index {idx}")
        cell = row[idx]
        if cell is None or (isinstance(cell, str) and not str(cell).strip()):
            raise ValueError(f"missing or empty kline field at index {idx}")

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


class BasisPoint(BaseModel):
    """One persisted basis row (matches ``basis_rate`` table, excluding ingested_at)."""

    model_config = {"frozen": True}

    pair: str = Field(..., min_length=1)
    contract_type: str = Field(..., min_length=1)
    period: str = Field(..., min_length=1)
    sample_time: datetime
    basis: Decimal
    basis_rate: Decimal
    annualized_basis_rate: Decimal
    futures_price: Decimal
    index_price: Decimal

    @field_validator("pair")
    @classmethod
    def upper_pair(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("contract_type")
    @classmethod
    def normalize_contract_type(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("period")
    @classmethod
    def strip_period(cls, v: str) -> str:
        return v.strip()

    @model_validator(mode="after")
    def basis_sanity(self) -> BasisPoint:
        if self.futures_price <= 0:
            raise ValueError("futures_price must be > 0")
        if self.index_price <= 0:
            raise ValueError("index_price must be > 0")
        return self


def parse_binance_basis_row(
    row: Mapping[str, Any],
    *,
    pair: str | None = None,
    contract_type: str | None = None,
    period: str | None = None,
) -> BasisPoint:
    """Parse one Binance `/futures/data/basis` object into ``BasisPoint``."""
    if not isinstance(row, Mapping):
        raise ValueError("Binance basis row must be an object")

    def _get_required(key: str) -> Any:
        v = row.get(key)
        if v is None or (isinstance(v, str) and not v.strip()):
            raise ValueError(f"missing or empty basis field '{key}'")
        return v

    def _dec(key: str) -> Decimal:
        try:
            return Decimal(str(_get_required(key)))
        except (InvalidOperation, TypeError) as e:
            raise ValueError(f"Invalid decimal for '{key}': {row.get(key)!r}") from e

    ts_raw = _get_required("timestamp")
    try:
        ts_ms = int(ts_raw)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid timestamp: {ts_raw!r}") from e

    resolved_pair = (pair or str(_get_required("pair"))).strip().upper()
    resolved_contract = (
        contract_type or str(_get_required("contractType"))
    ).strip().upper()
    resolved_period = (period or str(_get_required("period"))).strip()

    return BasisPoint(
        pair=resolved_pair,
        contract_type=resolved_contract,
        period=resolved_period,
        sample_time=_ms_to_utc_aware(ts_ms),
        basis=_dec("basis"),
        basis_rate=_dec("basisRate"),
        annualized_basis_rate=_dec("annualizedBasisRate"),
        futures_price=_dec("futuresPrice"),
        index_price=_dec("indexPrice"),
    )


def parse_binance_basis_rows(
    rows: list[Mapping[str, Any]],
    *,
    pair: str | None = None,
    contract_type: str | None = None,
    period: str | None = None,
) -> list[BasisPoint]:
    """Parse a list of basis objects (e.g. full API response)."""
    return [
        parse_binance_basis_row(
            r,
            pair=pair,
            contract_type=contract_type,
            period=period,
        )
        for r in rows
    ]
