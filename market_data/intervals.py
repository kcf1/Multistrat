"""Binance-style interval strings → millisecond bar length (Phase 4 jobs)."""

from __future__ import annotations

_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}


def interval_to_millis(interval: str) -> int:
    """Return bar length in ms for a Binance ``interval`` string (e.g. ``1m``, ``1h``)."""
    key = interval.strip()
    if key == "1M":
        return 30 * 86_400_000
    if key not in _INTERVAL_MS:
        raise ValueError(f"Unsupported OHLCV interval: {interval!r}")
    return _INTERVAL_MS[key]
