from __future__ import annotations

from market_data.symbol_resolution import resolve_binance_usdt_spot_symbols_for_bases


class _Cur:
    def __init__(self, rows):
        self._rows = rows
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((sql, params))

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self, rows):
        self._rows = rows
        self.cur = _Cur(rows)

    def cursor(self):
        return self.cur


def test_resolve_binance_usdt_spot_symbols_for_bases_filters_and_maps() -> None:
    conn = _Conn([("BTC", "BTCUSDT"), ("ETH", "ETHUSDT"), ("BTC", "BTCUSDT")])
    out = resolve_binance_usdt_spot_symbols_for_bases(conn, base_assets=["btc", "eth"], quote_asset="usdt")
    assert out == {"BTC": "BTCUSDT", "ETH": "ETHUSDT"}

