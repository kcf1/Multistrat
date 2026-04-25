#!/usr/bin/env python3
"""
Read-only checks: universe resolution, per-dataset initial-backfill cursor flags,
and light data freshness (max timestamps).

The base connection string is **MARKET_DATA_DATABASE_URL** or **DATABASE_URL** from
the repo root ``.env`` (same as other apps). Use ``--pg-host-port HOST:PORT`` to
point at another Postgres instance (e.g. prod) while **keeping user, password,
and database path** from that URL unchanged.

Usage:
  python scripts/check_market_data_prod_health.py
  python scripts/check_market_data_prod_health.py --pg-host-port 203.0.113.7:5432
  python scripts/check_market_data_prod_health.py --pg-host-port db.internal:5432 --stale-hours 48 --verbose
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse, urlunparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import psycopg2
from dotenv import load_dotenv
from psycopg2 import errors as pg_errors

from pgconn import configure_for_market_data
from market_data.config import (
    BASIS_CONTRACT_TYPES,
    BASIS_PERIODS,
    OHLCV_INTERVALS,
    OPEN_INTEREST_CONTRACT_TYPES,
    OPEN_INTEREST_PERIODS,
    TAKER_BUYSELL_VOLUME_PERIODS,
    TOP_TRADER_LONG_SHORT_PERIODS,
)
from market_data.storage import (
    query_basis_pairs_initial_backfill_complete,
    query_ohlcv_symbols_initial_backfill_complete,
    query_open_interest_symbols_initial_backfill_complete,
    query_taker_buy_sell_volume_symbols_initial_backfill_complete,
    query_top_trader_long_short_symbols_initial_backfill_complete,
    query_universe_base_assets,
)
from market_data.symbol_resolution import resolve_binance_usdt_spot_symbols_for_bases
from market_data.universe_runtime import resolve_runtime_symbols


def _base_database_url_from_env() -> str:
    load_dotenv(REPO_ROOT / ".env", override=True)
    url = (os.environ.get("MARKET_DATA_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if not url:
        print(
            "ERROR: Set MARKET_DATA_DATABASE_URL or DATABASE_URL in .env.",
            file=sys.stderr,
        )
        raise SystemExit(2)
    return url


def _apply_pg_host_port(base_url: str, host_port: str) -> str:
    """
    Replace only ``host:port`` in the URL netloc; preserve ``user:password@`` if present.

    ``host_port`` must be ``hostname:port`` or ``dotted.ip:port`` (not a full URL).
    """
    hp = host_port.strip()
    if not hp:
        return base_url
    if "@" in hp or "://" in hp:
        print(
            "ERROR: --pg-host-port must be HOST:PORT only (credentials stay from DATABASE_URL).",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if ":" not in hp:
        print("ERROR: --pg-host-port must include a port, e.g. db.example.com:5432", file=sys.stderr)
        raise SystemExit(2)
    parsed = urlparse(base_url)
    if not parsed.scheme or not parsed.netloc:
        print("ERROR: invalid base database URL in .env.", file=sys.stderr)
        raise SystemExit(2)
    if "@" in parsed.netloc:
        userinfo, _old_host = parsed.netloc.rsplit("@", 1)
        new_netloc = f"{userinfo}@{hp}"
    else:
        new_netloc = hp
    return urlunparse(
        (parsed.scheme, new_netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
    )


def _resolve_database_url(*, pg_host_port: str | None) -> str:
    base = _base_database_url_from_env()
    if not pg_host_port:
        return base
    return _apply_pg_host_port(base, pg_host_port)


def _has_column(conn, *, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = %s
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None


@dataclass
class HealthReport:
    mode: str
    universe_base_count: int = 0
    resolved_symbol_count: int = 0
    bases_without_symbol: list[str] = field(default_factory=list)
    ohlcv_incomplete: list[str] = field(default_factory=list)
    basis_incomplete: list[str] = field(default_factory=list)
    open_interest_incomplete: list[str] = field(default_factory=list)
    taker_incomplete: list[str] = field(default_factory=list)
    top_trader_incomplete: list[str] = field(default_factory=list)
    stale_ohlcv: list[tuple[str, str, datetime | None]] = field(default_factory=list)
    stale_basis: list[tuple[str, datetime | None]] = field(default_factory=list)
    stale_open_interest: list[tuple[str, datetime | None]] = field(default_factory=list)
    stale_taker: list[tuple[str, datetime | None]] = field(default_factory=list)
    stale_top_trader: list[tuple[str, datetime | None]] = field(default_factory=list)
    flag_anomalies: list[str] = field(default_factory=list)

    def has_errors(self) -> bool:
        return bool(
            self.bases_without_symbol
            or self.ohlcv_incomplete
            or self.basis_incomplete
            or self.open_interest_incomplete
            or self.taker_incomplete
            or self.top_trader_incomplete
        )

    def has_warnings(self) -> bool:
        return bool(
            self.stale_ohlcv
            or self.stale_basis
            or self.stale_open_interest
            or self.stale_taker
            or self.stale_top_trader
            or self.flag_anomalies
        )


def _check_flag_anomalies(conn, symbols: list[str], intervals: tuple[str, ...]) -> list[str]:
    """OHLCV: done=false but we already have bars (suggests stuck backfill)."""
    if not symbols or not intervals:
        return []
    msgs: list[str] = []
    ivs = list(intervals)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.symbol, c.interval, c.initial_backfill_done, o.mx
            FROM ingestion_cursor c
            LEFT JOIN LATERAL (
                SELECT MAX(open_time) AS mx FROM ohlcv o
                WHERE o.symbol = c.symbol AND o.interval = c.interval
            ) o ON true
            WHERE c.symbol = ANY(%s)
              AND c.interval = ANY(%s)
              AND c.initial_backfill_done = false
              AND o.mx IS NOT NULL
            """,
            (symbols, ivs),
        )
        for sym, iv, done, mx in cur.fetchall():
            msgs.append(
                f"ohlcv {sym} {iv}: initial_backfill_done={done} but max(open_time)={mx}"
            )
    return msgs


def _stale_cutoff(hours: float) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def _check_ohlcv_stale(
    conn, symbols: list[str], intervals: tuple[str, ...], cutoff: datetime
) -> list[tuple[str, str, datetime | None]]:
    out: list[tuple[str, str, datetime | None]] = []
    if not symbols:
        return out
    with conn.cursor() as cur:
        for iv in intervals:
            cur.execute(
                """
                SELECT symbol, MAX(open_time) AS mx
                FROM ohlcv
                WHERE symbol = ANY(%s) AND interval = %s
                GROUP BY symbol
                """,
                (symbols, iv),
            )
            mx_by_sym = {str(r[0]).upper(): r[1] for r in cur.fetchall()}
            for sym in symbols:
                mx = mx_by_sym.get(sym)
                if mx is None or mx < cutoff:
                    out.append((sym, iv, mx))
    return out


def _check_sample_stale(
    conn,
    *,
    symbols: list[str],
    table: str,
    cutoff: datetime,
    symbol_column: str,
) -> list[tuple[str, datetime | None]]:
    """Single max(sample_time) per symbol (any period/contract in table)."""
    out: list[tuple[str, datetime | None]] = []
    if not symbols:
        return out
    col = symbol_column
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {col}, MAX(sample_time) AS mx
            FROM {table}
            WHERE {col} = ANY(%s)
            GROUP BY {col}
            """,
            (symbols,),
        )
        mx_by = {str(r[0]).upper(): r[1] for r in cur.fetchall()}
        for sym in symbols:
            mx = mx_by.get(sym)
            if mx is None or mx < cutoff:
                out.append((sym, mx))
    return out


def run_checks(
    database_url: str,
    *,
    mode: str,
    stale_hours: float,
    verbose: bool,
) -> HealthReport:
    rep = HealthReport(mode=mode)
    settings_ns = SimpleNamespace(database_url=database_url)
    conn = psycopg2.connect(database_url)
    configure_for_market_data(conn)
    try:
        if not _has_column(conn, schema="market_data", table="ingestion_cursor", column="initial_backfill_done"):
            print(
                "ERROR: market_data.ingestion_cursor.initial_backfill_done is missing. "
                "Run Alembic migrations on this database (bf2a3b4c5d6e8+).",
                file=sys.stderr,
            )
            raise SystemExit(2)

        bases = query_universe_base_assets(conn, mode=mode)
        rep.universe_base_count = len(bases)
        mapping = resolve_binance_usdt_spot_symbols_for_bases(
            conn,
            base_assets=bases,
            quote_asset="USDT",
            broker="binance",
        )
        rep.bases_without_symbol = sorted(
            b for b in bases if not (mapping.get(b) or "").strip()
        )

        # Same symbol order/set as the scheduler (raises if universe / resolution is broken).
        try:
            symbols = sorted(resolve_runtime_symbols(settings_ns, mode=mode))
        except RuntimeError as e:
            print(f"ERROR: resolve_runtime_symbols: {e}", file=sys.stderr)
            raise SystemExit(1) from e

        rep.resolved_symbol_count = len(symbols)

        o_done = query_ohlcv_symbols_initial_backfill_complete(conn, intervals=OHLCV_INTERVALS)
        b_done = query_basis_pairs_initial_backfill_complete(
            conn, contract_types=BASIS_CONTRACT_TYPES, periods=BASIS_PERIODS
        )
        oi_done = query_open_interest_symbols_initial_backfill_complete(
            conn, contract_types=OPEN_INTEREST_CONTRACT_TYPES, periods=OPEN_INTEREST_PERIODS
        )
        tk_done = query_taker_buy_sell_volume_symbols_initial_backfill_complete(
            conn, periods=TAKER_BUYSELL_VOLUME_PERIODS
        )
        tt_done = query_top_trader_long_short_symbols_initial_backfill_complete(
            conn, periods=TOP_TRADER_LONG_SHORT_PERIODS
        )

        rep.ohlcv_incomplete = sorted(set(symbols) - o_done)
        rep.basis_incomplete = sorted(set(symbols) - b_done)
        rep.open_interest_incomplete = sorted(set(symbols) - oi_done)
        rep.taker_incomplete = sorted(set(symbols) - tk_done)
        rep.top_trader_incomplete = sorted(set(symbols) - tt_done)

        cutoff = _stale_cutoff(stale_hours)
        rep.stale_ohlcv = _check_ohlcv_stale(conn, symbols, OHLCV_INTERVALS, cutoff)
        rep.stale_basis = _check_sample_stale(conn, symbols=symbols, table="basis_rate", cutoff=cutoff, symbol_column="pair")
        rep.stale_open_interest = _check_sample_stale(
            conn, symbols=symbols, table="open_interest", cutoff=cutoff, symbol_column="symbol"
        )
        rep.stale_taker = _check_sample_stale(
            conn, symbols=symbols, table="taker_buy_sell_volume", cutoff=cutoff, symbol_column="symbol"
        )
        rep.stale_top_trader = _check_sample_stale(
            conn, symbols=symbols, table="top_trader_long_short", cutoff=cutoff, symbol_column="symbol"
        )

        rep.flag_anomalies.extend(_check_flag_anomalies(conn, symbols, OHLCV_INTERVALS))
    except pg_errors.UndefinedTable as e:
        print(f"ERROR: missing table or schema: {e}", file=sys.stderr)
        raise SystemExit(2) from e
    finally:
        conn.close()

    if verbose:
        print(f"mode={mode} universe_bases={rep.universe_base_count} resolved_symbols={rep.resolved_symbol_count}")
        if rep.bases_without_symbol:
            print(f"bases_without_oms_symbol ({len(rep.bases_without_symbol)}):", ", ".join(rep.bases_without_symbol[:50]))
            if len(rep.bases_without_symbol) > 50:
                print(f"  ... +{len(rep.bases_without_symbol) - 50} more")
        for name, lst in (
            ("ohlcv_incomplete", rep.ohlcv_incomplete),
            ("basis_incomplete", rep.basis_incomplete),
            ("open_interest_incomplete", rep.open_interest_incomplete),
            ("taker_incomplete", rep.taker_incomplete),
            ("top_trader_incomplete", rep.top_trader_incomplete),
        ):
            if lst:
                print(f"{name} ({len(lst)}):", ", ".join(lst[:40]))
                if len(lst) > 40:
                    print(f"  ... +{len(lst) - 40} more")
        if rep.stale_ohlcv:
            print(f"stale_ohlcv (>{stale_hours}h, {len(rep.stale_ohlcv)} series):", rep.stale_ohlcv[:20])
        if rep.stale_basis:
            print(f"stale_basis (>{stale_hours}h, {len(rep.stale_basis)}):", rep.stale_basis[:20])
        if rep.stale_open_interest:
            print(f"stale_open_interest: {rep.stale_open_interest[:20]}")
        if rep.stale_taker:
            print(f"stale_taker: {rep.stale_taker[:20]}")
        if rep.stale_top_trader:
            print(f"stale_top_trader: {rep.stale_top_trader[:20]}")
        if rep.flag_anomalies:
            for line in rep.flag_anomalies:
                print("ANOMALY:", line)

    print("summary:")
    print(f"  universe_bases:        {rep.universe_base_count}")
    print(f"  resolved_symbols:      {rep.resolved_symbol_count}")
    print(f"  missing_oms_mapping:   {len(rep.bases_without_symbol)}")
    print("  incomplete_backfill:")
    print(f"    ohlcv:                 {len(rep.ohlcv_incomplete)}")
    print(f"    basis_rate:            {len(rep.basis_incomplete)}")
    print(f"    open_interest:         {len(rep.open_interest_incomplete)}")
    print(f"    taker_buy_sell:        {len(rep.taker_incomplete)}")
    print(f"    top_trader_long_short: {len(rep.top_trader_incomplete)}")
    print(f"  stale (>{stale_hours}h):")
    print(f"    ohlcv_series:          {len(rep.stale_ohlcv)}")
    print(f"    basis (by pair):       {len(rep.stale_basis)}")
    print(f"    open_interest:         {len(rep.stale_open_interest)}")
    print(f"    taker_buy_sell:        {len(rep.stale_taker)}")
    print(f"    top_trader_long_short: {len(rep.stale_top_trader)}")
    print(f"  flag_anomalies:        {len(rep.flag_anomalies)}")
    return rep


def main() -> int:
    parser = argparse.ArgumentParser(description="Check market_data completeness and backfill flags.")
    parser.add_argument(
        "--pg-host-port",
        default=None,
        metavar="HOST:PORT",
        help=(
            "Replace only host:port in MARKET_DATA_DATABASE_URL or DATABASE_URL; "
            "user, password, and DB path are unchanged."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("ever_seen", "current_only"),
        default="ever_seen",
        help="Universe query mode (same as runtime resolver).",
    )
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=36.0,
        help="Warn if max(timestamp) is older than this many hours (per fact table / OHLCV interval).",
    )
    parser.add_argument("--verbose", action="store_true", help="Print symbol-level detail.")
    args = parser.parse_args()

    db_url = _resolve_database_url(pg_host_port=args.pg_host_port)
    report = run_checks(db_url, mode=args.mode, stale_hours=args.stale_hours, verbose=args.verbose)

    if report.has_errors():
        print("RESULT: FAIL (backfill incomplete or universe mapping gaps)", file=sys.stderr)
        return 1
    if report.has_warnings():
        print("RESULT: WARN (stale data or flag anomalies — see summary / --verbose)")
        return 0
    print("RESULT: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
