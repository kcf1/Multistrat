"""Upsert writers for ``strategies_daily`` tables."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Sequence

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from . import config
from . import validators


def _connect():
    from .data_loader import database_url

    return psycopg2.connect(database_url())


def _py_ts(val: Any) -> datetime | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    t = pd.Timestamp(val)
    if t.tzinfo is None:
        t = t.tz_localize("UTC")
    else:
        t = t.tz_convert("UTC")
    return t.to_pydatetime()


def _ordered_row(r: pd.Series, cols: Sequence[str], pipeline_version: str, source: str | None) -> tuple:
    out: list[Any] = []
    for c in cols:
        if c == "pipeline_version":
            out.append(pipeline_version)
        elif c == "source":
            out.append(source)
        elif c in ("bar_ts", "label_asof_ts"):
            out.append(_py_ts(r.get(c)))
        else:
            v = r.get(c)
            out.append(None if pd.isna(v) else v)
    return tuple(out)


def _upsert_table(
    conn,
    table: str,
    df: pd.DataFrame,
    write_order: Sequence[str],
    *,
    pipeline_version: str,
    source: str | None,
) -> None:
    if df.empty:
        return

    allowed = frozenset(write_order) | validators.COMMON_META
    extra = set(df.columns) - allowed
    if extra:
        raise ValueError(f"{table}: unexpected columns {sorted(extra)[:15]}")

    cols_sql = ", ".join(write_order)
    schema = config.SCHEMA_STRATEGIES_DAILY
    full_table = f'"{schema}"."{table}"'

    update_parts = [
        f'"{c}" = EXCLUDED."{c}"'
        for c in write_order
        if c not in ("bar_ts", "symbol")
    ]
    update_parts.append('"updated_at" = now()')
    update_sql = ", ".join(update_parts)

    insert_sql = (
        f'INSERT INTO {full_table} ({cols_sql}, "pipeline_version", "source") VALUES %s '
        f'ON CONFLICT ("bar_ts", "symbol") DO UPDATE SET {update_sql}'
    )

    rows = []
    for _, r in df.iterrows():
        base = _ordered_row(r, write_order, pipeline_version, source)
        rows.append(base)

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=500)


def upsert_l1feats(conn, df: pd.DataFrame, *, pipeline_version: str, source: str | None) -> None:
    sub = df[[c for c in validators.L1FEATS_WRITE_ORDER if c in df.columns]]
    missing = [c for c in validators.L1FEATS_WRITE_ORDER if c not in sub.columns]
    if missing:
        raise ValueError(f"l1feats_daily missing columns: {missing}")
    _upsert_table(conn, "l1feats_daily", sub, validators.L1FEATS_WRITE_ORDER, pipeline_version=pipeline_version, source=source)


def upsert_signals_daily(conn, df: pd.DataFrame, *, pipeline_version: str, source: str | None) -> None:
    sub = df[[c for c in validators.SIGNALS_WRITE_ORDER if c in df.columns]]
    missing = [c for c in validators.SIGNALS_WRITE_ORDER if c not in sub.columns]
    if missing:
        raise ValueError(f"signals_daily missing columns: {missing}")
    _upsert_table(conn, "signals_daily", sub, validators.SIGNALS_WRITE_ORDER, pipeline_version=pipeline_version, source=source)


def upsert_factors_daily(conn, df: pd.DataFrame, *, pipeline_version: str, source: str | None) -> None:
    sub = df[[c for c in validators.FACTORS_WRITE_ORDER if c in df.columns]]
    missing = [c for c in validators.FACTORS_WRITE_ORDER if c not in sub.columns]
    if missing:
        raise ValueError(f"factors_daily missing columns: {missing}")
    _upsert_table(conn, "factors_daily", sub, validators.FACTORS_WRITE_ORDER, pipeline_version=pipeline_version, source=source)


def upsert_xsecs_daily(conn, df: pd.DataFrame, *, pipeline_version: str, source: str | None) -> None:
    sub = df[[c for c in validators.XSECS_WRITE_ORDER if c in df.columns]]
    missing = [c for c in validators.XSECS_WRITE_ORDER if c not in sub.columns]
    if missing:
        raise ValueError(f"xsecs_daily missing columns: {missing}")
    _upsert_table(conn, "xsecs_daily", sub, validators.XSECS_WRITE_ORDER, pipeline_version=pipeline_version, source=source)


def upsert_labels_daily(conn, df: pd.DataFrame, *, pipeline_version: str, source: str | None) -> None:
    sub = df[[c for c in validators.LABELS_WRITE_ORDER if c in df.columns]]
    missing = [c for c in validators.LABELS_WRITE_ORDER if c not in sub.columns]
    if missing:
        raise ValueError(f"labels_daily missing columns: {missing}")
    _upsert_table(conn, "labels_daily", sub, validators.LABELS_WRITE_ORDER, pipeline_version=pipeline_version, source=source)


def persist_all(
    *,
    l1: pd.DataFrame,
    pre: pd.DataFrame | None,
    factors: pd.DataFrame,
    xsecs: pd.DataFrame,
    labels: pd.DataFrame,
    pipeline_version: str | None = None,
    source: str | None = "market_data.ohlcv",
    persist_precombined: bool | None = None,
) -> None:
    pv = pipeline_version or config.PIPELINE_VERSION
    do_pre = config.PERSIST_SIGNALS_PRECOMBINED if persist_precombined is None else persist_precombined
    conn = _connect()
    try:
        upsert_l1feats(conn, l1, pipeline_version=pv, source=source)
        if do_pre and pre is not None and not pre.empty:
            upsert_signals_daily(conn, pre, pipeline_version=pv, source=source)
        upsert_factors_daily(conn, factors, pipeline_version=pv, source=source)
        upsert_xsecs_daily(conn, xsecs, pipeline_version=pv, source=source)
        upsert_labels_daily(conn, labels, pipeline_version=pv, source=source)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
