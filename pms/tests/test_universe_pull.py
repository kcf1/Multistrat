import pytest

from pms.universe_pull import compute_pms_admissions


def test_compute_pms_admissions_excludes_missing_symbol() -> None:
    admitted, skipped = compute_pms_admissions(
        ["BTC", "ETH"],
        base_to_symbol={"BTC": "BTCUSDT"},
        feed_source="binance",
    )
    assert admitted == ["BTC"]
    assert skipped["ETH"] == "no_tradable_symbol"


def test_compute_pms_admissions_requires_feed_source() -> None:
    admitted, skipped = compute_pms_admissions(
        ["BTC"],
        base_to_symbol={"BTC": "BTCUSDT"},
        feed_source="",
    )
    assert admitted == []
    assert skipped["BTC"] == "price_feed_disabled"


def test_compute_pms_admissions_unknown_source_rejected() -> None:
    admitted, skipped = compute_pms_admissions(
        ["BTC"],
        base_to_symbol={"BTC": "BTCUSDT"},
        feed_source="nope",
    )
    assert admitted == []
    assert skipped["BTC"].startswith("unknown_feed_source")

