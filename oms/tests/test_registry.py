"""
Unit tests for OMS broker adapter registry (task 12.1.7).

Mock adapters; verify routing and error handling (unknown broker).
"""

from typing import Any, Dict

import pytest

from oms.registry import AdapterRegistry


def _make_mock_adapter(name: str):
    class MockAdapter:
        def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
            return {"broker_order_id": f"{name}-123", "status": "NEW"}

        def start_fill_listener(self, callback) -> None:
            pass
    return MockAdapter()


def test_register_and_get():
    registry = AdapterRegistry()
    binance = _make_mock_adapter("binance")
    registry.register("binance", binance)
    assert registry.get("binance") is binance
    assert registry.get("BINANCE") is binance


def test_unknown_broker_returns_none():
    registry = AdapterRegistry()
    assert registry.get("unknown") is None
    assert registry.get("") is None


def test_contains():
    registry = AdapterRegistry()
    registry.register("binance", _make_mock_adapter("binance"))
    assert "binance" in registry
    assert "BINANCE" in registry
    assert "unknown" not in registry


def test_routing_uses_correct_adapter():
    registry = AdapterRegistry()
    a1 = _make_mock_adapter("a1")
    a2 = _make_mock_adapter("a2")
    registry.register("broker1", a1)
    registry.register("broker2", a2)
    assert registry.get("broker1") is a1
    assert registry.get("broker2") is a2
    resp1 = registry.get("broker1").place_order({})  # type: ignore[union-attr]
    resp2 = registry.get("broker2").place_order({})  # type: ignore[union-attr]
    assert resp1["broker_order_id"] == "a1-123"
    assert resp2["broker_order_id"] == "a2-123"


def test_register_overwrites():
    registry = AdapterRegistry()
    a1 = _make_mock_adapter("a1")
    a2 = _make_mock_adapter("a2")
    registry.register("binance", a1)
    registry.register("binance", a2)
    assert registry.get("binance") is a2


def test_broker_names():
    """broker_names() returns list of registered broker names (12.1.11a bootstrap)."""
    registry = AdapterRegistry()
    assert registry.broker_names() == []
    registry.register("binance", _make_mock_adapter("binance"))
    assert registry.broker_names() == ["binance"]
    registry.register("bybit", _make_mock_adapter("bybit"))
    assert set(registry.broker_names()) == {"binance", "bybit"}
