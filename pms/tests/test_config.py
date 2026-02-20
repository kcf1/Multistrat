"""
Unit tests for PMS config (12.3.1b): Pydantic BaseSettings.
"""

import os
from unittest.mock import patch

import pytest

from pms.config import PmsSettings


class TestPmsSettings:
    """Test PmsSettings loading from env."""

    def test_defaults(self):
        # Defaults when env not set (redis_url/database_url may come from .env)
        s = PmsSettings()
        assert s.pms_tick_interval_seconds == 5.0
        assert s.pms_mark_price_source == "binance"
        assert s.pms_rebuild_from_orders_interval_seconds is None
        assert "redis" in s.redis_url

    def test_from_env(self):
        with patch.dict(
            os.environ,
            {
                "REDIS_URL": "redis://redis:6379/1",
                "PMS_TICK_INTERVAL_SECONDS": "10",
                "PMS_MARK_PRICE_SOURCE": "binance",
                "PMS_REBUILD_FROM_ORDERS_INTERVAL_SECONDS": "60",
            },
            clear=False,
        ):
            s = PmsSettings()
        assert s.redis_url == "redis://redis:6379/1"
        assert s.pms_tick_interval_seconds == 10.0
        assert s.pms_mark_price_source == "binance"
        assert s.pms_rebuild_from_orders_interval_seconds == 60.0

    def test_binance_base_url_override(self):
        with patch.dict(
            os.environ,
            {"BINANCE_BASE_URL": "https://api.binance.com"},
            clear=False,
        ):
            s = PmsSettings()
        assert s.binance_base_url == "https://api.binance.com"

    def test_tick_interval_ge_min(self):
        with patch.dict(os.environ, {"PMS_TICK_INTERVAL_SECONDS": "0.5"}, clear=False):
            s = PmsSettings()
        assert s.pms_tick_interval_seconds == 0.5
        with patch.dict(os.environ, {"PMS_TICK_INTERVAL_SECONDS": "0"}, clear=False):
            with pytest.raises(Exception):  # validation error (ge=0.1)
                PmsSettings()
