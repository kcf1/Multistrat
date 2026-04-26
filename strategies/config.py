"""Micro config for strategy runner and module registry."""

from __future__ import annotations

# Keep deployment-level settings in env; keep strategy tuning in code.
DEFAULT_REBALANCE_INTERVAL_SECONDS = 300
DEFAULT_ENABLED_STRATEGIES: tuple[str, ...] = ()

# Strategies runner service: seconds between ticks (micro-config; tune per env in code if needed).
RUNNER_TICK_INTERVAL_SECONDS = 60

