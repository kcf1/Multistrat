"""
OMS broker adapter registry (task 12.1.7).

Registry maps broker_name -> adapter; route by broker.
Interface is BrokerAdapter in brokers/base.py.
"""

from typing import Any, Dict, Optional

from oms.brokers.base import BrokerAdapter


class AdapterRegistry:
    """
    Registry of broker name -> adapter. OMS routes orders by message broker field.
    """

    def __init__(self) -> None:
        self._adapters: Dict[str, BrokerAdapter] = {}

    def register(self, broker_name: str, adapter: BrokerAdapter) -> None:
        """Register an adapter for the given broker name (e.g. 'binance')."""
        self._adapters[broker_name.strip().lower()] = adapter

    def get(self, broker_name: str) -> Optional[BrokerAdapter]:
        """Return the adapter for the broker, or None if unknown."""
        if not broker_name:
            return None
        return self._adapters.get(broker_name.strip().lower())

    def __contains__(self, broker_name: str) -> bool:
        return self.get(broker_name) is not None
