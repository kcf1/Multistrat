"""
OMS Redis account store (task 12.2.4).

Key layout:
- account:{broker}:{account_id} — Hash (broker, account_id, updated_at, payload)
- account:{broker}:{account_id}:balances — Hash (asset -> JSON balance dict)
- account:{broker}:{account_id}:positions — Hash (symbol -> JSON position dict)
- accounts:by_broker:{broker} — Set of account_id

Uses pipelines for atomic multi-key updates.
Store structure matches unified event shape from adapter (AccountEvent).
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from redis import Redis

from oms.log import logger


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _account_key(broker: str, account_id: str) -> str:
    return f"account:{broker}:{account_id}"


def _balances_key(broker: str, account_id: str) -> str:
    return f"account:{broker}:{account_id}:balances"


def _positions_key(broker: str, account_id: str) -> str:
    return f"account:{broker}:{account_id}:positions"


def _broker_accounts_set_key(broker: str) -> str:
    return f"accounts:by_broker:{broker}"


class RedisAccountStore:
    """
    Redis-backed account store for balances and positions.
    
    Stores account metadata, balances (per asset), and positions (per symbol)
    based on unified AccountEvent shape from broker adapters.
    """

    def __init__(self, redis_client: Redis):
        self._redis = redis_client

    def apply_account_position(
        self,
        broker: str,
        account_id: str,
        balances: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        updated_at: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Apply a full account position snapshot (outboundAccountPosition event).
        
        Replaces all balances and positions for the account. Atomic via pipeline.
        
        Args:
            broker: Broker name (e.g. "binance")
            account_id: Account identifier
            balances: List of balance dicts: [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}, ...]
            positions: List of position dicts: [{"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"}, ...]
            updated_at: ISO timestamp (defaults to current time)
            payload: Raw broker event blob (optional)
        """
        updated_at = updated_at or _now_iso()
        
        pipe = self._redis.pipeline()
        
        # Update account metadata hash
        account_data = {
            "broker": broker,
            "account_id": account_id,
            "updated_at": updated_at,
        }
        if payload is not None:
            account_data["payload"] = json.dumps(payload) if not isinstance(payload, str) else payload
        
        pipe.hset(_account_key(broker, account_id), mapping={
            k: str(v) for k, v in account_data.items()
        })
        
        # Replace all balances (delete old, set new)
        balances_key = _balances_key(broker, account_id)
        pipe.delete(balances_key)
        if balances:
            balances_dict = {}
            for bal in balances:
                asset = bal.get("asset", "")
                if asset:
                    balances_dict[asset] = json.dumps(bal)
            if balances_dict:
                pipe.hset(balances_key, mapping=balances_dict)
        
        # Replace all positions (delete old, set new)
        positions_key = _positions_key(broker, account_id)
        pipe.delete(positions_key)
        if positions:
            positions_dict = {}
            for pos in positions:
                symbol = pos.get("symbol", "")
                if symbol:
                    positions_dict[symbol] = json.dumps(pos)
            if positions_dict:
                pipe.hset(positions_key, mapping=positions_dict)
        
        # Add to broker accounts set
        pipe.sadd(_broker_accounts_set_key(broker), account_id)
        
        pipe.execute()
        logger.debug(
            "apply_account_position broker={} account_id={} balances={} positions={}",
            broker, account_id, len(balances), len(positions),
        )

    def apply_balance_update(
        self,
        broker: str,
        account_id: str,
        balance: Dict[str, Any],
        updated_at: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Apply a single-asset balance update (balanceUpdate event).
        
        Updates or creates balance for the asset. Merges with existing balance if present.
        Atomic via pipeline.
        
        Args:
            broker: Broker name (e.g. "binance")
            account_id: Account identifier
            balance: Balance dict: {"asset": "USDT", "available": "100.5", "locked": "0.0"}
                     Note: balanceUpdate events may only have delta in "available" field
            updated_at: ISO timestamp (defaults to current time)
            payload: Raw broker event blob (optional)
        """
        updated_at = updated_at or _now_iso()
        asset = balance.get("asset", "")
        if not asset:
            logger.warning("apply_balance_update: balance missing asset field")
            return
        
        pipe = self._redis.pipeline()
        
        # Update account metadata hash
        account_data = {
            "broker": broker,
            "account_id": account_id,
            "updated_at": updated_at,
        }
        if payload is not None:
            account_data["payload"] = json.dumps(payload) if not isinstance(payload, str) else payload
        
        pipe.hset(_account_key(broker, account_id), mapping={
            k: str(v) for k, v in account_data.items()
        })
        
        # Get existing balance for this asset (if any)
        balances_key = _balances_key(broker, account_id)
        existing_balance_json = self._redis.hget(balances_key, asset)
        
        if existing_balance_json:
            try:
                existing_balance = json.loads(existing_balance_json)
                # Merge: use new available/locked if provided, otherwise keep existing
                merged_balance = {
                    "asset": asset,
                    "available": balance.get("available", existing_balance.get("available", "0.0")),
                    "locked": balance.get("locked", existing_balance.get("locked", "0.0")),
                }
            except (json.JSONDecodeError, TypeError):
                # If existing balance is invalid, use new balance
                merged_balance = balance
        else:
            # No existing balance, use new balance (default locked to 0.0 if not provided)
            merged_balance = {
                "asset": asset,
                "available": balance.get("available", "0.0"),
                "locked": balance.get("locked", "0.0"),
            }
        
        # Update balance hash
        pipe.hset(balances_key, asset, json.dumps(merged_balance))
        
        # Add to broker accounts set
        pipe.sadd(_broker_accounts_set_key(broker), account_id)
        
        pipe.execute()
        logger.debug(
            "apply_balance_update broker={} account_id={} asset={} available={}",
            broker, account_id, asset, merged_balance.get("available"),
        )

    def get_account(self, broker: str, account_id: str) -> Optional[Dict[str, Any]]:
        """
        Get account metadata (broker, account_id, updated_at, payload).
        
        Returns:
            Account dict or None if not found
        """
        raw = self._redis.hgetall(_account_key(broker, account_id))
        if not raw:
            return None
        
        account = {}
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            if key == "payload":
                try:
                    account[key] = json.loads(val) if val else None
                except (json.JSONDecodeError, TypeError):
                    account[key] = val
            else:
                account[key] = val
        
        return account

    def get_balances(self, broker: str, account_id: str) -> List[Dict[str, Any]]:
        """
        Get all balances for the account.
        
        Returns:
            List of balance dicts: [{"asset": "USDT", "available": "1000.0", "locked": "0.0"}, ...]
        """
        balances_key = _balances_key(broker, account_id)
        raw = self._redis.hgetall(balances_key)
        if not raw:
            return []
        
        balances = []
        for asset, balance_json in raw.items():
            asset_str = asset.decode() if isinstance(asset, bytes) else asset
            balance_str = balance_json.decode() if isinstance(balance_json, bytes) else balance_json
            try:
                balance = json.loads(balance_str)
                balances.append(balance)
            except (json.JSONDecodeError, TypeError):
                logger.warning("get_balances: invalid balance JSON for asset={}", asset_str)
        
        # Sort by asset for consistency
        balances.sort(key=lambda b: b.get("asset", ""))
        return balances

    def get_positions(self, broker: str, account_id: str) -> List[Dict[str, Any]]:
        """
        Get all positions for the account.
        
        Returns:
            List of position dicts: [{"symbol": "BTCUSDT", "side": "long", "quantity": "0.1", "entry_price": "50000.0"}, ...]
        """
        positions_key = _positions_key(broker, account_id)
        raw = self._redis.hgetall(positions_key)
        if not raw:
            return []
        
        positions = []
        for symbol, position_json in raw.items():
            symbol_str = symbol.decode() if isinstance(symbol, bytes) else symbol
            position_str = position_json.decode() if isinstance(position_json, bytes) else position_json
            try:
                position = json.loads(position_str)
                positions.append(position)
            except (json.JSONDecodeError, TypeError):
                logger.warning("get_positions: invalid position JSON for symbol={}", symbol_str)
        
        # Sort by symbol for consistency
        positions.sort(key=lambda p: p.get("symbol", ""))
        return positions

    def get_account_ids_by_broker(self, broker: str) -> List[str]:
        """
        Get all account IDs for a broker.
        
        Returns:
            List of account_id strings
        """
        key = _broker_accounts_set_key(broker)
        members = self._redis.smembers(key)
        if not members:
            return []
        return [m.decode() if isinstance(m, bytes) else m for m in members]
