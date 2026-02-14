"""Print Binance testnet account balances (non-zero). Loads .env from repo root."""
from pathlib import Path
import os
import sys

# repo root = 4 levels up from this script (scripts -> binance -> brokers -> oms -> repo)
_repo_root = Path(__file__).resolve().parents[4]
_env = _repo_root / ".env"
if _env.is_file():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env)
    except ImportError:
        pass

# Ensure oms is importable (repo root on path)
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from oms.brokers.binance.api_client import BinanceAPIClient


def _env(key: str, default: str = "") -> str:
    v = (os.environ.get(key) or default).strip().replace("\r", "")
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
        v = v[1:-1].strip()
    return v


def main() -> None:
    key = _env("BINANCE_API_KEY")
    secret = _env("BINANCE_API_SECRET")
    base = _env("BINANCE_BASE_URL") or "https://testnet.binance.vision"
    if not key or not secret:
        print("Missing BINANCE_API_KEY or BINANCE_API_SECRET in .env")
        sys.exit(1)
    client = BinanceAPIClient(api_key=key, api_secret=secret, base_url=base, testnet=True)
    acc = client.get_account()
    print("Account (testnet):")
    print("  updateTime:", acc.get("updateTime"))
    print("  Balances (non-zero free or locked):")
    for b in acc.get("balances", []):
        free = float(b.get("free", 0) or 0)
        locked = float(b.get("locked", 0) or 0)
        if free > 0 or locked > 0:
            asset = (b.get("asset") or "").encode("ascii", errors="replace").decode("ascii")
            print(f"    {asset}: free={free}, locked={locked}")


if __name__ == "__main__":
    main()
