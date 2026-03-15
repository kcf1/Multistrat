"""
Risk service entrypoint (12.5.1–12.5.2).

Usage: python -m risk.main
Env: REDIS_URL, optional RISK_RULE_PIPELINE (comma-separated rule_ids; empty = pass-through).
"""

import os
import sys

from redis import Redis

from risk.loop import run_loop


def main() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    redis = Redis.from_url(redis_url, decode_responses=True)
    run_loop(redis)
    sys.exit(0)


if __name__ == "__main__":
    main()
