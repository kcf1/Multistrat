"""
Risk service config: pipeline of rule_ids (empty = pass-through).
"""

import os
from typing import List


def get_rule_pipeline() -> List[str]:
    """
    Return ordered list of rule_ids to run (empty = no rules, pass-through).

    Env RISK_RULE_PIPELINE: comma-separated rule_ids, e.g. "ORDER_01_MIN_QTY,ORDER_02_MAX_QTY".
    """
    raw = os.environ.get("RISK_RULE_PIPELINE", "").strip()
    if not raw:
        return []
    return [r.strip() for r in raw.split(",") if r.strip()]
