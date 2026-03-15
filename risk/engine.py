"""
Rule engine shell (12.5.2): rule interface, registry, pipeline.

When the pipeline is empty, the engine always returns ACCEPT (pass-through).
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


class RiskStatus(str, Enum):
    ACCEPT = "ACCEPT"
    REJECT = "REJECT"


@dataclass
class RuleResult:
    """Result of a single rule evaluation."""
    rule_id: str
    passed: bool
    message: Optional[str] = None


@dataclass
class RiskDecision:
    """Final risk decision for an order."""
    status: RiskStatus
    reason_codes: List[str] = field(default_factory=list)
    messages: List[str] = field(default_factory=list)


# Placeholder types for future state-dependent rules (Phase 5)
AccountSnapshot = Dict[str, Any]
RiskLimits = Dict[str, Any]


@runtime_checkable
class RiskRule(Protocol):
    """Interface for a single risk rule."""

    @property
    def rule_id(self) -> str:
        ...

    def evaluate(
        self,
        order: Dict[str, Any],
        account: Optional[AccountSnapshot],
        limits: Optional[RiskLimits],
    ) -> RuleResult:
        ...


class RuleRegistry:
    """Registry of rules by rule_id."""

    def __init__(self) -> None:
        self._rules: Dict[str, RiskRule] = {}

    def register(self, rule: RiskRule) -> None:
        self._rules[rule.rule_id] = rule

    def get(self, rule_id: str) -> Optional[RiskRule]:
        return self._rules.get(rule_id)

    def pipeline_results(
        self,
        order: Dict[str, Any],
        pipeline: List[str],
        account: Optional[AccountSnapshot] = None,
        limits: Optional[RiskLimits] = None,
    ) -> List[RuleResult]:
        """Run ordered list of rule_ids; return list of RuleResults."""
        results: List[RuleResult] = []
        for rid in pipeline:
            rule = self.get(rid)
            if rule is None:
                results.append(RuleResult(rule_id=rid, passed=True, message=f"rule {rid} not in registry (skipped)"))
                continue
            results.append(rule.evaluate(order, account, limits))
        return results


def evaluate(
    order: Dict[str, Any],
    pipeline: List[str],
    registry: RuleRegistry,
    account: Optional[AccountSnapshot] = None,
    limits: Optional[RiskLimits] = None,
) -> RiskDecision:
    """
    Evaluate order through the rule pipeline.

    When pipeline is empty, returns ACCEPT (no rules run).
    Otherwise runs each rule in order; if any fails, returns REJECT
    with all failed reason_codes and messages.
    """
    if not pipeline:
        return RiskDecision(status=RiskStatus.ACCEPT)

    results = registry.pipeline_results(order, pipeline, account=account, limits=limits)
    failed = [r for r in results if not r.passed]
    if not failed:
        return RiskDecision(status=RiskStatus.ACCEPT)

    return RiskDecision(
        status=RiskStatus.REJECT,
        reason_codes=[r.rule_id for r in failed],
        messages=[r.message or r.rule_id for r in failed if r.message or True],
    )
