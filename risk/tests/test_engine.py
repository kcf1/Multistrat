"""Unit tests for rule engine (12.5.2): empty pipeline = ACCEPT; one fail rule = REJECT."""

from typing import Any, Dict, Optional

import pytest

from risk.engine import (
    RiskDecision,
    RiskStatus,
    RuleRegistry,
    RuleResult,
    RiskRule,
    evaluate,
)


class AlwaysFailRule:
    """Rule that always fails (for testing)."""
    rule_id = "TEST_ALWAYS_FAIL"

    def evaluate(
        self,
        order: Dict[str, Any],
        account: Optional[Dict[str, Any]],
        limits: Optional[Dict[str, Any]],
    ) -> RuleResult:
        return RuleResult(rule_id=self.rule_id, passed=False, message="always fail")


class AlwaysPassRule:
    """Rule that always passes."""
    rule_id = "TEST_ALWAYS_PASS"

    def evaluate(
        self,
        order: Dict[str, Any],
        account: Optional[Dict[str, Any]],
        limits: Optional[Dict[str, Any]],
    ) -> RuleResult:
        return RuleResult(rule_id=self.rule_id, passed=True)


def test_evaluate_empty_pipeline_accepts():
    """Engine with empty pipeline returns ACCEPT (pass-through)."""
    registry = RuleRegistry()
    order = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001}
    decision = evaluate(order, [], registry)
    assert decision.status == RiskStatus.ACCEPT
    assert decision.reason_codes == []
    assert decision.messages == []


def test_evaluate_empty_pipeline_no_rules_run():
    """With empty pipeline, no rules are invoked."""
    registry = RuleRegistry()
    registry.register(AlwaysFailRule())
    order = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001}
    decision = evaluate(order, [], registry)  # pipeline empty = don't run any rule
    assert decision.status == RiskStatus.ACCEPT


def test_evaluate_one_fail_rule_rejects():
    """Engine with one always-fail rule returns REJECT with correct reason_code."""
    registry = RuleRegistry()
    registry.register(AlwaysFailRule())
    order = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001}
    decision = evaluate(order, ["TEST_ALWAYS_FAIL"], registry)
    assert decision.status == RiskStatus.REJECT
    assert decision.reason_codes == ["TEST_ALWAYS_FAIL"]
    assert "always fail" in decision.messages or "TEST_ALWAYS_FAIL" in decision.messages


def test_evaluate_one_pass_rule_accepts():
    """Engine with one always-pass rule returns ACCEPT."""
    registry = RuleRegistry()
    registry.register(AlwaysPassRule())
    order = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001}
    decision = evaluate(order, ["TEST_ALWAYS_PASS"], registry)
    assert decision.status == RiskStatus.ACCEPT
    assert decision.reason_codes == []


def test_evaluate_unknown_rule_id_skipped():
    """Pipeline with rule_id not in registry skips that rule (pass)."""
    registry = RuleRegistry()
    order = {"broker": "binance", "symbol": "BTCUSDT", "side": "BUY", "quantity": 0.001}
    decision = evaluate(order, ["MISSING_RULE"], registry)
    # Current implementation treats "not in registry" as passed/skip
    assert decision.status == RiskStatus.ACCEPT
