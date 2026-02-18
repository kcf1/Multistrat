"""
Unit tests for OMS account repairs (task 12.2.8).

Currently run_all_account_repairs is a dummy (returns 0). Verify it is callable.
"""

import pytest

from oms.account_repair import run_all_account_repairs


def test_run_all_account_repairs_returns_zero():
    """run_all_account_repairs is a no-op and returns 0."""
    result = run_all_account_repairs("postgres://localhost/dummy")
    assert result == 0


def test_run_all_account_repairs_accepts_callable():
    """run_all_account_repairs accepts a connection callable (no-op, does not call it)."""
    result = run_all_account_repairs(lambda: None)
    assert result == 0
