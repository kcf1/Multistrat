"""Tests for misc no-op heartbeat job (Phase 5 §4.7)."""

from __future__ import annotations

from scheduler.jobs.misc.noop_heartbeat import NoopHeartbeatJob
from scheduler.types import JobContext


def test_noop_heartbeat_runs_without_error() -> None:
    job = NoopHeartbeatJob()
    job.run(JobContext(job_id="noop_heartbeat"))


def test_noop_heartbeat_job_id() -> None:
    assert NoopHeartbeatJob().job_id == "noop_heartbeat"
