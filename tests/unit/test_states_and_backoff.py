"""State tables match the spec; backoff stays within its documented bounds."""

from __future__ import annotations

import random

import pytest

from relayflow.engine import backoff_delay
from relayflow.states import (
    RUN_TRANSITIONS,
    TASK_TRANSITIONS,
    IllegalTransition,
    RunStatus,
    TaskStatus,
    assert_run_transition,
    assert_task_transition,
)


def test_run_transitions_match_spec_table() -> None:
    assert len(RUN_TRANSITIONS) == 6  # R1..R6
    assert (RunStatus.FAILED, RunStatus.RUNNING) in RUN_TRANSITIONS
    for final in (RunStatus.SUCCEEDED, RunStatus.CANCELLED):
        assert not any(old == final for old, _ in RUN_TRANSITIONS)


def test_succeeded_and_cancelled_tasks_are_final() -> None:
    for final in (TaskStatus.SUCCEEDED, TaskStatus.CANCELLED):
        assert not any(old == final for old, _ in TASK_TRANSITIONS)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (TaskStatus.PENDING, TaskStatus.RUNNING),  # must be queued first
        (TaskStatus.SUCCEEDED, TaskStatus.QUEUED),
        (TaskStatus.BLOCKED, TaskStatus.QUEUED),
        (TaskStatus.QUEUED, TaskStatus.SUCCEEDED),
    ],
)
def test_illegal_task_transitions_raise(old: TaskStatus, new: TaskStatus) -> None:
    with pytest.raises(IllegalTransition):
        assert_task_transition(old, new)


def test_illegal_run_transition_raises() -> None:
    with pytest.raises(IllegalTransition):
        assert_run_transition(RunStatus.CANCELLED, RunStatus.RUNNING)


@pytest.mark.parametrize("failures", [1, 2, 3, 6, 12])
def test_backoff_is_within_equal_jitter_bounds(failures: int) -> None:
    rng = random.Random(failures)
    raw = min(30.0, 1.0 * 2 ** (failures - 1))
    for _ in range(200):
        delay = backoff_delay(failures, 1.0, 30.0, rng)
        assert raw / 2 <= delay <= raw


def test_backoff_grows_then_caps() -> None:
    rng = random.Random(0)
    assert backoff_delay(1, 1.0, 30.0, rng) <= 1.0
    assert 15.0 <= backoff_delay(10, 1.0, 30.0, rng) <= 30.0
