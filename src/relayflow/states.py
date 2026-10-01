"""Run, task, and attempt states and their allowed transitions.

The tables below mirror docs/architecture.md section 5. The engine asserts every
transition it performs against them, and unit tests check the tables match the
spec, so an accidental new transition fails loudly instead of silently.
"""

from __future__ import annotations

from enum import StrEnum


class RunStatus(StrEnum):
    RUNNING = "running"
    CANCELLING = "cancelling"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    CANCELLED = "cancelled"


class AttemptStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    LEASE_EXPIRED = "lease_expired"
    CANCELLED = "cancelled"
    RELEASED = "released"


TERMINAL_RUN_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED})
ACTIVE_TASK_STATUSES = frozenset({TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING})

# Attempt outcomes that count against a task's max_attempts budget.
COUNTED_FAILURES = frozenset(
    {AttemptStatus.FAILED, AttemptStatus.TIMED_OUT, AttemptStatus.LEASE_EXPIRED}
)

# (from, to) pairs; None as "from" means "created in this state".
RUN_TRANSITIONS: frozenset[tuple[RunStatus | None, RunStatus]] = frozenset(
    {
        (None, RunStatus.RUNNING),  # R1 submit
        (RunStatus.RUNNING, RunStatus.SUCCEEDED),  # R2
        (RunStatus.RUNNING, RunStatus.FAILED),  # R3
        (RunStatus.RUNNING, RunStatus.CANCELLING),  # R4
        (RunStatus.CANCELLING, RunStatus.CANCELLED),  # R5
        (RunStatus.FAILED, RunStatus.RUNNING),  # R6 manual retry
    }
)

TASK_TRANSITIONS: frozenset[tuple[TaskStatus | None, TaskStatus]] = frozenset(
    {
        (None, TaskStatus.PENDING),  # T1
        (None, TaskStatus.QUEUED),  # T2
        (TaskStatus.PENDING, TaskStatus.QUEUED),  # T3 dependencies succeeded
        (TaskStatus.QUEUED, TaskStatus.RUNNING),  # T4 claim
        (TaskStatus.RUNNING, TaskStatus.SUCCEEDED),  # T5
        (TaskStatus.RUNNING, TaskStatus.QUEUED),  # T6 retry / T8 release
        (TaskStatus.RUNNING, TaskStatus.FAILED),  # T7
        (TaskStatus.PENDING, TaskStatus.BLOCKED),  # T9
        (TaskStatus.PENDING, TaskStatus.CANCELLED),  # T10
        (TaskStatus.QUEUED, TaskStatus.CANCELLED),  # T10
        (TaskStatus.RUNNING, TaskStatus.CANCELLED),  # T11 / T12
        (TaskStatus.FAILED, TaskStatus.QUEUED),  # T13 manual retry
        (TaskStatus.BLOCKED, TaskStatus.PENDING),  # T14 manual retry
    }
)


class IllegalTransition(AssertionError):
    """Raised when engine code attempts a transition the state machine forbids (a bug)."""


def assert_run_transition(old: RunStatus | None, new: RunStatus) -> None:
    if (old, new) not in RUN_TRANSITIONS:
        raise IllegalTransition(f"run transition {old} -> {new} is not allowed")


def assert_task_transition(old: TaskStatus | None, new: TaskStatus) -> None:
    if (old, new) not in TASK_TRANSITIONS:
        raise IllegalTransition(f"task transition {old} -> {new} is not allowed")
