"""Domain errors raised by engine operations; the API maps them to HTTP statuses."""

from __future__ import annotations


class RelayFlowError(Exception):
    code = "error"


class ValidationFailed(RelayFlowError):
    code = "validation_failed"

    def __init__(self, errors: list[str]) -> None:
        super().__init__("; ".join(errors))
        self.errors = errors


class WorkflowNotFound(RelayFlowError):
    code = "workflow_not_found"


class RunNotFound(RelayFlowError):
    code = "run_not_found"


class IdempotencyConflict(RelayFlowError):
    code = "idempotency_conflict"


class DefinitionConflict(RelayFlowError):
    code = "definition_conflict"


class InvalidTransition(RelayFlowError):
    code = "invalid_transition"
