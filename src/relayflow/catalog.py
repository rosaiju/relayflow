"""Built-in workflow definitions seeded at startup."""

from __future__ import annotations

from typing import Any

DOCUMENT_PROCESSING_V1: dict[str, Any] = {
    "name": "document-processing",
    "version": 1,
    "description": (
        "Validate a synthetic text document, count words and extract keywords in "
        "parallel, build a report, and notify the mock notification service."
    ),
    "tasks": [
        {"key": "validate", "type": "doc.validate", "depends_on": [], "timeout_seconds": 30},
        {
            "key": "word_count",
            "type": "doc.word_count",
            "depends_on": ["validate"],
            "timeout_seconds": 30,
        },
        {
            "key": "keywords",
            "type": "doc.keywords",
            "depends_on": ["validate"],
            "timeout_seconds": 30,
            "params": {"top_n": 8},
        },
        {
            "key": "report",
            "type": "doc.report",
            "depends_on": ["word_count", "keywords"],
            "timeout_seconds": 30,
        },
        {
            "key": "notify",
            "type": "notify.report_ready",
            "depends_on": ["report"],
            "timeout_seconds": 30,
            "max_attempts": 5,
            "backoff_base_seconds": 0.5,
            "backoff_max_seconds": 10,
        },
    ],
}

# Small graphs used by tests and benchmarks (documented in docs/test-interfaces.md).
TEST_WORKFLOWS: dict[str, dict[str, Any]] = {
    "test-chain": {
        "name": "test-chain",
        "version": 1,
        "description": "a -> b -> c",
        "tasks": [
            {"key": "a", "type": "demo.sleep", "params": {"seconds": 0}},
            {"key": "b", "type": "demo.sleep", "depends_on": ["a"], "params": {"seconds": 0}},
            {"key": "c", "type": "demo.sleep", "depends_on": ["b"], "params": {"seconds": 0}},
        ],
    },
    "test-diamond": {
        "name": "test-diamond",
        "version": 1,
        "description": "a -> (b, c) -> d",
        "tasks": [
            {"key": "a", "type": "demo.sleep", "params": {"seconds": 0}},
            {"key": "b", "type": "demo.sleep", "depends_on": ["a"], "params": {"seconds": 0}},
            {"key": "c", "type": "demo.sleep", "depends_on": ["a"], "params": {"seconds": 0}},
            {"key": "d", "type": "demo.sleep", "depends_on": ["b", "c"], "params": {"seconds": 0}},
        ],
    },
    "test-sleep": {
        "name": "test-sleep",
        "version": 1,
        "description": "one cooperative sleep task",
        "tasks": [
            {"key": "s", "type": "demo.sleep", "timeout_seconds": 30, "params": {"seconds": 5}}
        ],
    },
    "test-fail": {
        "name": "test-fail",
        "version": 1,
        "description": "independent success and failure branches",
        "tasks": [
            {"key": "ok", "type": "demo.noop"},
            {
                "key": "bad",
                "type": "demo.fail",
                "max_attempts": 2,
                "backoff_base_seconds": 0.1,
                "backoff_max_seconds": 0.2,
            },
            {"key": "after_bad", "type": "demo.noop", "depends_on": ["bad"]},
            {"key": "after_ok", "type": "demo.noop", "depends_on": ["ok"]},
        ],
    },
    "test-timeout": {
        "name": "test-timeout",
        "version": 1,
        "description": "a sleep that exceeds its timeout",
        "tasks": [
            {
                "key": "s",
                "type": "demo.sleep",
                "timeout_seconds": 1,
                "max_attempts": 2,
                "backoff_base_seconds": 0.1,
                "backoff_max_seconds": 0.2,
                "params": {"seconds": 5},
            }
        ],
    },
    "bench-noop": {
        "name": "bench-noop",
        "version": 1,
        "description": "one no-op task (benchmarks)",
        "tasks": [{"key": "n", "type": "demo.noop"}],
    },
}

BUILTIN_WORKFLOWS: list[dict[str, Any]] = [DOCUMENT_PROCESSING_V1, *TEST_WORKFLOWS.values()]
