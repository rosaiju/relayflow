"""Deterministic document task handlers and the cooperative task context."""

from __future__ import annotations

import time
import uuid
from typing import Any

import pytest

from relayflow.tasks.document import keywords, report, validate, word_count
from relayflow.tasks.registry import (
    PermanentTaskError,
    TaskCancelled,
    TaskContext,
    TaskTimedOut,
    registered_types,
)

TEXT = "The storm passed. The lighthouse stood; the lighthouse keeper logged the storm."


def ctx(deadline_in: float = 30.0) -> TaskContext:
    return TaskContext(
        run_id=uuid.uuid4(),
        task_key="t",
        attempt_number=1,
        deadline=time.monotonic() + deadline_in,
    )


def doc(text: str = TEXT, title: str = "Log") -> dict[str, Any]:
    return {"run_input": {"title": title, "text": text}, "params": {}, "deps": {}}


def test_registry_has_only_the_expected_types() -> None:
    assert registered_types() == {
        "doc.validate",
        "doc.word_count",
        "doc.keywords",
        "doc.report",
        "notify.report_ready",
        "demo.noop",
        "demo.sleep",
        "demo.fail",
    }


def test_validate_rejects_bad_documents_permanently() -> None:
    for bad in ("   ", "x" * 20_001, "bad \x00 byte"):
        with pytest.raises(PermanentTaskError):
            validate(ctx(), doc(text=bad))
    assert validate(ctx(), doc())["valid"] is True


def test_word_count() -> None:
    out = word_count(ctx(), doc())
    assert out["words"] == 12
    assert out["unique_words"] == 7
    assert out["lines"] == 1


def test_keywords_are_ranked_deterministically() -> None:
    out = keywords(ctx(), {**doc(), "params": {"top_n": 3}})
    assert out["keywords"] == [
        {"term": "lighthouse", "count": 2},
        {"term": "storm", "count": 2},
        {"term": "keeper", "count": 1},
    ]


def test_report_uses_direct_dependency_outputs_deterministically() -> None:
    deps = {"word_count": word_count(ctx(), doc()), "keywords": keywords(ctx(), doc())}
    first = report(ctx(), {**doc(), "deps": deps})
    assert first == report(ctx(), {**doc(), "deps": deps})
    assert first["top_keywords"][:2] == ["lighthouse", "storm"]
    assert "12 words" in first["summary"]


def test_report_without_dependencies_fails_permanently() -> None:
    with pytest.raises(PermanentTaskError):
        report(ctx(), doc())


def test_context_cancellation_and_timeout() -> None:
    c = ctx()
    c.cancel_event.set()
    with pytest.raises(TaskCancelled):
        c.check()
    with pytest.raises(TaskTimedOut):
        ctx(deadline_in=-1).sleep(5)
