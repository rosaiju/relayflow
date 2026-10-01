"""Definition validation: schema, registered types, missing deps, cycles, ordering."""

from __future__ import annotations

import copy
from typing import Any

import pytest

from relayflow.catalog import BUILTIN_WORKFLOWS, DOCUMENT_PROCESSING_V1
from relayflow.definitions import descendants, find_cycle, topological_order, validate_definition
from relayflow.engine.errors import ValidationFailed

TYPES = frozenset({"demo.noop", "demo.sleep"})


def spec(*tasks: dict[str, Any]) -> dict[str, Any]:
    return {"name": "wf", "version": 1, "tasks": list(tasks)}


def errors_of(raw: dict[str, Any]) -> list[str]:
    with pytest.raises(ValidationFailed) as info:
        validate_definition(raw, TYPES)
    return info.value.errors


def test_builtin_workflows_are_valid() -> None:
    for raw in BUILTIN_WORKFLOWS:
        validate_definition(raw)


def test_defaults_are_filled_in() -> None:
    wf = validate_definition(spec({"key": "a", "type": "demo.noop"}), TYPES)
    assert wf.task("a").max_attempts == 3
    assert wf.task("a").depends_on == ()


def test_rejects_cycle_and_names_it() -> None:
    errs = errors_of(
        spec(
            {"key": "a", "type": "demo.noop", "depends_on": ["c"]},
            {"key": "b", "type": "demo.noop", "depends_on": ["a"]},
            {"key": "c", "type": "demo.noop", "depends_on": ["b"]},
        )
    )
    assert any("cycle" in e and "a" in e and "c" in e for e in errs)


def test_rejects_self_dependency() -> None:
    errs = errors_of(spec({"key": "a", "type": "demo.noop", "depends_on": ["a"]}))
    assert any("itself" in e for e in errs)


def test_rejects_missing_dependency() -> None:
    errs = errors_of(spec({"key": "a", "type": "demo.noop", "depends_on": ["ghost"]}))
    assert errs == ["task 'a': missing dependency 'ghost'"]


@pytest.mark.parametrize("bad_type", ["shell", "os.system", "", None, "doc.validate"])
def test_rejects_unsupported_task_types(bad_type: Any) -> None:
    errs = errors_of(spec({"key": "a", "type": bad_type}))
    assert any("unsupported task type" in e for e in errs)


def test_rejects_duplicate_keys_and_bad_policy() -> None:
    errs = errors_of(
        spec(
            {"key": "a", "type": "demo.noop", "max_attempts": 0},
            {"key": "a", "type": "demo.noop", "timeout_seconds": 10_000},
        )
    )
    assert any("duplicate task keys" in e for e in errs)
    assert any("max_attempts" in e for e in errs)
    assert any("timeout_seconds" in e for e in errs)


def test_rejects_unknown_fields_and_bad_names() -> None:
    raw = spec({"key": "A", "type": "demo.noop"})
    raw["name"] = "Bad Name"
    raw["command"] = "rm -rf /"
    errs = errors_of(raw)
    assert any("unknown fields ['command']" in e for e in errs)
    assert any(e.startswith("name must match") for e in errs)
    assert any("key must match" in e for e in errs)


def test_rejects_oversized_params_and_too_many_tasks() -> None:
    big = spec({"key": "a", "type": "demo.noop", "params": {"x": "y" * 5000}})
    assert any("params exceed" in e for e in errors_of(big))
    many = spec(*({"key": f"t{i}", "type": "demo.noop"} for i in range(51)))
    assert any("1..50" in e for e in errors_of(many))


def test_topological_order_is_deterministic() -> None:
    wf = validate_definition(DOCUMENT_PROCESSING_V1)
    assert topological_order(wf) == ["validate", "keywords", "word_count", "report", "notify"]


def test_descendants_are_transitive() -> None:
    normalized = validate_definition(DOCUMENT_PROCESSING_V1).to_dict()
    assert descendants(normalized, "word_count") == {"report", "notify"}
    assert descendants(normalized, "notify") == set()


def test_find_cycle_none_for_dag() -> None:
    assert find_cycle({"a": (), "b": ("a",), "c": ("a", "b")}) is None


def test_normalized_form_round_trips() -> None:
    wf = validate_definition(copy.deepcopy(DOCUMENT_PROCESSING_V1))
    assert validate_definition(wf.to_dict()).to_dict() == wf.to_dict()
