"""Workflow definition validation: schema, registered task types, and DAG checks.

See docs/architecture.md section 4.
"""

from __future__ import annotations

import heapq
import json
import re
from dataclasses import dataclass, field
from typing import Any

from relayflow.engine.errors import ValidationFailed

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
MAX_TASKS = 50
MAX_PARAMS_BYTES = 4 * 1024

TASK_DEFAULTS: dict[str, Any] = {
    "max_attempts": 3,
    "timeout_seconds": 60.0,
    "backoff_base_seconds": 1.0,
    "backoff_max_seconds": 30.0,
}


@dataclass(frozen=True)
class TaskSpec:
    key: str
    type: str
    depends_on: tuple[str, ...]
    max_attempts: int
    timeout_seconds: float
    backoff_base_seconds: float
    backoff_max_seconds: float
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "type": self.type,
            "depends_on": list(self.depends_on),
            "max_attempts": self.max_attempts,
            "timeout_seconds": self.timeout_seconds,
            "backoff_base_seconds": self.backoff_base_seconds,
            "backoff_max_seconds": self.backoff_max_seconds,
            "params": self.params,
        }


@dataclass(frozen=True)
class WorkflowSpec:
    name: str
    version: int
    description: str
    tasks: tuple[TaskSpec, ...]

    def to_dict(self) -> dict[str, Any]:
        """Canonical normalized form; this is what gets stored and snapshotted."""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "tasks": [t.to_dict() for t in self.tasks],
        }

    def task(self, key: str) -> TaskSpec:
        for t in self.tasks:
            if t.key == key:
                return t
        raise KeyError(key)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _validate_task(
    raw: Any, index: int, known_types: frozenset[str], errors: list[str]
) -> TaskSpec | None:
    where = f"tasks[{index}]"
    if not isinstance(raw, dict):
        errors.append(f"{where}: must be an object")
        return None
    allowed = {"key", "type", "depends_on", "params", *TASK_DEFAULTS}
    unknown = sorted(set(raw) - allowed)
    if unknown:
        errors.append(f"{where}: unknown fields {unknown}")

    key = raw.get("key")
    if not isinstance(key, str) or not KEY_RE.match(key):
        errors.append(f"{where}: key must match {KEY_RE.pattern}")
        return None
    where = f"task '{key}'"

    task_type = raw.get("type")
    if not isinstance(task_type, str) or task_type not in known_types:
        errors.append(f"{where}: unsupported task type {task_type!r}")

    deps = raw.get("depends_on", [])
    if not isinstance(deps, list) or not all(isinstance(d, str) for d in deps):
        errors.append(f"{where}: depends_on must be a list of task keys")
        deps = []
    if len(set(deps)) != len(deps):
        errors.append(f"{where}: duplicate entries in depends_on")
    if key in deps:
        errors.append(f"{where}: depends on itself")

    policy = {name: raw.get(name, default) for name, default in TASK_DEFAULTS.items()}
    if not _is_int(policy["max_attempts"]) or not 1 <= policy["max_attempts"] <= 10:
        errors.append(f"{where}: max_attempts must be an integer 1..10")
    if not _is_number(policy["timeout_seconds"]) or not 1 <= policy["timeout_seconds"] <= 600:
        errors.append(f"{where}: timeout_seconds must be 1..600")
    base, cap = policy["backoff_base_seconds"], policy["backoff_max_seconds"]
    if not (_is_number(base) and _is_number(cap) and 0 < base <= cap <= 300):
        errors.append(f"{where}: require 0 < backoff_base_seconds <= backoff_max_seconds <= 300")

    params = raw.get("params", {})
    if not isinstance(params, dict):
        errors.append(f"{where}: params must be an object")
        params = {}
    elif len(canonical_json(params).encode()) > MAX_PARAMS_BYTES:
        errors.append(f"{where}: params exceed {MAX_PARAMS_BYTES} bytes")

    return TaskSpec(
        key=key,
        type=str(task_type),
        depends_on=tuple(deps),
        max_attempts=int(policy["max_attempts"]) if _is_int(policy["max_attempts"]) else 1,
        timeout_seconds=float(policy["timeout_seconds"])
        if _is_number(policy["timeout_seconds"])
        else 1.0,
        backoff_base_seconds=float(base) if _is_number(base) else 1.0,
        backoff_max_seconds=float(cap) if _is_number(cap) else 1.0,
        params=params,
    )


def find_cycle(edges: dict[str, tuple[str, ...]]) -> list[str] | None:
    """Return one cycle (as a list of keys, first == last) or None. edges: task -> deps."""
    state: dict[str, int] = {}  # 1 = on stack, 2 = done
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        state[node] = 1
        stack.append(node)
        for dep in edges.get(node, ()):
            if dep not in edges:
                continue
            if state.get(dep) == 1:
                return [*stack[stack.index(dep) :], dep]
            if dep not in state and (cycle := visit(dep)) is not None:
                return cycle
        stack.pop()
        state[node] = 2
        return None

    for node in sorted(edges):
        if node not in state and (cycle := visit(node)) is not None:
            return cycle
    return None


def validate_definition(spec: Any, known_types: frozenset[str] | None = None) -> WorkflowSpec:
    """Validate a raw definition and return its normalized form, or raise ValidationFailed."""
    if known_types is None:
        from relayflow.tasks.registry import registered_types

        known_types = registered_types()

    errors: list[str] = []
    if not isinstance(spec, dict):
        raise ValidationFailed(["definition must be an object"])
    unknown = sorted(set(spec) - {"name", "version", "description", "tasks"})
    if unknown:
        errors.append(f"unknown fields {unknown}")
    name = spec.get("name")
    if not isinstance(name, str) or not NAME_RE.match(name):
        errors.append(f"name must match {NAME_RE.pattern}")
    version: Any = spec.get("version")
    if not _is_int(version) or not 1 <= version <= 10000:
        errors.append("version must be an integer 1..10000")
    description = spec.get("description", "")
    if not isinstance(description, str) or len(description) > 500:
        errors.append("description must be a string of at most 500 characters")
        description = ""

    raw_tasks = spec.get("tasks")
    if not isinstance(raw_tasks, list) or not 1 <= len(raw_tasks) <= MAX_TASKS:
        errors.append(f"tasks must be a list of 1..{MAX_TASKS} tasks")
        raise ValidationFailed(errors)

    tasks = [
        t for i, raw in enumerate(raw_tasks) if (t := _validate_task(raw, i, known_types, errors))
    ]
    keys = [t.key for t in tasks]
    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    if duplicates:
        errors.append(f"duplicate task keys {duplicates}")
    key_set = set(keys)
    for t in tasks:
        for dep in t.depends_on:
            if dep not in key_set:
                errors.append(f"task '{t.key}': missing dependency '{dep}'")
    if not duplicates:
        cycle = find_cycle({t.key: t.depends_on for t in tasks})
        if cycle:
            errors.append(f"dependency cycle: {' -> '.join(cycle)}")

    if errors:
        raise ValidationFailed(errors)
    assert isinstance(name, str) and _is_int(version)
    return WorkflowSpec(name=name, version=version, description=description, tasks=tuple(tasks))


def topological_order(spec: WorkflowSpec) -> list[str]:
    """Kahn's algorithm with alphabetical tie-breaking, so the order is deterministic."""
    indegree = {t.key: len(t.depends_on) for t in spec.tasks}
    children: dict[str, list[str]] = {t.key: [] for t in spec.tasks}
    for t in spec.tasks:
        for dep in t.depends_on:
            children[dep].append(t.key)
    ready = [k for k, d in indegree.items() if d == 0]
    heapq.heapify(ready)
    order: list[str] = []
    while ready:
        key = heapq.heappop(ready)
        order.append(key)
        for child in children[key]:
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(ready, child)
    if len(order) != len(spec.tasks):
        raise ValidationFailed(["dependency cycle"])
    return order


def descendants(spec: dict[str, Any], key: str) -> set[str]:
    """All transitive dependents of `key` in a stored (dict) spec."""
    children: dict[str, list[str]] = {}
    for t in spec["tasks"]:
        for dep in t["depends_on"]:
            children.setdefault(dep, []).append(t["key"])
    seen: set[str] = set()
    frontier = list(children.get(key, []))
    while frontier:
        node = frontier.pop()
        if node not in seen:
            seen.add(node)
            frontier.extend(children.get(node, []))
    return seen
