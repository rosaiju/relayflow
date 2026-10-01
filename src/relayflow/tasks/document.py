"""Deterministic document-processing task types for the demonstration workflow.

No external services or AI models: plain text statistics over a bounded,
synthetic document stored in the run input (so any worker can process it).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from typing import Any

from relayflow.tasks.registry import PermanentTaskError, TaskContext, task_type

MAX_TITLE_CHARS = 200
MAX_TEXT_CHARS = 20_000
WORD_RE = re.compile(r"[a-z0-9]+(?:'[a-z]+)?")

STOPWORDS = frozenset(
    """
    a about above after again against all am an and any are as at be because been before being
    below between both but by can could did do does doing down during each few for from further
    had has have having he her here hers him his how i if in into is it its itself just me more
    most my no nor not now of off on once only or other our ours out over own same she should
    so some such than that the their theirs them then there these they this those through to
    too under until up very was we were what when where which while who whom why will with
    would you your yours also may might must shall into than then
    """.split()  # noqa: SIM905 - a word list is easier to read and edit this way
)


def _document(task_input: dict[str, Any]) -> tuple[str, str]:
    run_input = task_input.get("run_input") or {}
    title, text = run_input.get("title"), run_input.get("text")
    if not isinstance(title, str) or not isinstance(text, str):
        raise PermanentTaskError("document requires string 'title' and 'text'")
    return title, text


def words(text: str) -> list[str]:
    return WORD_RE.findall(text.lower())


@task_type("doc.validate")
def validate(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    ctx.check()
    title, text = _document(task_input)
    problems = []
    if not 1 <= len(title.strip()) <= MAX_TITLE_CHARS:
        problems.append(f"title must be 1..{MAX_TITLE_CHARS} characters")
    if not 1 <= len(text.strip()) <= MAX_TEXT_CHARS:
        problems.append(f"text must be 1..{MAX_TEXT_CHARS} characters")
    if any(not (ch.isprintable() or ch in "\n\r\t") for ch in text):
        problems.append("text contains non-printable characters")
    if not words(text):
        problems.append("text contains no words")
    if problems:
        raise PermanentTaskError("; ".join(problems))
    return {
        "valid": True,
        "title": title.strip(),
        "characters": len(text),
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
    }


@task_type("doc.word_count")
def word_count(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    ctx.check()
    _, text = _document(task_input)
    tokens = words(text)
    return {
        "words": len(tokens),
        "unique_words": len(set(tokens)),
        "lines": len(text.splitlines()) or 1,
        "characters": len(text),
        "average_word_length": round(sum(map(len, tokens)) / len(tokens), 2) if tokens else 0.0,
    }


@task_type("doc.keywords")
def keywords(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    ctx.check()
    _, text = _document(task_input)
    top_n = (task_input.get("params") or {}).get("top_n", 8)
    if not isinstance(top_n, int) or not 1 <= top_n <= 50:
        raise PermanentTaskError("top_n must be an integer 1..50")
    counts = Counter(w for w in words(text) if len(w) >= 3 and w not in STOPWORDS)
    # Sort by descending count, then alphabetically, so results are deterministic.
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
    return {"keywords": [{"term": term, "count": count} for term, count in ranked]}


@task_type("doc.report")
def report(ctx: TaskContext, task_input: dict[str, Any]) -> dict[str, Any]:
    ctx.check()
    title, text = _document(task_input)
    deps = task_input.get("deps") or {}  # direct dependencies only (spec 6.2)
    try:
        counts, keys = deps["word_count"], deps["keywords"]
    except KeyError as exc:
        raise PermanentTaskError(f"report is missing dependency output {exc}") from None
    body = {
        "title": title.strip(),
        "document_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "statistics": counts,
        "top_keywords": [k["term"] for k in keys["keywords"]],
    }
    digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    summary = (
        f"'{body['title']}': {counts['words']} words, {counts['unique_words']} unique; "
        f"top keywords: {', '.join(body['top_keywords'][:5]) or '(none)'}"
    )
    return {**body, "summary": summary, "report_digest": digest}
