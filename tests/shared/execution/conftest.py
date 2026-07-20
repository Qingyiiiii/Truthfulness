"""Shared bounded fixtures for execution-contract runtime tests."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from video_truthfulness.core.execution.hashing import canonical_json_bytes, embedded_hash


DERIVED_EVENT_FIELDS = {
    "event_hash",
    "event_id",
    "sequence_no",
    "occurred_at",
    "task_id",
    "session_id",
    "attempt_no",
    "run_id",
    "stage_id",
    "dag_node_id",
    "previous_event_id",
    "previous_event_hash",
}


@pytest.fixture(scope="session")
def repository_root() -> Path:
    return Path(__file__).resolve().parents[3]


@pytest.fixture(scope="session")
def synthetic_root(repository_root: Path) -> Path:
    return repository_root / "examples" / "execution_contract" / "synthetic_run"


@pytest.fixture
def manifest_raw(synthetic_root: Path) -> dict[str, Any]:
    return json.loads((synthetic_root / "session_manifest.json").read_text(encoding="utf-8"))


@pytest.fixture
def event_rows(synthetic_root: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in (synthetic_root / "events.jsonl").read_text(encoding="utf-8").splitlines()]


def rehash_contract(raw: dict[str, Any], hash_field: str) -> dict[str, Any]:
    result = copy.deepcopy(raw)
    result[hash_field] = "0" * 64
    result[hash_field] = embedded_hash(result, hash_field)
    return result


def event_draft(row: dict[str, Any]) -> dict[str, Any]:
    return {key: copy.deepcopy(value) for key, value in row.items() if key not in DERIVED_EVENT_FIELDS}


def write_event_rows(path: Path, rows: list[dict[str, Any]]) -> bytes:
    data = b"".join(canonical_json_bytes(row) + b"\n" for row in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return data
