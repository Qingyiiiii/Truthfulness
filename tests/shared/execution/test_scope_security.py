"""Scope-segment and fail-closed security tests for execution contracts."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from video_truthfulness.core.execution.events import (
    EventLog,
    reject_sensitive_material,
    validate_event_stream,
    validate_manifest,
    validate_relative_path,
)
from video_truthfulness.core.execution.hashing import canonical_json_bytes, embedded_hash
from video_truthfulness.core.execution.models import (
    ExecutionContractError,
    ScopeViolationError,
    SensitiveMaterialError,
)


_DERIVED_EVENT_FIELDS = {
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


def rehash_contract(raw: dict[str, Any], hash_field: str) -> dict[str, Any]:
    result = copy.deepcopy(raw)
    result[hash_field] = "0" * 64
    result[hash_field] = embedded_hash(result, hash_field)
    return result


def event_draft(row: dict[str, Any]) -> dict[str, Any]:
    return {key: copy.deepcopy(value) for key, value in row.items() if key not in _DERIVED_EVENT_FIELDS}


def write_event_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(canonical_json_bytes(row) + b"\n" for row in rows))


@pytest.mark.parametrize(
    "path",
    [
        "D:/private/file.json",
        "folder\\file.json",
        "../escape.json",
        "safe/../escape.json",
        "safe/latest/file.json",
        "safe/*.json",
        "$TASK_ROOT/file.json",
        "%TASK_ROOT%/file.json",
    ],
)
def test_path_boundary_rejects_unsafe_forms(path: str) -> None:
    with pytest.raises(ScopeViolationError):
        validate_relative_path(path)


def test_path_boundary_uses_segments_not_string_prefixes() -> None:
    assert validate_relative_path("runs/V02/example/control.json").as_posix().startswith("runs/V02/")
    assert validate_relative_path("safe/latest_file.json").as_posix() == "safe/latest_file.json"


@pytest.mark.parametrize(
    "unsafe_path",
    [
        "D:/private/session_manifest.json",
        "examples/latest/session_manifest.json",
    ],
)
def test_session_started_rejects_unsafe_payload_and_path_ref_after_rehash(
    unsafe_path: str,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    started = copy.deepcopy(event_rows[0])
    started["payload"]["manifest_path"] = unsafe_path
    started["path_refs"][0]["relative_path"] = unsafe_path
    started = rehash_contract(started, "event_hash")

    with pytest.raises(ExecutionContractError):
        validate_event_stream([started], manifest_raw)


@pytest.mark.parametrize("root", [".", "runs", "data"])
def test_manifest_rejects_broad_recursive_roots(root: str, manifest_raw: dict[str, Any]) -> None:
    changed = copy.deepcopy(manifest_raw)
    changed["declared_write_set"] = [
        {"scope_type": "path_prefix", "relative_path": root, "recursive": True, "purpose": "too broad"}
    ]
    changed = rehash_contract(changed, "manifest_hash")
    with pytest.raises(ExecutionContractError):
        validate_manifest(changed)


@pytest.mark.parametrize(
    "root",
    ["runs/V02", "data/V02", "runtime", "cookie", "cookie-catch", "secrets"],
)
def test_manifest_rejects_sensitive_or_shared_recursive_roots(
    root: str, manifest_raw: dict[str, Any]
) -> None:
    changed = copy.deepcopy(manifest_raw)
    changed["declared_write_set"] = [
        {
            "scope_type": "path_prefix",
            "relative_path": root,
            "recursive": True,
            "purpose": "unacceptably shared control root",
        }
    ]
    changed = rehash_contract(changed, "manifest_hash")

    with pytest.raises(ExecutionContractError):
        validate_manifest(changed)


@pytest.mark.parametrize(
    "task_root",
    [
        "runs/V02/run_01j00000000000000000000000/control/tasks/task_01j00000000000000000000000",
        "runtime/V02/execution/tasks/task_01j00000000000000000000000",
    ],
)
def test_manifest_allows_exact_execution_task_roots(
    task_root: str, manifest_raw: dict[str, Any]
) -> None:
    changed = copy.deepcopy(manifest_raw)
    changed["declared_write_set"] = [
        {
            "scope_type": "path_prefix",
            "relative_path": task_root,
            "recursive": True,
            "purpose": "bounded task control files",
        }
    ]
    changed = rehash_contract(changed, "manifest_hash")

    assert validate_manifest(changed).declared_write_set[0].relative_path == task_root


@pytest.mark.parametrize("field", ["bootstrap", "code_ref"])
def test_manifest_rejects_absolute_bootstrap_or_code_path_after_rehash(
    field: str, manifest_raw: dict[str, Any]
) -> None:
    changed = copy.deepcopy(manifest_raw)
    absolute = "D:/private/working_tree_manifest.json"
    if field == "bootstrap":
        changed["bootstrap_refs"][2]["relative_path"] = absolute
    else:
        changed["code_ref"]["working_tree_manifest_path"] = absolute
    changed = rehash_contract(changed, "manifest_hash")

    with pytest.raises(ExecutionContractError):
        validate_manifest(changed)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("created_at", "2026-02-30T00:00:00Z"),
        ("illegal_schema", "not-a-schema-version"),
        ("missing_manifest_schema", None),
    ],
)
def test_manifest_runtime_rejects_schema_parity_violations(
    field: str, value: str | None, manifest_raw: dict[str, Any]
) -> None:
    changed = copy.deepcopy(manifest_raw)
    if field == "created_at":
        changed["created_at"] = value
    elif field == "illegal_schema":
        changed["schema_versions"].append(value)
    else:
        changed["schema_versions"].remove("session_manifest_v1.0.0")
    changed = rehash_contract(changed, "manifest_hash")

    with pytest.raises(ExecutionContractError):
        validate_manifest(changed)


def test_workflow_actor_event_is_schema_legal_and_runtime_accepted(
    manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    started = copy.deepcopy(event_rows[0])
    started["actor"] = {
        "actor_type": "workflow",
        "actor_id": "youtube_truthfulness_workflow_v1.1.0",
        "agent_profile_version": None,
        "agent_runtime_version": None,
    }
    started = rehash_contract(started, "event_hash")

    summary = validate_event_stream([started], manifest_raw)
    assert summary.event_count == 1
    assert summary.frozen is False


def test_recursive_prefix_does_not_match_lookalike_sibling(
    tmp_path: Path, manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    path = tmp_path / "events.jsonl"
    write_event_rows(path, event_rows[:3])
    draft = event_draft(event_rows[4])
    outside = "examples/execution_contract/synthetic_runner/output.json"
    draft["artifact_refs"][0]["relative_path"] = outside
    draft["path_refs"][0]["relative_path"] = outside
    before = path.read_bytes()

    with pytest.raises(ScopeViolationError, match="outside declared scope"):
        EventLog(path, manifest_raw).append(draft)
    assert path.read_bytes() == before


def test_nonrecursive_prefix_allows_one_child_but_not_descendant(
    tmp_path: Path, manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    manifest = copy.deepcopy(manifest_raw)
    manifest["declared_write_set"] = [
        {"scope_type": "path_prefix", "relative_path": "bounded/out", "recursive": False, "purpose": "one level"}
    ]
    manifest = rehash_contract(manifest, "manifest_hash")
    bound_rows = copy.deepcopy(event_rows[:3])
    bound_rows[0]["payload"]["manifest_hash"] = manifest["manifest_hash"]
    bound_rows[0]["path_refs"][0]["content_hash"] = manifest["manifest_hash"]
    bound_rows[0] = rehash_contract(bound_rows[0], "event_hash")
    for index in range(1, len(bound_rows)):
        bound_rows[index]["previous_event_id"] = bound_rows[index - 1]["event_id"]
        bound_rows[index]["previous_event_hash"] = bound_rows[index - 1]["event_hash"]
        bound_rows[index] = rehash_contract(bound_rows[index], "event_hash")

    direct = event_draft(event_rows[4])
    direct["artifact_refs"][0]["relative_path"] = "bounded/out/file.json"
    direct["path_refs"][0]["relative_path"] = "bounded/out/file.json"
    direct_path = tmp_path / "direct.jsonl"
    write_event_rows(direct_path, bound_rows)
    EventLog(direct_path, manifest).append(direct)

    nested = event_draft(event_rows[4])
    nested["artifact_refs"][0]["relative_path"] = "bounded/out/deep/file.json"
    nested["path_refs"][0]["relative_path"] = "bounded/out/deep/file.json"
    nested_path = tmp_path / "nested.jsonl"
    write_event_rows(nested_path, bound_rows)
    with pytest.raises(ScopeViolationError):
        EventLog(nested_path, manifest).append(nested)


@pytest.mark.parametrize(
    "value",
    [
        {"token": "synthetic"},
        {"nested": [{"authorization_header": "synthetic"}]},
        {"summary": "Bearer abc.def.ghi"},
        {"summary": "api_key=synthetic"},
        {"summary": "password: synthetic"},
        {"summary": "C:\\Users\\private\\secret.txt"},
        {"summary": "/home/private/secret.txt"},
        {"summary": "x" * 4097},
    ],
)
def test_sensitive_material_is_rejected_recursively(value: Any) -> None:
    with pytest.raises(SensitiveMaterialError):
        reject_sensitive_material(value)


def test_sensitive_payload_is_rejected_before_schema_acceptance(
    synthetic_root: Path, manifest_raw: dict[str, Any]
) -> None:
    raw = json.loads((synthetic_root / "invalid" / "sensitive_payload_event.json").read_text(encoding="utf-8"))
    with pytest.raises(SensitiveMaterialError):
        validate_event_stream([raw], manifest_raw)


def test_out_of_scope_frozen_fixture_is_rejected(
    synthetic_root: Path, manifest_raw: dict[str, Any]
) -> None:
    rows = [
        json.loads(line)
        for line in (synthetic_root / "invalid" / "out_of_scope_write_events.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    with pytest.raises(ScopeViolationError, match="outside declared scope"):
        validate_event_stream(rows, manifest_raw)
