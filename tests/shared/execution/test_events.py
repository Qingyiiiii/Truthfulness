"""Append-only event-chain, identity and lifecycle tests."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any

import pytest

from video_truthfulness.core.execution.events import (
    EventLog,
    validate_event_stream,
    validate_manifest,
    validate_session_started_file_binding,
)
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_bytes,
)
from video_truthfulness.core.execution.models import (
    EventChainError,
    ExecutionHashError,
    ExecutionSchemaError,
    SessionFrozenError,
    parse_execution_event,
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
    return {
        key: copy.deepcopy(value)
        for key, value in row.items()
        if key not in _DERIVED_EVENT_FIELDS
    }


def v101_manifest(manifest_raw: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(manifest_raw)
    changed["schema_versions"] = [
        "execution_event_v1.0.1" if version == "execution_event_v1.0.0" else version
        for version in changed["schema_versions"]
    ]
    return rehash_contract(changed, "manifest_hash")


def successor_manifest(
    manifest_raw: dict[str, Any], *, stage_id: str = "S01"
) -> dict[str, Any]:
    changed = copy.deepcopy(manifest_raw)
    changed["session_manifest_version"] = "session_manifest_v1.1.0"
    changed["stage_id"] = stage_id
    changed["dag_node_id"] = None
    changed["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    changed["workflow_version"] = (
        "youtube_truthfulness_workflow_v1.3.0"
        if stage_id == "S02"
        else "youtube_truthfulness_workflow_v1.1.0"
    )
    changed["schema_versions"] = [
        "session_manifest_v1.1.0" if value == "session_manifest_v1.0.0" else value
        for value in changed["schema_versions"]
    ]
    return rehash_contract(changed, "manifest_hash")


def v101_event_prefix(
    event_rows: list[dict[str, Any]],
    count: int,
    manifest_hash: str,
) -> list[dict[str, Any]]:
    changed = copy.deepcopy(event_rows[:count])
    for index, event in enumerate(changed):
        event["event_schema_version"] = "execution_event_v1.0.1"
        if index == 0:
            event["payload"]["manifest_hash"] = manifest_hash
        if index > 0:
            event["previous_event_hash"] = changed[index - 1]["event_hash"]
        changed[index] = rehash_contract(event, "event_hash")
    return changed


def write_event_rows(path: Path, rows: list[dict[str, Any]]) -> bytes:
    data = b"".join(canonical_json_bytes(row) + b"\n" for row in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return data


def physical_session_binding(
    repository_root: Path,
    manifest: dict[str, Any],
    event: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    relative = event["payload"]["manifest_path"]
    path = repository_root / relative
    data = canonical_json_bytes(manifest) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    bound = copy.deepcopy(event)
    bound["path_refs"][0]["content_hash"] = sha256_bytes(data)
    bound = rehash_contract(bound, "event_hash")
    return path, bound


def test_session_started_binds_semantic_and_physical_manifest_hashes(
    tmp_path: Path,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    path, event = physical_session_binding(tmp_path, manifest_raw, event_rows[0])

    model = validate_session_started_file_binding(tmp_path, manifest_raw, event)

    assert model.payload["manifest_hash"] == manifest_raw["manifest_hash"]
    assert model.path_refs[0].content_hash == sha256_bytes(path.read_bytes())
    assert model.path_refs[0].content_hash != model.payload["manifest_hash"]


def test_session_started_rejects_tampered_manifest_file_bytes(
    tmp_path: Path,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    path, event = physical_session_binding(tmp_path, manifest_raw, event_rows[0])
    path.write_bytes(path.read_bytes() + b" ")

    with pytest.raises(EventChainError, match="not canonical JSON"):
        validate_session_started_file_binding(tmp_path, manifest_raw, event)


def test_session_started_rejects_wrong_physical_file_hash(
    tmp_path: Path,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    _, event = physical_session_binding(tmp_path, manifest_raw, event_rows[0])
    event["path_refs"][0]["content_hash"] = "0" * 64
    event = rehash_contract(event, "event_hash")

    with pytest.raises(ExecutionHashError, match="physical manifest file"):
        validate_session_started_file_binding(tmp_path, manifest_raw, event)


def test_session_started_rejects_wrong_manifest_path_even_for_identical_bytes(
    tmp_path: Path,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    original, event = physical_session_binding(tmp_path, manifest_raw, event_rows[0])
    wrong_relative = (
        "examples/execution_contract/synthetic_run/not_the_session_manifest.json"
    )
    wrong = tmp_path / wrong_relative
    wrong.parent.mkdir(parents=True, exist_ok=True)
    wrong.write_bytes(original.read_bytes())
    event["payload"]["manifest_path"] = wrong_relative
    event["path_refs"][0]["relative_path"] = wrong_relative
    event = rehash_contract(event, "event_hash")

    with pytest.raises(EventChainError, match="named session_manifest.json"):
        validate_session_started_file_binding(tmp_path, manifest_raw, event)


def test_session_started_rejects_event_identity_drift(
    tmp_path: Path,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    _, event = physical_session_binding(tmp_path, manifest_raw, event_rows[0])
    event["session_id"] = "session_01j00000000000000000000009"
    event = rehash_contract(event, "event_hash")

    with pytest.raises(EventChainError, match="Event identity mismatch for session_id"):
        validate_session_started_file_binding(tmp_path, manifest_raw, event)


def test_complete_synthetic_stream_validates_and_is_frozen(
    manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    summary = validate_event_stream(event_rows, manifest_raw, require_terminal=True)
    assert summary.event_count == 9
    assert summary.terminal_state == "COMPLETED"
    assert summary.checkpoint_id == "checkpoint_01j00000000000000000000000"
    assert summary.frozen is True
    assert summary.head_event_id == event_rows[-1]["event_id"]


@pytest.mark.parametrize("event_index", [3, 4])
def test_event_v101_runtime_rejects_read_or_write_without_any_observed_reference(
    event_index: int,
    event_rows: list[dict[str, Any]],
) -> None:
    changed = copy.deepcopy(event_rows[event_index])
    changed["event_schema_version"] = "execution_event_v1.0.1"
    changed["artifact_refs"] = []
    changed["path_refs"] = []
    changed = rehash_contract(changed, "event_hash")

    with pytest.raises(
        ExecutionSchemaError, match=r"at least one Artifact ref or path\+hash ref"
    ):
        parse_execution_event(changed)


@pytest.mark.parametrize(
    ("event_index", "retained_ref"), [(3, "path_refs"), (4, "artifact_refs")]
)
def test_event_v101_runtime_preserves_path_only_and_artifact_only_access(
    event_index: int,
    retained_ref: str,
    event_rows: list[dict[str, Any]],
) -> None:
    changed = copy.deepcopy(event_rows[event_index])
    changed["event_schema_version"] = "execution_event_v1.0.1"
    for field in {"artifact_refs", "path_refs"} - {retained_ref}:
        changed[field] = []
    changed = rehash_contract(changed, "event_hash")

    assert (
        parse_execution_event(changed).event_schema_version == "execution_event_v1.0.1"
    )


@pytest.mark.parametrize("event_index", [3, 4])
def test_event_v101_runtime_accepts_both_observed_reference_kinds(
    event_index: int,
    event_rows: list[dict[str, Any]],
) -> None:
    changed = copy.deepcopy(event_rows[event_index])
    changed["event_schema_version"] = "execution_event_v1.0.1"
    changed = rehash_contract(changed, "event_hash")

    model = parse_execution_event(changed)
    assert model.artifact_refs
    assert model.path_refs


def test_event_v101_runtime_allows_non_access_event_without_observed_references(
    event_rows: list[dict[str, Any]],
) -> None:
    changed = copy.deepcopy(event_rows[1])
    changed["event_schema_version"] = "execution_event_v1.0.1"
    assert changed["artifact_refs"] == []
    assert changed["path_refs"] == []
    changed = rehash_contract(changed, "event_hash")

    assert parse_execution_event(changed).event_type == "task.created"


@pytest.mark.parametrize("event_index", [3, 4])
def test_event_v100_runtime_retains_frozen_empty_reference_compatibility(
    event_index: int,
    event_rows: list[dict[str, Any]],
) -> None:
    changed = copy.deepcopy(event_rows[event_index])
    changed["artifact_refs"] = []
    changed["path_refs"] = []
    changed = rehash_contract(changed, "event_hash")

    assert (
        parse_execution_event(changed).event_schema_version == "execution_event_v1.0.0"
    )


def test_event_log_defaults_to_the_event_schema_declared_by_the_manifest(
    tmp_path: Path,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    manifest_v101 = v101_manifest(manifest_raw)
    draft = event_draft(event_rows[0])
    draft.pop("event_schema_version")
    draft["payload"]["manifest_hash"] = manifest_v101["manifest_hash"]

    model = EventLog(tmp_path / "events.jsonl", manifest_v101).append(
        draft,
        event_id=event_rows[0]["event_id"],
        occurred_at=event_rows[0]["occurred_at"],
    )

    assert model.event_schema_version == "execution_event_v1.0.1"


@pytest.mark.parametrize("event_index", [3, 4])
def test_event_v101_append_rejects_empty_access_refs_without_writing(
    event_index: int,
    tmp_path: Path,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    manifest_v101 = v101_manifest(manifest_raw)
    prefix = v101_event_prefix(event_rows, event_index, manifest_v101["manifest_hash"])
    path = tmp_path / "events.jsonl"
    before = write_event_rows(path, prefix)
    draft = event_draft(event_rows[event_index])
    draft["event_schema_version"] = "execution_event_v1.0.1"
    draft["artifact_refs"] = []
    draft["path_refs"] = []

    with pytest.raises(
        ExecutionSchemaError, match=r"at least one Artifact ref or path\+hash ref"
    ):
        EventLog(path, manifest_v101).append(
            draft,
            event_id=event_rows[event_index]["event_id"],
            occurred_at=event_rows[event_index]["occurred_at"],
        )

    assert path.read_bytes() == before


def test_event_stream_rejects_schema_version_not_declared_by_manifest(
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    changed = copy.deepcopy(event_rows[0])
    changed["event_schema_version"] = "execution_event_v1.0.1"
    changed = rehash_contract(changed, "event_hash")

    with pytest.raises(EventChainError, match="does not match the Session manifest"):
        validate_event_stream([changed], manifest_raw)


@pytest.mark.parametrize("declaration", ["missing", "dual", "unknown"])
def test_manifest_requires_exactly_one_supported_event_schema_version(
    declaration: str,
    manifest_raw: dict[str, Any],
) -> None:
    changed = copy.deepcopy(manifest_raw)
    if declaration == "missing":
        changed["schema_versions"].remove("execution_event_v1.0.0")
    elif declaration == "dual":
        changed["schema_versions"].append("execution_event_v1.0.1")
    else:
        changed["schema_versions"] = [
            "execution_event_v9.9.9" if version == "execution_event_v1.0.0" else version
            for version in changed["schema_versions"]
        ]
    changed = rehash_contract(changed, "manifest_hash")

    with pytest.raises(ExecutionSchemaError, match="exactly one supported"):
        validate_manifest(changed)


@pytest.mark.parametrize(
    ("stage_id", "workflow_version"),
    [
        ("S01", "youtube_truthfulness_workflow_v1.1.0"),
        ("S02", "youtube_truthfulness_workflow_v1.3.0"),
        ("S03", "youtube_truthfulness_workflow_v1.1.0"),
    ],
)
def test_successor_manifest_accepts_only_stage_scoped_workflow(
    manifest_raw: dict[str, Any], stage_id: str, workflow_version: str
) -> None:
    model = validate_manifest(successor_manifest(manifest_raw, stage_id=stage_id))
    assert model.workflow_version == workflow_version
    assert model.dag_node_id is None


def test_successor_manifest_rejects_node_scope_or_wrong_workflow(
    manifest_raw: dict[str, Any],
) -> None:
    node_scoped = successor_manifest(manifest_raw)
    node_scoped["dag_node_id"] = "source_identity"
    node_scoped = rehash_contract(node_scoped, "manifest_hash")
    with pytest.raises(ExecutionSchemaError, match="dag_node_id=null"):
        validate_manifest(node_scoped)

    wrong_workflow = successor_manifest(manifest_raw, stage_id="S02")
    wrong_workflow["workflow_version"] = "youtube_truthfulness_workflow_v1.1.0"
    wrong_workflow = rehash_contract(wrong_workflow, "manifest_hash")
    with pytest.raises(ExecutionSchemaError, match="does not match stage"):
        validate_manifest(wrong_workflow)


@pytest.mark.parametrize("version", [None, "execution_event_v9.9.9"])
def test_event_runtime_rejects_missing_or_unknown_schema_version(
    version: str | None,
    event_rows: list[dict[str, Any]],
) -> None:
    changed = copy.deepcopy(event_rows[0])
    if version is None:
        changed.pop("event_schema_version")
    else:
        changed["event_schema_version"] = version

    with pytest.raises(ExecutionSchemaError, match="unsupported or missing"):
        parse_execution_event(changed)


def test_event_hash_tampering_is_detected(
    manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    tampered = copy.deepcopy(event_rows)
    tampered[3]["payload"]["purpose"] = "changed after publication"
    with pytest.raises(ExecutionHashError, match="event_hash mismatch"):
        validate_event_stream(tampered, manifest_raw)


def test_broken_history_causes_zero_append(
    tmp_path: Path, manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    broken = copy.deepcopy(event_rows[:3])
    broken[2]["previous_event_hash"] = "0" * 64
    broken[2] = rehash_contract(broken[2], "event_hash")
    path = tmp_path / "events.jsonl"
    before = write_event_rows(path, broken)
    log = EventLog(path, manifest_raw)

    with pytest.raises(EventChainError, match="Broken previous-event link"):
        log.append(event_draft(event_rows[3]))

    assert path.read_bytes() == before


def test_append_derives_chain_fields_fsyncs_and_revalidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    path = tmp_path / "events.jsonl"
    calls: list[int] = []
    real_fsync = os.fsync

    def observing_fsync(fd: int) -> None:
        calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr(
        "video_truthfulness.core.execution.events.os.fsync", observing_fsync
    )
    model = EventLog(path, manifest_raw).append(
        event_draft(event_rows[0]),
        event_id=event_rows[0]["event_id"],
        occurred_at=event_rows[0]["occurred_at"],
    )

    assert calls
    assert model.sequence_no == 1
    assert model.previous_event_id is None
    assert model.previous_event_hash is None
    assert (
        path.read_bytes() == canonical_json_bytes(model.model_dump(mode="json")) + b"\n"
    )
    assert EventLog(path, manifest_raw).validate().head_event_hash == model.event_hash


def test_post_write_revalidation_failure_is_fail_closed_without_fake_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    path = tmp_path / "events.jsonl"
    before = write_event_rows(path, event_rows[:3])
    log = EventLog(path, manifest_raw)

    def fail_after_write(*, require_terminal: bool = False) -> None:
        del require_terminal
        raise EventChainError("synthetic post-write verification failure")

    monkeypatch.setattr(log, "validate", fail_after_write)
    with pytest.raises(EventChainError, match="history was not truncated"):
        log.append(
            event_draft(event_rows[3]),
            event_id=event_rows[3]["event_id"],
            occurred_at=event_rows[3]["occurred_at"],
        )

    after = path.read_bytes()
    assert after.startswith(before)
    assert len(after) > len(before)
    assert EventLog(path, manifest_raw).validate().event_count == 4


def test_append_rejects_conflicting_derived_identity_without_writing(
    tmp_path: Path, manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    path = tmp_path / "events.jsonl"
    draft = event_draft(event_rows[0])
    draft["task_id"] = "task_01j00000000000000000000009"

    with pytest.raises(EventChainError, match="Caller-supplied task_id conflicts"):
        EventLog(path, manifest_raw).append(draft)

    assert not path.exists()


def test_terminal_allows_only_checkpoint_then_handoff_and_freezes(
    tmp_path: Path, manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    path = tmp_path / "events.jsonl"
    write_event_rows(path, event_rows[:7])
    log = EventLog(path, manifest_raw)
    terminal_bytes = path.read_bytes()

    with pytest.raises(EventChainError, match="Illegal event after terminal"):
        log.append(event_draft(event_rows[3]))
    assert path.read_bytes() == terminal_bytes

    log.append(
        event_draft(event_rows[7]),
        event_id=event_rows[7]["event_id"],
        occurred_at=event_rows[7]["occurred_at"],
    )
    log.append(
        event_draft(event_rows[8]),
        event_id=event_rows[8]["event_id"],
        occurred_at=event_rows[8]["occurred_at"],
    )
    frozen_bytes = path.read_bytes()
    assert log.validate(require_terminal=True).frozen is True

    with pytest.raises(SessionFrozenError, match="frozen"):
        log.append(event_draft(event_rows[3]))
    assert path.read_bytes() == frozen_bytes


def test_checkpoint_before_terminal_and_duplicate_terminal_are_rejected(
    manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    checkpoint = copy.deepcopy(event_rows[7])
    checkpoint["sequence_no"] = 4
    checkpoint["previous_event_id"] = event_rows[2]["event_id"]
    checkpoint["previous_event_hash"] = event_rows[2]["event_hash"]
    checkpoint = rehash_contract(checkpoint, "event_hash")
    with pytest.raises(EventChainError, match="requires a preceding terminal"):
        validate_event_stream([*event_rows[:3], checkpoint], manifest_raw)

    duplicate = copy.deepcopy(event_rows[6])
    duplicate["event_id"] = "event_01j00000000000000000000008"
    duplicate["sequence_no"] = 8
    duplicate["previous_event_id"] = event_rows[6]["event_id"]
    duplicate["previous_event_hash"] = event_rows[6]["event_hash"]
    duplicate = rehash_contract(duplicate, "event_hash")
    with pytest.raises(
        EventChainError, match="Illegal event after terminal|only one terminal"
    ):
        validate_event_stream([*event_rows[:7], duplicate], manifest_raw)


@pytest.mark.parametrize(
    ("payload_update", "message"),
    [
        (
            {"new_session_id": "session_01j00000000000000000000000"},
            "current retry Session",
        ),
        ({"new_attempt_no": 3}, "current retry attempt"),
        (
            {"parent_checkpoint_id": "checkpoint_01j00000000000000000000009"},
            "match the retry Session manifest",
        ),
    ],
)
def test_retry_requires_new_session_and_exact_next_attempt(
    payload_update: dict[str, Any],
    message: str,
    manifest_raw: dict[str, Any],
    event_rows: list[dict[str, Any]],
) -> None:
    retry_manifest = copy.deepcopy(manifest_raw)
    retry_manifest["session_id"] = "session_01j00000000000000000000001"
    retry_manifest["attempt_no"] = 2
    retry_manifest["parent_checkpoint_id"] = "checkpoint_01j00000000000000000000000"
    retry_manifest = rehash_contract(retry_manifest, "manifest_hash")

    started = copy.deepcopy(event_rows[0])
    started["session_id"] = retry_manifest["session_id"]
    started["attempt_no"] = retry_manifest["attempt_no"]
    started["payload"]["manifest_hash"] = retry_manifest["manifest_hash"]
    started["path_refs"][0]["content_hash"] = retry_manifest["manifest_hash"]
    started = rehash_contract(started, "event_hash")

    retry = copy.deepcopy(event_rows[2])
    retry.update(
        {
            "event_id": "event_01j00000000000000000000002",
            "sequence_no": 2,
            "occurred_at": "2026-01-01T00:00:02Z",
            "event_type": "task.retried",
            "session_id": retry_manifest["session_id"],
            "attempt_no": retry_manifest["attempt_no"],
            "artifact_refs": [],
            "path_refs": [],
            "checkpoint_id": None,
            "payload": {
                "new_session_id": "session_01j00000000000000000000001",
                "new_attempt_no": 2,
                "parent_checkpoint_id": "checkpoint_01j00000000000000000000000",
                "change_summary": "use a newly approved bounded input",
            },
            "previous_event_id": started["event_id"],
            "previous_event_hash": started["event_hash"],
        }
    )
    retry = rehash_contract(retry, "event_hash")
    assert validate_event_stream([started, retry], retry_manifest).event_count == 2
    retry["payload"].update(payload_update)
    retry = rehash_contract(retry, "event_hash")

    with pytest.raises(EventChainError, match=message):
        validate_event_stream([started, retry], retry_manifest)


def test_event_identity_must_equal_manifest(
    manifest_raw: dict[str, Any], event_rows: list[dict[str, Any]]
) -> None:
    changed = copy.deepcopy(event_rows[:2])
    changed[1]["session_id"] = "session_01j00000000000000000000009"
    changed[1] = rehash_contract(changed[1], "event_hash")
    with pytest.raises(EventChainError, match="identity mismatch for session_id"):
        validate_event_stream(changed, manifest_raw)


def test_frozen_negative_fixtures_fail_for_the_intended_invariant(
    synthetic_root: Path, manifest_raw: dict[str, Any]
) -> None:
    expected = {
        "broken_event_chain.jsonl": "Broken previous-event link",
        "duplicate_event_id.jsonl": "Duplicate event_id",
        "post_terminal_event.jsonl": "Illegal event after terminal",
    }
    for name, message in expected.items():
        rows = [
            json.loads(line)
            for line in (synthetic_root / "invalid" / name)
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        with pytest.raises(EventChainError, match=message):
            validate_event_stream(rows, manifest_raw)
