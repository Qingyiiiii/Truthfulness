from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from video_truthfulness.core.artifacts.dag import load_dag
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
)
from video_truthfulness.core.execution.events import validate_manifest
from video_truthfulness.core.execution.models import ExecutionHashError
from video_truthfulness.core.execution import state as state_runtime
from video_truthfulness.core.execution.state import (
    StateProjectionError,
    build_current_state,
    current_state_bytes,
    snapshot_registry,
    validate_current_state,
    validate_state_projection,
    write_current_state,
)


ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "execution_contract" / "synthetic_run"
RUN_ID = "run_01j00000000000000000000000"
RECORD_2 = "record_01j00000000000000000000002"


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _sources(
    *, head_record_id: str | None = None
) -> tuple[dict[str, Any], list[dict[str, Any]], Any, Any]:
    manifest = _json(EXAMPLE / "session_manifest.json")
    events = _jsonl(EXAMPLE / "events.jsonl")
    snapshot = snapshot_registry(
        EXAMPLE / "artifact_registry.jsonl",
        scope="run",
        expected_run_id=RUN_ID,
        head_record_id=head_record_id,
        repository_root=ROOT,
    )
    dag = load_dag(EXAMPLE / "youtube_truthfulness_dag_v1_1.yaml")
    return manifest, events, snapshot, dag


def _rehash_from(events: list[dict[str, Any]], start: int) -> list[dict[str, Any]]:
    rows = copy.deepcopy(events)
    for index in range(start, len(rows)):
        if index:
            rows[index]["previous_event_id"] = rows[index - 1]["event_id"]
            rows[index]["previous_event_hash"] = rows[index - 1]["event_hash"]
        rows[index]["event_hash"] = "0" * 64
        rows[index]["event_hash"] = embedded_hash(rows[index], "event_hash")
    return rows


def _successor_state_raw(*, stage_id: str = "S01") -> dict[str, Any]:
    raw = _json(EXAMPLE / "current_state.json")
    raw["current_state_version"] = "current_state_v1.1.0"
    raw["stage_id"] = stage_id
    raw["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    raw["workflow_version"] = (
        "youtube_truthfulness_workflow_v1.3.0"
        if stage_id == "S02"
        else "youtube_truthfulness_workflow_v1.1.0"
    )
    raw["state_hash"] = embedded_hash(raw, "state_hash")
    return raw


@pytest.mark.parametrize("stage_id", ["S01", "S02", "S03"])
def test_current_state_successor_accepts_stage_workflow(stage_id: str) -> None:
    validate_current_state(_successor_state_raw(stage_id=stage_id))


def test_current_state_successor_rejects_wrong_stage_workflow() -> None:
    raw = _successor_state_raw(stage_id="S02")
    raw["workflow_version"] = "youtube_truthfulness_workflow_v1.1.0"
    raw["state_hash"] = embedded_hash(raw, "state_hash")
    with pytest.raises(StateProjectionError, match="does not match stage"):
        validate_current_state(raw)


def test_build_current_state_uses_stage_workflow_from_dag_v12() -> None:
    manifest, events, snapshot, _ = _sources()
    manifest["session_manifest_version"] = "session_manifest_v1.1.0"
    manifest["dag_node_id"] = None
    manifest["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    manifest["schema_versions"] = [
        {
            "session_manifest_v1.0.0": "session_manifest_v1.1.0",
            "current_state_v1.0.0": "current_state_v1.1.0",
            "execution_checkpoint_v1.0.0": "execution_checkpoint_v1.1.0",
            "handoff_v2.0.0": "handoff_v2.1.0",
        }.get(value, value)
        for value in manifest["schema_versions"]
    ]
    manifest["manifest_hash"] = embedded_hash(manifest, "manifest_hash")
    for event in events:
        event["dag_node_id"] = None
    events[0]["payload"]["manifest_hash"] = manifest["manifest_hash"]
    events = _rehash_from(events, 0)
    dag = load_dag(ROOT / "configs/workflows/youtube_truthfulness_dag_v1_2.yaml")

    state = build_current_state(manifest, events, [snapshot], dag)

    assert state["current_state_version"] == "current_state_v1.1.0"
    assert state["dag_version"] == "youtube_truthfulness_dag_v1.2.0"
    assert state["workflow_version"] == "youtube_truthfulness_workflow_v1.1.0"
    validate_current_state(state)


def test_successor_state_exposes_only_current_and_adjacent_stage_candidates() -> None:
    manifest = _json(EXAMPLE / "session_manifest.json")
    manifest["session_manifest_version"] = "session_manifest_v1.1.0"
    manifest["dag_node_id"] = None
    manifest["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    manifest["schema_versions"] = [
        "session_manifest_v1.1.0" if value == "session_manifest_v1.0.0" else value
        for value in manifest["schema_versions"]
    ]
    manifest["manifest_hash"] = embedded_hash(manifest, "manifest_hash")
    manifest_model = validate_manifest(manifest)
    dag = load_dag(ROOT / "configs/workflows/youtube_truthfulness_dag_v1_2.yaml")
    decision = SimpleNamespace(
        artifact_id="artifact_01j00000000000000000000009",
        record_revision=1,
        storage_scope="run",
        run_id=manifest["run_id"],
        validation_status="passed",
        lifecycle_state="frozen",
        dag_node_id="source_depth_decision",
        artifact_type="source_depth.decision",
    )

    candidates = state_runtime._candidate_nodes(dag, [decision], manifest_model)

    assert {item["stage_id"] for item in candidates} <= {"S01", "S02"}
    assert {item["node_id"] for item in candidates if item["stage_id"] == "S02"} == {
        "screening_sync",
        "source_depth_prompt",
    }


def _rehash_state(state: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(state)
    changed["state_hash"] = "0" * 64
    changed["state_hash"] = embedded_hash(changed, "state_hash")
    return changed


def test_static_state_hash_mismatch_fixture_is_rejected() -> None:
    state = _json(EXAMPLE / "invalid" / "state_hash_mismatch.json")

    with pytest.raises(ExecutionHashError, match="state_hash mismatch"):
        validate_current_state(state)


def _write_registry(path: Path, records: list[dict[str, Any]]) -> None:
    path.write_bytes(
        b"".join(canonical_json_bytes(record) + b"\n" for record in records)
    )


def _new_record(base: dict[str, Any], **updates: Any) -> dict[str, Any]:
    record = copy.deepcopy(base)
    record.update(updates)
    record["record_hash"] = "0" * 64
    record["record_hash"] = embedded_hash(record, "record_hash")
    return record


def test_final_projection_is_schema_valid_and_uses_only_authoritative_sources() -> None:
    manifest, events, snapshot, dag = _sources()
    state = build_current_state(manifest, events, [snapshot], dag)

    schema = _json(ROOT / "schemas" / "execution" / "current_state_v1.schema.json")
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(state)
    validate_state_projection(state, manifest, events, [snapshot], dag)
    assert state["status"] == "COMPLETED"
    assert state["event_count"] == 9
    assert state["registry_heads"][0]["head_record_id"].endswith("00003")
    assert [item["event_id"] for item in state["actual_read_set"]] == [
        events[3]["event_id"]
    ]
    assert [item["event_id"] for item in state["actual_write_set"]] == [
        events[4]["event_id"]
    ]
    assert {item["artifact_type"] for item in state["input_artifacts"]} == {
        "synthetic.input"
    }
    assert {item["artifact_type"] for item in state["output_artifacts"]} == {
        "synthetic.output"
    }
    assert state["invalidated_artifacts"] == []
    assert state["validation_summary"]["overall_status"] == "passed"
    assert state["pending_human_decisions"] == []
    candidate_ids = {item["node_id"] for item in state["candidate_next_nodes"]}
    assert "source_identity" in candidate_ids
    assert "source_depth_prompt" not in candidate_ids
    assert all(
        "HANDOFF" not in item["reason"] for item in state["candidate_next_nodes"]
    )
    assert {item["action_key"] for item in state["completed_actions"]} == {
        "task_completed",
        "checkpoint_created",
        "handoff_finalized",
    }
    assert "execute_source_identity" in {
        item["action_key"] for item in state["remaining_actions"]
    }


def test_historical_registry_prefix_projects_terminal_boundary() -> None:
    manifest, events, snapshot, dag = _sources(head_record_id=RECORD_2)
    state = build_current_state(manifest, events[:7], [snapshot], dag)
    checkpoint = _json(next((EXAMPLE / "checkpoints").glob("checkpoint_*.json")))

    assert snapshot.head() == checkpoint["registry_heads"][0]
    assert state["event_count"] == 7
    assert state["as_of_event_id"] == events[6]["event_id"]
    assert state["registry_heads"][0]["record_count"] == 2
    assert {item["action_key"] for item in state["completed_actions"]} == {
        "task_completed"
    }


def test_delete_and_rebuild_produces_identical_canonical_bytes_and_file_hash() -> None:
    manifest, events, snapshot, dag = _sources()
    first = build_current_state(manifest, events, [snapshot], dag)
    temp_parent = ROOT / "tmp"
    temp_parent.mkdir(parents=True, exist_ok=True)
    target = temp_parent / "wp4_state_delete_rebuild_test.json"
    assert not target.exists(), f"bounded test target already exists: {target}"
    try:
        first_file_hash = write_current_state(target, first)
        first_bytes = target.read_bytes()
        target.unlink()

        second = build_current_state(manifest, events, [snapshot], dag)
        second_file_hash = write_current_state(target, second)
        assert first["state_hash"] == second["state_hash"]
        assert first_file_hash == second_file_hash
        assert first_bytes == target.read_bytes() == current_state_bytes(second)
    finally:
        target.unlink(missing_ok=True)


def test_dag_node_input_order_does_not_change_projection() -> None:
    manifest, events, snapshot, dag = _sources()
    baseline = build_current_state(manifest, events, [snapshot], dag)
    reordered = dag.model_copy(update={"nodes": list(reversed(dag.nodes))})
    rebuilt = build_current_state(
        manifest, events, tuple(reversed([snapshot])), reordered
    )
    assert current_state_bytes(rebuilt) == current_state_bytes(baseline)


def test_pure_build_does_not_read_handoff_stat_mtime_or_wall_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest, events, snapshot, dag = _sources()

    def forbidden(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("pure state projection attempted filesystem/time access")

    monkeypatch.setattr(Path, "read_text", forbidden)
    monkeypatch.setattr(Path, "stat", forbidden)
    state = build_current_state(manifest, events, [snapshot], dag)
    assert state["as_of_occurred_at"] == events[-1]["occurred_at"]


@pytest.mark.parametrize(
    "mutation", ["unknown_record", "artifact_mismatch", "metadata_mismatch"]
)
def test_event_registry_cross_reference_mismatch_is_rejected(mutation: str) -> None:
    manifest, events, snapshot, dag = _sources()
    changed = copy.deepcopy(events)
    if mutation == "unknown_record":
        changed[3]["artifact_refs"][0]["record_id"] = (
            "record_01j00000000000000000000999"
        )
    elif mutation == "artifact_mismatch":
        changed[3]["artifact_refs"][0]["content_hash"] = "0" * 64
        changed[3]["path_refs"][0]["content_hash"] = "0" * 64
    else:
        changed[3]["artifact_refs"][0].update(
            {
                "input_fingerprint": "0" * 64,
                "validation_status": "failed",
                "lifecycle_state": "invalid",
            }
        )
    changed = _rehash_from(changed, 3)
    with pytest.raises(StateProjectionError, match="absent|mismatch"):
        build_current_state(manifest, changed, [snapshot], dag)


def test_wrong_dag_workflow_pair_is_rejected() -> None:
    manifest, events, snapshot, dag = _sources()
    wrong = dag.model_copy(
        update={
            "dag_version": "youtube_truthfulness_dag_v1.0.0",
            "workflow_version": "youtube_truthfulness_workflow_v1.0.0",
        }
    )
    with pytest.raises(StateProjectionError, match="does not match"):
        build_current_state(manifest, events, [snapshot], wrong)


def test_state_hash_tampering_is_rejected() -> None:
    manifest, events, snapshot, dag = _sources()
    state = build_current_state(manifest, events, [snapshot], dag)
    tampered = copy.deepcopy(state)
    tampered["remaining_actions"][0]["summary"] = "tampered"
    with pytest.raises(ExecutionHashError, match="state_hash mismatch"):
        validate_state_projection(tampered, manifest, events, [snapshot], dag)


@pytest.mark.parametrize(
    "mutation", ["nested_extra", "bad_timestamp", "bad_task_id", "wrong_nested_type"]
)
def test_current_state_runtime_validation_is_strict_below_top_level(
    mutation: str,
) -> None:
    manifest, events, snapshot, dag = _sources()
    changed = build_current_state(manifest, events, [snapshot], dag)
    if mutation == "nested_extra":
        changed["registry_heads"][0]["unexpected"] = True
    elif mutation == "bad_timestamp":
        changed["as_of_occurred_at"] = "2026-02-30T00:00:00Z"
    elif mutation == "bad_task_id":
        changed["task_id"] = "task_not_a_canonical_ulid"
    else:
        changed["registry_heads"][0]["record_count"] = "3"
    changed = _rehash_state(changed)
    schema = _json(ROOT / "schemas" / "execution" / "current_state_v1.schema.json")
    assert not Draft202012Validator(schema, format_checker=FormatChecker()).is_valid(
        changed
    )
    with pytest.raises(StateProjectionError):
        validate_current_state(changed)


def test_path_only_artifact_read_is_preserved_as_unbound_observed_access() -> None:
    manifest, events, snapshot, dag = _sources()
    changed = copy.deepcopy(events)
    changed[3]["artifact_refs"] = []
    changed = _rehash_from(changed, 3)
    state = build_current_state(manifest, changed, [snapshot], dag)
    assert state["actual_read_set"] == [
        {
            "artifact_id": None,
            "record_id": None,
            "relative_path": changed[3]["path_refs"][0]["relative_path"],
            "content_hash_algorithm": "sha256",
            "content_hash": changed[3]["path_refs"][0]["content_hash"],
            "event_id": changed[3]["event_id"],
        }
    ]


def test_path_only_artifact_write_is_preserved_as_unbound_observed_access() -> None:
    manifest, events, snapshot, dag = _sources()
    changed = copy.deepcopy(events[:5])
    changed[4]["artifact_refs"] = []
    changed = _rehash_from(changed, 4)
    state = build_current_state(manifest, changed, [snapshot], dag)
    assert state["actual_write_set"] == [
        {
            "artifact_id": None,
            "record_id": None,
            "relative_path": changed[4]["path_refs"][0]["relative_path"],
            "content_hash_algorithm": "sha256",
            "content_hash": changed[4]["path_refs"][0]["content_hash"],
            "event_id": changed[4]["event_id"],
        }
    ]


def test_invalid_registry_output_cannot_unlock_a_dag_candidate() -> None:
    manifest, events, _, dag = _sources()
    records = _jsonl(EXAMPLE / "artifact_registry.jsonl")
    invalid = _new_record(
        records[0],
        artifact_id="artifact_01j00000000000000000001000",
        record_id="record_01j00000000000000000001000",
        artifact_type="run.identity",
        dag_node_id="source_identity",
        lifecycle_state="invalid",
        validation_status="failed",
        logical_name="Invalid synthetic run identity",
    )
    target = ROOT / "tmp" / "wp4_state_invalid_candidate_registry.jsonl"
    assert not target.exists()
    try:
        _write_registry(target, [*records, invalid])
        snapshot = snapshot_registry(
            target, scope="run", expected_run_id=RUN_ID, repository_root=ROOT
        )
        state = build_current_state(manifest, events, [snapshot], dag)
        assert "acquisition_decision" not in {
            candidate["node_id"] for candidate in state["candidate_next_nodes"]
        }
    finally:
        target.unlink(missing_ok=True)


def test_cross_run_registry_can_unlock_only_a_cross_run_stage_candidate() -> None:
    manifest, events, _, dag = _sources()
    manifest = copy.deepcopy(manifest)
    manifest.update(
        {
            "task_scope": "cross_run",
            "run_id": None,
            "stage_id": "S03",
            "dag_node_id": "screening_pool_update",
        }
    )
    manifest["manifest_hash"] = "0" * 64
    manifest["manifest_hash"] = embedded_hash(manifest, "manifest_hash")

    changed_events = copy.deepcopy(events[:3])
    for event in changed_events:
        event.update(
            {"run_id": None, "stage_id": "S03", "dag_node_id": "screening_pool_update"}
        )
    changed_events[0]["payload"]["manifest_hash"] = manifest["manifest_hash"]
    changed_events[0]["path_refs"][0]["content_hash"] = manifest["manifest_hash"]
    changed_events[1]["payload"]["task_scope"] = "cross_run"
    changed_events = _rehash_from(changed_events, 0)

    source = _jsonl(EXAMPLE / "artifact_registry.jsonl")[0]
    cross_record = _new_record(
        source,
        storage_scope="cross_run",
        run_id=None,
        batch_id="batch_01j00000000000000000000000",
        artifact_type="screening.sync_record",
        dag_node_id="screening_sync",
        logical_name="Synthetic cross-run screening sync",
    )
    target = ROOT / "tmp" / "wp4_state_cross_run_registry.jsonl"
    assert not target.exists()
    try:
        _write_registry(target, [cross_record])
        snapshot = snapshot_registry(target, scope="cross_run", repository_root=ROOT)
        state = build_current_state(manifest, changed_events, [snapshot], dag)
        assert "screening_pool_update" in {
            candidate["node_id"] for candidate in state["candidate_next_nodes"]
        }
        assert "source_identity" not in {
            candidate["node_id"] for candidate in state["candidate_next_nodes"]
        }
    finally:
        target.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "claimed", ["../escape/artifacts.jsonl", "tmp/not_the_real_registry.jsonl"]
)
def test_registry_relative_path_must_be_safe_and_bind_the_real_file(
    claimed: str,
) -> None:
    with pytest.raises(StateProjectionError, match="unsafe|bind"):
        snapshot_registry(
            EXAMPLE / "artifact_registry.jsonl",
            scope="run",
            expected_run_id=RUN_ID,
            repository_root=ROOT,
            relative_path=claimed,
        )


def test_duplicate_registry_snapshots_are_rejected_and_distinct_order_is_canonical() -> (
    None
):
    manifest, events, snapshot, dag = _sources()
    with pytest.raises(StateProjectionError, match="Duplicate Registry snapshot"):
        build_current_state(manifest, events, [snapshot, snapshot], dag)

    target = ROOT / "tmp" / "wp4_state_empty_registry.jsonl"
    assert not target.exists()
    try:
        target.write_bytes(b"")
        empty = snapshot_registry(
            target, scope="run", expected_run_id=RUN_ID, repository_root=ROOT
        )
        _write_registry(target, [_jsonl(EXAMPLE / "artifact_registry.jsonl")[0]])
        forward = build_current_state(manifest, events, [snapshot, empty], dag)
        reverse = build_current_state(manifest, events, [empty, snapshot], dag)
        assert current_state_bytes(forward) == current_state_bytes(reverse)
    finally:
        target.unlink(missing_ok=True)


def test_event_record_id_keeps_its_historical_revision_after_registry_advances() -> (
    None
):
    manifest, events, _, dag = _sources()
    records = _jsonl(EXAMPLE / "artifact_registry.jsonl")
    revision = _new_record(
        records[0],
        record_id="record_01j00000000000000000001001",
        record_revision=2,
        recorded_at="2026-01-01T00:00:10Z",
        previous_record_id=records[0]["record_id"],
        previous_record_hash=records[0]["record_hash"],
        metadata_revision_reason="synthetic metadata-only revision",
    )
    target = ROOT / "tmp" / "wp4_state_historical_record_registry.jsonl"
    assert not target.exists()
    try:
        _write_registry(target, [*records, revision])
        snapshot = snapshot_registry(
            target, scope="run", expected_run_id=RUN_ID, repository_root=ROOT
        )
        state = build_current_state(manifest, events, [snapshot], dag)
        assert state["input_artifacts"][0]["record_id"] == records[0]["record_id"]
        assert state["registry_heads"][0]["head_record_id"] == revision["record_id"]
    finally:
        target.unlink(missing_ok=True)


def test_validation_counts_are_event_counts_with_deterministic_distinct_summaries() -> (
    None
):
    manifest, events, snapshot, dag = _sources()
    second = copy.deepcopy(events[5])
    second["event_id"] = "event_01j00000000000000000000010"
    second["payload"]["validator_id"] = "synthetic.secondary"
    second["payload"]["result"] = "partial"
    changed = [*copy.deepcopy(events[:6]), second, *copy.deepcopy(events[6:])]
    for sequence, event in enumerate(changed, start=1):
        event["sequence_no"] = sequence
    changed = _rehash_from(changed, 0)
    state = build_current_state(manifest, changed, [snapshot], dag)
    assert state["validation_summary"] == {
        "overall_status": "partial",
        "passed_count": 1,
        "failed_count": 0,
        "partial_count": 1,
        "validators": [
            {
                "validator_id": "synthetic.schema",
                "validator_version": "1.0.0",
                "result": "passed",
                "validation_artifact_id": None,
            },
            {
                "validator_id": "synthetic.secondary",
                "validator_version": "1.0.0",
                "result": "partial",
                "validation_artifact_id": None,
            },
        ],
    }


def test_repeated_validation_event_is_counted_even_when_validator_summary_is_deduplicated() -> (
    None
):
    manifest, events, snapshot, dag = _sources()
    second = copy.deepcopy(events[5])
    second.update(
        {
            "event_id": "event_01j00000000000000000000010",
            "occurred_at": "2026-01-01T00:00:06.5Z",
        }
    )
    changed = [*copy.deepcopy(events[:6]), second, *copy.deepcopy(events[6:])]
    for sequence, event in enumerate(changed, start=1):
        event["sequence_no"] = sequence
    changed = _rehash_from(changed, 0)

    state = build_current_state(manifest, changed, [snapshot], dag)
    assert state["validation_summary"]["passed_count"] == 2
    assert len(state["validation_summary"]["validators"]) == 1


def test_historical_registry_prefix_ignores_a_corrupt_later_tail() -> None:
    manifest, events, _, dag = _sources()
    records = _jsonl(EXAMPLE / "artifact_registry.jsonl")
    target = ROOT / "tmp" / "wp4_state_corrupt_registry_tail.jsonl"
    assert not target.exists()
    try:
        _write_registry(target, records[:2])
        snapshot = snapshot_registry(
            target,
            scope="run",
            expected_run_id=RUN_ID,
            head_record_id=RECORD_2,
            repository_root=ROOT,
        )
        target.write_bytes(target.read_bytes() + b"{corrupt-tail\n")

        state = build_current_state(manifest, events[:7], [snapshot], dag)
        assert state["registry_heads"][0]["head_record_id"] == RECORD_2
        with pytest.raises(StateProjectionError, match="Invalid Registry line"):
            snapshot_registry(
                target, scope="run", expected_run_id=RUN_ID, repository_root=ROOT
            )
    finally:
        target.unlink(missing_ok=True)


def test_forged_snapshot_metadata_is_rebound_to_the_real_registry_path() -> None:
    manifest, events, snapshot, dag = _sources()
    forged = replace(snapshot, relative_path="tmp/forged_registry_identity.jsonl")
    with pytest.raises(StateProjectionError, match="does not bind"):
        build_current_state(manifest, events, [forged], dag)
