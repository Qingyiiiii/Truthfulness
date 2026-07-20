from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from video_truthfulness.core.artifacts.dag import load_dag
from video_truthfulness.core.artifacts.registry import AppendOnlyRegistry
from video_truthfulness.core.execution import handoff as handoff_runtime
from video_truthfulness.core.execution.events import EventLog
from video_truthfulness.core.execution.handoff import (
    HandoffImmutableError,
    HandoffMarkdownDriftError,
    HandoffRegistrationError,
    HandoffSources,
    HandoffValidationError,
    build_handoff_registry_record,
    build_handoff,
    parse_handoff,
    create_handoff,
    handoff_created_draft,
    read_handoff,
    register_handoff,
    render_handoff_markdown,
    validate_handoff,
    validate_handoff_created_event,
    validate_handoff_markdown,
    validate_handoff_registration,
    validate_handoff_registry_record,
    write_handoff_markdown,
)
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_file,
)
from video_truthfulness.core.execution.models import (
    ExecutionContractError,
    ExecutionHashError,
)
from video_truthfulness.core.execution.state import (
    build_current_state,
    snapshot_registry,
)


ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "execution_contract" / "synthetic_run"
HANDOFF_ID = "artifact_01j00000000000000000000003"
RECORD_2 = "record_01j00000000000000000000002"
RUN_ID = "run_01j00000000000000000000000"


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _successor_handoff_raw(
    *,
    source_stage: str,
    target_stage: str,
    target_workflow: str,
    execution_authorized: bool,
) -> dict[str, Any]:
    raw = _json(EXAMPLE / "handoff.json")
    raw["handoff_version"] = "handoff_v2.1.0"
    raw["stage_id"] = source_stage
    raw["workflow_version"] = (
        "youtube_truthfulness_workflow_v1.3.0"
        if source_stage == "S02"
        else "youtube_truthfulness_workflow_v1.1.0"
    )
    raw["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    raw["schema_versions"] = [
        "handoff_v2.1.0" if value == "handoff_v2.0.0" else value
        for value in raw["schema_versions"]
    ]
    required_paths = raw["next_action"]["required_read_paths"]
    raw["next_action"] = {
        "action_type": "next_stage",
        "next_stage": target_stage,
        "workflow_reference": f"Optmize/workflows/{int(target_stage[1:]):02d}_target.md",
        "prompt_reference": f"Optmize/workflows/{int(target_stage[1:]):02d}_target.md",
        "target_workflow_version": target_workflow,
        "execution_authorized": execution_authorized,
        "required_input_artifact_ids": [],
        "required_read_paths": required_paths,
        "reason": "bounded successor transition test",
    }
    raw["handoff_hash"] = embedded_hash(raw, "handoff_hash")
    return raw


@pytest.mark.parametrize(
    (
        "source_stage",
        "target_stage",
        "target_workflow",
        "execution_authorized",
    ),
    [
        (
            "S01",
            "S02",
            "youtube_truthfulness_workflow_v1.3.0",
            True,
        ),
        (
            "S02",
            "S03",
            "youtube_truthfulness_workflow_v1.1.0",
            False,
        ),
    ],
)
def test_handoff_v21_accepts_only_two_declared_adjacent_transitions(
    source_stage: str,
    target_stage: str,
    target_workflow: str,
    execution_authorized: bool,
) -> None:
    model = parse_handoff(
        _successor_handoff_raw(
            source_stage=source_stage,
            target_stage=target_stage,
            target_workflow=target_workflow,
            execution_authorized=execution_authorized,
        )
    )
    assert model.handoff_version == "handoff_v2.1.0"
    assert model.next_action.execution_authorized is execution_authorized


@pytest.mark.parametrize(
    ("source_stage", "target_stage", "target_workflow", "authorization"),
    [
        ("S01", "S02", "youtube_truthfulness_workflow_v1.3.0", False),
        ("S02", "S03", "youtube_truthfulness_workflow_v1.1.0", True),
        ("S01", "S03", "youtube_truthfulness_workflow_v1.1.0", False),
    ],
)
def test_handoff_v21_rejects_undeclared_or_wrongly_authorized_transition(
    source_stage: str,
    target_stage: str,
    target_workflow: str,
    authorization: bool,
) -> None:
    raw = _successor_handoff_raw(
        source_stage=source_stage,
        target_stage=target_stage,
        target_workflow=target_workflow,
        execution_authorized=authorization,
    )
    with pytest.raises(HandoffValidationError, match="undeclared Workflow transition"):
        parse_handoff(raw)


def _sources(
    *,
    root: Path = ROOT,
    example: Path = EXAMPLE,
    registry_head_record_id: str = RECORD_2,
    events: list[dict[str, Any]] | None = None,
) -> HandoffSources:
    manifest = _json(example / "session_manifest.json")
    event_rows = events if events is not None else _jsonl(example / "events.jsonl")
    registry = snapshot_registry(
        example / "artifact_registry.jsonl",
        scope="run",
        expected_run_id=RUN_ID,
        head_record_id=registry_head_record_id,
        repository_root=root,
    )
    dag_path = example / "youtube_truthfulness_dag_v1_1.yaml"
    terminal_state = build_current_state(
        manifest,
        event_rows[:7],
        [registry],
        load_dag(dag_path),
    )
    checkpoint_path = next((example / "checkpoints").glob("checkpoint_*.json"))
    return HandoffSources(
        repository_root=root,
        manifest=manifest,
        events=event_rows,
        terminal_state=terminal_state,
        registry_snapshots=(registry,),
        dag_path=dag_path,
        checkpoint=_json(checkpoint_path),
    )


def _isolated_sources(tmp_path: Path) -> tuple[Path, Path, HandoffSources]:
    root = tmp_path / "repository"
    example = root / "examples" / "execution_contract" / "synthetic_run"
    shutil.copytree(EXAMPLE, example)
    registry_path = example / "artifact_registry.jsonl"
    registry_lines = registry_path.read_bytes().splitlines()
    registry_path.write_bytes(b"\n".join(registry_lines[:2]) + b"\n")
    (example / "handoff.json").unlink()
    (example / "HANDOFF.md").unlink()
    return root, example, _sources(root=root, example=example)


def _next_action() -> dict[str, Any]:
    return copy.deepcopy(_json(EXAMPLE / "handoff.json")["next_action"])


def _terminate_action() -> dict[str, Any]:
    return {
        "action_type": "terminate",
        "termination_kind": "project_complete",
        "reason": "the bounded synthetic contract exercise is complete",
    }


def _return_action() -> dict[str, Any]:
    return {
        "action_type": "return_to_stage",
        "target_stage": "S01",
        "workflow_reference": "Optmize/workflows/01_单视频采集与机器初筛.md",
        "prompt_reference": "Optmize/workflows/01_单视频采集与机器初筛.md",
        "required_input_artifact_ids": [],
        "required_read_paths": _exact_recovery_paths(),
        "reason": "execute the unresolved source_identity candidate",
    }


def _forward_action() -> dict[str, Any]:
    return {
        "action_type": "next_stage",
        "next_stage": "S02",
        "workflow_reference": "Optmize/workflows/02_深度溯源与结果导入.md",
        "prompt_reference": "Optmize/workflows/02_深度溯源与结果导入.md",
        "required_input_artifact_ids": ["artifact_01j00000000000000000000002"],
        "required_read_paths": _exact_recovery_paths(),
        "reason": "attempt to advance to the adjacent synthetic stage",
    }


def _exact_recovery_paths() -> list[str]:
    base = "examples/execution_contract/synthetic_run"
    return sorted(
        {
            f"{base}/session_manifest.json",
            f"{base}/events.jsonl",
            f"{base}/handoff.json",
            f"{base}/checkpoints/checkpoint_01j00000000000000000000000.json",
            f"{base}/artifact_registry.jsonl",
            f"{base}/youtube_truthfulness_dag_v1_1.yaml",
            f"{base}/working_tree_manifest.json",
            f"{base}/artifacts/input.json",
            f"{base}/artifacts/output.json",
        }
    )


def _artifact_id(index: int) -> str:
    return f"artifact_{index:026d}"


def _handoff(
    sources: HandoffSources | None = None,
    *,
    next_action: dict[str, Any] | None = None,
    risks: list[dict[str, Any]] | None = None,
) -> Any:
    return build_handoff(
        sources or _sources(),
        next_action=next_action or _terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
        risks=risks or (),
    )


def _rehash_handoff(raw: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(raw)
    changed["handoff_hash"] = "0" * 64
    changed["handoff_hash"] = embedded_hash(changed, "handoff_hash")
    return changed


def _publication_record(
    publication: Any,
    sources: HandoffSources,
    *,
    record_id: str = "record_01j00000000000000000000003",
) -> Any:
    return build_handoff_registry_record(
        publication,
        sources,
        recorded_at="2026-01-01T00:00:09Z",
        privacy_class="public_synthetic",
        access_scope="public",
        retention_policy="retain with public synthetic examples",
        record_id=record_id,
        writer_agent_id="synthetic-agent",
        tool_versions={"artifact_core": "synthetic-example-v2"},
    )


def _identity_only_sources(
    sources: HandoffSources,
    updates: list[dict[str, Any]],
) -> Any:
    records = sources.registry_snapshots[0].records
    assert len(updates) == len(records)
    return SimpleNamespace(
        registry_snapshots=(
            SimpleNamespace(
                records=tuple(
                    record.model_copy(update=record_updates)
                    for record, record_updates in zip(records, updates, strict=True)
                )
            ),
        )
    )


def _registered_handoff(tmp_path: Path) -> tuple[Path, Path, HandoffSources, Any]:
    root, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    record = _publication_record(publication, sources)
    registry = AppendOnlyRegistry(
        example / "artifact_registry.jsonl",
        scope="run",
        expected_run_id=RUN_ID,
    )
    registration = register_handoff(registry, publication, record, sources)
    return root, example, sources, registration


def _append_handoff_receipt(
    tmp_path: Path,
) -> tuple[Path, HandoffSources, Any, Any]:
    _, example, sources, registration = _registered_handoff(tmp_path)
    event_path = example / "events.jsonl"
    event_lines = event_path.read_bytes().splitlines()
    event_path.write_bytes(b"\n".join(event_lines[:8]) + b"\n")
    event_log = EventLog(event_path, sources.manifest)
    draft = handoff_created_draft(
        registration,
        actor=sources.events[7]["actor"],
    )
    event = event_log.append(
        draft,
        event_id="event_01j00000000000000000000009",
        occurred_at="2026-01-01T00:00:09Z",
    )
    return event_path, sources, registration, event


def test_build_freezes_checkpoint_receipt_and_pre_registration_registry_prefix() -> (
    None
):
    sources = _sources()
    handoff = _handoff(sources)

    assert handoff.source_event_head.sequence_no == 8
    assert handoff.source_event_head.event_id == "event_01j00000000000000000000008"
    assert handoff.source_registry_heads[0].head_record_id == RECORD_2
    assert handoff.source_registry_heads[0].record_count == 2
    assert handoff.checkpoint_id == "checkpoint_01j00000000000000000000000"
    assert handoff.metrics.event_count == 8
    assert handoff.metrics.rebuild_hash_match is True
    assert (
        handoff.metrics.validation_passed_count
        == handoff.validation_summary.passed_count
    )
    assert (
        handoff.metrics.validation_failed_count
        == handoff.validation_summary.failed_count
    )
    assert {item.action_key for item in handoff.completed_actions} == {
        "task_completed",
        "checkpoint_created",
    }
    assert "handoff_finalized" not in {
        item.action_key for item in handoff.completed_actions
    }

    bounded_ids = {
        item.artifact_id
        for collection in (
            handoff.input_artifacts,
            handoff.output_artifacts,
            handoff.invalidated_artifacts,
        )
        for item in collection
    }
    assert handoff.handoff_artifact_id not in bounded_ids
    validate_handoff(handoff, sources)


def test_later_handoff_receipt_event_is_not_promoted_to_the_source_head() -> None:
    sources = _sources()
    assert len(sources.events) == 9
    assert sources.events[-1]["event_type"] == "handoff.created"

    handoff = _handoff(sources)

    assert handoff.source_event_head.sequence_no == 8
    assert handoff.source_event_head.event_id == sources.events[7]["event_id"]
    assert handoff.source_event_head.event_hash == sources.events[7]["event_hash"]


@pytest.mark.parametrize(
    "mutation",
    [
        "nested_extra",
        "bad_timestamp",
        "bad_id",
        "wrong_nested_type",
        "duplicate_keyed_action",
        "duplicate_registry_binding",
    ],
)
def test_parse_handoff_enforces_strict_nested_and_unique_boundaries(
    mutation: str,
) -> None:
    raw = _handoff().model_dump(mode="json")
    if mutation == "nested_extra":
        raw["source_event_head"]["unexpected"] = True
    elif mutation == "bad_timestamp":
        raw["created_at"] = "2026-02-30T00:00:08Z"
    elif mutation == "bad_id":
        raw["handoff_artifact_id"] = "artifact_not_a_canonical_ulid"
    elif mutation == "wrong_nested_type":
        raw["source_registry_heads"][0]["record_count"] = "2"
    elif mutation == "duplicate_keyed_action":
        duplicate = copy.deepcopy(raw["completed_actions"][0])
        duplicate["summary"] = "same action identity with conflicting content"
        raw["completed_actions"].append(duplicate)
    else:
        duplicate = copy.deepcopy(raw["source_registry_heads"][0])
        duplicate["content_hash"] = "0" * 64
        raw["source_registry_heads"].append(duplicate)
    raw = _rehash_handoff(raw)

    with pytest.raises(ExecutionContractError):
        parse_handoff(raw)


def test_parse_handoff_rejects_wrong_semantic_hash_domain() -> None:
    raw = _handoff().model_dump(mode="json")
    raw["handoff_hash"] = "0" * 64

    with pytest.raises(ExecutionHashError, match="handoff_hash mismatch"):
        parse_handoff(raw)


def test_static_handoff_hash_mismatch_fixture_is_rejected() -> None:
    raw = _json(EXAMPLE / "invalid" / "handoff_hash_mismatch.json")

    with pytest.raises(ExecutionHashError, match="handoff_hash mismatch"):
        parse_handoff(raw)


@pytest.mark.parametrize(
    "mutation",
    [
        "event9_as_source_head",
        "post_registration_registry_head",
        "checkpoint_identity",
        "runtime_identity",
        "self_referential_output",
        "metric_count",
    ],
)
def test_rehashed_handoff_cannot_forge_authoritative_source_projections(
    mutation: str,
) -> None:
    sources = _sources()
    raw = _handoff(sources).model_dump(mode="json")
    if mutation == "event9_as_source_head":
        event9 = sources.events[8]
        raw["source_event_head"] = {
            "event_id": event9["event_id"],
            "sequence_no": event9["sequence_no"],
            "event_hash": event9["event_hash"],
            "occurred_at": event9["occurred_at"],
        }
        raw["metrics"]["event_count"] = 9
        raw["created_at"] = event9["occurred_at"]
    elif mutation == "post_registration_registry_head":
        raw["source_registry_heads"] = _json(EXAMPLE / "current_state.json")[
            "registry_heads"
        ]
    elif mutation == "checkpoint_identity":
        raw["checkpoint_id"] = "checkpoint_01j00000000000000000000098"
    elif mutation == "runtime_identity":
        raw["agent_runtime_version"] = "forged-runtime"
    elif mutation == "self_referential_output":
        raw["output_artifacts"][0]["artifact_id"] = raw["handoff_artifact_id"]
    else:
        raw["metrics"]["out_of_scope_detection_count"] = 99
    raw = _rehash_handoff(raw)

    with pytest.raises(HandoffValidationError, match="mismatch"):
        validate_handoff(raw, sources)


def test_post_registration_registry_prefix_cannot_be_reused_as_handoff_source() -> None:
    sources = _sources(registry_head_record_id="record_01j00000000000000000000003")

    with pytest.raises(
        ExecutionContractError,
        match="Registry|registry|source mismatch|fixed sources",
    ):
        _handoff(sources)


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ("non_adjacent_stage", "adjacent"),
        ("unknown_artifact", "unknown input Artifacts"),
        ("omitted_artifact_path", "omits required Artifact paths"),
        ("missing_workflow", "canonical S01 workflow entry|missing repository file"),
        ("duplicate_artifact", "must be unique"),
        ("path_escape", "relative|escape|parent|Private absolute"),
    ],
)
def test_next_action_rejects_non_unique_unsafe_or_unbound_continuations(
    mutation: str,
    match: str,
) -> None:
    action = _return_action()
    if mutation == "non_adjacent_stage":
        action = _forward_action()
        action["next_stage"] = "S03"
    elif mutation == "unknown_artifact":
        action["required_input_artifact_ids"] = ["artifact_01j00000000000000000000099"]
    elif mutation == "omitted_artifact_path":
        action["required_input_artifact_ids"] = ["artifact_01j00000000000000000000002"]
        action["required_read_paths"] = []
    elif mutation == "missing_workflow":
        action["workflow_reference"] = "Optmize/workflows/does_not_exist.md"
    elif mutation == "duplicate_artifact":
        action["required_input_artifact_ids"] = [
            "artifact_01j00000000000000000000002",
            "artifact_01j00000000000000000000002",
        ]
        action["required_read_paths"] = [
            "examples/execution_contract/synthetic_run/artifacts/output.json"
        ]
    else:
        action["required_read_paths"] = ["../private/run.json"]

    with pytest.raises(ExecutionContractError, match=match):
        _handoff(next_action=action)


def test_return_action_uses_the_exact_deduplicated_recovery_package() -> None:
    action = _return_action()

    handoff = _handoff(next_action=action)

    assert len(action["required_read_paths"]) == 9
    assert set(handoff.next_action.required_read_paths) == set(_exact_recovery_paths())
    validate_handoff(handoff, _sources())


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_return_action_rejects_inexact_recovery_package(mutation: str) -> None:
    action = _return_action()
    if mutation == "missing":
        action["required_read_paths"].remove(
            "examples/execution_contract/synthetic_run/events.jsonl"
        )
    else:
        action["required_read_paths"].append(
            "examples/execution_contract/synthetic_run/current_state.json"
        )

    with pytest.raises(HandoffValidationError, match="exact recovery package"):
        _handoff(next_action=action)


def test_blocking_risk_cannot_advertise_next_stage_as_the_unique_action() -> None:
    risk = {
        "risk_key": "source_integrity_blocked",
        "severity": "critical",
        "summary": "the required source identity is unresolved",
        "mitigation": "resolve and validate the source identity before continuing",
        "blocking": True,
    }

    with pytest.raises(HandoffValidationError, match="blocking risk"):
        _handoff(next_action=_forward_action(), risks=[risk])


def test_terminate_action_must_match_the_terminal_state() -> None:
    action = {
        "action_type": "terminate",
        "termination_kind": "terminal_failure",
        "reason": "synthetic mismatch",
    }

    with pytest.raises(HandoffValidationError, match="status=FAILED"):
        _handoff(next_action=action)


def test_created_at_cannot_precede_checkpoint_receipt_head() -> None:
    with pytest.raises(HandoffValidationError, match="cannot precede"):
        build_handoff(
            _sources(),
            next_action=_next_action(),
            created_at="2026-01-01T00:00:07.999Z",
            handoff_artifact_id=HANDOFF_ID,
        )


def test_immutable_publication_separates_semantic_and_file_hash_domains(
    tmp_path: Path,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    target = example / "handoff.json"

    publication = create_handoff(
        target,
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    original = target.read_bytes()

    assert publication.file_hash == sha256_file(target)
    assert publication.file_hash != publication.handoff.handoff_hash
    assert publication.size_bytes == len(original)
    assert original.endswith(b"\n")
    assert read_handoff(target) == publication.handoff
    with pytest.raises(HandoffImmutableError, match="already exists"):
        create_handoff(
            target,
            sources,
            next_action=_terminate_action(),
            created_at="2026-01-01T00:00:08Z",
            handoff_artifact_id=HANDOFF_ID,
        )
    assert target.read_bytes() == original


def test_publication_readback_failure_preserves_immutable_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    target = example / "handoff.json"

    def fail_readback(_path: Path) -> Any:
        raise HandoffValidationError("injected HANDOFF read-back failure")

    monkeypatch.setattr(handoff_runtime, "read_handoff", fail_readback)
    with pytest.raises(HandoffImmutableError, match="bytes were preserved"):
        create_handoff(
            target,
            sources,
            next_action=_terminate_action(),
            created_at="2026-01-01T00:00:08Z",
            handoff_artifact_id=HANDOFF_ID,
        )
    assert target.is_file()
    assert target.read_bytes().endswith(b"\n")


def test_publication_rejects_wrong_filename_before_writing(tmp_path: Path) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    target = example / "handoff-copy.json"

    with pytest.raises(HandoffValidationError, match="named handoff.json"):
        create_handoff(
            target,
            sources,
            next_action=_terminate_action(),
            created_at="2026-01-01T00:00:08Z",
            handoff_artifact_id=HANDOFF_ID,
        )
    assert not target.exists()


def test_registry_record_binds_file_semantic_and_upstream_domains(
    tmp_path: Path,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    record = _publication_record(publication, sources)

    assert record.artifact_type == "handoff.run"
    assert record.authority_level == "machine_derived"
    assert record.relative_path == publication.relative_path
    assert record.content_hash == publication.file_hash
    assert record.semantic_hash == publication.handoff.handoff_hash
    assert record.size_bytes == publication.size_bytes
    assert record.upstream_artifact_ids == [
        "artifact_01j00000000000000000000001",
        "artifact_01j00000000000000000000002",
    ]
    assert record.content_hash != record.semantic_hash
    validate_handoff_registry_record(publication, record, sources)


def test_run_handoff_registry_record_inherits_unique_canonical_source_identity(
    tmp_path: Path,
) -> None:
    _, _, legacy_sources = _isolated_sources(tmp_path)
    canonical_sources = _identity_only_sources(
        legacy_sources,
        [
            {"source_platform": "youtube", "source_id": "youtube_dQw4w9WgXcQ"},
            {"source_platform": "youtube", "source_id": "youtube_dQw4w9WgXcQ"},
        ],
    )

    assert handoff_runtime._handoff_run_source_identity(  # noqa: SLF001
        _handoff(legacy_sources), canonical_sources
    ) == ("youtube", "youtube_dQw4w9WgXcQ")


def test_run_handoff_registry_record_rejects_conflicting_source_identities(
    tmp_path: Path,
) -> None:
    _, _, legacy_sources = _isolated_sources(tmp_path)
    conflicting_sources = _identity_only_sources(
        legacy_sources,
        [
            {"source_platform": "youtube", "source_id": "youtube_dQw4w9WgXcQ"},
            {"source_platform": "youtube", "source_id": "youtube_9bZkp7q19f0"},
        ],
    )

    with pytest.raises(HandoffRegistrationError, match="conflicting canonical source"):
        handoff_runtime._handoff_run_source_identity(  # noqa: SLF001
            _handoff(legacy_sources), conflicting_sources
        )


def test_run_handoff_registry_record_rejects_real_all_null_source_identity(
    tmp_path: Path,
) -> None:
    _, _, legacy_sources = _isolated_sources(tmp_path)
    real_null_sources = _identity_only_sources(
        legacy_sources,
        [
            {"privacy_class": "private_derived", "access_scope": "project_private"},
            {"privacy_class": "private_derived", "access_scope": "project_private"},
        ],
    )

    with pytest.raises(
        HandoffRegistrationError, match="cannot derive one canonical source"
    ):
        handoff_runtime._handoff_run_source_identity(  # noqa: SLF001
            _handoff(legacy_sources), real_null_sources
        )


def test_run_handoff_registry_record_keeps_narrow_legacy_public_null_compatibility(
    tmp_path: Path,
) -> None:
    _, example, legacy_sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        legacy_sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )

    record = _publication_record(publication, legacy_sources)

    assert record.source_platform is None
    assert record.source_id is None
    validate_handoff_registry_record(publication, record, legacy_sources)


def test_run_handoff_registry_record_validation_rechecks_snapshot_source_identity(
    tmp_path: Path,
) -> None:
    _, example, legacy_sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        legacy_sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    record = _publication_record(publication, legacy_sources)
    forged = record.model_copy(
        update={
            "source_platform": "youtube",
            "source_id": "youtube_dQw4w9WgXcQ",
        }
    )

    with pytest.raises(HandoffRegistrationError, match="source_platform"):
        validate_handoff_registry_record(publication, forged, legacy_sources)


@pytest.mark.parametrize("field", ["content_hash", "semantic_hash", "size_bytes"])
def test_registry_record_rejects_crossed_hash_domain_or_size(
    tmp_path: Path,
    field: str,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    record = _publication_record(publication, sources)
    if field == "content_hash":
        changed = record.model_copy(update={field: publication.handoff.handoff_hash})
    elif field == "semantic_hash":
        changed = record.model_copy(update={field: publication.file_hash})
    else:
        changed = record.model_copy(update={field: publication.size_bytes + 1})

    with pytest.raises(HandoffRegistrationError, match=field):
        validate_handoff_registry_record(publication, changed, sources)


def test_registration_appends_after_fixed_prefix_without_backfilling_source(
    tmp_path: Path,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    record = _publication_record(publication, sources)
    registry = AppendOnlyRegistry(
        example / "artifact_registry.jsonl",
        scope="run",
        expected_run_id=RUN_ID,
    )

    registration = register_handoff(registry, publication, record, sources)

    assert registration.before_head.head_record_id == RECORD_2
    assert registration.before_head.record_count == 2
    assert registration.after_head.head_record_id == record.record_id
    assert registration.after_head.head_record_hash == record.record_hash
    assert registration.after_head.record_count == 3
    assert publication.handoff.source_registry_heads[0].head_record_id == RECORD_2
    validate_handoff_registration(registration, sources)


def test_registration_rejects_registry_drift_before_append(tmp_path: Path) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    record = _publication_record(
        publication,
        sources,
        record_id="record_01j00000000000000000000098",
    )
    registry_path = example / "artifact_registry.jsonl"
    original_record3 = (
        (EXAMPLE / "artifact_registry.jsonl").read_bytes().splitlines()[2]
    )
    registry_path.write_bytes(registry_path.read_bytes() + original_record3 + b"\n")
    before = registry_path.read_bytes()
    registry = AppendOnlyRegistry(
        registry_path,
        scope="run",
        expected_run_id=RUN_ID,
    )

    with pytest.raises(HandoffRegistrationError, match="changed after HANDOFF source"):
        register_handoff(registry, publication, record, sources)
    assert registry_path.read_bytes() == before


def test_registration_readback_failure_preserves_appended_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    record = _publication_record(publication, sources)
    registry_path = example / "artifact_registry.jsonl"
    registry = AppendOnlyRegistry(
        registry_path,
        scope="run",
        expected_run_id=RUN_ID,
    )
    real_snapshot_registry = handoff_runtime.snapshot_registry
    calls = 0

    def fail_second_snapshot(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise HandoffRegistrationError("injected Registry read-back failure")
        return real_snapshot_registry(*args, **kwargs)

    monkeypatch.setattr(handoff_runtime, "snapshot_registry", fail_second_snapshot)
    with pytest.raises(HandoffRegistrationError, match="evidence was preserved"):
        register_handoff(registry, publication, record, sources)
    rows = _jsonl(registry_path)
    assert len(rows) == 3
    assert rows[-1]["record_id"] == record.record_id


def test_handoff_created_receipt_binds_three_hash_domains_and_exact_order(
    tmp_path: Path,
) -> None:
    _, sources, registration, event = _append_handoff_receipt(tmp_path)
    publication = registration.publication
    record = registration.record

    assert event.sequence_no == 9
    assert event.previous_event_id == sources.events[7]["event_id"]
    assert event.previous_event_hash == sources.events[7]["event_hash"]
    assert event.payload["handoff_hash"] == publication.handoff.handoff_hash
    assert event.payload["record_hash"] == record.record_hash
    assert event.artifact_refs[0].content_hash == publication.file_hash
    assert event.path_refs[0].content_hash == publication.file_hash
    assert publication.handoff.handoff_hash != publication.file_hash
    assert record.record_hash not in {
        publication.handoff.handoff_hash,
        publication.file_hash,
    }
    validate_handoff_created_event(registration, event, sources)


@pytest.mark.parametrize(
    "mutation",
    [
        "semantic_hash_domain",
        "record_hash_domain",
        "artifact_file_hash_domain",
        "path_file_hash_domain",
        "sequence",
        "previous_head",
        "time",
    ],
)
def test_handoff_created_receipt_rejects_crossed_hash_domain_or_order(
    tmp_path: Path,
    mutation: str,
) -> None:
    _, sources, registration, event = _append_handoff_receipt(tmp_path)
    publication = registration.publication
    raw = event.model_dump(mode="json")
    if mutation == "semantic_hash_domain":
        raw["payload"]["handoff_hash"] = publication.file_hash
    elif mutation == "record_hash_domain":
        raw["payload"]["record_hash"] = publication.handoff.handoff_hash
    elif mutation == "artifact_file_hash_domain":
        raw["artifact_refs"][0]["content_hash"] = publication.handoff.handoff_hash
    elif mutation == "path_file_hash_domain":
        raw["path_refs"][0]["content_hash"] = publication.handoff.handoff_hash
    elif mutation == "sequence":
        raw["sequence_no"] = 10
    elif mutation == "previous_head":
        raw["previous_event_hash"] = "0" * 64
    else:
        raw["occurred_at"] = "2026-01-01T00:00:08.999Z"
    raw["event_hash"] = "0" * 64
    raw["event_hash"] = embedded_hash(raw, "event_hash")

    with pytest.raises(ExecutionContractError):
        validate_handoff_created_event(registration, raw, sources)


def test_handoff_receipt_writeback_failure_preserves_appended_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, example, sources, registration = _registered_handoff(tmp_path)
    event_path = example / "events.jsonl"
    event_lines = event_path.read_bytes().splitlines()
    event_path.write_bytes(b"\n".join(event_lines[:8]) + b"\n")
    event_log = EventLog(event_path, sources.manifest)
    draft = handoff_created_draft(registration, actor=sources.events[7]["actor"])

    def fail_readback(*_args: Any, **_kwargs: Any) -> Any:
        raise HandoffValidationError("injected event read-back failure")

    monkeypatch.setattr(event_log, "validate", fail_readback)
    with pytest.raises(ExecutionContractError, match="not truncated"):
        event_log.append(
            draft,
            event_id="event_01j00000000000000000000009",
            occurred_at="2026-01-01T00:00:09Z",
        )
    rows = _jsonl(event_path)
    assert len(rows) == 9
    assert rows[-1]["event_type"] == "handoff.created"


def test_markdown_delete_and_rebuild_is_byte_identical_and_json_only(
    tmp_path: Path,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    direct = render_handoff_markdown(publication.handoff)
    first = write_handoff_markdown(publication, sources)
    first_bytes = first.path.read_bytes()
    first.path.unlink()
    second = write_handoff_markdown(publication, sources)

    assert first_bytes == direct == second.path.read_bytes()
    assert (
        first.file_hash
        == second.file_hash
        == validate_handoff_markdown(
            publication.handoff,
            second.path,
        )
    )
    assert b"the sibling `handoff.json`" in direct
    assert b"examples/execution_contract/synthetic_run/handoff.json" not in direct


def test_markdown_validation_rejects_any_projection_drift(tmp_path: Path) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_handoff(
        example / "handoff.json",
        sources,
        next_action=_terminate_action(),
        created_at="2026-01-01T00:00:08Z",
        handoff_artifact_id=HANDOFF_ID,
    )
    markdown = write_handoff_markdown(publication, sources)
    markdown.path.write_bytes(markdown.path.read_bytes() + b"forged fact\n")

    with pytest.raises(HandoffMarkdownDriftError, match="byte drift"):
        validate_handoff_markdown(publication.handoff, markdown.path)


def test_static_markdown_drift_fixture_is_rejected() -> None:
    handoff = read_handoff(EXAMPLE / "handoff.json")

    with pytest.raises(HandoffMarkdownDriftError, match="byte drift"):
        validate_handoff_markdown(
            handoff,
            EXAMPLE / "invalid" / "HANDOFF_drift.md",
        )


def test_markdown_renderer_escapes_html_and_markdown_in_free_text() -> None:
    risk = {
        "risk_key": "renderer_injection",
        "severity": "medium",
        "summary": "<script>*forged*</script>",
        "mitigation": "render `facts` from JSON only",
        "blocking": False,
    }
    rendered = render_handoff_markdown(_handoff(risks=[risk]))

    assert b"<script>" not in rendered
    assert b"&lt;script&gt;" in rendered
    assert b"\\*forged\\*" in rendered


def _budget_risks(count: int, *, text_length: int = 1) -> list[dict[str, Any]]:
    text = "x" * text_length
    return [
        {
            "risk_key": f"budget_risk_{index:04d}",
            "severity": "low",
            "summary": text,
            "mitigation": text,
            "blocking": False,
        }
        for index in range(count)
    ]


def test_collection_budget_accepts_1024_and_rejects_1025_items() -> None:
    raw = _handoff().model_dump(mode="json")
    raw["risks"] = _budget_risks(1_024)
    allowed = _rehash_handoff(raw)

    assert len(parse_handoff(allowed).risks) == 1_024
    raw["risks"] = _budget_risks(1_025)
    with pytest.raises(HandoffValidationError, match="array exceeds 1024 items"):
        parse_handoff(raw)


@pytest.mark.parametrize(
    "field",
    ["required_input_artifact_ids", "required_read_paths"],
)
def test_next_action_array_budget_accepts_1024_and_rejects_1025_items(
    field: str,
) -> None:
    raw = _handoff().model_dump(mode="json")
    action = _return_action()
    if field == "required_input_artifact_ids":
        values = [_artifact_id(index + 1) for index in range(1_024)]
    else:
        values = [f"recovery/path_{index:04d}.json" for index in range(1_024)]
    action[field] = values
    raw["next_action"] = action
    allowed = _rehash_handoff(raw)

    assert len(getattr(parse_handoff(allowed).next_action, field)) == 1_024
    action[field] = (
        [*values, _artifact_id(2_000)]
        if field.endswith("ids")
        else [
            *values,
            "recovery/path_2000.json",
        ]
    )
    raw["next_action"] = action
    with pytest.raises(HandoffValidationError, match="array exceeds 1024 items"):
        parse_handoff(raw)


def test_canonical_byte_budget_accepts_bounded_payload_and_rejects_over_1_mib() -> None:
    raw = _handoff().model_dump(mode="json")
    raw["risks"] = _budget_risks(850, text_length=500)
    allowed = _rehash_handoff(raw)
    assert len(canonical_json_bytes(allowed)) + 1 <= 1_048_576
    parse_handoff(allowed)

    raw["risks"] = _budget_risks(1_024, text_length=500)
    assert len(canonical_json_bytes(raw)) + 1 > 1_048_576
    with pytest.raises(HandoffValidationError, match="1048576-byte"):
        parse_handoff(raw)


def test_parser_rejects_nesting_deeper_than_32() -> None:
    raw = _handoff().model_dump(mode="json")
    probe: Any = "leaf"
    for _ in range(33):
        probe = [probe]
    raw["depth_probe"] = probe

    with pytest.raises(HandoffValidationError, match="maximum nesting depth 32"):
        parse_handoff(raw)


def test_parser_rejects_more_than_20000_nodes_before_schema_walk() -> None:
    raw = _handoff().model_dump(mode="json")
    raw["node_probe"] = [["x"] * 1_024 for _ in range(20)]
    assert len(canonical_json_bytes(raw)) + 1 < 1_048_576

    with pytest.raises(HandoffValidationError, match="20000-node"):
        parse_handoff(raw)


@pytest.mark.parametrize("status", ["FAILED", "BLOCKED_BY_INPUT"])
def test_non_success_terminal_state_cannot_advertise_next_stage(status: str) -> None:
    raw = _handoff().model_dump(mode="json")
    raw["status"] = status
    raw["next_action"] = _forward_action()
    raw = _rehash_handoff(raw)

    with pytest.raises(HandoffValidationError, match="next_stage|status|terminal"):
        parse_handoff(raw)


@pytest.mark.parametrize("separator", ["\r", "\n", "\x00", "\u2028", "\u2029"])
def test_free_text_rejects_control_and_line_separator_injection(separator: str) -> None:
    action = _terminate_action()
    action["reason"] = f"safe prefix{separator}forged Markdown or terminal line"

    with pytest.raises(ExecutionContractError, match="control|line separator|text"):
        _handoff(next_action=action)


def test_forward_action_cannot_skip_ready_current_stage_or_invent_target_candidate() -> (
    None
):
    sources = _sources()
    candidate_stages = {
        item["stage_id"] for item in sources.terminal_state["candidate_next_nodes"]
    }
    assert candidate_stages == {"S01"}

    with pytest.raises(HandoffValidationError, match="candidate|ready"):
        _handoff(sources, next_action=_forward_action())
