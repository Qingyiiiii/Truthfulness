from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from video_truthfulness.core.artifacts.hashing import canonical_json_bytes
from video_truthfulness.core.artifacts.models import ArtifactRecordWire, new_typed_ulid
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryValidationError,
    create_artifact_record,
    create_metadata_revision,
)


RUN_ID = "run_01j00000000000000000000000"


@pytest.fixture
def registry_path(tmp_path: Path) -> Path:
    return tmp_path / f"artifact-registry-{new_typed_ulid('test')}.jsonl"


def _record(number: int, **updates: object):
    values: dict[str, object] = {
        "artifact_id": f"artifact_{number:026d}",
        "artifact_type": "run.identity",
        "logical_name": f"synthetic-{number}",
        "container_kind": "file",
        "project_version": "v0.2",
        "storage_version": "V02",
        "source_platform": "youtube",
        "source_id": "youtube_synth3tic01",
        "run_id": RUN_ID,
        "stage_id": "S01",
        "dag_node_id": "source_identity",
        "relative_path": f"runs/V02/{RUN_ID}/artifact-{number}.json",
        "storage_scope": "run",
        "media_type": "application/json",
        "size_bytes": number,
        "content_hash": f"{number:064x}",
        "producer_type": "workflow",
        "schema_versions": ["artifact_record_v1.1.0"],
        "tool_versions": {"synthetic": "1"},
        "authority_level": "machine_derived",
        "lifecycle_state": "created",
        "validation_status": "not_validated",
        "privacy_class": "public_synthetic",
        "access_scope": "public",
        "retention_policy": "test only",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    values.update(updates)
    return create_artifact_record(**values)


def _reference_update(field_name: str, artifact_id: str) -> dict[str, object]:
    if field_name == "upstream_entity_refs":
        return {
            field_name: [
                {
                    "entity_id": "claim_synthetic_001",
                    "entity_type": "claim",
                    "container_artifact_id": artifact_id,
                }
            ]
        }
    return {field_name: [artifact_id]}


def _write_existing_history(path: Path, *records: ArtifactRecordWire) -> None:
    path.write_bytes(
        b"".join(
            canonical_json_bytes(record.model_dump(mode="json")) + b"\n"
            for record in records
        )
    )


def test_metadata_revision_appends_without_rewriting_history(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    first = _record(1)
    registry.append(first)
    original = registry_path.read_bytes()

    revision = create_metadata_revision(
        first,
        metadata_revision_reason="validation completed",
        logical_name="synthetic validated identity",
        lifecycle_state="validated",
        validation_status="passed",
        validated_at=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
    )
    registry.append(revision)

    assert registry_path.read_bytes().startswith(original)
    assert registry.validate() == {
        "record_count": 2,
        "artifact_count": 1,
        "revision_count": 1,
    }
    assert registry.latest_records()[first.artifact_id].record_id == revision.record_id
    assert revision.previous_record_hash == first.record_hash


def test_content_identity_change_requires_new_artifact_id(registry_path: Path) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    first = _record(1)
    registry.append(first)

    with pytest.raises(RegistryValidationError, match="content identity"):
        create_metadata_revision(
            first, metadata_revision_reason="forbidden", content_hash="f" * 64
        )

    payload = first.model_dump(mode="json")
    payload.update(
        {
            "record_id": new_typed_ulid("record"),
            "record_revision": 2,
            "recorded_at": datetime.now(timezone.utc),
            "previous_record_id": first.record_id,
            "previous_record_hash": first.record_hash,
            "record_hash": "0" * 64,
            "content_hash": "f" * 64,
            "metadata_revision_reason": "forbidden mutation",
        }
    )
    conflicting = create_artifact_record(**payload)
    with pytest.raises(
        RegistryValidationError, match="content changed under existing ID"
    ):
        registry.append(conflicting)


def test_scope_references_and_supersedes_are_enforced(registry_path: Path) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    first = _record(1)
    registry.append(first)

    cross_run = _record(
        2,
        storage_scope="cross_run",
        run_id=None,
        batch_id="batch_01j00000000000000000000000",
    )
    with pytest.raises(RegistryValidationError, match="cannot contain cross_run"):
        registry.append(cross_run)

    unknown = _record(3, supersedes=["artifact_01j00000000000000000000999"])
    with pytest.raises(RegistryValidationError, match="unknown Artifacts"):
        registry.append(unknown)

    self_superseding = _record(4)
    self_payload = self_superseding.model_dump(mode="json")
    self_payload["supersedes"] = [self_superseding.artifact_id]
    self_payload["record_hash"] = "0" * 64
    self_superseding = create_artifact_record(**self_payload)
    with pytest.raises(RegistryValidationError, match="cannot supersede itself"):
        registry.append(self_superseding)


def test_new_content_can_supersede_existing_artifact(registry_path: Path) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    first = _record(1, lifecycle_state="validated", validation_status="passed")
    replacement = _record(
        2, supersedes=[first.artifact_id], change_reason="corrected synthetic content"
    )
    registry.append_many([first, replacement])
    assert registry.validate()["artifact_count"] == 2


def test_validate_full_history_is_read_only_and_append_many_uses_same_rules(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    first = _record(1, lifecycle_state="validated", validation_status="passed")
    second = _record(2, upstream_artifact_ids=[first.artifact_id])

    assert registry.validate_full_history(candidate_records=[first, second]) == {
        "record_count": 2,
        "artifact_count": 2,
        "revision_count": 0,
        "candidate_record_count": 2,
    }
    assert not registry_path.exists()

    registry.append_many([first, second])
    prefix = registry_path.read_bytes()
    invalid = _record(3, upstream_artifact_ids=["artifact_01j00000000000000000000999"])
    with pytest.raises(RegistryValidationError, match="unknown Artifacts"):
        registry.validate_full_history(candidate_records=[invalid])
    assert registry_path.read_bytes() == prefix
    with pytest.raises(RegistryValidationError, match="unknown Artifacts"):
        registry.append_many([invalid])
    assert registry_path.read_bytes() == prefix


@pytest.mark.parametrize(
    "reference_update",
    [
        {"upstream_artifact_ids": ["artifact_01j00000000000000000000999"]},
        {"validation_artifact_ids": ["artifact_01j00000000000000000000999"]},
        {"supersedes": ["artifact_01j00000000000000000000999"]},
    ],
)
def test_validate_full_history_rejects_each_unknown_reference_without_writing(
    registry_path: Path,
    reference_update: dict[str, list[str]],
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    with pytest.raises(RegistryValidationError, match="unknown Artifacts"):
        registry.validate_full_history(
            candidate_records=[_record(1, **reference_update)]
        )
    assert not registry_path.exists()


def test_validate_full_history_rejects_self_and_forward_supersedes_without_writing(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    self_superseding = _record(1)
    payload = self_superseding.model_dump(mode="json")
    payload.update(
        {"supersedes": [self_superseding.artifact_id], "record_hash": "0" * 64}
    )
    self_superseding = create_artifact_record(**payload)
    with pytest.raises(RegistryValidationError, match="cannot supersede itself"):
        registry.validate_full_history(candidate_records=[self_superseding])
    assert not registry_path.exists()

    future = _record(2)
    replacement = _record(3, supersedes=[future.artifact_id])
    with pytest.raises(
        RegistryValidationError, match="already present in Registry history"
    ):
        registry.validate_full_history(candidate_records=[replacement, future])
    assert not registry_path.exists()


@pytest.mark.parametrize(
    "source_update",
    [
        {
            "source_platform": "bilibili",
            "source_id": "bilibili_BV1234567890",
        },
        {"source_id": "youtube_synth3tic02"},
    ],
)
def test_validate_full_history_rejects_conflicting_run_source_identity_without_writing(
    registry_path: Path,
    source_update: dict[str, str],
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    with pytest.raises(
        RegistryValidationError, match="conflicting canonical source identity"
    ):
        registry.validate_full_history(
            candidate_records=[_record(1), _record(2, **source_update)]
        )
    assert not registry_path.exists()


def test_validate_full_history_rejects_platform_source_id_mismatch_without_writing(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    mismatched = _record(
        1,
        source_platform="bilibili",
        source_id="youtube_synth3tic01",
    )
    with pytest.raises(
        RegistryValidationError, match="incompatible source_platform/source_id"
    ):
        registry.validate_full_history(candidate_records=[mismatched])
    assert not registry_path.exists()


def test_legacy_public_synthetic_history_accepts_null_handoff_completion(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    legacy_input = _record(
        1,
        artifact_type="synthetic.input",
        source_platform=None,
        source_id=None,
        lifecycle_state="validated",
        validation_status="passed",
    )
    legacy_output = _record(
        2,
        artifact_type="synthetic.output",
        source_platform=None,
        source_id=None,
        upstream_artifact_ids=[legacy_input.artifact_id],
        lifecycle_state="validated",
        validation_status="passed",
    )
    _write_existing_history(registry_path, legacy_input, legacy_output)
    handoff = _record(
        3,
        artifact_type="handoff.run",
        source_platform=None,
        source_id=None,
        upstream_artifact_ids=[legacy_input.artifact_id, legacy_output.artifact_id],
        lifecycle_state="frozen",
        validation_status="passed",
    )

    registry.append(handoff)

    assert registry.latest_records()[handoff.artifact_id].record_id == handoff.record_id


@pytest.mark.parametrize(
    "candidate_update",
    [
        {"artifact_type": "run.identity"},
        {
            "artifact_type": "handoff.run",
            "privacy_class": "private_derived",
            "access_scope": "project_private",
        },
        {
            "artifact_type": "handoff.run",
            "access_scope": "project_private",
        },
    ],
)
def test_legacy_null_handoff_exception_does_not_expand_to_adjacent_candidates(
    registry_path: Path,
    candidate_update: dict[str, str],
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    legacy = _record(
        1,
        artifact_type="synthetic.input",
        source_platform=None,
        source_id=None,
        lifecycle_state="validated",
        validation_status="passed",
    )
    _write_existing_history(registry_path, legacy)
    prefix = registry_path.read_bytes()
    candidate = _record(
        2,
        source_platform=None,
        source_id=None,
        **candidate_update,
    )

    with pytest.raises(
        RegistryValidationError,
        match="requires canonical source_platform and source_id",
    ):
        registry.append(candidate)
    assert registry_path.read_bytes() == prefix


@pytest.mark.parametrize(
    "identity_update",
    [
        {"source_platform": None, "source_id": None},
        {"source_platform": "youtube", "source_id": None},
        {"source_platform": None, "source_id": "youtube_synth3tic01"},
    ],
)
def test_new_revision_one_candidate_requires_complete_source_identity(
    registry_path: Path,
    identity_update: dict[str, str | None],
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    missing_identity = _record(1, **identity_update)

    with pytest.raises(
        RegistryValidationError,
        match="requires canonical source_platform and source_id",
    ):
        registry.validate_full_history(candidate_records=[missing_identity])
    assert not registry_path.exists()


def test_existing_v10_null_source_identity_remains_readable(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    legacy_v10 = _record(
        1,
        registry_schema_version="artifact_record_v1.0.0",
        source_platform=None,
        source_id=None,
        privacy_class="private_derived",
        access_scope="project_private",
        schema_versions=["artifact_record_v1.0.0"],
    )
    _write_existing_history(registry_path, legacy_v10)

    assert registry.validate_full_history() == {
        "record_count": 1,
        "artifact_count": 1,
        "revision_count": 0,
        "candidate_record_count": 0,
    }


def test_legacy_null_metadata_revision_preserves_immutable_source_identity(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    legacy = _record(
        1,
        source_platform=None,
        source_id=None,
        lifecycle_state="validated",
        validation_status="passed",
    )
    _write_existing_history(registry_path, legacy)
    revision = create_metadata_revision(
        legacy,
        metadata_revision_reason="legacy synthetic metadata correction",
        logical_name="corrected legacy synthetic identity metadata",
    )

    registry.append(revision)

    assert registry.latest_records()[legacy.artifact_id].source_platform is None
    assert registry.latest_records()[legacy.artifact_id].source_id is None


def test_candidate_can_establish_one_canonical_pair_after_legacy_null_history(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    legacy = _record(
        1,
        source_platform=None,
        source_id=None,
        lifecycle_state="validated",
        validation_status="passed",
    )
    _write_existing_history(registry_path, legacy)
    canonical = _record(2)
    registry.append(canonical)
    prefix = registry_path.read_bytes()
    conflict = _record(
        3,
        source_platform="bilibili",
        source_id="bilibili_BV1234567890",
    )

    with pytest.raises(
        RegistryValidationError, match="conflicting canonical source identity"
    ):
        registry.append(conflict)
    assert registry_path.read_bytes() == prefix


@pytest.mark.parametrize(
    ("lifecycle_state", "validation_status"),
    [
        ("created", "failed"),
        ("stale", "passed"),
        ("superseded", "passed"),
        ("invalid", "failed"),
        ("archived", "passed"),
        ("purged", "passed"),
    ],
)
@pytest.mark.parametrize(
    ("reference_field", "error_pattern"),
    [
        ("upstream_artifact_ids", "upstream_artifact_ids.*latest revision 2"),
        ("validation_artifact_ids", "validation_artifact_ids.*latest revision 2"),
        ("upstream_entity_refs", "upstream_entity_refs.*latest revision 2"),
    ],
)
def test_active_candidate_dependencies_require_latest_active_target_without_writing(
    registry_path: Path,
    lifecycle_state: str,
    validation_status: str,
    reference_field: str,
    error_pattern: str,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    target = _record(1, lifecycle_state="validated", validation_status="passed")
    latest_target = create_metadata_revision(
        target,
        metadata_revision_reason="synthetic latest lifecycle transition",
        lifecycle_state=lifecycle_state,
        validation_status=validation_status,
    )
    candidate = _record(2, **_reference_update(reference_field, target.artifact_id))

    with pytest.raises(RegistryValidationError, match=error_pattern):
        registry.validate_full_history(
            candidate_records=[target, latest_target, candidate]
        )
    assert not registry_path.exists()


def test_active_candidate_accepts_created_validated_and_frozen_latest_targets(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    upstream = _record(1, lifecycle_state="created", validation_status="not_validated")
    validation = _record(2, lifecycle_state="validated", validation_status="passed")
    entity_container = _record(3, lifecycle_state="frozen", validation_status="passed")
    supersedes_target = _record(
        4, lifecycle_state="validated", validation_status="passed"
    )
    candidate = _record(
        5,
        upstream_artifact_ids=[upstream.artifact_id],
        validation_artifact_ids=[validation.artifact_id],
        upstream_entity_refs=_reference_update(
            "upstream_entity_refs", entity_container.artifact_id
        )["upstream_entity_refs"],
        supersedes=[supersedes_target.artifact_id],
    )

    assert (
        registry.validate_full_history(
            candidate_records=[
                upstream,
                validation,
                entity_container,
                supersedes_target,
                candidate,
            ]
        )["candidate_record_count"]
        == 5
    )
    assert not registry_path.exists()


@pytest.mark.parametrize(
    "reference_field",
    ["upstream_artifact_ids", "validation_artifact_ids", "upstream_entity_refs"],
)
def test_candidate_dependency_forward_references_are_rejected_without_writing(
    registry_path: Path,
    reference_field: str,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    future = _record(2, lifecycle_state="validated", validation_status="passed")
    candidate = _record(1, **_reference_update(reference_field, future.artifact_id))

    with pytest.raises(
        RegistryValidationError,
        match=rf"{reference_field} can only reference Artifacts already present",
    ):
        registry.validate_full_history(candidate_records=[candidate, future])
    assert not registry_path.exists()


def test_stale_metadata_revision_can_preserve_historical_dependencies(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    upstream = _record(1, lifecycle_state="validated", validation_status="passed")
    dependent = _record(
        2,
        lifecycle_state="validated",
        validation_status="passed",
        upstream_artifact_ids=[upstream.artifact_id],
    )
    registry.append_many([upstream, dependent])
    prefix = registry_path.read_bytes()
    upstream_stale = create_metadata_revision(
        upstream,
        metadata_revision_reason="synthetic upstream invalidation",
        lifecycle_state="stale",
        validation_status="passed",
    )
    dependent_stale = create_metadata_revision(
        dependent,
        metadata_revision_reason="synthetic forward stale propagation",
        lifecycle_state="stale",
        validation_status="passed",
    )

    assert (
        registry.validate_full_history(
            candidate_records=[upstream_stale, dependent_stale]
        )["candidate_record_count"]
        == 2
    )
    assert registry_path.read_bytes() == prefix


@pytest.mark.parametrize(
    ("lifecycle_state", "validation_status"),
    [("stale", "passed"), ("invalid", "failed")],
)
def test_new_artifact_can_supersede_stale_or_invalid_latest_content(
    registry_path: Path,
    lifecycle_state: str,
    validation_status: str,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    target = _record(1, lifecycle_state="validated", validation_status="passed")
    registry.append(target)
    prefix = registry_path.read_bytes()
    inactive_target = create_metadata_revision(
        target,
        metadata_revision_reason="synthetic correction prerequisite",
        lifecycle_state=lifecycle_state,
        validation_status=validation_status,
    )
    replacement = _record(2, supersedes=[target.artifact_id])

    assert (
        registry.validate_full_history(
            candidate_records=[inactive_target, replacement]
        )["candidate_record_count"]
        == 2
    )
    assert registry_path.read_bytes() == prefix


def test_supersedes_rejects_target_with_later_latest_revision_without_writing(
    registry_path: Path,
) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    target = _record(1, lifecycle_state="validated", validation_status="passed")
    candidate = _record(2, supersedes=[target.artifact_id])
    later_revision = create_metadata_revision(
        target,
        metadata_revision_reason="synthetic later revision",
        lifecycle_state="frozen",
        validation_status="passed",
    )

    with pytest.raises(
        RegistryValidationError, match="latest revision.*after that revision"
    ):
        registry.validate_full_history(
            candidate_records=[target, candidate, later_revision]
        )
    assert not registry_path.exists()
