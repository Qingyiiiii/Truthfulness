from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from video_truthfulness.core.artifacts.models import new_typed_ulid
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryValidationError,
    create_artifact_record,
    create_metadata_revision,
)


RUN_ID = "run_01j00000000000000000000000"
ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def registry_path() -> Path:
    root = ROOT / ".tmp"
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"artifact-registry-{new_typed_ulid('test')}.jsonl"
    yield path
    path.unlink(missing_ok=True)


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
        "schema_versions": ["artifact_record_v1.0.0"],
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


def test_metadata_revision_appends_without_rewriting_history(registry_path: Path) -> None:
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
    assert registry.validate() == {"record_count": 2, "artifact_count": 1, "revision_count": 1}
    assert registry.latest_records()[first.artifact_id].record_id == revision.record_id
    assert revision.previous_record_hash == first.record_hash


def test_content_identity_change_requires_new_artifact_id(registry_path: Path) -> None:
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    first = _record(1)
    registry.append(first)

    with pytest.raises(RegistryValidationError, match="content identity"):
        create_metadata_revision(first, metadata_revision_reason="forbidden", content_hash="f" * 64)

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
    with pytest.raises(RegistryValidationError, match="content changed under existing ID"):
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
    first = _record(1)
    replacement = _record(2, supersedes=[first.artifact_id], change_reason="corrected synthetic content")
    registry.append_many([first, replacement])
    assert registry.validate()["artifact_count"] == 2
