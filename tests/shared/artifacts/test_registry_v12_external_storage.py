from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import ValidationError

from video_truthfulness.core.artifacts.hashing import record_hash
from video_truthfulness.core.artifacts.models import (
    ArtifactRecord,
    ArtifactRecordV1_1,
    ArtifactRecordV1_2,
    to_artifact_record_view,
)
from video_truthfulness.core.artifacts.projection import (
    query_artifact,
    rebuild_sqlite_projection,
)
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryValidationError,
    create_artifact_record,
    create_metadata_revision,
)


ROOT = Path(__file__).resolve().parents[3]
RUN_ID = "run_01j00000000000000000000000"
SCHEMA_VERSION = "artifact_record_v1.2.0"
EXTERNAL_ROOT = "ubuntu_v02_claim_warehouse"


def _record(
    number: int,
    *,
    version: str = SCHEMA_VERSION,
    **updates: object,
):
    values: dict[str, object] = {
        "registry_schema_version": version,
        "artifact_id": f"artifact_{number:026d}",
        "artifact_type": "warehouse.export_batch",
        "logical_name": f"synthetic-export-{number}",
        "container_kind": "package",
        "project_version": "v0.2",
        "storage_version": "V02",
        "source_platform": "youtube",
        "source_id": "youtube_synth3tic01",
        "run_id": RUN_ID,
        "stage_id": "S01",
        "dag_node_id": "warehouse_export",
        "relative_path": (
            f"exports/export_{number:026d}/manifest.json"
            if version == SCHEMA_VERSION
            else f"runs/V02/{RUN_ID}/artifact-{number}.json"
        ),
        "storage_scope": "run",
        "media_type": "application/json",
        "size_bytes": number,
        "content_hash": f"{number:064x}",
        "producer_type": "workflow",
        "schema_versions": [version],
        "tool_versions": {"synthetic": "1"},
        "authority_level": "machine_derived",
        "lifecycle_state": "validated",
        "validation_status": "passed",
        "privacy_class": "public_synthetic",
        "access_scope": "public",
        "retention_policy": "test only",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    if version == "artifact_record_v1.0.0":
        values.update(
            {
                "release_version": "truthfulness_v0.2_youtube_video",
                "agent_version": "legacy-runtime-1",
            }
        )
    else:
        values.update(
            {
                "release_id": "truthfulness_v0.2_youtube_video",
                "agent_profile_version": "truthfulness_agent_v1.1.0",
                "agent_runtime_version": "runtime-1",
            }
        )
    if version == SCHEMA_VERSION:
        values["storage_root_ref"] = EXTERNAL_ROOT
    values.update(updates)
    return create_artifact_record(**values)


def _schema() -> dict[str, object]:
    return json.loads(
        (
            ROOT
            / "schemas"
            / "artifact_registry"
            / "artifact_record_v1_2.schema.json"
        ).read_text(encoding="utf-8")
    )


def test_v12_external_record_validates_and_preserves_wire_hash() -> None:
    record = _record(1)
    assert isinstance(record, ArtifactRecordV1_2)
    raw = record.model_dump(mode="json")
    Draft202012Validator(_schema(), format_checker=FormatChecker()).validate(raw)
    assert record.record_hash == record_hash(raw)

    view = to_artifact_record_view(record)
    assert view.source_registry_schema_version == SCHEMA_VERSION
    assert view.storage_root_ref == EXTERNAL_ROOT
    assert view.relative_path == "exports/export_00000000000000000000000001/manifest.json"


@pytest.mark.parametrize(
    "relative_path",
    [
        "/home/example/export.json",
        "C:/warehouse/export.json",
        "../exports/export.json",
        "exports/../export.json",
        r"exports\export.json",
    ],
)
def test_v12_rejects_absolute_escape_and_non_posix_paths(relative_path: str) -> None:
    with pytest.raises(ValidationError, match="relative_path"):
        _record(1, relative_path=relative_path)


def test_v12_rejects_unknown_storage_root_in_model_and_schema() -> None:
    with pytest.raises(ValidationError, match="storage_root_ref"):
        _record(1, storage_root_ref="unknown_root")

    raw = _record(1).model_dump(mode="json")
    raw["storage_root_ref"] = "unknown_root"
    with pytest.raises(JsonSchemaValidationError):
        Draft202012Validator(_schema(), format_checker=FormatChecker()).validate(raw)


def test_mixed_v10_v11_v12_registry_preserves_old_wire_and_root_identity(
    tmp_path: Path,
) -> None:
    path = tmp_path / "mixed.jsonl"
    registry = AppendOnlyRegistry(path, scope="run", expected_run_id=RUN_ID)
    legacy = _record(1, version="artifact_record_v1.0.0")
    canonical = _record(2, version="artifact_record_v1.1.0")
    external = _record(3)
    registry.append_many([legacy, canonical, external])

    entries = registry.read_entries()
    assert [entry.wire_record.registry_schema_version for entry in entries] == [
        "artifact_record_v1.0.0",
        "artifact_record_v1.1.0",
        SCHEMA_VERSION,
    ]
    assert isinstance(entries[0].wire_record, ArtifactRecord)
    assert isinstance(entries[1].wire_record, ArtifactRecordV1_1)
    assert isinstance(entries[2].wire_record, ArtifactRecordV1_2)
    assert [entry.canonical_view.storage_root_ref for entry in entries] == [
        "repository",
        "repository",
        EXTERNAL_ROOT,
    ]
    assert "storage_root_ref" not in entries[0].wire_record.model_dump(mode="json")
    assert "storage_root_ref" not in entries[1].wire_record.model_dump(mode="json")


def test_v12_sqlite_projection_retains_storage_root_identity(tmp_path: Path) -> None:
    registry_path = tmp_path / "external.jsonl"
    projection_path = tmp_path / "external.sqlite3"
    registry = AppendOnlyRegistry(
        registry_path,
        scope="run",
        expected_run_id=RUN_ID,
    )
    repository_record = _record(1, version="artifact_record_v1.1.0")
    external_record = _record(2)
    registry.append_many([repository_record, external_record])

    rebuild_sqlite_projection(projection_path, [registry])
    repository_row = query_artifact(projection_path, repository_record.artifact_id)
    external_row = query_artifact(projection_path, external_record.artifact_id)
    assert repository_row is not None and external_row is not None
    assert repository_row["storage_root_ref"] == "repository"
    assert external_row["storage_root_ref"] == EXTERNAL_ROOT
    assert external_row["relative_path"] == external_record.relative_path

    connection = sqlite3.connect(projection_path)
    try:
        raw_json = connection.execute(
            "SELECT raw_json FROM registry_records WHERE record_id = ?",
            (external_record.record_id,),
        ).fetchone()[0]
    finally:
        connection.close()
    assert json.loads(raw_json)["storage_root_ref"] == EXTERNAL_ROOT


def test_metadata_upgrade_defaults_to_repository_and_v12_cannot_downgrade() -> None:
    canonical = _record(1, version="artifact_record_v1.1.0")
    upgraded = create_metadata_revision(
        canonical,
        registry_schema_version=SCHEMA_VERSION,
        metadata_revision_reason="adopt explicit repository storage root",
    )
    assert isinstance(upgraded, ArtifactRecordV1_2)
    assert upgraded.storage_root_ref == "repository"

    with pytest.raises(RegistryValidationError, match="cannot downgrade"):
        create_metadata_revision(
            upgraded,
            registry_schema_version="artifact_record_v1.1.0",
            metadata_revision_reason="forbidden downgrade",
        )


def test_storage_root_is_immutable_for_metadata_revisions() -> None:
    external = _record(1)
    with pytest.raises(RegistryValidationError, match="content identity"):
        create_metadata_revision(
            external,
            metadata_revision_reason="forbidden storage move",
            storage_root_ref="repository",
        )
