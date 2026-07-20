from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from video_truthfulness.core.artifacts.hashing import record_hash
from video_truthfulness.core.artifacts.models import (
    ArtifactRecord,
    ArtifactRecordV1_1,
    parse_artifact_record,
    to_artifact_record_view,
)
from video_truthfulness.core.artifacts.projection import query_artifact, rebuild_sqlite_projection
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryValidationError,
    create_artifact_record,
    create_metadata_revision,
)


ROOT = Path(__file__).resolve().parents[3]
RUN_ID = "run_01j00000000000000000000000"
LEGACY_EXP_ID = "experiment_01arz3ndektsv4rrffq69g5fav"
CANONICAL_EXP_ID = "exp_01arz3ndektsv4rrffq69g5fav"


def _record(
    number: int,
    *,
    version: str | None = None,
    **updates: object,
):
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
        "schema_versions": [version or "artifact_record_v1.1.0"],
        "tool_versions": {"synthetic": "1"},
        "authority_level": "machine_derived",
        "lifecycle_state": "validated",
        "validation_status": "passed",
        "privacy_class": "public_synthetic",
        "access_scope": "public",
        "retention_policy": "test only",
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    if version is not None:
        values["registry_schema_version"] = version
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
    values.update(updates)
    return create_artifact_record(**values)


def test_new_records_default_to_v11_schema_and_canonical_fields() -> None:
    record = _record(1)
    assert isinstance(record, ArtifactRecordV1_1)
    raw = record.model_dump(mode="json")
    schema = json.loads(
        (ROOT / "schemas" / "artifact_registry" / "artifact_record_v1_1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(raw)
    assert record.record_hash == record_hash(raw)
    assert not {"release_version", "experiment_id", "agent_version"}.intersection(raw)


def test_v11_rejects_legacy_fields_and_legacy_experiment_value() -> None:
    raw = _record(1).model_dump(mode="json")
    for legacy_name in ("release_version", "experiment_id", "agent_version"):
        candidate = dict(raw)
        candidate[legacy_name] = None
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            ArtifactRecordV1_1.model_validate(candidate)

    candidate = dict(raw)
    candidate["exp_id"] = LEGACY_EXP_ID
    with pytest.raises(ValidationError, match="String should match pattern"):
        ArtifactRecordV1_1.model_validate(candidate)


def test_v10_mapping_preserves_wire_and_exposes_canonical_aliases() -> None:
    record = _record(
        1,
        version="artifact_record_v1.0.0",
        experiment_id=LEGACY_EXP_ID,
    )
    assert isinstance(record, ArtifactRecord)
    wire_before = record.model_dump(mode="json")
    view = to_artifact_record_view(record)
    assert view.source_registry_schema_version == "artifact_record_v1.0.0"
    assert view.release_id == "truthfulness_v0.2_youtube_video"
    assert view.exp_id == CANONICAL_EXP_ID
    assert view.legacy_experiment_id == LEGACY_EXP_ID
    assert view.storage_root_ref == "repository"
    assert view.agent_profile_version is None
    assert view.agent_runtime_version == "legacy-runtime-1"
    assert record.model_dump(mode="json") == wire_before
    assert record_hash(view.model_dump(mode="json")) != record.record_hash


def test_mixed_registry_reads_wire_entries_then_canonical_views(tmp_path: Path) -> None:
    path = tmp_path / "mixed.jsonl"
    registry = AppendOnlyRegistry(path, scope="run", expected_run_id=RUN_ID)
    legacy = _record(1, version="artifact_record_v1.0.0")
    registry.append(legacy)
    legacy_prefix = path.read_bytes()
    canonical = _record(2)
    registry.append(canonical)

    assert path.read_bytes().startswith(legacy_prefix)
    entries = registry.read_entries()
    assert [entry.wire_record.registry_schema_version for entry in entries] == [
        "artifact_record_v1.0.0",
        "artifact_record_v1.1.0",
    ]
    assert [record.source_registry_schema_version for record in registry.read_records()] == [
        "artifact_record_v1.0.0",
        "artifact_record_v1.1.0",
    ]
    assert [record.storage_root_ref for record in registry.read_records()] == [
        "repository",
        "repository",
    ]


def test_revision_chain_can_upgrade_but_never_downgrade(tmp_path: Path) -> None:
    path = tmp_path / "upgrade.jsonl"
    registry = AppendOnlyRegistry(path, scope="run", expected_run_id=RUN_ID)
    legacy = _record(
        1,
        version="artifact_record_v1.0.0",
        experiment_id=LEGACY_EXP_ID,
    )
    registry.append(legacy)
    legacy_prefix = path.read_bytes()
    upgraded = create_metadata_revision(
        legacy,
        registry_schema_version="artifact_record_v1.1.0",
        metadata_revision_reason="adopt canonical Registry fields",
        agent_profile_version="truthfulness_agent_v1.1.0",
        agent_runtime_version="runtime-2",
    )
    registry.append(upgraded)

    assert isinstance(upgraded, ArtifactRecordV1_1)
    assert upgraded.previous_record_hash == legacy.record_hash
    assert upgraded.exp_id == CANONICAL_EXP_ID
    assert path.read_bytes().startswith(legacy_prefix)
    assert registry.validate() == {"record_count": 2, "artifact_count": 1, "revision_count": 1}
    with pytest.raises(RegistryValidationError, match="cannot downgrade"):
        create_metadata_revision(
            upgraded,
            registry_schema_version="artifact_record_v1.0.0",
            metadata_revision_reason="forbidden downgrade",
        )


@pytest.mark.parametrize("version", [None, "artifact_record_v9.9.9"])
def test_unknown_or_missing_wire_version_reports_registry_line(tmp_path: Path, version: str | None) -> None:
    path = tmp_path / "invalid.jsonl"
    raw = {"record_id": "record_01j00000000000000000000000"}
    if version is not None:
        raw["registry_schema_version"] = version
    path.write_text(json.dumps(raw) + "\n", encoding="utf-8")
    registry = AppendOnlyRegistry(path, scope="run", expected_run_id=RUN_ID)
    with pytest.raises(RegistryValidationError, match=r"invalid\.jsonl:1"):
        registry.read_entries()


def test_projection_preserves_wire_json_and_exposes_canonical_columns(tmp_path: Path) -> None:
    registry_path = tmp_path / "projection.jsonl"
    projection_path = tmp_path / "projection.sqlite3"
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    legacy = _record(
        1,
        version="artifact_record_v1.0.0",
        experiment_id=LEGACY_EXP_ID,
    )
    canonical = _record(2, exp_id="exp_01arz3ndektsv4rrffq69g5faw")
    registry.append_many([legacy, canonical])
    authoritative_bytes = registry_path.read_bytes()
    rebuild_sqlite_projection(projection_path, [registry])

    legacy_row = query_artifact(projection_path, legacy.artifact_id)
    canonical_row = query_artifact(projection_path, canonical.artifact_id)
    assert legacy_row is not None and canonical_row is not None
    assert legacy_row["exp_id"] == CANONICAL_EXP_ID
    assert legacy_row["agent_profile_version"] is None
    assert legacy_row["agent_runtime_version"] == "legacy-runtime-1"
    assert canonical_row["agent_profile_version"] == "truthfulness_agent_v1.1.0"

    connection = sqlite3.connect(projection_path)
    try:
        source_version, raw_json = connection.execute(
            "SELECT source_registry_schema_version, raw_json FROM registry_records WHERE record_id = ?",
            (legacy.record_id,),
        ).fetchone()
    finally:
        connection.close()
    assert source_version == "artifact_record_v1.0.0"
    assert '"experiment_id"' in raw_json
    assert '"exp_id"' not in raw_json
    assert registry_path.read_bytes() == authoritative_bytes


def test_wire_dispatch_rejects_unknown_version_before_normalization() -> None:
    with pytest.raises(ValueError, match="Unsupported registry_schema_version"):
        parse_artifact_record({"registry_schema_version": "artifact_record_v9.9.9"})
