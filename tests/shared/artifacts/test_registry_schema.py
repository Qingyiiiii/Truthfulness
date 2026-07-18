from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from video_truthfulness.core.artifacts.hashing import record_hash, sha256_file
from video_truthfulness.core.artifacts.models import ArtifactRecord, EntityIndexDocument


ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "artifact_registry" / "synthetic_run"


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_public_valid_record_matches_schema_model_and_file_bytes() -> None:
    raw = _load(EXAMPLE / "valid_artifact_record.json")
    schema = _load(ROOT / "schemas" / "artifact_registry" / "artifact_record_v1.schema.json")
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(raw)
    record = ArtifactRecord.model_validate(raw)

    run_path = EXAMPLE / "run.json"
    assert record.record_hash == record_hash(raw)
    assert record.size_bytes == run_path.stat().st_size
    assert record.content_hash == sha256_file(run_path)


def test_absolute_path_fixture_fails_schema_and_model() -> None:
    raw = _load(EXAMPLE / "invalid_absolute_path_record.json")
    schema = _load(ROOT / "schemas" / "artifact_registry" / "artifact_record_v1.schema.json")
    errors = list(Draft202012Validator(schema).iter_errors(raw))
    assert any(list(error.path) == ["relative_path"] for error in errors)
    with pytest.raises(ValidationError, match="repository-relative POSIX path"):
        ArtifactRecord.model_validate(raw)


def test_registry_model_rejects_credentials_and_scope_without_identity() -> None:
    raw = _load(EXAMPLE / "valid_artifact_record.json")
    raw["tool_versions"] = {"cookie": "synthetic-placeholder"}
    with pytest.raises(ValidationError, match="Credential-bearing field"):
        ArtifactRecord.model_validate(raw)

    raw = _load(EXAMPLE / "valid_artifact_record.json")
    raw.update({"storage_scope": "cross_run", "run_id": None})
    with pytest.raises(ValidationError, match="cross-run records require"):
        ArtifactRecord.model_validate(raw)


def test_registry_model_rejects_incomplete_revision_links() -> None:
    raw = _load(EXAMPLE / "valid_artifact_record.json")
    raw["record_revision"] = 2
    with pytest.raises(ValidationError, match="require previous record"):
        ArtifactRecord.model_validate(raw)

    raw = _load(EXAMPLE / "valid_artifact_record.json")
    raw["previous_record_id"] = "record_01j00000000000000000000000"
    raw["previous_record_hash"] = "a" * 64
    with pytest.raises(ValidationError, match="Revision 1 cannot"):
        ArtifactRecord.model_validate(raw)


def test_entity_index_schema_and_model_accept_stable_row_reference() -> None:
    container_id = "artifact_01j00000000000000000000003"
    raw = {
        "entity_index_schema_version": "entity_index_v1.0.0",
        "container_artifact_id": container_id,
        "created_at": "2026-01-01T00:00:00Z",
        "entries": [
            {
                "entity_id": "claim_synthetic_001",
                "entity_type": "claim",
                "semantic_hash": "a" * 64,
                "container_artifact_id": container_id,
                "upstream_artifact_ids": ["artifact_01j00000000000000000000001"],
                "upstream_entity_refs": [],
                "source_locator": {"kind": "jsonl_line", "value": "1"},
            }
        ],
    }
    schema = _load(ROOT / "schemas" / "artifact_registry" / "entity_index_v1.schema.json")
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(raw)
    assert EntityIndexDocument.model_validate(raw).entries[0].entity_id == "claim_synthetic_001"
