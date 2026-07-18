"""Append-only JSONL Artifact Registry with immutable content identity."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from video_truthfulness.core.artifacts.hashing import canonical_json_bytes, record_hash
from video_truthfulness.core.artifacts.models import ArtifactRecord, new_typed_ulid


class RegistryValidationError(ValueError):
    """Raised when a Registry record or history violates an invariant."""


RegistryScope = Literal["run", "cross_run"]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_artifact_record(**values: Any) -> ArtifactRecord:
    """Create and hash one new Artifact record from explicit metadata."""

    payload = dict(values)
    payload.setdefault("record_id", new_typed_ulid("record"))
    payload.setdefault("record_revision", 1)
    payload.setdefault("recorded_at", _utc_now())
    payload.setdefault("previous_record_id", None)
    payload.setdefault("previous_record_hash", None)
    payload.setdefault("record_hash", "0" * 64)
    payload.setdefault("registry_schema_version", "artifact_record_v1.0.0")
    payload.setdefault("release_version", None)
    payload.setdefault("source_platform", None)
    payload.setdefault("source_id", None)
    payload.setdefault("run_id", None)
    payload.setdefault("batch_id", None)
    payload.setdefault("dataset_build_id", None)
    payload.setdefault("dataset_version", None)
    payload.setdefault("experiment_id", None)
    payload.setdefault("stage_id", None)
    payload.setdefault("dag_node_id", None)
    payload.setdefault("semantic_hash_algorithm", None)
    payload.setdefault("semantic_hash", None)
    payload.setdefault("entity_index_artifact_id", None)
    payload.setdefault("writer_agent_id", None)
    payload.setdefault("agent_version", None)
    payload.setdefault("workflow_id", None)
    payload.setdefault("workflow_version", None)
    payload.setdefault("schema_versions", [])
    payload.setdefault("prompt_id", None)
    payload.setdefault("prompt_version", None)
    payload.setdefault("dag_version", None)
    payload.setdefault("code_commit", None)
    payload.setdefault("tool_versions", {})
    payload.setdefault("parameters_hash", None)
    payload.setdefault("upstream_artifact_ids", [])
    payload.setdefault("upstream_entity_refs", [])
    payload.setdefault("input_fingerprint", None)
    payload.setdefault("validation_artifact_ids", [])
    payload.setdefault("validated_at", None)
    payload.setdefault("frozen_at", None)
    payload.setdefault("archived_at", None)
    payload.setdefault("supersedes", [])
    payload.setdefault("change_reason", None)
    payload.setdefault("metadata_revision_reason", None)
    provisional = ArtifactRecord.model_validate(payload)
    normalized = provisional.model_dump(mode="json")
    normalized["record_hash"] = record_hash(normalized)
    return ArtifactRecord.model_validate(normalized)


def create_metadata_revision(
    previous: ArtifactRecord,
    *,
    metadata_revision_reason: str,
    **updates: Any,
) -> ArtifactRecord:
    """Create a full metadata snapshot while preserving immutable content identity."""

    immutable_fields = {
        "artifact_id",
        "artifact_type",
        "container_kind",
        "storage_version",
        "source_platform",
        "source_id",
        "run_id",
        "batch_id",
        "dataset_build_id",
        "dataset_version",
        "experiment_id",
        "relative_path",
        "storage_scope",
        "media_type",
        "size_bytes",
        "content_hash_algorithm",
        "content_hash",
        "semantic_hash_algorithm",
        "semantic_hash",
    }
    if immutable_fields.intersection(updates):
        raise RegistryValidationError("Metadata revisions cannot change Artifact content identity fields.")
    payload = previous.model_dump(mode="json")
    payload.update(updates)
    payload.update(
        {
            "record_id": new_typed_ulid("record"),
            "record_revision": previous.record_revision + 1,
            "recorded_at": _utc_now(),
            "previous_record_id": previous.record_id,
            "previous_record_hash": previous.record_hash,
            "record_hash": "0" * 64,
            "metadata_revision_reason": metadata_revision_reason,
        }
    )
    provisional = ArtifactRecord.model_validate(payload)
    normalized = provisional.model_dump(mode="json")
    normalized["record_hash"] = record_hash(normalized)
    return ArtifactRecord.model_validate(normalized)


class AppendOnlyRegistry:
    """Validate and append complete record snapshots without rewriting history."""

    def __init__(
        self,
        path: Path,
        *,
        scope: RegistryScope,
        expected_run_id: str | None = None,
    ) -> None:
        self.path = path
        self.scope = scope
        self.expected_run_id = expected_run_id
        if scope == "run" and expected_run_id is None:
            raise ValueError("Run Registry construction requires expected_run_id.")

    def read_records(self) -> list[ArtifactRecord]:
        if not self.path.exists():
            return []
        records: list[ArtifactRecord] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    raise RegistryValidationError(f"Blank Registry line at {self.path}:{line_number}")
                try:
                    raw = json.loads(line)
                    records.append(ArtifactRecord.model_validate(raw))
                except Exception as exc:  # noqa: BLE001 - normalize schema/JSON diagnostics
                    raise RegistryValidationError(f"Invalid Registry line at {self.path}:{line_number}: {exc}") from exc
        self._validate_history(records)
        return records

    def latest_records(self) -> dict[str, ArtifactRecord]:
        latest: dict[str, ArtifactRecord] = {}
        for record in self.read_records():
            latest[record.artifact_id] = record
        return latest

    def validate(self) -> dict[str, int]:
        records = self.read_records()
        return {
            "record_count": len(records),
            "artifact_count": len({record.artifact_id for record in records}),
            "revision_count": sum(max(0, record.record_revision - 1) for record in records),
        }

    def append(self, record: ArtifactRecord | Mapping[str, Any]) -> ArtifactRecord:
        return self.append_many([record])[0]

    def append_many(self, records: Iterable[ArtifactRecord | Mapping[str, Any]]) -> list[ArtifactRecord]:
        additions = [
            record if isinstance(record, ArtifactRecord) else ArtifactRecord.model_validate(record)
            for record in records
        ]
        if not additions:
            return []
        existing = self.read_records()
        combined = existing + additions
        self._validate_history(combined)
        self._validate_references(combined)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            for record in additions:
                handle.write(canonical_json_bytes(record.model_dump(mode="json")).decode("utf-8"))
                handle.write("\n")
            handle.flush()
        return additions

    def _validate_history(self, records: list[ArtifactRecord]) -> None:
        record_ids: set[str] = set()
        latest: dict[str, ArtifactRecord] = {}
        for record in records:
            if record.record_id in record_ids:
                raise RegistryValidationError(f"Duplicate record_id: {record.record_id}")
            record_ids.add(record.record_id)
            if record_hash(record.model_dump(mode="json")) != record.record_hash:
                raise RegistryValidationError(f"record_hash mismatch: {record.record_id}")
            if record.storage_scope != self.scope:
                raise RegistryValidationError(
                    f"Registry scope {self.scope} cannot contain {record.storage_scope} record {record.record_id}."
                )
            if self.scope == "run" and record.run_id != self.expected_run_id:
                raise RegistryValidationError(
                    f"Run Registry {self.expected_run_id} cannot contain run_id={record.run_id}."
                )
            previous = latest.get(record.artifact_id)
            if previous is None:
                if record.record_revision != 1:
                    raise RegistryValidationError(f"First record for {record.artifact_id} must be revision 1.")
            else:
                if record.record_revision != previous.record_revision + 1:
                    raise RegistryValidationError(f"Non-contiguous revision for {record.artifact_id}.")
                if record.previous_record_id != previous.record_id or record.previous_record_hash != previous.record_hash:
                    raise RegistryValidationError(f"Broken revision chain for {record.artifact_id}.")
                immutable = (
                    "artifact_type",
                    "container_kind",
                    "storage_version",
                    "source_platform",
                    "source_id",
                    "run_id",
                    "batch_id",
                    "dataset_build_id",
                    "dataset_version",
                    "experiment_id",
                    "relative_path",
                    "storage_scope",
                    "media_type",
                    "size_bytes",
                    "content_hash_algorithm",
                    "content_hash",
                    "semantic_hash_algorithm",
                    "semantic_hash",
                )
                if any(getattr(record, field) != getattr(previous, field) for field in immutable):
                    raise RegistryValidationError(
                        f"Artifact content changed under existing ID {record.artifact_id}; create a new artifact_id and supersedes relation."
                    )
            latest[record.artifact_id] = record

    @staticmethod
    def _validate_references(records: list[ArtifactRecord]) -> None:
        artifact_ids = {record.artifact_id for record in records}
        first_position: dict[str, int] = {}
        for index, record in enumerate(records):
            first_position.setdefault(record.artifact_id, index)
            referenced = (
                record.upstream_artifact_ids
                + record.validation_artifact_ids
                + record.supersedes
                + [ref.container_artifact_id for ref in record.upstream_entity_refs]
            )
            missing = sorted(set(referenced) - artifact_ids)
            if missing:
                raise RegistryValidationError(f"Record {record.record_id} references unknown Artifacts: {missing}")
            if record.artifact_id in record.supersedes:
                raise RegistryValidationError(f"Artifact {record.artifact_id} cannot supersede itself.")
            not_historical = sorted(
                artifact_id
                for artifact_id in record.supersedes
                if first_position[artifact_id] >= index
            )
            if not_historical:
                raise RegistryValidationError(
                    f"Artifact {record.artifact_id} can only supersede Artifacts already present in Registry history: "
                    f"{not_historical}"
                )
