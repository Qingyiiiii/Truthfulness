"""Append-only JSONL Artifact Registry with immutable content identity."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from video_truthfulness.core.artifacts.hashing import canonical_json_bytes, record_hash
from video_truthfulness.core.artifacts.models import (
    ArtifactRecordView,
    ArtifactRecordWire,
    new_typed_ulid,
    parse_artifact_record,
    to_artifact_record_view,
)


class RegistryValidationError(ValueError):
    """Raised when a Registry record or history violates an invariant."""


RegistryScope = Literal["run", "cross_run"]
RegistrySchemaVersion = Literal[
    "artifact_record_v1.0.0",
    "artifact_record_v1.1.0",
    "artifact_record_v1.2.0",
]
_REGISTRY_VERSION_ORDER: dict[RegistrySchemaVersion, int] = {
    "artifact_record_v1.0.0": 0,
    "artifact_record_v1.1.0": 1,
    "artifact_record_v1.2.0": 2,
}
_ACTIVE_LIFECYCLES = frozenset({"created", "validated", "frozen"})


@dataclass(frozen=True)
class RegistryEntry:
    """One validated wire record paired with its version-neutral consumer view."""

    wire_record: ArtifactRecordWire
    canonical_view: ArtifactRecordView


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fsync_directory(path: Path) -> None:
    """Persist a directory entry where the host exposes POSIX directory fsync."""

    if os.name == "nt":
        return
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def create_artifact_record(**values: Any) -> ArtifactRecordWire:
    """Create and hash one new Artifact record from explicit metadata."""

    payload = dict(values)
    schema_version: RegistrySchemaVersion = payload.setdefault(
        "registry_schema_version",
        "artifact_record_v1.1.0",
    )
    if schema_version not in _REGISTRY_VERSION_ORDER:
        raise RegistryValidationError(
            f"Unsupported registry_schema_version: {schema_version}"
        )
    payload.setdefault("record_id", new_typed_ulid("record"))
    payload.setdefault("record_revision", 1)
    payload.setdefault("recorded_at", _utc_now())
    payload.setdefault("previous_record_id", None)
    payload.setdefault("previous_record_hash", None)
    payload.setdefault("record_hash", "0" * 64)
    if schema_version == "artifact_record_v1.0.0":
        payload.setdefault("release_version", None)
        payload.setdefault("experiment_id", None)
        payload.setdefault("agent_version", None)
    else:
        payload.setdefault("release_id", "truthfulness_v0.2_youtube_video")
        payload.setdefault("exp_id", None)
        payload.setdefault("agent_profile_version", None)
        payload.setdefault("agent_runtime_version", None)
    payload.setdefault("source_platform", None)
    payload.setdefault("source_id", None)
    payload.setdefault("run_id", None)
    payload.setdefault("batch_id", None)
    payload.setdefault("dataset_build_id", None)
    payload.setdefault("dataset_version", None)
    payload.setdefault("stage_id", None)
    payload.setdefault("dag_node_id", None)
    payload.setdefault("semantic_hash_algorithm", None)
    payload.setdefault("semantic_hash", None)
    payload.setdefault("entity_index_artifact_id", None)
    payload.setdefault("writer_agent_id", None)
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
    provisional = parse_artifact_record(payload)
    normalized = provisional.model_dump(mode="json")
    normalized["record_hash"] = record_hash(normalized)
    return parse_artifact_record(normalized)


def create_metadata_revision(
    previous: ArtifactRecordWire,
    *,
    metadata_revision_reason: str,
    registry_schema_version: RegistrySchemaVersion | None = None,
    **updates: Any,
) -> ArtifactRecordWire:
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
        "exp_id",
        "release_version",
        "release_id",
        "storage_root_ref",
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
        raise RegistryValidationError(
            "Metadata revisions cannot change Artifact content identity fields."
        )
    target_version = registry_schema_version or previous.registry_schema_version
    if _REGISTRY_VERSION_ORDER[target_version] < _REGISTRY_VERSION_ORDER[
        previous.registry_schema_version
    ]:
        raise RegistryValidationError(
            "Registry metadata revisions cannot downgrade wire schema versions."
        )
    if target_version == previous.registry_schema_version:
        payload = previous.model_dump(mode="json")
    elif target_version in {
        "artifact_record_v1.1.0",
        "artifact_record_v1.2.0",
    }:
        payload = to_artifact_record_view(previous).model_dump(mode="json")
        payload.pop("source_registry_schema_version")
        payload.pop("legacy_experiment_id")
        payload["registry_schema_version"] = target_version
        if target_version == "artifact_record_v1.1.0":
            payload.pop("storage_root_ref")
    else:
        raise RegistryValidationError(
            f"Unsupported Registry revision version transition: {previous.registry_schema_version} -> {target_version}"
        )
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
    return create_artifact_record(**payload)


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

    def read_entries(self) -> list[RegistryEntry]:
        if not self.path.exists():
            return []
        entries: list[RegistryEntry] = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    raise RegistryValidationError(
                        f"Blank Registry line at {self.path}:{line_number}"
                    )
                try:
                    raw = json.loads(line)
                    wire_record = parse_artifact_record(raw)
                    entries.append(
                        RegistryEntry(
                            wire_record=wire_record,
                            canonical_view=to_artifact_record_view(wire_record),
                        )
                    )
                except Exception as exc:  # noqa: BLE001 - normalize schema/JSON diagnostics
                    raise RegistryValidationError(
                        f"Invalid Registry line at {self.path}:{line_number}: {exc}"
                    ) from exc
        self._validate_history(entries)
        return entries

    def read_records(self) -> list[ArtifactRecordView]:
        """Return canonical views after every wire hash and history invariant passes."""

        return [entry.canonical_view for entry in self.read_entries()]

    def latest_records(self) -> dict[str, ArtifactRecordView]:
        latest: dict[str, ArtifactRecordView] = {}
        for record in self.read_records():
            latest[record.artifact_id] = record
        return latest

    def validate(self) -> dict[str, int]:
        entries = self.read_entries()
        records = [entry.canonical_view for entry in entries]
        return {
            "record_count": len(records),
            "artifact_count": len({record.artifact_id for record in records}),
            "revision_count": sum(
                max(0, record.record_revision - 1) for record in records
            ),
        }

    def validate_full_history(
        self,
        candidate_records: Iterable[ArtifactRecordWire | Mapping[str, Any]] = (),
    ) -> dict[str, int]:
        """Validate stored history plus candidate records without writing any bytes.

        Unlike :meth:`validate`, this public dry-run also validates every Artifact,
        validation, entity-container and supersedes reference. Candidate records are
        evaluated in their proposed append order and the Registry file is never
        created or modified.
        """

        additions = [parse_artifact_record(record) for record in candidate_records]
        entries = self.read_entries()
        candidate_start_index = len(entries)
        entries.extend(
            RegistryEntry(
                wire_record=record, canonical_view=to_artifact_record_view(record)
            )
            for record in additions
        )
        self._validate_history(entries)
        records = [entry.canonical_view for entry in entries]
        self._validate_run_source_identity(
            records,
            candidate_start_index=candidate_start_index,
        )
        self._validate_references(records, candidate_start_index=candidate_start_index)
        return {
            "record_count": len(records),
            "artifact_count": len({record.artifact_id for record in records}),
            "revision_count": sum(
                max(0, record.record_revision - 1) for record in records
            ),
            "candidate_record_count": len(additions),
        }

    def append(
        self, record: ArtifactRecordWire | Mapping[str, Any]
    ) -> ArtifactRecordWire:
        return self.append_many([record])[0]

    def append_many(
        self,
        records: Iterable[ArtifactRecordWire | Mapping[str, Any]],
    ) -> list[ArtifactRecordWire]:
        additions = [parse_artifact_record(record) for record in records]
        if not additions:
            return []
        self.validate_full_history(candidate_records=additions)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existing = self.path.read_bytes() if self.path.exists() else b""
        appended = b"".join(
            canonical_json_bytes(record.model_dump(mode="json")) + b"\n"
            for record in additions
        )
        data = existing + appended
        fd, pending_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".pending", dir=self.path.parent
        )
        pending = Path(pending_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            if self.path.exists():
                os.replace(pending, self.path)
            else:
                os.link(pending, self.path)
            _fsync_directory(self.path.parent)
        finally:
            if pending.exists():
                pending.unlink()
        self.validate_full_history()
        return additions

    def _validate_history(self, entries: list[RegistryEntry]) -> None:
        record_ids: set[str] = set()
        latest: dict[str, RegistryEntry] = {}
        for entry in entries:
            wire_record = entry.wire_record
            record = entry.canonical_view
            if record.record_id in record_ids:
                raise RegistryValidationError(
                    f"Duplicate record_id: {record.record_id}"
                )
            record_ids.add(record.record_id)
            if (
                record_hash(wire_record.model_dump(mode="json"))
                != wire_record.record_hash
            ):
                raise RegistryValidationError(
                    f"record_hash mismatch: {record.record_id}"
                )
            if record.storage_scope != self.scope:
                raise RegistryValidationError(
                    f"Registry scope {self.scope} cannot contain {record.storage_scope} record {record.record_id}."
                )
            if self.scope == "run" and record.run_id != self.expected_run_id:
                raise RegistryValidationError(
                    f"Run Registry {self.expected_run_id} cannot contain run_id={record.run_id}."
                )
            previous_entry = latest.get(record.artifact_id)
            if previous_entry is None:
                if record.record_revision != 1:
                    raise RegistryValidationError(
                        f"First record for {record.artifact_id} must be revision 1."
                    )
            else:
                previous_wire = previous_entry.wire_record
                previous = previous_entry.canonical_view
                if record.record_revision != previous.record_revision + 1:
                    raise RegistryValidationError(
                        f"Non-contiguous revision for {record.artifact_id}."
                    )
                if (
                    record.previous_record_id != previous.record_id
                    or record.previous_record_hash != previous.record_hash
                ):
                    raise RegistryValidationError(
                        f"Broken revision chain for {record.artifact_id}."
                    )
                if _REGISTRY_VERSION_ORDER[
                    wire_record.registry_schema_version
                ] < _REGISTRY_VERSION_ORDER[previous_wire.registry_schema_version]:
                    raise RegistryValidationError(
                        f"Registry schema downgrade for {record.artifact_id} is forbidden."
                    )
                immutable = (
                    "artifact_type",
                    "container_kind",
                    "storage_version",
                    "release_id",
                    "source_platform",
                    "source_id",
                    "run_id",
                    "batch_id",
                    "dataset_build_id",
                    "dataset_version",
                    "exp_id",
                    "storage_root_ref",
                    "relative_path",
                    "storage_scope",
                    "media_type",
                    "size_bytes",
                    "content_hash_algorithm",
                    "content_hash",
                    "semantic_hash_algorithm",
                    "semantic_hash",
                )
                if any(
                    getattr(record, field) != getattr(previous, field)
                    for field in immutable
                ):
                    raise RegistryValidationError(
                        f"Artifact content changed under existing ID {record.artifact_id}; create a new artifact_id and supersedes relation."
                    )
            latest[record.artifact_id] = entry

    @staticmethod
    def _validate_run_source_identity(
        records: list[ArtifactRecordView],
        *,
        candidate_start_index: int,
    ) -> None:
        existing_records = records[:candidate_start_index]
        legacy_null_artifact_ids = {
            record.artifact_id
            for record in existing_records
            if record.source_platform is None
            and record.source_id is None
            and (
                record.source_registry_schema_version == "artifact_record_v1.0.0"
                or (
                    record.privacy_class == "public_synthetic"
                    and record.access_scope == "public"
                )
            )
        }
        existing_records_by_run: dict[str, list[ArtifactRecordView]] = {}
        for existing in existing_records:
            if existing.run_id is not None:
                existing_records_by_run.setdefault(existing.run_id, []).append(existing)
        canonical_by_run: dict[str, tuple[str, str]] = {}
        legacy_handoff_completion_runs: set[str] = set()
        for index, record in enumerate(records):
            if record.run_id is None:
                continue
            has_platform = record.source_platform is not None
            has_source_id = record.source_id is not None
            if has_platform != has_source_id:
                raise RegistryValidationError(
                    f"Run record {record.record_id} requires canonical source_platform and source_id."
                )
            if not has_platform:
                if index < candidate_start_index:
                    if record.artifact_id in legacy_null_artifact_ids:
                        continue
                    raise RegistryValidationError(
                        f"Run record {record.record_id} requires canonical source_platform and source_id."
                    )
                if (
                    record.record_revision > 1
                    and record.artifact_id in legacy_null_artifact_ids
                ):
                    continue
                existing_run_records = existing_records_by_run.get(record.run_id, [])
                is_legacy_handoff_completion = (
                    record.record_revision == 1
                    and record.artifact_type == "handoff.run"
                    and record.privacy_class == "public_synthetic"
                    and record.access_scope == "public"
                    and record.run_id not in canonical_by_run
                    and record.run_id not in legacy_handoff_completion_runs
                    and bool(existing_run_records)
                    and all(
                        existing.source_platform is None
                        and existing.source_id is None
                        and existing.privacy_class == "public_synthetic"
                        and existing.access_scope == "public"
                        for existing in existing_run_records
                    )
                )
                if is_legacy_handoff_completion:
                    legacy_handoff_completion_runs.add(record.run_id)
                    continue
                raise RegistryValidationError(
                    f"Run record {record.record_id} requires canonical source_platform and source_id."
                )
            if (
                record.source_platform == "youtube"
                and not record.source_id.startswith("youtube_")
            ) or (
                record.source_platform == "bilibili"
                and not record.source_id.startswith("bilibili_BV")
            ):
                raise RegistryValidationError(
                    f"Run record {record.record_id} has incompatible source_platform/source_id: "
                    f"{record.source_platform}/{record.source_id}."
                )
            observed = (record.source_platform, record.source_id)
            canonical = canonical_by_run.setdefault(record.run_id, observed)
            if observed != canonical:
                raise RegistryValidationError(
                    f"Run {record.run_id} has conflicting canonical source identity: "
                    f"expected {canonical[0]}/{canonical[1]}, got {observed[0]}/{observed[1]} "
                    f"at record {record.record_id}."
                )

    @staticmethod
    def _validate_references(
        records: list[ArtifactRecordView],
        *,
        candidate_start_index: int,
    ) -> None:
        artifact_ids = {record.artifact_id for record in records}
        first_position: dict[str, int] = {}
        latest_position: dict[str, int] = {}
        latest: dict[str, ArtifactRecordView] = {}
        for index, record in enumerate(records):
            first_position.setdefault(record.artifact_id, index)
            latest_position[record.artifact_id] = index
            latest[record.artifact_id] = record
        for index, record in enumerate(records):
            dependency_groups = {
                "upstream_artifact_ids": record.upstream_artifact_ids,
                "validation_artifact_ids": record.validation_artifact_ids,
                "upstream_entity_refs": [
                    ref.container_artifact_id for ref in record.upstream_entity_refs
                ],
            }
            referenced = [
                artifact_id
                for values in dependency_groups.values()
                for artifact_id in values
            ] + record.supersedes
            missing = sorted(set(referenced) - artifact_ids)
            if missing:
                raise RegistryValidationError(
                    f"Record {record.record_id} references unknown Artifacts: {missing}"
                )
            if record.artifact_id in record.supersedes:
                raise RegistryValidationError(
                    f"Artifact {record.artifact_id} cannot supersede itself."
                )
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
            if index < candidate_start_index:
                continue
            for field_name, values in dependency_groups.items():
                for artifact_id in values:
                    if first_position[artifact_id] >= index:
                        raise RegistryValidationError(
                            f"Candidate record {record.record_id} {field_name} can only reference "
                            f"Artifacts already present in Registry history: {artifact_id}."
                        )
                    if record.lifecycle_state not in _ACTIVE_LIFECYCLES:
                        continue
                    target = latest[artifact_id]
                    if latest_position[artifact_id] >= index:
                        raise RegistryValidationError(
                            f"Candidate record {record.record_id} {field_name} must reference the "
                            f"latest revision of {artifact_id} only after that revision is present "
                            "in Registry history."
                        )
                    target_is_active = (
                        target.lifecycle_state in _ACTIVE_LIFECYCLES
                        and (
                            (
                                target.lifecycle_state == "created"
                                and target.validation_status != "failed"
                            )
                            or (
                                target.lifecycle_state in {"validated", "frozen"}
                                and target.validation_status == "passed"
                            )
                        )
                    )
                    if not target_is_active:
                        raise RegistryValidationError(
                            f"Candidate record {record.record_id} {field_name} references Artifact "
                            f"{artifact_id} whose latest revision {target.record_revision} "
                            f"({target.record_id}) is not active: "
                            f"lifecycle_state={target.lifecycle_state}, "
                            f"validation_status={target.validation_status}."
                        )
            for artifact_id in record.supersedes:
                if latest_position[artifact_id] >= index:
                    raise RegistryValidationError(
                        f"Artifact {record.artifact_id} can only supersede the latest revision of "
                        f"{artifact_id} after that revision is present in Registry history."
                    )
