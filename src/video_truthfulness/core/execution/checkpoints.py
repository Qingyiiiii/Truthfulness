"""Immutable execution-checkpoint models, construction and source validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Callable, Literal, Mapping, Sequence

from pydantic import Field, ValidationError, field_validator, model_validator

from video_truthfulness.core.artifacts.dag import load_dag
from video_truthfulness.core.artifacts.hashing import directory_hash
from video_truthfulness.core.artifacts.models import new_typed_ulid
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryValidationError,
)
from video_truthfulness.core.execution.events import (
    TERMINAL_EVENT_TYPES,
    reject_sensitive_material,
    validate_event_stream,
    validate_manifest,
    validate_relative_path,
    validate_session_started_file_binding,
)
from video_truthfulness.core.execution.hashing import embedded_hash, sha256_file
from video_truthfulness.core.execution.io import ContractIOError, read_json, write_json
from video_truthfulness.core.execution.models import (
    AGENT_PROFILE,
    ARTIFACT_ID,
    CHECKPOINT_ID,
    EVENT_ID,
    PROMPT_VERSION,
    RECORD_ID,
    RUN_ID,
    SESSION_ID,
    SHA256,
    STAGE_ID,
    TASK_ID,
    UTC_TIMESTAMP,
    ArtifactRef,
    BootstrapRef,
    CodeRef,
    ExecutionContractError,
    ExecutionEvent,
    ExecutionHashError,
    PathRef,
    SchemaVersion,
    SessionManifest,
    StrictFrozenModel,
    parse_execution_event,
)
from video_truthfulness.core.execution.state import (
    RegistrySnapshot,
    validate_state_projection,
)


class CheckpointValidationError(ExecutionContractError):
    """Raised when a checkpoint or one of its fixed sources violates the contract."""


class CheckpointImmutableError(CheckpointValidationError):
    """Raised when an immutable checkpoint cannot be safely published."""


class RegisteredArtifactRef(ArtifactRef):
    """Checkpoint Artifact reference; unlike event writes it must be Registry-backed."""

    record_id: str = Field(pattern=RECORD_ID)


class EventHead(StrictFrozenModel):
    event_id: str = Field(pattern=EVENT_ID)
    sequence_no: int = Field(ge=1)
    event_hash: str = Field(pattern=SHA256)
    occurred_at: str = Field(pattern=UTC_TIMESTAMP)

    @field_validator("occurred_at")
    @classmethod
    def validate_occurred_at(cls, value: str) -> str:
        return _validate_utc_timestamp(value)


class RegistryHead(StrictFrozenModel):
    registry_scope: Literal["run", "cross_run"]
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    record_count: int = Field(ge=0)
    artifact_count: int = Field(ge=0)
    head_record_id: str | None = Field(pattern=RECORD_ID)
    head_record_hash: str | None = Field(pattern=SHA256)

    @model_validator(mode="after")
    def validate_empty_head(self) -> RegistryHead:
        if self.record_count == 0:
            if (
                self.artifact_count != 0
                or self.head_record_id is not None
                or self.head_record_hash is not None
            ):
                raise ValueError(
                    "empty Registry head must have zero Artifacts and null record identity"
                )
        elif self.head_record_id is None or self.head_record_hash is None:
            raise ValueError(
                "non-empty Registry head requires record identity and hash"
            )
        return self


class DagRef(StrictFrozenModel):
    dag_id: Literal["youtube_truthfulness_dag"]
    dag_version: Literal[
        "youtube_truthfulness_dag_v1.1.0",
        "youtube_truthfulness_dag_v1.2.0",
    ]
    workflow_version: Literal[
        "youtube_truthfulness_workflow_v1.1.0",
        "youtube_truthfulness_workflow_v1.3.0",
    ]
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)


class ObservedAccess(StrictFrozenModel):
    artifact_id: str | None = Field(pattern=ARTIFACT_ID)
    record_id: str | None = Field(pattern=RECORD_ID)
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    event_id: str = Field(pattern=EVENT_ID)


class ValidatorResult(StrictFrozenModel):
    validator_id: str = Field(pattern=r"^[a-z][a-z0-9_.-]*$", max_length=160)
    validator_version: str = Field(min_length=1, max_length=120)
    result: Literal["passed", "failed", "partial"]
    validation_artifact_id: str | None = Field(pattern=ARTIFACT_ID)


class ValidationSummary(StrictFrozenModel):
    overall_status: Literal["passed", "failed", "partial", "not_run"]
    passed_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    partial_count: int = Field(ge=0)
    validators: list[ValidatorResult]

    @model_validator(mode="after")
    def validate_counts(self) -> ValidationSummary:
        distinct = {
            "passed": sum(item.result == "passed" for item in self.validators),
            "failed": sum(item.result == "failed" for item in self.validators),
            "partial": sum(item.result == "partial" for item in self.validators),
        }
        counts = {
            "passed": self.passed_count,
            "failed": self.failed_count,
            "partial": self.partial_count,
        }
        if any(counts[result] < distinct[result] for result in counts):
            raise ValueError(
                "validation event counts cannot be smaller than distinct validators"
            )
        expected = (
            "failed"
            if counts["failed"]
            else "partial"
            if counts["partial"]
            else "passed"
            if counts["passed"]
            else "not_run"
        )
        if self.overall_status != expected:
            raise ValueError(
                "validation summary overall_status does not match validators"
            )
        return self


CheckpointKind = Literal[
    "stage_boundary",
    "failure_boundary",
    "human_gate_boundary",
    "retry_boundary",
]
TerminalState = Literal[
    "COMPLETED",
    "FAILED",
    "WAITING_FOR_HUMAN",
    "BLOCKED_BY_INPUT",
    "SKIPPED_BY_GATE",
]


class ExecutionCheckpoint(StrictFrozenModel):
    checkpoint_schema_version: Literal[
        "execution_checkpoint_v1.0.0",
        "execution_checkpoint_v1.1.0",
    ]
    checkpoint_id: str = Field(pattern=CHECKPOINT_ID)
    parent_checkpoint_id: str | None = Field(pattern=CHECKPOINT_ID)
    checkpoint_kind: CheckpointKind
    project_version: Literal["v0.2"]
    storage_version: Literal["V02"]
    release_id: Literal["truthfulness_v0.2_youtube_video"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str | None = Field(pattern=RUN_ID)
    stage_id: str = Field(pattern=STAGE_ID)
    created_at: str = Field(pattern=UTC_TIMESTAMP)
    bootstrap_refs: list[BootstrapRef]
    event_head: EventHead
    registry_heads: list[RegistryHead] = Field(min_length=1)
    dag_ref: DagRef
    workflow_version: Literal[
        "youtube_truthfulness_workflow_v1.1.0",
        "youtube_truthfulness_workflow_v1.3.0",
    ]
    schema_versions: list[SchemaVersion] = Field(min_length=1)
    prompt_version: str = Field(pattern=PROMPT_VERSION)
    agent_profile_version: str = Field(pattern=AGENT_PROFILE)
    agent_runtime_version: str = Field(min_length=1, max_length=120)
    code_ref: CodeRef
    state_hash: str = Field(pattern=SHA256)
    input_artifacts: list[RegisteredArtifactRef]
    output_artifacts: list[RegisteredArtifactRef]
    actual_read_set: list[ObservedAccess]
    actual_write_set: list[ObservedAccess]
    invalidated_artifacts: list[RegisteredArtifactRef]
    validation_summary: ValidationSummary
    terminal_state: TerminalState
    checkpoint_hash: str = Field(pattern=SHA256)

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return _validate_utc_timestamp(value)

    @model_validator(mode="after")
    def validate_boundary(self) -> ExecutionCheckpoint:
        if self.parent_checkpoint_id == self.checkpoint_id:
            raise ValueError("checkpoint cannot be its own parent")
        if self.parent_checkpoint_id is None and not self.bootstrap_refs:
            raise ValueError("root checkpoint requires bootstrap_refs")
        if self.checkpoint_schema_version not in self.schema_versions:
            raise ValueError(
                "schema_versions must contain the exact checkpoint schema version"
            )
        if self.checkpoint_schema_version == "execution_checkpoint_v1.0.0":
            if (
                self.dag_ref.dag_version != "youtube_truthfulness_dag_v1.1.0"
                or self.workflow_version != "youtube_truthfulness_workflow_v1.1.0"
                or self.dag_ref.workflow_version != self.workflow_version
            ):
                raise ValueError(
                    "execution_checkpoint_v1.0.0 requires Workflow/DAG v1.1"
                )
        else:
            if self.dag_ref.dag_version != "youtube_truthfulness_dag_v1.2.0":
                raise ValueError("execution_checkpoint_v1.1.0 requires DAG v1.2")
            expected_workflow = (
                "youtube_truthfulness_workflow_v1.3.0"
                if self.stage_id == "S02"
                else "youtube_truthfulness_workflow_v1.1.0"
            )
            if (
                self.workflow_version != expected_workflow
                or self.dag_ref.workflow_version != expected_workflow
            ):
                raise ValueError(
                    "execution_checkpoint_v1.1.0 source Workflow does not match stage"
                )
        if (
            self.checkpoint_kind == "failure_boundary"
            and self.terminal_state != "FAILED"
        ):
            raise ValueError("failure_boundary requires terminal_state=FAILED")
        if (
            self.checkpoint_kind == "human_gate_boundary"
            and self.terminal_state != "WAITING_FOR_HUMAN"
        ):
            raise ValueError(
                "human_gate_boundary requires terminal_state=WAITING_FOR_HUMAN"
            )
        if _utc_datetime(self.created_at) < _utc_datetime(self.event_head.occurred_at):
            raise ValueError("created_at cannot precede the terminal event")
        return self


class ExecutionCheckpointV1_1(ExecutionCheckpoint):
    """Named checkpoint successor for DAG v1.2 stage-scoped Sessions."""

    checkpoint_schema_version: Literal["execution_checkpoint_v1.1.0"]


@dataclass(frozen=True)
class CheckpointSources:
    repository_root: Path
    manifest: Mapping[str, Any] | SessionManifest
    events: Sequence[Mapping[str, Any] | ExecutionEvent]
    terminal_state: Mapping[str, Any]
    registry_snapshots: tuple[RegistrySnapshot, ...]
    dag_path: Path
    dag_relative_path: str
    supplemental_bootstrap_refs: tuple[Mapping[str, Any], ...] = ()
    receipt_bound_input_refs: tuple[ReceiptBoundInputRef, ...] = ()


@dataclass(frozen=True)
class ReceiptBoundInputRef:
    """Exact joined Artifact/receipt pair allowed to avoid a second byte read."""

    artifact_ref: ArtifactRef
    receipt_path_ref: PathRef


@dataclass(frozen=True)
class CheckpointPublication:
    checkpoint: ExecutionCheckpoint
    path: Path
    relative_path: str
    file_hash: str

    @property
    def checkpoint_id(self) -> str:
        return self.checkpoint.checkpoint_id

    @property
    def checkpoint_hash(self) -> str:
        return self.checkpoint.checkpoint_hash


def parse_checkpoint(raw: Mapping[str, Any]) -> ExecutionCheckpoint:
    """Parse one strict checkpoint and verify its embedded semantic hash."""

    payload = dict(raw)
    reject_sensitive_material(payload, location="checkpoint")
    model_type: type[ExecutionCheckpoint] = (
        ExecutionCheckpointV1_1
        if payload.get("checkpoint_schema_version") == "execution_checkpoint_v1.1.0"
        else ExecutionCheckpoint
    )
    try:
        checkpoint = model_type.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors(include_url=False)[0]
        location = "/".join(str(part) for part in first["loc"]) or "<root>"
        raise CheckpointValidationError(
            f"Invalid checkpoint at {location}: {first['msg']}"
        ) from exc
    _validate_unique_arrays(checkpoint)
    _validate_checkpoint_paths(checkpoint)
    try:
        expected = embedded_hash(payload, "checkpoint_hash")
    except ValueError as exc:
        raise ExecutionHashError(str(exc)) from exc
    if checkpoint.checkpoint_hash != expected:
        raise ExecutionHashError(
            f"checkpoint_hash mismatch: expected {expected}, observed {checkpoint.checkpoint_hash}"
        )
    return checkpoint


def read_checkpoint(path: Path) -> ExecutionCheckpoint:
    """Read one checkpoint from its required checkpoints/<checkpoint_id>.json path."""

    if path.parent.name != "checkpoints" or path.suffix != ".json":
        raise CheckpointValidationError(
            "Checkpoint must be stored at checkpoints/<checkpoint_id>.json"
        )
    try:
        raw = read_json(path)
    except ContractIOError as exc:
        raise CheckpointValidationError(str(exc)) from exc
    checkpoint = parse_checkpoint(raw)
    if path.name != f"{checkpoint.checkpoint_id}.json":
        raise CheckpointValidationError(
            f"Checkpoint filename must be {checkpoint.checkpoint_id}.json; observed {path.name}"
        )
    return checkpoint


def build_checkpoint(
    sources: CheckpointSources,
    *,
    checkpoint_kind: CheckpointKind,
    created_at: str,
    checkpoint_id: str | None = None,
) -> ExecutionCheckpoint:
    """Build a checkpoint from a validated terminal event/state/Registry prefix."""

    manifest, event_head, terminal_state, dag_ref, bootstrap_refs = _validate_sources(
        sources
    )
    raw: dict[str, Any] = {
        "checkpoint_schema_version": (
            "execution_checkpoint_v1.1.0"
            if manifest.session_manifest_version == "session_manifest_v1.1.0"
            else "execution_checkpoint_v1.0.0"
        ),
        "checkpoint_id": checkpoint_id or new_typed_ulid("checkpoint"),
        "parent_checkpoint_id": manifest.parent_checkpoint_id,
        "checkpoint_kind": checkpoint_kind,
        "project_version": manifest.project_version,
        "storage_version": manifest.storage_version,
        "release_id": manifest.release_id,
        "task_id": manifest.task_id,
        "session_id": manifest.session_id,
        "attempt_no": manifest.attempt_no,
        "run_id": manifest.run_id,
        "stage_id": manifest.stage_id,
        "created_at": created_at,
        "bootstrap_refs": bootstrap_refs,
        "event_head": event_head,
        "registry_heads": _registry_heads(sources.registry_snapshots),
        "dag_ref": dag_ref,
        "workflow_version": manifest.workflow_version,
        "schema_versions": list(manifest.schema_versions),
        "prompt_version": manifest.prompt_version,
        "agent_profile_version": manifest.agent_profile_version,
        "agent_runtime_version": manifest.agent_runtime_version,
        "code_ref": manifest.code_ref.model_dump(mode="json"),
        "state_hash": terminal_state["state_hash"],
        "input_artifacts": terminal_state["input_artifacts"],
        "output_artifacts": terminal_state["output_artifacts"],
        "actual_read_set": terminal_state["actual_read_set"],
        "actual_write_set": terminal_state["actual_write_set"],
        "invalidated_artifacts": terminal_state["invalidated_artifacts"],
        "validation_summary": terminal_state["validation_summary"],
        "terminal_state": terminal_state["status"],
        "checkpoint_hash": "0" * 64,
    }
    raw["checkpoint_hash"] = embedded_hash(raw, "checkpoint_hash")
    checkpoint = parse_checkpoint(raw)
    validate_checkpoint(checkpoint, sources)
    return checkpoint


def create_checkpoint(
    checkpoints_dir: Path,
    sources: CheckpointSources,
    *,
    checkpoint_kind: CheckpointKind,
    created_at: str,
    checkpoint_id: str | None = None,
) -> CheckpointPublication:
    """Build, publish once, read back and source-validate one checkpoint."""

    root = sources.repository_root.resolve()
    directory = checkpoints_dir.resolve()
    if directory.name != "checkpoints" or not directory.is_relative_to(root):
        raise CheckpointValidationError(
            "Checkpoint directory must be a repository-local checkpoints directory"
        )
    checkpoint = build_checkpoint(
        sources,
        checkpoint_kind=checkpoint_kind,
        created_at=created_at,
        checkpoint_id=checkpoint_id,
    )
    path = directory / f"{checkpoint.checkpoint_id}.json"
    validate_checkpoint(checkpoint, sources, path=path)
    relative_path = path.relative_to(root).as_posix()
    validate_relative_path(relative_path)
    manifest = validate_manifest(sources.manifest)
    if not _write_path_is_declared(relative_path, manifest):
        raise CheckpointValidationError(
            f"Checkpoint path is outside the Session declared write scope: {relative_path}"
        )
    try:
        file_hash = write_json(path, checkpoint.model_dump(mode="json"), immutable=True)
    except ContractIOError as exc:
        raise CheckpointImmutableError(str(exc)) from exc
    try:
        observed_file_hash = sha256_file(path)
        if observed_file_hash != file_hash:
            raise CheckpointValidationError(
                "Checkpoint publication file hash changed during read-back validation"
            )
        read_back = read_checkpoint(path)
        validate_checkpoint(read_back, sources, path=path)
        if read_back.model_dump(mode="json") != checkpoint.model_dump(mode="json"):
            raise CheckpointValidationError(
                "Checkpoint read-back content differs from the published object"
            )
    except Exception as exc:
        raise CheckpointImmutableError(
            "Checkpoint was published but write-back validation failed; immutable bytes were preserved"
        ) from exc
    return CheckpointPublication(
        checkpoint=read_back,
        path=path,
        relative_path=relative_path,
        file_hash=file_hash,
    )


def checkpoint_created_draft(
    publication: CheckpointPublication,
    *,
    actor: Mapping[str, Any],
    purpose: str = "bind immutable checkpoint publication",
) -> dict[str, Any]:
    """Return the post-terminal event draft for EventLog.append()."""

    checkpoint = publication.checkpoint
    return {
        "event_type": "checkpoint.created",
        "actor": dict(actor),
        "checkpoint_id": checkpoint.checkpoint_id,
        "artifact_refs": [],
        "path_refs": [
            {
                "relative_path": publication.relative_path,
                "content_hash_algorithm": "sha256",
                "content_hash": publication.file_hash,
                "purpose": purpose,
            }
        ],
        "payload": {
            "checkpoint_id": checkpoint.checkpoint_id,
            "checkpoint_path": publication.relative_path,
            "checkpoint_hash": checkpoint.checkpoint_hash,
            "checkpoint_kind": checkpoint.checkpoint_kind,
        },
    }


def validate_checkpoint_created_event(
    publication: CheckpointPublication,
    event: Mapping[str, Any] | ExecutionEvent,
    sources: CheckpointSources,
) -> ExecutionEvent:
    """Validate an existing checkpoint.created receipt against both hash domains."""

    raw = (
        event.model_dump(mode="json")
        if isinstance(event, ExecutionEvent)
        else dict(event)
    )
    reject_sensitive_material(raw, location="checkpoint.created")
    checkpoint = publication.checkpoint
    root = sources.repository_root.resolve()
    expected_path = (root / validate_relative_path(publication.relative_path)).resolve()
    if publication.path.resolve() != expected_path or not expected_path.is_relative_to(
        root
    ):
        raise CheckpointValidationError(
            "Checkpoint publication path does not bind its repository-relative path"
        )
    try:
        validate_event_stream(
            [*sources.events[: checkpoint.event_head.sequence_no], raw],
            sources.manifest,
            require_terminal=True,
        )
    except ExecutionContractError as exc:
        raise CheckpointValidationError(
            f"Invalid checkpoint.created event stream: {exc}"
        ) from exc
    model = parse_execution_event(raw)
    if model.event_type != "checkpoint.created":
        raise CheckpointValidationError(
            "Checkpoint receipt must use event_type=checkpoint.created"
        )
    if model.sequence_no != checkpoint.event_head.sequence_no + 1:
        raise CheckpointValidationError(
            "checkpoint.created must immediately follow the terminal event"
        )
    if _utc_datetime(model.occurred_at) < _utc_datetime(checkpoint.created_at):
        raise CheckpointValidationError(
            "checkpoint.created cannot precede checkpoint creation"
        )
    if (
        model.previous_event_id != checkpoint.event_head.event_id
        or model.previous_event_hash != checkpoint.event_head.event_hash
    ):
        raise CheckpointValidationError(
            "checkpoint.created does not bind the terminal event head"
        )
    if (
        model.task_id != checkpoint.task_id
        or model.session_id != checkpoint.session_id
        or model.attempt_no != checkpoint.attempt_no
        or model.run_id != checkpoint.run_id
        or model.stage_id != checkpoint.stage_id
    ):
        raise CheckpointValidationError(
            "checkpoint.created identity differs from the checkpoint"
        )
    if model.checkpoint_id != checkpoint.checkpoint_id:
        raise CheckpointValidationError(
            "checkpoint.created envelope checkpoint_id mismatch"
        )
    if model.artifact_refs or len(model.path_refs) != 1:
        raise CheckpointValidationError(
            "checkpoint.created requires zero Artifact refs and exactly one path ref"
        )
    path_ref = model.path_refs[0]
    payload = model.payload
    expected_payload = {
        "checkpoint_id": checkpoint.checkpoint_id,
        "checkpoint_path": publication.relative_path,
        "checkpoint_hash": checkpoint.checkpoint_hash,
        "checkpoint_kind": checkpoint.checkpoint_kind,
    }
    if payload != expected_payload:
        raise CheckpointValidationError(
            "checkpoint.created payload does not match the checkpoint publication"
        )
    if (
        path_ref.relative_path != publication.relative_path
        or path_ref.content_hash_algorithm != "sha256"
        or path_ref.content_hash != publication.file_hash
    ):
        raise CheckpointValidationError(
            "checkpoint.created path ref does not match the checkpoint file"
        )
    if sha256_file(publication.path) != publication.file_hash:
        raise CheckpointValidationError(
            "Published checkpoint file hash no longer matches its receipt"
        )
    read_back = read_checkpoint(publication.path)
    validate_checkpoint(read_back, sources, path=publication.path)
    if read_back.model_dump(mode="json") != checkpoint.model_dump(mode="json"):
        raise CheckpointValidationError(
            "Published checkpoint object no longer matches its receipt"
        )
    return model


def validate_checkpoint_chain(
    checkpoint_path: Path,
    *,
    sources_for: Callable[[str], CheckpointSources],
    path_for: Callable[[str], Path],
) -> tuple[ExecutionCheckpoint, ...]:
    """Validate one exact child-to-root parent chain without directory discovery."""

    chain: list[ExecutionCheckpoint] = []
    visited: set[str] = set()
    current_path = checkpoint_path
    child: ExecutionCheckpoint | None = None
    while True:
        try:
            current = read_checkpoint(current_path)
        except CheckpointValidationError as exc:
            if child is not None:
                raise CheckpointValidationError(
                    f"Cannot read parent checkpoint at {current_path}"
                ) from exc
            raise
        if current.checkpoint_id in visited:
            raise CheckpointValidationError(
                f"Checkpoint parent cycle detected at {current.checkpoint_id}"
            )
        visited.add(current.checkpoint_id)
        try:
            sources = sources_for(current.checkpoint_id)
        except Exception as exc:
            raise CheckpointValidationError(
                f"Checkpoint sources resolver failed for {current.checkpoint_id}"
            ) from exc
        root = sources.repository_root.resolve()
        resolved_path = current_path.resolve()
        if not resolved_path.is_relative_to(root):
            raise CheckpointValidationError(
                f"Checkpoint path is outside its repository root: {current.checkpoint_id}"
            )
        validate_checkpoint(current, sources, path=resolved_path)
        if child is not None:
            if child.parent_checkpoint_id != current.checkpoint_id:
                raise CheckpointValidationError(
                    "Parent resolver returned the wrong checkpoint ID"
                )
            if (
                child.project_version != current.project_version
                or child.storage_version != current.storage_version
                or child.release_id != current.release_id
            ):
                raise CheckpointValidationError(
                    "Parent and child use incompatible project storage identity"
                )
            if _utc_datetime(child.created_at) < _utc_datetime(current.created_at):
                raise CheckpointValidationError(
                    "Child checkpoint cannot predate its parent"
                )
        chain.append(current)
        parent_id = current.parent_checkpoint_id
        if parent_id is None:
            break
        if parent_id in visited:
            raise CheckpointValidationError(
                f"Checkpoint parent cycle detected at {parent_id}"
            )
        try:
            parent_path = path_for(parent_id)
        except Exception as exc:
            raise CheckpointValidationError(
                f"Checkpoint path resolver failed for {parent_id}"
            ) from exc
        if parent_path.name != f"{parent_id}.json":
            raise CheckpointValidationError(
                f"Parent path must resolve exactly to {parent_id}.json"
            )
        child = current
        current_path = parent_path
    return tuple(chain)


def validate_checkpoint(
    checkpoint: ExecutionCheckpoint | Mapping[str, Any],
    sources: CheckpointSources,
    *,
    path: Path | None = None,
) -> ExecutionCheckpoint:
    """Verify a checkpoint against its exact terminal event and historical source slices."""

    model = (
        checkpoint
        if isinstance(checkpoint, ExecutionCheckpoint)
        else parse_checkpoint(checkpoint)
    )
    if isinstance(checkpoint, ExecutionCheckpoint):
        parse_checkpoint(model.model_dump(mode="json"))
    if path is not None and (
        path.suffix != ".json" or path.stem != model.checkpoint_id
    ):
        raise CheckpointValidationError(
            f"Checkpoint filename must be {model.checkpoint_id}.json; observed {path.name}"
        )
    manifest, event_head, terminal_state, dag_ref, bootstrap_refs = _validate_sources(
        sources,
        event_head_sequence=model.event_head.sequence_no,
    )
    expected_identity = {
        "parent_checkpoint_id": manifest.parent_checkpoint_id,
        "project_version": manifest.project_version,
        "storage_version": manifest.storage_version,
        "release_id": manifest.release_id,
        "task_id": manifest.task_id,
        "session_id": manifest.session_id,
        "attempt_no": manifest.attempt_no,
        "run_id": manifest.run_id,
        "stage_id": manifest.stage_id,
        "workflow_version": manifest.workflow_version,
        "schema_versions": list(manifest.schema_versions),
        "prompt_version": manifest.prompt_version,
        "agent_profile_version": manifest.agent_profile_version,
        "agent_runtime_version": manifest.agent_runtime_version,
        "code_ref": manifest.code_ref.model_dump(mode="json"),
        "event_head": event_head,
        "registry_heads": _registry_heads(sources.registry_snapshots),
        "dag_ref": dag_ref,
        "state_hash": terminal_state["state_hash"],
        "input_artifacts": terminal_state["input_artifacts"],
        "output_artifacts": terminal_state["output_artifacts"],
        "actual_read_set": terminal_state["actual_read_set"],
        "actual_write_set": terminal_state["actual_write_set"],
        "invalidated_artifacts": terminal_state["invalidated_artifacts"],
        "validation_summary": terminal_state["validation_summary"],
        "terminal_state": terminal_state["status"],
    }
    observed = model.model_dump(mode="json")
    _validate_checkpoint_bootstrap_refs(
        observed["bootstrap_refs"], manifest, bootstrap_refs
    )
    for field, expected in expected_identity.items():
        if observed[field] != expected:
            raise CheckpointValidationError(f"Checkpoint/source mismatch for {field}")
    return model


def _validate_sources(
    sources: CheckpointSources,
    *,
    event_head_sequence: int | None = None,
) -> tuple[
    SessionManifest,
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
]:
    manifest = validate_manifest(sources.manifest)
    raw_sequence = (
        event_head_sequence
        if event_head_sequence is not None
        else sources.terminal_state.get("event_count")
    )
    if not isinstance(raw_sequence, int) or isinstance(raw_sequence, bool):
        raise CheckpointValidationError(
            "Checkpoint event_head sequence must be an integer"
        )
    sequence = raw_sequence
    if sequence < 1 or sequence > len(sources.events):
        raise CheckpointValidationError(
            "Checkpoint event_head sequence is outside the supplied event history"
        )
    events = [
        event
        if isinstance(event, ExecutionEvent)
        else parse_execution_event(dict(event))
        for event in sources.events[:sequence]
    ]
    summary = validate_event_stream(events, manifest, require_terminal=True)
    validate_session_started_file_binding(sources.repository_root, manifest, events[0])
    head = events[-1]
    if head.event_type not in TERMINAL_EVENT_TYPES:
        raise CheckpointValidationError(
            "Checkpoint event_head must be the unique terminal event"
        )
    terminal_state = dict(sources.terminal_state)
    if terminal_state.get("event_count") != sequence:
        raise CheckpointValidationError(
            "Terminal state event_count does not match checkpoint event prefix"
        )
    if (
        terminal_state.get("as_of_event_id") != head.event_id
        or terminal_state.get("event_head_hash") != head.event_hash
        or terminal_state.get("as_of_occurred_at") != head.occurred_at
        or terminal_state.get("status") != summary.terminal_state
    ):
        raise CheckpointValidationError(
            "Terminal state does not bind the checkpoint event head"
        )
    if not sources.registry_snapshots:
        raise CheckpointValidationError(
            "Checkpoint requires at least one Registry historical prefix"
        )
    validate_state_projection(
        terminal_state,
        manifest,
        events,
        sources.registry_snapshots,
        sources.dag_path,
    )
    _validate_payload_hashes(sources, terminal_state)
    dag_ref = _validate_dag_source(sources, manifest)
    _validate_registry_sources(sources)
    bootstrap_refs = _checkpoint_bootstrap_refs(
        manifest,
        sources.registry_snapshots,
        dag_ref,
        sources.supplemental_bootstrap_refs,
    )
    _validate_bootstrap_sources(sources, manifest, dag_ref, bootstrap_refs)
    event_head = {
        "event_id": head.event_id,
        "sequence_no": head.sequence_no,
        "event_hash": head.event_hash,
        "occurred_at": head.occurred_at,
    }
    return manifest, event_head, terminal_state, dag_ref, bootstrap_refs


def _validate_payload_hashes(
    sources: CheckpointSources,
    terminal_state: Mapping[str, Any],
) -> None:
    """Recompute fixed Artifact/access payload hashes before checkpointing."""

    root = sources.repository_root.resolve()
    registry_snapshots: dict[str, RegistrySnapshot] = {}
    for snapshot in sources.registry_snapshots:
        relative = validate_relative_path(snapshot.relative_path).as_posix()
        if relative in registry_snapshots:
            raise CheckpointValidationError(
                f"Duplicate Registry snapshot payload path: {relative}"
            )
        registry_snapshots[relative] = snapshot
    bindings: dict[str, str] = {}
    binding_origins: dict[str, set[str]] = {}
    for field in (
        "input_artifacts",
        "output_artifacts",
        "invalidated_artifacts",
        "actual_read_set",
        "actual_write_set",
    ):
        values = terminal_state.get(field)
        if not isinstance(values, list):
            raise CheckpointValidationError(f"Terminal state {field} must be an array")
        for reference in values:
            relative = validate_relative_path(
                str(reference["relative_path"])
            ).as_posix()
            content_hash = str(reference["content_hash"])
            prior = bindings.get(relative)
            if prior is not None and prior != content_hash:
                raise CheckpointValidationError(
                    f"Checkpoint payload path has conflicting hashes: {relative}"
                )
            bindings[relative] = content_hash
            binding_origins.setdefault(relative, set()).add(field)

    # A caller may explicitly identify receipt-bound inputs whose bytes were
    # already read exactly once through a separately hashed materialization
    # PathRef.  Only those exact inputs may use the joined artifact.read Event
    # instead of reopening the authoritative Registry path.  All ordinary
    # inputs, outputs, and invalidations remain subject to physical hash
    # recomputation.
    receipt_bound_inputs: dict[str, ReceiptBoundInputRef] = {}
    for declared in sources.receipt_bound_input_refs:
        artifact = declared.artifact_ref
        relative = validate_relative_path(artifact.relative_path).as_posix()
        prior = receipt_bound_inputs.get(relative)
        if prior is not None:
            raise CheckpointValidationError(
                f"Duplicate receipt-bound input declaration: {relative}"
            )
        receipt_bound_inputs[relative] = declared

    joined_read_attestations: dict[
        str, tuple[tuple[ArtifactRef, ...], tuple[PathRef, ...]]
    ] = {}
    joined_read_attestation_counts: dict[str, int] = {}
    event_count = terminal_state.get("event_count")
    if not isinstance(event_count, int) or isinstance(event_count, bool):
        raise CheckpointValidationError("Terminal state event_count must be an integer")
    for raw_event in sources.events[:event_count]:
        event = (
            raw_event
            if isinstance(raw_event, ExecutionEvent)
            else parse_execution_event(raw_event)
        )
        payload = event.payload
        hash_verified = (
            payload.get("hash_verified", False)
            if isinstance(payload, Mapping)
            else getattr(payload, "hash_verified", False)
        )
        if (
            event.event_type != "artifact.read"
            or not event.artifact_refs
            or not event.path_refs
            or hash_verified is not True
        ):
            continue
        event_artifacts = tuple(event.artifact_refs)
        paired_paths = tuple(event.path_refs)
        for artifact in event.artifact_refs:
            relative = validate_relative_path(artifact.relative_path).as_posix()
            candidate = (event_artifacts, paired_paths)
            prior = joined_read_attestations.get(relative)
            if prior is not None and prior != candidate:
                raise CheckpointValidationError(
                    f"Checkpoint joined read has conflicting attestations: {relative}"
                )
            joined_read_attestations[relative] = candidate
            joined_read_attestation_counts[relative] = (
                joined_read_attestation_counts.get(relative, 0) + 1
            )

    for relative, expected_hash in sorted(bindings.items()):
        declared_receipt_bound = receipt_bound_inputs.get(relative)
        if declared_receipt_bound is not None:
            if binding_origins.get(relative, set()) != {
                "input_artifacts",
                "actual_read_set",
            }:
                raise CheckpointValidationError(
                    f"Receipt-bound checkpoint exemption is not input-only: {relative}"
                )
            if declared_receipt_bound.artifact_ref.content_hash != expected_hash:
                raise CheckpointValidationError(
                    f"Receipt-bound input declaration hash mismatch: {relative}"
                )
            attestation = joined_read_attestations.get(relative)
            if attestation is None:
                raise CheckpointValidationError(
                    f"Receipt-bound input lacks joined read attestation: {relative}"
                )
            if joined_read_attestation_counts.get(relative) != 1:
                raise CheckpointValidationError(
                    f"Receipt-bound input requires exactly one joined read: {relative}"
                )
            attested_artifacts, paired_paths = attestation
            if attested_artifacts != (declared_receipt_bound.artifact_ref,):
                raise CheckpointValidationError(
                    f"Checkpoint joined read ArtifactRef mismatch: {relative}"
                )
            expected_path_ref = declared_receipt_bound.receipt_path_ref
            if paired_paths != (expected_path_ref,):
                raise CheckpointValidationError(
                    f"Checkpoint joined read receipt PathRef mismatch: {relative}"
                )
            if (
                bindings.get(expected_path_ref.relative_path)
                != expected_path_ref.content_hash
            ):
                raise CheckpointValidationError(
                    "Checkpoint joined read PathRef is absent or changed: "
                    f"{expected_path_ref.relative_path}"
                )
            continue
        try:
            path = (root / relative).resolve()
        except OSError as exc:
            raise CheckpointValidationError(
                f"Checkpoint payload path cannot be resolved: {relative}"
            ) from exc
        if not path.is_relative_to(root):
            raise CheckpointValidationError(
                f"Checkpoint payload escapes repository_root: {relative}"
            )
        registry_snapshot = registry_snapshots.get(relative)
        if registry_snapshot is not None:
            if expected_hash != registry_snapshot.content_hash:
                raise CheckpointValidationError(
                    f"Checkpoint Registry payload hash differs from its frozen prefix: {relative}"
                )
            if not path.is_file():
                raise CheckpointValidationError(
                    f"Checkpoint Registry payload is missing or unsupported: {relative}"
                )
            try:
                current_bytes = path.read_bytes()
            except OSError as exc:
                raise CheckpointValidationError(
                    f"Checkpoint Registry payload cannot be read: {relative}"
                ) from exc
            if not current_bytes.startswith(registry_snapshot.prefix_bytes):
                raise CheckpointValidationError(
                    f"Checkpoint Registry historical prefix mismatch: {relative}"
                )
            try:
                AppendOnlyRegistry(
                    path,
                    scope=registry_snapshot.registry_scope,
                    expected_run_id=registry_snapshot.expected_run_id,
                ).read_entries()
            except (OSError, RegistryValidationError, ValueError) as exc:
                raise CheckpointValidationError(
                    f"Checkpoint Registry current history is invalid: {relative}: {exc}"
                ) from exc
            continue
        try:
            if path.is_file():
                observed_hash = sha256_file(path)
            elif path.is_dir():
                observed_hash = directory_hash(path)
            else:
                raise CheckpointValidationError(
                    f"Checkpoint payload is missing or unsupported: {relative}"
                )
        except OSError as exc:
            raise CheckpointValidationError(
                f"Checkpoint payload cannot be inspected: {relative}"
            ) from exc
        if observed_hash != expected_hash:
            raise CheckpointValidationError(
                f"Checkpoint payload hash mismatch: {relative}"
            )


def _write_path_is_declared(relative_path: str, manifest: SessionManifest) -> bool:
    target = PurePosixPath(relative_path)
    for entry in manifest.declared_write_set:
        raw = entry.model_dump(mode="json")
        scope_type = raw["scope_type"]
        if scope_type == "artifact":
            continue
        scope_path = validate_relative_path(raw["relative_path"])
        if scope_type == "path" and target == scope_path:
            return True
        if scope_type == "path_prefix" and (
            target == scope_path
            or raw["recursive"] is True
            and target.is_relative_to(scope_path)
            or raw["recursive"] is False
            and target.parent == scope_path
        ):
            return True
    return False


def _validate_dag_source(
    sources: CheckpointSources, manifest: SessionManifest
) -> dict[str, Any]:
    root = sources.repository_root.resolve()
    relative = validate_relative_path(sources.dag_relative_path).as_posix()
    expected_path = (root / relative).resolve()
    if expected_path != sources.dag_path.resolve() or not expected_path.is_relative_to(
        root
    ):
        raise CheckpointValidationError(
            "DAG path does not match its repository-relative reference"
        )
    dag = load_dag(expected_path)
    if dag.dag_version != manifest.dag_version:
        raise CheckpointValidationError(
            "DAG version does not match the Session manifest"
        )
    if dag.dag_version == "youtube_truthfulness_dag_v1.2.0":
        workflow_for_stage = getattr(dag, "workflow_version_for_stage", None)
        if workflow_for_stage is None:
            raise CheckpointValidationError(
                "DAG v1.2 lacks a stage-scoped Workflow mapping"
            )
        observed_workflow = workflow_for_stage(manifest.stage_id)
    else:
        observed_workflow = dag.workflow_version
    if observed_workflow != manifest.workflow_version:
        raise CheckpointValidationError(
            "DAG stage Workflow does not match the Session manifest"
        )
    return {
        "dag_id": dag.dag_id,
        "dag_version": dag.dag_version,
        "workflow_version": observed_workflow,
        "relative_path": relative,
        "content_hash_algorithm": "sha256",
        "content_hash": sha256_file(expected_path),
    }


def _validate_registry_sources(sources: CheckpointSources) -> None:
    root = sources.repository_root.resolve()
    seen: set[tuple[str, str]] = set()
    for snapshot in sources.registry_snapshots:
        head = snapshot.head()
        key = (head["registry_scope"], head["relative_path"])
        if key in seen:
            raise CheckpointValidationError(f"Duplicate Registry snapshot: {key}")
        seen.add(key)
        relative = validate_relative_path(head["relative_path"]).as_posix()
        expected_path = (root / relative).resolve()
        if expected_path != snapshot.path.resolve() or not expected_path.is_relative_to(
            root
        ):
            raise CheckpointValidationError(
                "Registry path does not match its repository-relative reference"
            )


def _validate_bootstrap_sources(
    sources: CheckpointSources,
    manifest: SessionManifest,
    dag_ref: Mapping[str, Any],
    refs: Sequence[Mapping[str, Any]],
) -> None:
    if manifest.parent_checkpoint_id is None:
        required = {
            "git_commit",
            "git_tree",
            "working_tree_manifest",
            "registry",
            "dag_config",
        }
        observed = {item["ref_type"] for item in refs}
        missing = sorted(required - observed)
        if missing:
            raise CheckpointValidationError(
                f"Root checkpoint bootstrap is missing required refs: {missing}"
            )
        _validate_root_code_bindings(manifest, refs)
    root = sources.repository_root.resolve()
    registry_heads = {
        (snapshot.relative_path, snapshot.content_hash)
        for snapshot in sources.registry_snapshots
    }
    matched_registry: set[tuple[str, str]] = set()
    matched_dag = False
    manifest_dag_refs = {
        (
            validate_relative_path(item.relative_path).as_posix(),
            item.content_hash,
        )
        for item in manifest.bootstrap_refs
        if item.ref_type == "dag_config"
    }
    for ref in refs:
        ref_type = ref["ref_type"]
        if ref_type in {"git_commit", "git_tree"}:
            continue
        relative = validate_relative_path(ref["relative_path"]).as_posix()
        if ref_type == "registry":
            key = (relative, ref["content_hash"])
            if key not in registry_heads:
                raise CheckpointValidationError(
                    "Registry bootstrap ref does not match a historical Registry prefix"
                )
            matched_registry.add(key)
            continue
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise CheckpointValidationError(
                f"Bootstrap file is missing or outside repository root: {relative}"
            )
        if sha256_file(path) != ref["content_hash"]:
            raise CheckpointValidationError(f"Bootstrap file hash mismatch: {relative}")
        if ref_type == "dag_config":
            dag_identity = (relative, ref["content_hash"])
            checkpoint_dag_identity = (
                dag_ref["relative_path"],
                dag_ref["content_hash"],
            )
            if dag_identity == checkpoint_dag_identity:
                matched_dag = True
            elif dag_identity not in manifest_dag_refs:
                raise CheckpointValidationError(
                    "DAG bootstrap ref is neither the checkpoint DAG nor a fixed Session source"
                )
    if manifest.parent_checkpoint_id is None:
        if matched_registry != registry_heads:
            raise CheckpointValidationError(
                "Bootstrap Registry refs do not cover every Registry historical prefix"
            )
        if not matched_dag:
            raise CheckpointValidationError(
                "Bootstrap refs do not bind the checkpoint DAG"
            )


def _checkpoint_bootstrap_refs(
    manifest: SessionManifest,
    snapshots: Sequence[RegistrySnapshot],
    dag_ref: Mapping[str, Any],
    supplemental_refs: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Return deterministic refs, supplementing only a canonical root checkpoint."""

    refs = [item.model_dump(mode="json") for item in manifest.bootstrap_refs]
    for raw in supplemental_refs:
        item = dict(raw)
        if item.get("ref_type") != "document":
            raise CheckpointValidationError(
                "Supplemental checkpoint bootstrap refs are restricted to documents"
            )
        refs.append(item)
    if manifest.parent_checkpoint_id is not None:
        _validate_bootstrap_conflicts(refs)
        return refs
    existing = {(item["ref_type"], item.get("relative_path")): item for item in refs}
    for snapshot in sorted(
        snapshots,
        key=lambda item: (item.registry_scope, item.relative_path),
    ):
        derived = {
            "ref_type": "registry",
            "relative_path": snapshot.relative_path,
            "content_hash_algorithm": "sha256",
            "content_hash": snapshot.content_hash,
            "purpose": f"fixed {snapshot.registry_scope} Registry historical prefix",
        }
        key = ("registry", snapshot.relative_path)
        prior = existing.get(key)
        if prior is None:
            refs.append(derived)
            existing[key] = derived
        elif _file_ref_identity(prior) != _file_ref_identity(derived):
            raise CheckpointValidationError(
                f"Manifest Registry bootstrap conflicts with validated prefix: {snapshot.relative_path}"
            )
    derived_dag = {
        "ref_type": "dag_config",
        "relative_path": dag_ref["relative_path"],
        "content_hash_algorithm": "sha256",
        "content_hash": dag_ref["content_hash"],
        "purpose": "fixed checkpoint DAG declaration",
    }
    dag_key = ("dag_config", dag_ref["relative_path"])
    prior_dag = existing.get(dag_key)
    if prior_dag is None:
        refs.append(derived_dag)
    elif _file_ref_identity(prior_dag) != _file_ref_identity(derived_dag):
        raise CheckpointValidationError(
            "Manifest DAG bootstrap conflicts with validated DAG bytes"
        )
    _validate_bootstrap_conflicts(refs)
    return refs


def _validate_checkpoint_bootstrap_refs(
    observed: Sequence[Mapping[str, Any]],
    manifest: SessionManifest,
    expected: Sequence[Mapping[str, Any]],
) -> None:
    observed_refs = [dict(item) for item in observed]
    _validate_bootstrap_conflicts(observed_refs)
    manifest_refs = [item.model_dump(mode="json") for item in manifest.bootstrap_refs]
    observed_exact = {_canonical_item(item) for item in observed_refs}
    if any(_canonical_item(item) not in observed_exact for item in manifest_refs):
        raise CheckpointValidationError(
            "Checkpoint does not preserve every Session bootstrap ref"
        )
    expected_exact = {_canonical_item(item) for item in expected}
    if observed_exact != expected_exact or len(observed_refs) != len(expected):
        boundary = "Child" if manifest.parent_checkpoint_id is not None else "Root"
        raise CheckpointValidationError(
            f"{boundary} checkpoint bootstrap refs do not exactly bind the fixed sources"
        )


def _validate_root_code_bindings(
    manifest: SessionManifest,
    refs: Sequence[Mapping[str, Any]],
) -> None:
    commits = [item for item in refs if item["ref_type"] == "git_commit"]
    trees = [item for item in refs if item["ref_type"] == "git_tree"]
    worktrees = [item for item in refs if item["ref_type"] == "working_tree_manifest"]
    if len(commits) != 1 or commits[0]["object_id"] != manifest.code_ref.git_commit:
        raise CheckpointValidationError(
            "Root git_commit bootstrap must equal code_ref.git_commit"
        )
    if len(trees) != 1:
        raise CheckpointValidationError(
            "Root checkpoint requires exactly one git_tree bootstrap ref"
        )
    if len(worktrees) != 1:
        raise CheckpointValidationError(
            "Root checkpoint requires exactly one working_tree_manifest ref"
        )
    worktree = worktrees[0]
    if (
        not manifest.code_ref.working_tree_dirty
        or worktree["relative_path"] != manifest.code_ref.working_tree_manifest_path
        or worktree["content_hash"] != manifest.code_ref.working_tree_manifest_hash
    ):
        raise CheckpointValidationError(
            "Root working_tree_manifest bootstrap must equal the dirty code_ref binding"
        )


def _validate_bootstrap_conflicts(refs: Sequence[Mapping[str, Any]]) -> None:
    keys: set[tuple[str, str]] = set()
    path_hashes: dict[str, str] = {}
    for ref in refs:
        ref_type = str(ref["ref_type"])
        identity = str(ref.get("relative_path", ref.get("object_id", "")))
        key = (ref_type, identity)
        if key in keys:
            raise CheckpointValidationError(
                f"Duplicate bootstrap ref identity: {ref_type}:{identity}"
            )
        keys.add(key)
        relative = ref.get("relative_path")
        if relative is None:
            continue
        content_hash = str(ref["content_hash"])
        prior = path_hashes.get(str(relative))
        if prior is not None and prior != content_hash:
            raise CheckpointValidationError(
                f"Bootstrap path is bound to conflicting hashes: {relative}"
            )
        path_hashes[str(relative)] = content_hash


def _file_ref_identity(ref: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        str(ref["ref_type"]),
        str(ref["relative_path"]),
        str(ref["content_hash_algorithm"]),
        str(ref["content_hash"]),
    )


def _canonical_item(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _registry_heads(snapshots: Sequence[RegistrySnapshot]) -> list[dict[str, Any]]:
    return sorted(
        (snapshot.head() for snapshot in snapshots),
        key=lambda item: (item["registry_scope"], item["relative_path"]),
    )


def _validate_checkpoint_paths(checkpoint: ExecutionCheckpoint) -> None:
    paths: list[str] = [checkpoint.dag_ref.relative_path]
    paths.extend(item.relative_path for item in checkpoint.registry_heads)
    paths.extend(item.relative_path for item in checkpoint.input_artifacts)
    paths.extend(item.relative_path for item in checkpoint.output_artifacts)
    paths.extend(item.relative_path for item in checkpoint.invalidated_artifacts)
    paths.extend(item.relative_path for item in checkpoint.actual_read_set)
    paths.extend(item.relative_path for item in checkpoint.actual_write_set)
    for ref in checkpoint.bootstrap_refs:
        relative = getattr(ref, "relative_path", None)
        if relative is not None:
            paths.append(relative)
    if checkpoint.code_ref.working_tree_manifest_path is not None:
        paths.append(checkpoint.code_ref.working_tree_manifest_path)
    for value in paths:
        validate_relative_path(value)


def _validate_unique_arrays(checkpoint: ExecutionCheckpoint) -> None:
    arrays = {
        "bootstrap_refs": checkpoint.bootstrap_refs,
        "registry_heads": checkpoint.registry_heads,
        "schema_versions": checkpoint.schema_versions,
        "input_artifacts": checkpoint.input_artifacts,
        "output_artifacts": checkpoint.output_artifacts,
        "actual_read_set": checkpoint.actual_read_set,
        "actual_write_set": checkpoint.actual_write_set,
        "invalidated_artifacts": checkpoint.invalidated_artifacts,
        "validators": checkpoint.validation_summary.validators,
    }
    for name, values in arrays.items():
        serialized = [
            json.dumps(
                value.model_dump(mode="json")
                if isinstance(value, StrictFrozenModel)
                else value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            for value in values
        ]
        if len(serialized) != len(set(serialized)):
            raise CheckpointValidationError(f"Checkpoint {name} values must be unique")
    registry_keys = {
        (item.registry_scope, item.relative_path) for item in checkpoint.registry_heads
    }
    if len(registry_keys) != len(checkpoint.registry_heads):
        raise CheckpointValidationError(
            "Checkpoint Registry heads must have unique scope/path identities"
        )
    _validate_bootstrap_conflicts(
        [item.model_dump(mode="json") for item in checkpoint.bootstrap_refs]
    )


def _validate_utc_timestamp(value: str) -> str:
    _utc_datetime(value)
    return value


def _utc_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ValueError("timestamp must be a real UTC calendar instant") from exc
    if not value.endswith("Z") or parsed.tzinfo != timezone.utc:
        raise ValueError("timestamp must use the UTC Z suffix")
    return parsed
