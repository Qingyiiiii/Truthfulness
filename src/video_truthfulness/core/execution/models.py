"""Strict Pydantic models for execution contracts without optional runtime dependencies."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)


class ExecutionContractError(ValueError):
    """Base error for execution contract validation failures."""


class ExecutionSchemaError(ExecutionContractError):
    pass


class ExecutionHashError(ExecutionContractError):
    pass


class EventChainError(ExecutionContractError):
    pass


class ScopeViolationError(ExecutionContractError):
    pass


class SensitiveMaterialError(ExecutionContractError):
    pass


class SessionFrozenError(ExecutionContractError):
    pass


TASK_ID = r"^task_[0-9a-hjkmnp-tv-z]{26}$"
SESSION_ID = r"^session_[0-9a-hjkmnp-tv-z]{26}$"
RUN_ID = r"^run_[0-9a-hjkmnp-tv-z]{26}$"
ARTIFACT_ID = r"^artifact_[0-9a-hjkmnp-tv-z]{26}$"
RECORD_ID = r"^record_[0-9a-hjkmnp-tv-z]{26}$"
CHECKPOINT_ID = r"^checkpoint_[0-9a-hjkmnp-tv-z]{26}$"
EVENT_ID = r"^event_[0-9a-hjkmnp-tv-z]{26}$"
STAGE_ID = r"^S0[1-9]$"
DAG_NODE_ID = r"^[a-z][a-z0-9_]*$"
SHA256 = r"^[0-9a-f]{64}$"
GIT_OBJECT_ID = r"^[0-9a-f]{40}$"
UTC_TIMESTAMP = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z$"
ARTIFACT_TYPE = r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$"
AGENT_PROFILE = r"^[a-z][a-z0-9_]*_agent_v[0-9]+\.[0-9]+\.[0-9]+$"
PROMPT_VERSION = r"^[a-z][a-z0-9_]*_prompt_v[0-9]+\.[0-9]+\.[0-9]+$"
SCHEMA_VERSION = r"^[a-z][a-z0-9_]*_v[0-9]+\.[0-9]+\.[0-9]+$"
SchemaVersion = Annotated[str, Field(pattern=SCHEMA_VERSION)]
ArtifactId = Annotated[str, Field(pattern=ARTIFACT_ID)]


class StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class GitBootstrapRef(StrictFrozenModel):
    ref_type: Literal["git_commit", "git_tree"]
    object_id: str = Field(pattern=GIT_OBJECT_ID)
    description: str = Field(min_length=1, max_length=240)


class FileBootstrapRef(StrictFrozenModel):
    ref_type: Literal["document", "working_tree_manifest", "registry", "dag_config"]
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    purpose: str = Field(min_length=1, max_length=240)


BootstrapRef = GitBootstrapRef | FileBootstrapRef


class ArtifactScope(StrictFrozenModel):
    scope_type: Literal["artifact"]
    artifact_id: str = Field(pattern=ARTIFACT_ID)
    purpose: str = Field(min_length=1, max_length=200)


class PathScope(StrictFrozenModel):
    scope_type: Literal["path"]
    relative_path: str = Field(min_length=1, max_length=512)
    purpose: str = Field(min_length=1, max_length=200)


class PathPrefixScope(StrictFrozenModel):
    scope_type: Literal["path_prefix"]
    relative_path: str = Field(min_length=1, max_length=512)
    recursive: bool
    purpose: str = Field(min_length=1, max_length=200)


ScopeEntry = Annotated[
    ArtifactScope | PathScope | PathPrefixScope, Field(discriminator="scope_type")
]


class CodeRef(StrictFrozenModel):
    git_commit: str = Field(pattern=GIT_OBJECT_ID)
    working_tree_dirty: bool
    working_tree_manifest_path: str | None
    working_tree_manifest_hash: str | None = Field(pattern=SHA256)

    @model_validator(mode="after")
    def validate_dirty_binding(self) -> CodeRef:
        if self.working_tree_dirty:
            if (
                not self.working_tree_manifest_path
                or not self.working_tree_manifest_hash
            ):
                raise ValueError(
                    "dirty code_ref requires a bounded manifest path and hash"
                )
        elif (
            self.working_tree_manifest_path is not None
            or self.working_tree_manifest_hash is not None
        ):
            raise ValueError("clean code_ref must not claim a working-tree manifest")
        return self


class EnvironmentRef(StrictFrozenModel):
    runtime_name: str = Field(min_length=1, max_length=80)
    runtime_version: str = Field(min_length=1, max_length=80)
    os_family: Literal["windows", "linux", "macos", "other"]
    architecture: str = Field(min_length=1, max_length=80)
    dependency_manifest_path: str | None
    dependency_manifest_hash: str | None = Field(pattern=SHA256)

    @model_validator(mode="after")
    def validate_dependency_binding(self) -> EnvironmentRef:
        if (self.dependency_manifest_path is None) != (
            self.dependency_manifest_hash is None
        ):
            raise ValueError(
                "dependency manifest path/hash must both be set or both be null"
            )
        return self


class HumanGatePolicy(StrictFrozenModel):
    approval_required: bool
    gate_node_ids: list[str]
    decision_artifact_required: bool
    implicit_approval_allowed: Literal[False]


class SessionManifest(StrictFrozenModel):
    """A version-dispatched Session contract.

    v1.0 remains the Stage 4 compatibility generation.  v1.1 is deliberately
    stage-scoped: a Session identifies exactly one source Workflow while the
    immutable DAG v1.2 snapshot may contain the approved S01/S02/S03
    transition set.
    """

    session_manifest_version: Literal[
        "session_manifest_v1.0.0",
        "session_manifest_v1.1.0",
        "session_manifest_v1.2.0",
        "session_manifest_v1.3.0",
    ]
    project_version: Literal["v0.2"]
    storage_version: Literal["V02"]
    release_id: Literal["truthfulness_v0.2_youtube_video"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    task_scope: Literal["run", "cross_run"]
    run_id: str | None = Field(pattern=RUN_ID)
    stage_id: str = Field(pattern=STAGE_ID)
    dag_node_id: str | None = Field(pattern=DAG_NODE_ID)
    parent_checkpoint_id: str | None = Field(pattern=CHECKPOINT_ID)
    bootstrap_refs: list[BootstrapRef]
    agent_profile_version: str = Field(pattern=AGENT_PROFILE)
    agent_runtime_version: str = Field(min_length=1, max_length=120)
    workflow_version: Literal[
        "youtube_truthfulness_workflow_v1.1.0",
        "youtube_truthfulness_workflow_v1.2.0",
        "youtube_truthfulness_workflow_v1.3.0",
    ]
    dag_version: Literal[
        "youtube_truthfulness_dag_v1.1.0",
        "youtube_truthfulness_dag_v1.2.0",
        "youtube_truthfulness_dag_v1.3.0",
        "youtube_truthfulness_dag_v1.4.0",
    ]
    schema_versions: list[SchemaVersion] = Field(min_length=1)
    prompt_version: str = Field(pattern=PROMPT_VERSION)
    code_ref: CodeRef
    environment_ref: EnvironmentRef
    declared_read_set: list[ScopeEntry]
    declared_write_set: list[ScopeEntry]
    human_gate_policy: HumanGatePolicy
    created_at: str = Field(pattern=UTC_TIMESTAMP)
    manifest_hash: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def validate_scope_and_bootstrap(self) -> SessionManifest:
        if self.task_scope == "run" and self.run_id is None:
            raise ValueError("run-scoped manifest requires run_id")
        if self.task_scope == "cross_run" and self.run_id is not None:
            raise ValueError("cross-run manifest requires run_id=null")
        if self.parent_checkpoint_id is None and not self.bootstrap_refs:
            raise ValueError("root Session requires bootstrap_refs")
        if self.session_manifest_version not in self.schema_versions:
            raise ValueError(
                "schema_versions must contain the exact Session manifest version"
            )
        if self.session_manifest_version == "session_manifest_v1.0.0":
            if (
                self.workflow_version != "youtube_truthfulness_workflow_v1.1.0"
                or self.dag_version != "youtube_truthfulness_dag_v1.1.0"
            ):
                raise ValueError("session_manifest_v1.0.0 requires Workflow/DAG v1.1")
        elif self.session_manifest_version == "session_manifest_v1.1.0":
            if self.dag_version != "youtube_truthfulness_dag_v1.2.0":
                raise ValueError("session_manifest_v1.1.0 requires DAG v1.2")
            expected_workflow = (
                "youtube_truthfulness_workflow_v1.3.0"
                if self.stage_id == "S02"
                else "youtube_truthfulness_workflow_v1.1.0"
            )
            if self.workflow_version != expected_workflow:
                raise ValueError(
                    "session_manifest_v1.1.0 source Workflow does not match stage"
                )
            if self.dag_node_id is not None:
                raise ValueError(
                    "session_manifest_v1.1.0 stage-level Session requires dag_node_id=null"
                )
        elif self.session_manifest_version == "session_manifest_v1.2.0":
            if (
                self.stage_id != "S01"
                or self.workflow_version != "youtube_truthfulness_workflow_v1.2.0"
                or self.dag_version != "youtube_truthfulness_dag_v1.3.0"
            ):
                raise ValueError(
                    "session_manifest_v1.2.0 requires S01 Workflow v1.2 and DAG v1.3"
                )
            if self.dag_node_id is not None:
                raise ValueError(
                    "session_manifest_v1.2.0 stage-level Session requires dag_node_id=null"
                )
        else:
            if (
                self.stage_id != "S01"
                or self.workflow_version != "youtube_truthfulness_workflow_v1.3.0"
                or self.dag_version != "youtube_truthfulness_dag_v1.4.0"
            ):
                raise ValueError(
                    "session_manifest_v1.3.0 requires S01 Workflow v1.3 and DAG v1.4"
                )
            if self.dag_node_id is not None:
                raise ValueError(
                    "session_manifest_v1.3.0 stage-level Session requires dag_node_id=null"
                )
        return self

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return _validate_utc_timestamp(value)


class SessionManifestV1_1(SessionManifest):
    """Named successor type retained for explicit imports and documentation."""

    session_manifest_version: Literal["session_manifest_v1.1.0"]


class SessionManifestV1_2(SessionManifest):
    """S01 successor Session bound to Workflow v1.2 / DAG v1.3."""

    session_manifest_version: Literal["session_manifest_v1.2.0"]


class SessionManifestV1_3(SessionManifest):
    """Warehouse-export S01 successor bound to Workflow v1.3 / DAG v1.4."""

    session_manifest_version: Literal["session_manifest_v1.3.0"]


class ActorRef(StrictFrozenModel):
    actor_type: Literal["agent", "human", "workflow", "tool", "system"]
    actor_id: str = Field(min_length=1, max_length=160)
    agent_profile_version: str | None = Field(pattern=AGENT_PROFILE)
    agent_runtime_version: str | None = Field(min_length=1, max_length=120)


class ArtifactRef(StrictFrozenModel):
    artifact_id: str = Field(pattern=ARTIFACT_ID)
    artifact_type: str = Field(pattern=ARTIFACT_TYPE)
    record_id: str | None = Field(pattern=RECORD_ID)
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    input_fingerprint: str | None = Field(pattern=SHA256)
    validation_status: Literal["not_validated", "passed", "failed", "partial"]
    lifecycle_state: Literal[
        "created",
        "validated",
        "frozen",
        "stale",
        "superseded",
        "invalid",
        "archived",
        "purged",
    ]


class PathRef(StrictFrozenModel):
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    purpose: str = Field(min_length=1, max_length=240)


class SessionStartedPayload(StrictFrozenModel):
    manifest_path: str = Field(min_length=1, max_length=512)
    manifest_hash: str = Field(pattern=SHA256)


class TaskCreatedPayload(StrictFrozenModel):
    task_scope: Literal["run", "cross_run"]
    objective: str = Field(min_length=1, max_length=500)
    parent_checkpoint_id: str | None = Field(pattern=CHECKPOINT_ID)


class TaskStartedPayload(StrictFrozenModel):
    action_summary: str = Field(min_length=1, max_length=500)
    required_input_artifact_ids: list[ArtifactId]


class ArtifactReadPayload(StrictFrozenModel):
    purpose: str = Field(min_length=1, max_length=240)
    hash_verified: Literal[True]


class ArtifactWrittenPayload(StrictFrozenModel):
    write_method: Literal["atomic_replace", "append_only"]
    size_bytes: int = Field(ge=0)


class ArtifactValidatedPayload(StrictFrozenModel):
    validator_id: str = Field(pattern=r"^[a-z][a-z0-9_.-]*$", max_length=160)
    validator_version: str = Field(min_length=1, max_length=120)
    result: Literal["passed", "failed", "partial"]
    validation_artifact_id: str | None = Field(pattern=ARTIFACT_ID)


class ArtifactInvalidatedPayload(StrictFrozenModel):
    reason_code: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    reason_summary: str = Field(min_length=1, max_length=500)
    replacement_artifact_id: str | None = Field(pattern=ARTIFACT_ID)


class ToolFailedPayload(StrictFrozenModel):
    tool_name: str = Field(min_length=1, max_length=120)
    error_class: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_.-]*$", max_length=160)
    exit_code: int | None
    summary: str = Field(min_length=1, max_length=500)
    diagnostic_artifact_id: str | None = Field(pattern=ARTIFACT_ID)
    retryable: bool


class TaskRetriedPayload(StrictFrozenModel):
    new_session_id: str = Field(pattern=SESSION_ID)
    new_attempt_no: int = Field(ge=2)
    parent_checkpoint_id: str = Field(pattern=CHECKPOINT_ID)
    change_summary: str = Field(min_length=1, max_length=500)


class ApprovalRequestedPayload(StrictFrozenModel):
    gate_node_id: str = Field(pattern=DAG_NODE_ID)
    decision_type: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    question_summary: str = Field(min_length=1, max_length=500)
    decision_artifact_id: str = Field(pattern=ARTIFACT_ID)


class ApprovalReceivedPayload(StrictFrozenModel):
    gate_node_id: str = Field(pattern=DAG_NODE_ID)
    decision: Literal["approved", "rejected", "deferred"]
    decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    sanitized_summary: str = Field(min_length=1, max_length=500)


class CheckpointCreatedPayload(StrictFrozenModel):
    checkpoint_id: str = Field(pattern=CHECKPOINT_ID)
    checkpoint_path: str = Field(min_length=1, max_length=512)
    checkpoint_hash: str = Field(pattern=SHA256)
    checkpoint_kind: Literal[
        "stage_boundary", "failure_boundary", "human_gate_boundary", "retry_boundary"
    ]


class HandoffCreatedPayload(StrictFrozenModel):
    handoff_artifact_id: str = Field(pattern=ARTIFACT_ID)
    record_id: str = Field(pattern=RECORD_ID)
    handoff_path: str = Field(min_length=1, max_length=512)
    handoff_hash: str = Field(pattern=SHA256)
    record_hash: str = Field(pattern=SHA256)


class TaskCompletedPayload(StrictFrozenModel):
    result_summary: str = Field(min_length=1, max_length=500)


class TaskFailedPayload(StrictFrozenModel):
    error_class: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_.-]*$", max_length=160)
    summary: str = Field(min_length=1, max_length=500)
    diagnostic_artifact_ids: list[ArtifactId]


class TaskWaitingPayload(StrictFrozenModel):
    decision_artifact_ids: list[ArtifactId] = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=500)


class TaskBlockedPayload(StrictFrozenModel):
    missing_input_refs: list[str] = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=500)


class TaskSkippedPayload(StrictFrozenModel):
    gate_node_id: str = Field(pattern=DAG_NODE_ID)
    decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    reason: str = Field(min_length=1, max_length=500)


PAYLOAD_MODELS: dict[str, type[StrictFrozenModel]] = {
    "session.started": SessionStartedPayload,
    "task.created": TaskCreatedPayload,
    "task.started": TaskStartedPayload,
    "artifact.read": ArtifactReadPayload,
    "artifact.written": ArtifactWrittenPayload,
    "artifact.validated": ArtifactValidatedPayload,
    "artifact.invalidated": ArtifactInvalidatedPayload,
    "tool.failed": ToolFailedPayload,
    "task.retried": TaskRetriedPayload,
    "human.approval_requested": ApprovalRequestedPayload,
    "human.approval_received": ApprovalReceivedPayload,
    "checkpoint.created": CheckpointCreatedPayload,
    "handoff.created": HandoffCreatedPayload,
    "task.completed": TaskCompletedPayload,
    "task.failed": TaskFailedPayload,
    "task.waiting_for_human": TaskWaitingPayload,
    "task.blocked_by_input": TaskBlockedPayload,
    "task.skipped_by_gate": TaskSkippedPayload,
}


class ExecutionEvent(StrictFrozenModel):
    event_schema_version: Literal["execution_event_v1.0.0"]
    event_id: str = Field(pattern=EVENT_ID)
    sequence_no: int = Field(ge=1)
    occurred_at: str = Field(pattern=UTC_TIMESTAMP)
    event_type: str
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str | None = Field(pattern=RUN_ID)
    stage_id: str = Field(pattern=STAGE_ID)
    dag_node_id: str | None = Field(pattern=DAG_NODE_ID)
    actor: ActorRef
    artifact_refs: list[ArtifactRef]
    path_refs: list[PathRef]
    checkpoint_id: str | None = Field(pattern=CHECKPOINT_ID)
    payload: dict[str, Any]
    previous_event_id: str | None = Field(pattern=EVENT_ID)
    previous_event_hash: str | None = Field(pattern=SHA256)
    event_hash: str = Field(pattern=SHA256)

    @field_validator("occurred_at")
    @classmethod
    def validate_occurred_at(cls, value: str) -> str:
        return _validate_utc_timestamp(value)


class ExecutionEventV1_0_1(ExecutionEvent):
    event_schema_version: Literal["execution_event_v1.0.1"]

    @model_validator(mode="after")
    def validate_observed_access_reference(self) -> ExecutionEventV1_0_1:
        if (
            self.event_type in {"artifact.read", "artifact.written"}
            and not self.artifact_refs
            and not self.path_refs
        ):
            raise ValueError(
                "execution_event_v1.0.1 artifact.read/artifact.written requires "
                "at least one Artifact ref or path+hash ref"
            )
        return self


def _validate_utc_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ValueError("timestamp must be a real UTC calendar instant") from exc
    if parsed.tzinfo != timezone.utc:
        raise ValueError("timestamp must use the UTC Z suffix")
    return value


def _raise_schema(kind: str, exc: ValidationError) -> None:
    first = exc.errors(include_url=False)[0]
    location = "/".join(str(part) for part in first["loc"]) or "<root>"
    raise ExecutionSchemaError(
        f"Invalid {kind} contract at {location}: {first['msg']}"
    ) from exc


def _require_unique(kind: str, values: list[Any]) -> None:
    serialized = [
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for value in values
    ]
    if len(serialized) != len(set(serialized)):
        raise ExecutionSchemaError(
            f"Invalid {kind} contract: array values must be unique"
        )


def parse_session_manifest(raw: dict[str, Any]) -> SessionManifest:
    model_type: type[SessionManifest] = {
        "session_manifest_v1.1.0": SessionManifestV1_1,
        "session_manifest_v1.2.0": SessionManifestV1_2,
        "session_manifest_v1.3.0": SessionManifestV1_3,
    }.get(raw.get("session_manifest_version"), SessionManifest)
    try:
        model = model_type.model_validate(raw)
    except ValidationError as exc:
        _raise_schema("manifest", exc)
    _require_unique("manifest schema_versions", model.schema_versions)
    _require_unique(
        "manifest declared_read_set",
        [item.model_dump(mode="json") for item in model.declared_read_set],
    )
    _require_unique(
        "manifest declared_write_set",
        [item.model_dump(mode="json") for item in model.declared_write_set],
    )
    _require_unique("manifest human gate nodes", model.human_gate_policy.gate_node_ids)
    return model


def parse_execution_event(raw: dict[str, Any]) -> ExecutionEvent:
    model_type = {
        "execution_event_v1.0.0": ExecutionEvent,
        "execution_event_v1.0.1": ExecutionEventV1_0_1,
    }.get(raw.get("event_schema_version"))
    if model_type is None:
        raise ExecutionSchemaError(
            "Invalid event contract at event_schema_version: "
            "unsupported or missing execution Event schema version"
        )
    try:
        model = model_type.model_validate(raw)
    except ValidationError as exc:
        _raise_schema("event", exc)
    payload_model = PAYLOAD_MODELS.get(model.event_type)
    if payload_model is None:
        raise ExecutionSchemaError(
            f"Invalid event contract: unsupported event_type {model.event_type!r}"
        )
    try:
        payload = payload_model.model_validate(model.payload)
    except ValidationError as exc:
        _raise_schema(f"event payload for {model.event_type}", exc)
    _require_unique(
        "event artifact_refs",
        [item.model_dump(mode="json") for item in model.artifact_refs],
    )
    _require_unique(
        "event path_refs", [item.model_dump(mode="json") for item in model.path_refs]
    )
    for field in (
        "required_input_artifact_ids",
        "diagnostic_artifact_ids",
        "decision_artifact_ids",
        "missing_input_refs",
    ):
        values = getattr(payload, field, None)
        if values is not None:
            _require_unique(f"event payload {field}", values)
    return model
