"""Strict HANDOFF v2 models, deterministic construction and source validation."""

from __future__ import annotations

import html
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal, Mapping, Sequence

from pydantic import Field, ValidationError, field_validator, model_validator

from video_truthfulness.core.artifacts.dag import load_dag
from video_truthfulness.core.artifacts.models import ArtifactRecordV1_1, new_typed_ulid
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryValidationError,
    create_artifact_record,
)
from video_truthfulness.core.execution.checkpoints import (
    CheckpointPublication,
    CheckpointSources,
    EventHead,
    ExecutionCheckpoint,
    ObservedAccess,
    RegisteredArtifactRef,
    RegistryHead,
    TerminalState,
    ValidationSummary,
    parse_checkpoint,
    validate_checkpoint,
    validate_checkpoint_created_event,
)
from video_truthfulness.core.execution.events import (
    reject_sensitive_material,
    validate_event_stream,
    validate_manifest,
    validate_relative_path,
)
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_bytes,
    sha256_file,
)
from video_truthfulness.core.execution.io import ContractIOError, read_json, write_json
from video_truthfulness.core.execution.models import (
    AGENT_PROFILE,
    ARTIFACT_ID,
    CHECKPOINT_ID,
    DAG_NODE_ID,
    PROMPT_VERSION,
    RUN_ID,
    SESSION_ID,
    SHA256,
    STAGE_ID,
    TASK_ID,
    UTC_TIMESTAMP,
    CodeRef,
    ExecutionContractError,
    ExecutionEvent,
    ExecutionHashError,
    SchemaVersion,
    ScopeEntry,
    SessionManifest,
    StrictFrozenModel,
    parse_execution_event,
)
from video_truthfulness.core.execution.state import (
    RegistrySnapshot,
    build_current_state,
    snapshot_registry,
)


MAX_HANDOFF_BYTES = 1_048_576
MAX_HANDOFF_COLLECTION_ITEMS = 1_024
MAX_HANDOFF_DEPTH = 32
MAX_HANDOFF_NODES = 20_000


class HandoffValidationError(ExecutionContractError):
    """Raised when a HANDOFF or one of its authoritative sources is invalid."""


class HandoffImmutableError(HandoffValidationError):
    """Raised when immutable HANDOFF publication cannot complete safely."""


class HandoffRegistrationError(HandoffValidationError):
    """Raised when a HANDOFF Registry receipt is invalid or cannot be appended."""


class HandoffMarkdownDriftError(HandoffValidationError):
    """Raised when HANDOFF.md differs byte-for-byte from its machine source."""


class CompletedAction(StrictFrozenModel):
    action_key: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    summary: str = Field(min_length=1, max_length=500)
    source_event_id: str = Field(pattern=r"^event_[0-9a-hjkmnp-tv-z]{26}$")


class RemainingAction(StrictFrozenModel):
    action_key: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    summary: str = Field(min_length=1, max_length=500)


class Risk(StrictFrozenModel):
    risk_key: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    severity: Literal["low", "medium", "high", "critical"]
    summary: str = Field(min_length=1, max_length=500)
    mitigation: str = Field(min_length=1, max_length=500)
    blocking: bool


class HumanDecisionRequirement(StrictFrozenModel):
    decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    gate_node_id: str = Field(pattern=DAG_NODE_ID)
    reason: str = Field(min_length=1, max_length=500)


class HandoffMetrics(StrictFrozenModel):
    event_count: int = Field(ge=1)
    actual_read_count: int = Field(ge=0)
    actual_write_count: int = Field(ge=0)
    input_artifact_count: int = Field(ge=0)
    output_artifact_count: int = Field(ge=0)
    validation_passed_count: int = Field(ge=0)
    validation_failed_count: int = Field(ge=0)
    out_of_scope_detection_count: int = Field(ge=0)
    rebuild_hash_match: Literal[True]


class NextStageAction(StrictFrozenModel):
    action_type: Literal["next_stage"]
    next_stage: str = Field(pattern=STAGE_ID)
    workflow_reference: str = Field(min_length=1, max_length=512)
    prompt_reference: str = Field(min_length=1, max_length=512)
    required_input_artifact_ids: list[str]
    required_read_paths: list[str]
    reason: str = Field(min_length=1, max_length=500)

    @field_validator("required_input_artifact_ids")
    @classmethod
    def validate_artifact_ids(cls, values: list[str]) -> list[str]:
        for value in values:
            if not _matches(ARTIFACT_ID, value):
                raise ValueError(f"invalid Artifact ID: {value}")
        return values


class NextStageActionV2_1(NextStageAction):
    """Explicit adjacent Workflow transition used only by HANDOFF v2.1."""

    target_workflow_version: Literal[
        "youtube_truthfulness_workflow_v1.1.0",
        "youtube_truthfulness_workflow_v1.3.0",
    ]
    execution_authorized: bool


class WaitForHumanAction(StrictFrozenModel):
    action_type: Literal["wait_for_human"]
    decision_artifact_ids: list[str] = Field(min_length=1)
    reason: str = Field(min_length=1, max_length=500)

    @field_validator("decision_artifact_ids")
    @classmethod
    def validate_artifact_ids(cls, values: list[str]) -> list[str]:
        for value in values:
            if not _matches(ARTIFACT_ID, value):
                raise ValueError(f"invalid decision Artifact ID: {value}")
        return values


class ReturnToStageAction(StrictFrozenModel):
    action_type: Literal["return_to_stage"]
    target_stage: str = Field(pattern=STAGE_ID)
    workflow_reference: str = Field(min_length=1, max_length=512)
    prompt_reference: str = Field(min_length=1, max_length=512)
    required_input_artifact_ids: list[str]
    required_read_paths: list[str]
    reason: str = Field(min_length=1, max_length=500)

    @field_validator("required_input_artifact_ids")
    @classmethod
    def validate_artifact_ids(cls, values: list[str]) -> list[str]:
        for value in values:
            if not _matches(ARTIFACT_ID, value):
                raise ValueError(f"invalid Artifact ID: {value}")
        return values


class TerminateAction(StrictFrozenModel):
    action_type: Literal["terminate"]
    termination_kind: Literal[
        "project_complete", "process_terminated", "terminal_failure"
    ]
    reason: str = Field(min_length=1, max_length=500)


NextAction = Annotated[
    NextStageAction | WaitForHumanAction | ReturnToStageAction | TerminateAction,
    Field(discriminator="action_type"),
]

NextActionV2_1 = Annotated[
    NextStageActionV2_1 | WaitForHumanAction | ReturnToStageAction | TerminateAction,
    Field(discriminator="action_type"),
]


class HandoffV2(StrictFrozenModel):
    handoff_version: Literal["handoff_v2.0.0"]
    handoff_artifact_id: str = Field(pattern=ARTIFACT_ID)
    handoff_scope: Literal["run", "project"]
    project_version: Literal["v0.2"]
    storage_version: Literal["V02"]
    release_id: Literal["truthfulness_v0.2_youtube_video"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str | None = Field(pattern=RUN_ID)
    stage_id: str = Field(pattern=STAGE_ID)
    status: TerminalState
    parent_checkpoint_id: str | None = Field(pattern=CHECKPOINT_ID)
    checkpoint_id: str = Field(pattern=CHECKPOINT_ID)
    agent_profile_version: str = Field(pattern=AGENT_PROFILE)
    agent_runtime_version: str = Field(min_length=1, max_length=120)
    workflow_version: Literal["youtube_truthfulness_workflow_v1.1.0"]
    dag_version: Literal["youtube_truthfulness_dag_v1.1.0"]
    schema_versions: list[SchemaVersion] = Field(min_length=1)
    prompt_version: str = Field(pattern=PROMPT_VERSION)
    code_ref: CodeRef
    source_event_head: EventHead
    source_registry_heads: list[RegistryHead] = Field(min_length=1)
    input_artifacts: list[RegisteredArtifactRef]
    output_artifacts: list[RegisteredArtifactRef]
    declared_read_set: list[ScopeEntry]
    declared_write_set: list[ScopeEntry]
    actual_read_set: list[ObservedAccess]
    actual_write_set: list[ObservedAccess]
    invalidated_artifacts: list[RegisteredArtifactRef]
    completed_actions: list[CompletedAction]
    remaining_actions: list[RemainingAction]
    risks: list[Risk]
    human_decisions_required: list[HumanDecisionRequirement]
    validation_summary: ValidationSummary
    metrics: HandoffMetrics
    next_action: NextAction
    render_profile_version: str = Field(
        pattern=r"^handoff_markdown_renderer_v[0-9]+\.[0-9]+\.[0-9]+$"
    )
    created_at: str = Field(pattern=UTC_TIMESTAMP)
    handoff_hash: str = Field(pattern=SHA256)

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: str) -> str:
        return _validate_utc_timestamp(value)

    @model_validator(mode="after")
    def validate_contract_relations(self) -> HandoffV2:
        if (self.handoff_scope == "run") != (self.run_id is not None):
            raise ValueError(
                "run HANDOFF requires run_id; project HANDOFF requires run_id=null"
            )
        if "handoff_v2.0.0" not in self.schema_versions:
            raise ValueError("schema_versions must contain handoff_v2.0.0")
        if _utc_datetime(self.created_at) < _utc_datetime(
            self.source_event_head.occurred_at
        ):
            raise ValueError("HANDOFF created_at cannot precede its source event head")
        if self.metrics.event_count != self.source_event_head.sequence_no:
            raise ValueError("metrics.event_count must equal the fixed source sequence")
        metric_counts = (
            self.metrics.actual_read_count,
            self.metrics.actual_write_count,
            self.metrics.input_artifact_count,
            self.metrics.output_artifact_count,
            self.metrics.validation_passed_count,
            self.metrics.validation_failed_count,
        )
        observed_counts = (
            len(self.actual_read_set),
            len(self.actual_write_set),
            len(self.input_artifacts),
            len(self.output_artifacts),
            self.validation_summary.passed_count,
            self.validation_summary.failed_count,
        )
        if metric_counts != observed_counts:
            raise ValueError("HANDOFF metrics do not match the projected collections")
        waiting = isinstance(self.next_action, WaitForHumanAction)
        if (self.status == "WAITING_FOR_HUMAN") != waiting:
            raise ValueError(
                "WAITING_FOR_HUMAN requires the unique wait_for_human next action"
            )
        if isinstance(self.next_action, NextStageAction):
            if self.status in {"FAILED", "BLOCKED_BY_INPUT"}:
                raise ValueError(f"{self.status} cannot advance with next_stage")
            if any(risk.blocking for risk in self.risks):
                raise ValueError("blocking risks cannot advance with next_stage")
        return self


class HandoffV2_1(HandoffV2):
    """Stage-scoped Workflow transition contract for Stage 5."""

    handoff_version: Literal["handoff_v2.1.0"]
    workflow_version: Literal[
        "youtube_truthfulness_workflow_v1.1.0",
        "youtube_truthfulness_workflow_v1.3.0",
    ]
    dag_version: Literal["youtube_truthfulness_dag_v1.2.0"]
    next_action: NextActionV2_1

    @model_validator(mode="after")
    def validate_contract_relations(self) -> HandoffV2_1:
        if (self.handoff_scope == "run") != (self.run_id is not None):
            raise ValueError(
                "run HANDOFF requires run_id; project HANDOFF requires run_id=null"
            )
        if "handoff_v2.1.0" not in self.schema_versions:
            raise ValueError("schema_versions must contain handoff_v2.1.0")
        if _utc_datetime(self.created_at) < _utc_datetime(
            self.source_event_head.occurred_at
        ):
            raise ValueError("HANDOFF created_at cannot precede its source event head")
        if self.metrics.event_count != self.source_event_head.sequence_no:
            raise ValueError("metrics.event_count must equal the fixed source sequence")
        metric_counts = (
            self.metrics.actual_read_count,
            self.metrics.actual_write_count,
            self.metrics.input_artifact_count,
            self.metrics.output_artifact_count,
            self.metrics.validation_passed_count,
            self.metrics.validation_failed_count,
        )
        observed_counts = (
            len(self.actual_read_set),
            len(self.actual_write_set),
            len(self.input_artifacts),
            len(self.output_artifacts),
            self.validation_summary.passed_count,
            self.validation_summary.failed_count,
        )
        if metric_counts != observed_counts:
            raise ValueError("HANDOFF metrics do not match the projected collections")
        expected_source_workflow = (
            "youtube_truthfulness_workflow_v1.3.0"
            if self.stage_id == "S02"
            else "youtube_truthfulness_workflow_v1.1.0"
        )
        if self.workflow_version != expected_source_workflow:
            raise ValueError("HANDOFF v2.1 source Workflow does not match stage")
        waiting = isinstance(self.next_action, WaitForHumanAction)
        if (self.status == "WAITING_FOR_HUMAN") != waiting:
            raise ValueError(
                "WAITING_FOR_HUMAN requires the unique wait_for_human next action"
            )
        if isinstance(self.next_action, NextStageActionV2_1):
            transition = (
                self.stage_id,
                self.workflow_version,
                self.next_action.next_stage,
                self.next_action.target_workflow_version,
                self.next_action.execution_authorized,
            )
            allowed = {
                (
                    "S01",
                    "youtube_truthfulness_workflow_v1.1.0",
                    "S02",
                    "youtube_truthfulness_workflow_v1.3.0",
                    True,
                ),
                (
                    "S02",
                    "youtube_truthfulness_workflow_v1.3.0",
                    "S03",
                    "youtube_truthfulness_workflow_v1.1.0",
                    False,
                ),
            }
            if transition not in allowed:
                raise ValueError("HANDOFF v2.1 uses an undeclared Workflow transition")
            if self.status in {"FAILED", "BLOCKED_BY_INPUT"}:
                raise ValueError(f"{self.status} cannot advance with next_stage")
            if any(risk.blocking for risk in self.risks):
                raise ValueError("blocking risks cannot advance with next_stage")
        return self


class HandoffV2_2(HandoffV2_1):
    """Route-only S01 successor; it records S02 without authorizing execution."""

    handoff_version: Literal["handoff_v2.2.0"]
    workflow_version: Literal["youtube_truthfulness_workflow_v1.2.0"]
    dag_version: Literal["youtube_truthfulness_dag_v1.3.0"]
    next_action: NextActionV2_1

    @model_validator(mode="after")
    def validate_contract_relations(self) -> "HandoffV2_2":
        if self.handoff_scope != "run" or self.run_id is None or self.stage_id != "S01":
            raise ValueError("HANDOFF v2.2 is one run-scoped S01 successor")
        if "handoff_v2.2.0" not in self.schema_versions:
            raise ValueError("schema_versions must contain handoff_v2.2.0")
        if _utc_datetime(self.created_at) < _utc_datetime(
            self.source_event_head.occurred_at
        ):
            raise ValueError("HANDOFF created_at cannot precede its source event head")
        if self.metrics.event_count != self.source_event_head.sequence_no:
            raise ValueError("metrics.event_count must equal the fixed source sequence")
        metric_counts = (
            self.metrics.actual_read_count,
            self.metrics.actual_write_count,
            self.metrics.input_artifact_count,
            self.metrics.output_artifact_count,
            self.metrics.validation_passed_count,
            self.metrics.validation_failed_count,
        )
        observed_counts = (
            len(self.actual_read_set),
            len(self.actual_write_set),
            len(self.input_artifacts),
            len(self.output_artifacts),
            self.validation_summary.passed_count,
            self.validation_summary.failed_count,
        )
        if metric_counts != observed_counts:
            raise ValueError("HANDOFF metrics do not match the projected collections")
        if not isinstance(self.next_action, NextStageActionV2_1):
            raise ValueError("completed S01 HANDOFF v2.2 requires next_stage")
        transition = (
            self.status,
            self.next_action.next_stage,
            self.next_action.target_workflow_version,
            self.next_action.execution_authorized,
        )
        if transition != (
            "COMPLETED",
            "S02",
            "youtube_truthfulness_workflow_v1.3.0",
            False,
        ):
            raise ValueError("HANDOFF v2.2 requires the frozen route-only S01 to S02 transition")
        if any(risk.blocking for risk in self.risks):
            raise ValueError("blocking risks cannot advance with next_stage")
        return self


class WarehouseProjectionHandoffV1(StrictFrozenModel):
    """Immutable S01 export identity; the analytical projection is still pending."""

    export_artifact_id: str = Field(pattern=ARTIFACT_ID)
    storage_root_ref: Literal["ubuntu_v02_claim_warehouse"]
    manifest_relative_path: str
    manifest_hash: str = Field(pattern=SHA256)
    rows_relative_path: str
    rows_hash: str = Field(pattern=SHA256)
    logical_hash: str = Field(pattern=SHA256)
    row_count: int = Field(ge=1)
    row_counts: dict[str, int]
    database_schema_version: Literal["truthfulness_db_v02.1.0"]
    label_taxonomy_version: Literal["truthfulness_taxonomy_v02.1.0"]
    warehouse_export_schema_version: Literal["claim_warehouse_export_v1.0.0"]
    exporter_version: Literal["warehouse_export_v1.0.0"]
    projection_status: Literal["pending"]

    @field_validator("manifest_relative_path", "rows_relative_path")
    @classmethod
    def validate_storage_relative_path(cls, value: str) -> str:
        return validate_relative_path(value).as_posix()

    @model_validator(mode="after")
    def validate_projection_identity(self) -> "WarehouseProjectionHandoffV1":
        if self.manifest_relative_path == self.rows_relative_path:
            raise ValueError("warehouse manifest and rows paths must differ")
        if not self.row_counts or sum(self.row_counts.values()) != self.row_count:
            raise ValueError("warehouse row_counts must be non-empty and sum to row_count")
        if any(
            not key or value <= 0 or "/" in key or "\\" in key
            for key, value in self.row_counts.items()
        ):
            raise ValueError("warehouse row_counts contain an invalid table key/count")
        return self


class HandoffV2_3(HandoffV2_2):
    """S01 warehouse-export successor; it never authorizes S02 execution."""

    handoff_version: Literal["handoff_v2.3.0"]
    workflow_version: Literal["youtube_truthfulness_workflow_v1.3.0"]
    dag_version: Literal["youtube_truthfulness_dag_v1.4.0"]
    warehouse_projection: WarehouseProjectionHandoffV1

    @model_validator(mode="after")
    def validate_contract_relations(self) -> "HandoffV2_3":
        if self.handoff_scope != "run" or self.run_id is None or self.stage_id != "S01":
            raise ValueError("HANDOFF v2.3 is one run-scoped S01 successor")
        if "handoff_v2.3.0" not in self.schema_versions:
            raise ValueError("schema_versions must contain handoff_v2.3.0")
        if _utc_datetime(self.created_at) < _utc_datetime(
            self.source_event_head.occurred_at
        ):
            raise ValueError("HANDOFF created_at cannot precede its source event head")
        if self.metrics.event_count != self.source_event_head.sequence_no:
            raise ValueError("metrics.event_count must equal the fixed source sequence")
        metric_counts = (
            self.metrics.actual_read_count,
            self.metrics.actual_write_count,
            self.metrics.input_artifact_count,
            self.metrics.output_artifact_count,
            self.metrics.validation_passed_count,
            self.metrics.validation_failed_count,
        )
        observed_counts = (
            len(self.actual_read_set),
            len(self.actual_write_set),
            len(self.input_artifacts),
            len(self.output_artifacts),
            self.validation_summary.passed_count,
            self.validation_summary.failed_count,
        )
        if metric_counts != observed_counts:
            raise ValueError("HANDOFF metrics do not match the projected collections")
        exports = [
            item
            for item in self.output_artifacts
            if item.artifact_type == "warehouse.export_batch"
        ]
        if (
            len(exports) != 1
            or exports[0].artifact_id
            != self.warehouse_projection.export_artifact_id
        ):
            raise ValueError("HANDOFF v2.3 must bind one exact warehouse export")
        waiting = isinstance(self.next_action, WaitForHumanAction)
        if (self.status == "WAITING_FOR_HUMAN") != waiting:
            raise ValueError(
                "WAITING_FOR_HUMAN requires the unique wait_for_human next action"
            )
        if not waiting:
            if not isinstance(self.next_action, NextStageActionV2_1):
                raise ValueError("completed S01 HANDOFF v2.3 requires next_stage")
            transition = (
                self.status,
                self.next_action.next_stage,
                self.next_action.target_workflow_version,
                self.next_action.execution_authorized,
            )
            if transition != (
                "COMPLETED",
                "S02",
                "youtube_truthfulness_workflow_v1.3.0",
                False,
            ):
                raise ValueError(
                    "HANDOFF v2.3 requires the route-only S01 to S02 transition"
                )
            if any(risk.blocking for risk in self.risks):
                raise ValueError("blocking risks cannot advance with next_stage")
        return self


@dataclass(frozen=True)
class HandoffSources:
    repository_root: Path
    manifest: Mapping[str, Any] | SessionManifest
    events: Sequence[Mapping[str, Any] | ExecutionEvent]
    terminal_state: Mapping[str, Any]
    registry_snapshots: tuple[RegistrySnapshot, ...]
    dag_path: Path
    checkpoint: Mapping[str, Any] | ExecutionCheckpoint


@dataclass(frozen=True)
class HandoffPublication:
    """One immutable machine HANDOFF publication before Registry registration."""

    handoff: HandoffV2
    path: Path
    relative_path: str
    file_hash: str
    size_bytes: int

    @property
    def handoff_artifact_id(self) -> str:
        return self.handoff.handoff_artifact_id

    @property
    def handoff_hash(self) -> str:
        return self.handoff.handoff_hash


@dataclass(frozen=True)
class HandoffRegistration:
    """The append-only Registry receipt for one published machine HANDOFF."""

    publication: HandoffPublication
    record: ArtifactRecordV1_1
    registry_path: Path
    registry_relative_path: str
    before_head: RegistryHead
    after_head: RegistryHead


@dataclass(frozen=True)
class HandoffMarkdownPublication:
    """A rebuildable Markdown projection and its exact byte identity."""

    path: Path
    file_hash: str
    size_bytes: int


@dataclass(frozen=True)
class _ValidatedSources:
    manifest: SessionManifest
    events: tuple[ExecutionEvent, ...]
    checkpoint: ExecutionCheckpoint
    source_event_head: dict[str, Any]
    registry_heads: list[dict[str, Any]]
    terminal_state: dict[str, Any]


def parse_handoff(raw: Mapping[str, Any]) -> HandoffV2:
    """Parse one strict HANDOFF and verify its embedded semantic hash."""

    payload = dict(raw)
    _validate_handoff_resource_budget(payload)
    _reject_unsafe_handoff_text(payload)
    reject_sensitive_material(payload, location="handoff")
    model_type: type[HandoffV2] = {
        "handoff_v2.1.0": HandoffV2_1,
        "handoff_v2.2.0": HandoffV2_2,
        "handoff_v2.3.0": HandoffV2_3,
    }.get(payload.get("handoff_version"), HandoffV2)
    try:
        handoff = model_type.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors(include_url=False)[0]
        location = "/".join(str(part) for part in first["loc"]) or "<root>"
        raise HandoffValidationError(
            f"Invalid HANDOFF at {location}: {first['msg']}"
        ) from exc
    _validate_unique_collections(handoff)
    _validate_handoff_paths(handoff)
    try:
        expected = embedded_hash(payload, "handoff_hash")
    except ValueError as exc:
        raise ExecutionHashError(str(exc)) from exc
    if handoff.handoff_hash != expected:
        raise ExecutionHashError(
            f"handoff_hash mismatch: expected {expected}, observed {handoff.handoff_hash}"
        )
    return handoff


def build_handoff(
    sources: HandoffSources,
    *,
    next_action: Mapping[str, Any] | NextAction,
    created_at: str,
    handoff_artifact_id: str | None = None,
    risks: Sequence[Mapping[str, Any] | Risk] = (),
    render_profile_version: str = "handoff_markdown_renderer_v1.0.0",
    warehouse_projection: Mapping[str, Any] | WarehouseProjectionHandoffV1 | None = None,
) -> HandoffV2:
    """Build HANDOFF v2 from fixed pre-registration sources."""

    validated = _validate_sources(sources)
    raw = _handoff_raw(
        validated,
        handoff_artifact_id=handoff_artifact_id or new_typed_ulid("artifact"),
        next_action=_dump(next_action),
        created_at=created_at,
        risks=[_dump(item) for item in risks],
        render_profile_version=render_profile_version,
        warehouse_projection=(
            _dump(warehouse_projection)
            if warehouse_projection is not None
            else None
        ),
    )
    raw["handoff_hash"] = embedded_hash(raw, "handoff_hash")
    handoff = parse_handoff(raw)
    validate_handoff(handoff, sources)
    return handoff


def validate_handoff(
    handoff: HandoffV2 | Mapping[str, Any],
    sources: HandoffSources,
) -> HandoffV2:
    """Cross-check HANDOFF against its exact manifest/event/Registry/DAG/checkpoint/state sources."""

    model = (
        parse_handoff(handoff.model_dump(mode="json"))
        if isinstance(handoff, HandoffV2)
        else parse_handoff(handoff)
    )
    validated = _validate_sources(sources)
    if any(
        record.artifact_id == model.handoff_artifact_id
        for snapshot in sources.registry_snapshots
        for record in snapshot.records
    ):
        raise HandoffValidationError(
            "HANDOFF Artifact must not already exist in the pre-registration Registry prefix"
        )
    expected = _handoff_raw(
        validated,
        handoff_artifact_id=model.handoff_artifact_id,
        next_action=model.next_action.model_dump(mode="json"),
        created_at=model.created_at,
        risks=[item.model_dump(mode="json") for item in model.risks],
        render_profile_version=model.render_profile_version,
        warehouse_projection=(
            model.warehouse_projection.model_dump(mode="json")
            if isinstance(model, HandoffV2_3)
            else None
        ),
    )
    observed = model.model_dump(mode="json")
    for field, value in expected.items():
        if field == "handoff_hash":
            continue
        if observed[field] != value:
            raise HandoffValidationError(f"HANDOFF/source mismatch for {field}")
    _validate_next_action(model, validated, sources)
    return model


def read_handoff(path: Path) -> HandoffV2:
    """Read one canonical LF-terminated machine HANDOFF from its fixed filename."""

    if path.name != "handoff.json":
        raise HandoffValidationError(
            f"Machine HANDOFF filename must be handoff.json; observed {path.name}"
        )
    try:
        if path.stat().st_size > MAX_HANDOFF_BYTES:
            raise HandoffValidationError(
                f"Machine HANDOFF exceeds {MAX_HANDOFF_BYTES} bytes"
            )
        raw = read_json(path)
        observed = path.read_bytes()
    except (ContractIOError, OSError) as exc:
        raise HandoffValidationError(str(exc)) from exc
    model = parse_handoff(raw)
    expected = canonical_json_bytes(model.model_dump(mode="json")) + b"\n"
    if observed != expected:
        raise HandoffValidationError(
            "Machine HANDOFF must use canonical JSON with one final LF"
        )
    return model


def publish_handoff(
    path: Path,
    handoff: HandoffV2 | Mapping[str, Any],
    sources: HandoffSources,
) -> HandoffPublication:
    """Publish one HANDOFF exactly once, then re-read and source-validate its bytes."""

    model = validate_handoff(handoff, sources)
    root = sources.repository_root.resolve()
    target = path.resolve()
    if path.name != "handoff.json" or not target.is_relative_to(root):
        raise HandoffValidationError(
            "Machine HANDOFF must be a repository-local file named handoff.json"
        )
    relative_path = target.relative_to(root).as_posix()
    validate_relative_path(relative_path)
    manifest = validate_manifest(sources.manifest)
    if not _write_path_is_declared(relative_path, manifest):
        raise HandoffValidationError(
            f"HANDOFF path is outside the Session declared write scope: {relative_path}"
        )
    try:
        file_hash = write_json(target, model.model_dump(mode="json"), immutable=True)
    except ContractIOError as exc:
        raise HandoffImmutableError(str(exc)) from exc
    try:
        observed_file_hash = sha256_file(target)
        if observed_file_hash != file_hash:
            raise HandoffValidationError(
                "HANDOFF publication file hash changed during read-back validation"
            )
        read_back = read_handoff(target)
        validate_handoff(read_back, sources)
        if read_back.model_dump(mode="json") != model.model_dump(mode="json"):
            raise HandoffValidationError(
                "HANDOFF read-back content differs from the published object"
            )
    except Exception as exc:
        raise HandoffImmutableError(
            "HANDOFF was published but write-back validation failed; immutable bytes were preserved"
        ) from exc
    return HandoffPublication(
        handoff=read_back,
        path=target,
        relative_path=relative_path,
        file_hash=file_hash,
        size_bytes=target.stat().st_size,
    )


def create_handoff(
    path: Path,
    sources: HandoffSources,
    *,
    next_action: Mapping[str, Any] | NextAction,
    created_at: str,
    handoff_artifact_id: str | None = None,
    risks: Sequence[Mapping[str, Any] | Risk] = (),
    render_profile_version: str = "handoff_markdown_renderer_v1.0.0",
) -> HandoffPublication:
    """Build and immutably publish one pre-registration machine HANDOFF."""

    handoff = build_handoff(
        sources,
        next_action=next_action,
        created_at=created_at,
        handoff_artifact_id=handoff_artifact_id,
        risks=risks,
        render_profile_version=render_profile_version,
    )
    return publish_handoff(path, handoff, sources)


def build_handoff_registry_record(
    publication: HandoffPublication,
    sources: HandoffSources,
    *,
    recorded_at: str,
    privacy_class: Literal[
        "private_raw",
        "private_derived",
        "restricted_human",
        "public_synthetic",
        "public_aggregate",
    ],
    access_scope: Literal["local_private", "project_private", "public"],
    retention_policy: str,
    record_id: str | None = None,
    cross_run_identity: Mapping[str, str | None] | None = None,
    writer_agent_id: str | None = None,
    tool_versions: Mapping[str, str] | None = None,
    logical_name: str = "Machine HANDOFF v2",
) -> ArtifactRecordV1_1:
    """Build Registry v1.1 record3 from already-published HANDOFF bytes."""

    handoff = _validate_publication(publication, sources)
    manifest = validate_manifest(sources.manifest)
    source_platform, source_id = _handoff_run_source_identity(handoff, sources)
    recorded = _utc_datetime(recorded_at)
    if recorded < _utc_datetime(handoff.created_at):
        raise HandoffRegistrationError("HANDOFF record cannot predate HANDOFF creation")
    allowed_identity = {"batch_id", "dataset_build_id", "dataset_version", "exp_id"}
    supplied_identity = dict(cross_run_identity or {})
    unknown_identity = set(supplied_identity) - allowed_identity
    if unknown_identity:
        raise HandoffRegistrationError(
            f"Unsupported cross-run Registry identity fields: {sorted(unknown_identity)}"
        )
    explicit_identity = {
        key: value for key, value in supplied_identity.items() if value is not None
    }
    if handoff.handoff_scope == "run" and explicit_identity:
        raise HandoffRegistrationError(
            "Run HANDOFF cannot claim a cross-run Registry identity"
        )
    if handoff.handoff_scope == "project" and not explicit_identity:
        raise HandoffRegistrationError(
            "Project HANDOFF registration requires an explicit batch/dataset/experiment identity"
        )
    upstream_artifact_ids = sorted(
        {
            reference.artifact_id
            for reference in (*handoff.input_artifacts, *handoff.output_artifacts)
        }
    )
    values: dict[str, Any] = {
        "registry_schema_version": "artifact_record_v1.1.0",
        "recorded_at": recorded_at,
        "artifact_id": handoff.handoff_artifact_id,
        "artifact_type": "handoff.run"
        if handoff.handoff_scope == "run"
        else "handoff.project",
        "logical_name": logical_name,
        "container_kind": "file",
        "project_version": handoff.project_version,
        "storage_version": handoff.storage_version,
        "release_id": handoff.release_id,
        "source_platform": source_platform,
        "source_id": source_id,
        "run_id": handoff.run_id,
        "stage_id": handoff.stage_id,
        "dag_node_id": manifest.dag_node_id,
        "relative_path": publication.relative_path,
        "storage_scope": "run" if handoff.handoff_scope == "run" else "cross_run",
        "media_type": "application/json",
        "size_bytes": publication.size_bytes,
        "content_hash_algorithm": "sha256",
        "content_hash": publication.file_hash,
        "semantic_hash_algorithm": "sha256",
        "semantic_hash": handoff.handoff_hash,
        "producer_type": "agent",
        "writer_agent_id": writer_agent_id,
        "workflow_id": handoff.stage_id,
        "workflow_version": handoff.workflow_version,
        "schema_versions": ["artifact_record_v1.1.0", handoff.handoff_version],
        "prompt_version": handoff.prompt_version,
        "dag_version": handoff.dag_version,
        "code_commit": handoff.code_ref.git_commit,
        "tool_versions": dict(sorted((tool_versions or {}).items())),
        "upstream_artifact_ids": upstream_artifact_ids,
        "authority_level": "machine_derived",
        "lifecycle_state": "frozen",
        "validation_status": "passed",
        "privacy_class": privacy_class,
        "access_scope": access_scope,
        "retention_policy": retention_policy,
        "created_at": handoff.created_at,
        "validated_at": recorded_at,
        "frozen_at": recorded_at,
        **supplied_identity,
    }
    if record_id is not None:
        values["record_id"] = record_id
    try:
        record = create_artifact_record(**values)
    except Exception as exc:
        raise HandoffRegistrationError(
            f"Cannot construct HANDOFF Registry record: {exc}"
        ) from exc
    if not isinstance(record, ArtifactRecordV1_1):
        raise HandoffRegistrationError("HANDOFF registration must use Registry v1.1")
    validate_handoff_registry_record(publication, record, sources)
    return record


def _handoff_run_source_identity(
    handoff: HandoffV2,
    sources: HandoffSources,
) -> tuple[str | None, str | None]:
    """Derive the one canonical run source identity frozen by HANDOFF inputs.

    The only null compatibility case is an entirely legacy, public-synthetic
    run prefix.  A real or mixed run can never use that compatibility branch.
    """

    if handoff.handoff_scope != "run":
        return None, None
    run_records = [
        record
        for snapshot in sources.registry_snapshots
        for record in snapshot.records
        if record.run_id == handoff.run_id
    ]
    if not run_records:
        raise HandoffRegistrationError(
            "Run HANDOFF source snapshots contain no records for the HANDOFF run_id"
        )
    incomplete = [
        record.record_id
        for record in run_records
        if (record.source_platform is None) != (record.source_id is None)
    ]
    if incomplete:
        raise HandoffRegistrationError(
            f"Run HANDOFF source snapshots contain incomplete source identity: {incomplete}"
        )
    identities = {
        (record.source_platform, record.source_id)
        for record in run_records
        if record.source_platform is not None and record.source_id is not None
    }
    if len(identities) == 1:
        platform, source_id = identities.pop()
        return platform, source_id
    if len(identities) > 1:
        raise HandoffRegistrationError(
            "Run HANDOFF source snapshots contain conflicting canonical source identities"
        )
    if all(
        record.source_platform is None
        and record.source_id is None
        and record.privacy_class == "public_synthetic"
        and record.access_scope == "public"
        for record in run_records
    ):
        return None, None
    raise HandoffRegistrationError(
        "Run HANDOFF cannot derive one canonical source identity from its source snapshots"
    )


def validate_handoff_registry_record(
    publication: HandoffPublication,
    record: ArtifactRecordV1_1,
    sources: HandoffSources,
) -> ArtifactRecordV1_1:
    """Cross-check record3 against the two HANDOFF hash domains and source identity."""

    handoff = _validate_publication(publication, sources)
    manifest = validate_manifest(sources.manifest)
    expected_scope = "run" if handoff.handoff_scope == "run" else "cross_run"
    expected_type = (
        "handoff.run" if handoff.handoff_scope == "run" else "handoff.project"
    )
    source_platform, source_id = _handoff_run_source_identity(handoff, sources)
    expected_upstream = sorted(
        {
            reference.artifact_id
            for reference in (*handoff.input_artifacts, *handoff.output_artifacts)
        }
    )
    expected = {
        "registry_schema_version": "artifact_record_v1.1.0",
        "artifact_id": handoff.handoff_artifact_id,
        "artifact_type": expected_type,
        "container_kind": "file",
        "project_version": handoff.project_version,
        "storage_version": handoff.storage_version,
        "release_id": handoff.release_id,
        "source_platform": source_platform,
        "source_id": source_id,
        "run_id": handoff.run_id,
        "stage_id": handoff.stage_id,
        "dag_node_id": manifest.dag_node_id,
        "relative_path": publication.relative_path,
        "storage_scope": expected_scope,
        "media_type": "application/json",
        "size_bytes": publication.size_bytes,
        "content_hash_algorithm": "sha256",
        "content_hash": publication.file_hash,
        "semantic_hash_algorithm": "sha256",
        "semantic_hash": handoff.handoff_hash,
        "producer_type": "agent",
        "workflow_id": handoff.stage_id,
        "workflow_version": handoff.workflow_version,
        "schema_versions": ["artifact_record_v1.1.0", handoff.handoff_version],
        "prompt_version": handoff.prompt_version,
        "dag_version": handoff.dag_version,
        "code_commit": handoff.code_ref.git_commit,
        "upstream_artifact_ids": expected_upstream,
        "authority_level": "machine_derived",
        "lifecycle_state": "frozen",
        "validation_status": "passed",
    }
    for field, value in expected.items():
        if getattr(record, field) != value:
            raise HandoffRegistrationError(
                f"HANDOFF Registry record mismatch for {field}"
            )
    if (
        record.record_revision != 1
        or record.previous_record_id
        or record.previous_record_hash
    ):
        raise HandoffRegistrationError(
            "HANDOFF Artifact must be registered as a new revision-1 identity"
        )
    if record.created_at != _utc_datetime(handoff.created_at):
        raise HandoffRegistrationError(
            "HANDOFF Registry created_at must equal HANDOFF created_at"
        )
    if record.recorded_at < record.created_at:
        raise HandoffRegistrationError(
            "HANDOFF Registry record cannot predate HANDOFF creation"
        )
    if (
        record.validated_at != record.recorded_at
        or record.frozen_at != record.recorded_at
    ):
        raise HandoffRegistrationError(
            "HANDOFF Registry validation/freeze timestamps must equal recorded_at"
        )
    cross_identity = (
        record.batch_id,
        record.dataset_build_id,
        record.dataset_version,
        record.exp_id,
    )
    if expected_scope == "run" and any(value is not None for value in cross_identity):
        raise HandoffRegistrationError(
            "Run HANDOFF Registry record contains cross-run identity"
        )
    if expected_scope == "cross_run" and not any(
        value is not None for value in cross_identity
    ):
        raise HandoffRegistrationError(
            "Project HANDOFF Registry record lacks cross-run identity"
        )
    return record


def register_handoff(
    registry: AppendOnlyRegistry,
    publication: HandoffPublication,
    record: ArtifactRecordV1_1,
    sources: HandoffSources,
) -> HandoffRegistration:
    """Append record3 after an exact pre-head check; never truncate evidence on failure."""

    handoff = _validate_publication(publication, sources)
    validate_handoff_registry_record(publication, record, sources)
    root = sources.repository_root.resolve()
    try:
        registry_path = registry.path.resolve(strict=True)
        registry_relative_path = registry_path.relative_to(root).as_posix()
    except (OSError, ValueError) as exc:
        raise HandoffRegistrationError(
            "HANDOFF Registry is missing or outside repository_root"
        ) from exc
    validate_relative_path(registry_relative_path)
    expected_scope = "run" if handoff.handoff_scope == "run" else "cross_run"
    if registry.scope != expected_scope:
        raise HandoffRegistrationError(
            "HANDOFF scope does not match target Registry scope"
        )
    if expected_scope == "run" and registry.expected_run_id != handoff.run_id:
        raise HandoffRegistrationError("HANDOFF run_id does not match target Registry")
    matching_heads = [
        head
        for head in handoff.source_registry_heads
        if head.registry_scope == expected_scope
        and head.relative_path == registry_relative_path
    ]
    if len(matching_heads) != 1:
        raise HandoffRegistrationError(
            "HANDOFF must bind exactly one pre-registration head for its target Registry"
        )
    before_snapshot = snapshot_registry(
        registry_path,
        scope=registry.scope,
        expected_run_id=registry.expected_run_id,
        repository_root=root,
        relative_path=registry_relative_path,
    )
    before_head = RegistryHead.model_validate(before_snapshot.head())
    if before_head != matching_heads[0]:
        raise HandoffRegistrationError(
            "Target Registry changed after HANDOFF source head was fixed"
        )
    appended = False
    try:
        registry.append(record)
        appended = True
        after_snapshot = snapshot_registry(
            registry_path,
            scope=registry.scope,
            expected_run_id=registry.expected_run_id,
            repository_root=root,
            relative_path=registry_relative_path,
        )
        after_head = RegistryHead.model_validate(after_snapshot.head())
        if (
            after_head.record_count != before_head.record_count + 1
            or after_head.artifact_count != before_head.artifact_count + 1
            or after_head.head_record_id != record.record_id
            or after_head.head_record_hash != record.record_hash
        ):
            raise HandoffRegistrationError(
                "Registry post-head does not bind the appended HANDOFF record"
            )
        if (
            not after_snapshot.entries
            or after_snapshot.entries[-1].wire_record != record
        ):
            raise HandoffRegistrationError(
                "Registry read-back differs from the appended HANDOFF record"
            )
    except Exception as exc:
        if appended:
            raise HandoffRegistrationError(
                "HANDOFF record was appended but read-back validation failed; Registry evidence was preserved"
            ) from exc
        if isinstance(exc, HandoffRegistrationError):
            raise
        if isinstance(exc, RegistryValidationError):
            raise HandoffRegistrationError(str(exc)) from exc
        raise HandoffRegistrationError(
            f"HANDOFF Registry append failed: {exc}"
        ) from exc
    return HandoffRegistration(
        publication=publication,
        record=record,
        registry_path=registry_path,
        registry_relative_path=registry_relative_path,
        before_head=before_head,
        after_head=after_head,
    )


def validate_handoff_registration(
    registration: HandoffRegistration,
    sources: HandoffSources,
) -> HandoffRegistration:
    """Re-read and validate a completed Registry registration without rewriting it."""

    handoff = _validate_publication(registration.publication, sources)
    validate_handoff_registry_record(
        registration.publication, registration.record, sources
    )
    expected_pre_head = next(
        (
            head
            for head in handoff.source_registry_heads
            if head.registry_scope == registration.before_head.registry_scope
            and head.relative_path == registration.registry_relative_path
        ),
        None,
    )
    if expected_pre_head != registration.before_head:
        raise HandoffRegistrationError(
            "Registration pre-head differs from HANDOFF source head"
        )
    scope = registration.after_head.registry_scope
    expected_run_id = handoff.run_id if scope == "run" else None
    observed = snapshot_registry(
        registration.registry_path,
        scope=scope,
        expected_run_id=expected_run_id,
        repository_root=sources.repository_root,
        relative_path=registration.registry_relative_path,
    )
    if RegistryHead.model_validate(observed.head()) != registration.after_head:
        raise HandoffRegistrationError("Registered HANDOFF Registry head changed")
    if not observed.entries or observed.entries[-1].wire_record != registration.record:
        raise HandoffRegistrationError(
            "Registered HANDOFF record is no longer the bound Registry head"
        )
    return registration


def handoff_created_draft(
    registration: HandoffRegistration,
    *,
    actor: Mapping[str, Any],
    purpose: str = "bind immutable registered machine HANDOFF",
) -> dict[str, Any]:
    """Return the final post-terminal receipt draft for EventLog.append()."""

    publication = registration.publication
    handoff = publication.handoff
    record = registration.record
    return {
        "event_type": "handoff.created",
        "actor": dict(actor),
        "checkpoint_id": handoff.checkpoint_id,
        "artifact_refs": [
            {
                "artifact_id": record.artifact_id,
                "artifact_type": record.artifact_type,
                "record_id": record.record_id,
                "relative_path": record.relative_path,
                "content_hash_algorithm": record.content_hash_algorithm,
                "content_hash": record.content_hash,
                "input_fingerprint": record.input_fingerprint,
                "validation_status": record.validation_status,
                "lifecycle_state": record.lifecycle_state,
            }
        ],
        "path_refs": [
            {
                "relative_path": publication.relative_path,
                "content_hash_algorithm": "sha256",
                "content_hash": publication.file_hash,
                "purpose": purpose,
            }
        ],
        "payload": {
            "handoff_artifact_id": handoff.handoff_artifact_id,
            "record_id": record.record_id,
            "handoff_path": publication.relative_path,
            "handoff_hash": handoff.handoff_hash,
            "record_hash": record.record_hash,
        },
    }


def validate_handoff_created_event(
    registration: HandoffRegistration,
    event: Mapping[str, Any] | ExecutionEvent,
    sources: HandoffSources,
) -> ExecutionEvent:
    """Validate event9 against publication, record3 and the pre-HANDOFF event head."""

    validate_handoff_registration(registration, sources)
    validated = _validate_sources(sources)
    raw = (
        event.model_dump(mode="json")
        if isinstance(event, ExecutionEvent)
        else dict(event)
    )
    reject_sensitive_material(raw, location="handoff.created")
    try:
        validate_event_stream(
            [*validated.events, raw], validated.manifest, require_terminal=True
        )
        model = parse_execution_event(raw)
    except ExecutionContractError as exc:
        raise HandoffValidationError(
            f"Invalid handoff.created event stream: {exc}"
        ) from exc
    publication = registration.publication
    handoff = publication.handoff
    record = registration.record
    if model.event_type != "handoff.created":
        raise HandoffValidationError(
            "HANDOFF receipt must use event_type=handoff.created"
        )
    if model.sequence_no != handoff.source_event_head.sequence_no + 1:
        raise HandoffValidationError(
            "handoff.created must immediately follow checkpoint.created"
        )
    if _utc_datetime(model.occurred_at) < record.recorded_at:
        raise HandoffValidationError(
            "handoff.created cannot predate Registry registration"
        )
    if model.checkpoint_id != handoff.checkpoint_id:
        raise HandoffValidationError("handoff.created checkpoint_id mismatch")
    if len(model.artifact_refs) != 1 or len(model.path_refs) != 1:
        raise HandoffValidationError(
            "handoff.created requires exactly one Artifact ref and one path ref"
        )
    expected_payload = {
        "handoff_artifact_id": handoff.handoff_artifact_id,
        "record_id": record.record_id,
        "handoff_path": publication.relative_path,
        "handoff_hash": handoff.handoff_hash,
        "record_hash": record.record_hash,
    }
    if model.payload != expected_payload:
        raise HandoffValidationError(
            "handoff.created payload does not match publication/record3"
        )
    artifact = model.artifact_refs[0]
    expected_artifact = {
        "artifact_id": record.artifact_id,
        "artifact_type": record.artifact_type,
        "record_id": record.record_id,
        "relative_path": record.relative_path,
        "content_hash_algorithm": record.content_hash_algorithm,
        "content_hash": record.content_hash,
        "input_fingerprint": record.input_fingerprint,
        "validation_status": record.validation_status,
        "lifecycle_state": record.lifecycle_state,
    }
    if artifact.model_dump(mode="json") != expected_artifact:
        raise HandoffValidationError(
            "handoff.created Artifact ref does not match record3"
        )
    path = model.path_refs[0]
    if (
        path.relative_path != publication.relative_path
        or path.content_hash_algorithm != "sha256"
        or path.content_hash != publication.file_hash
    ):
        raise HandoffValidationError(
            "handoff.created path ref does not match HANDOFF file bytes"
        )
    return model


def render_handoff_markdown(handoff: HandoffV2 | Mapping[str, Any]) -> bytes:
    """Render the fixed human projection from HANDOFF JSON alone."""

    model = (
        parse_handoff(handoff.model_dump(mode="json"))
        if isinstance(handoff, HandoffV2)
        else parse_handoff(handoff)
    )
    lines = [
        "# Execution HANDOFF",
        "",
        "> Deterministic projection of the sibling `handoff.json`; the JSON file is authoritative.",
        "",
        "## 1. Identity and versions",
        "",
        (
            f"- Project/storage/release: {_md_code(model.project_version)} / "
            f"{_md_code(model.storage_version)} / {_md_code(model.release_id)}."
        ),
        (
            f"- Task/session/attempt: {_md_code(model.task_id)} / "
            f"{_md_code(model.session_id)} / {_md_code(str(model.attempt_no))}."
        ),
        (
            f"- Run/stage/status: {_md_code(model.run_id or 'none')} / "
            f"{_md_code(model.stage_id)} / {_md_code(model.status)}."
        ),
        (
            f"- Workflow/DAG/profile: {_md_code(model.workflow_version)} / "
            f"{_md_code(model.dag_version)} / {_md_code(model.agent_profile_version)}."
        ),
        (
            f"- HANDOFF Artifact/hash: {_md_code(model.handoff_artifact_id)} / "
            f"{_md_code(model.handoff_hash)}."
        ),
        f"- Render profile: {_md_code(model.render_profile_version)}.",
        "",
        "## 2. Objective, explicit inputs, and terminal result",
        "",
        (
            f"- Objective boundary: continue only task {_md_code(model.task_id)} "
            f"within stage {_md_code(model.stage_id)} and its fixed workflow."
        ),
        f"- Terminal result: {_md_code(model.status)}.",
        f"- Explicit input Artifact count: {_md_code(str(len(model.input_artifacts)))}.",
    ]
    lines.extend(_artifact_lines("Input", model.input_artifacts))
    lines.extend(
        [
            "",
            "## 3. Completed and remaining actions",
            "",
        ]
    )
    lines.extend(_action_lines("Completed", model.completed_actions))
    lines.extend(_action_lines("Remaining", model.remaining_actions))
    lines.extend(
        [
            (
                "- Human decisions required: "
                f"{_md_code(str(len(model.human_decisions_required)))}."
            ),
            "",
            "## 4. Artifact and validation boundary",
            "",
            f"- Output Artifact count: {_md_code(str(len(model.output_artifacts)))}.",
        ]
    )
    lines.extend(_artifact_lines("Output", model.output_artifacts))
    lines.extend(
        [
            (
                f"- Validation: {_md_code(model.validation_summary.overall_status)} "
                f"({_md_code(str(model.validation_summary.passed_count))} passed, "
                f"{_md_code(str(model.validation_summary.failed_count))} failed, "
                f"{_md_code(str(model.validation_summary.partial_count))} partial)."
            ),
            (
                "- Invalidated Artifact count: "
                f"{_md_code(str(len(model.invalidated_artifacts)))}."
            ),
            "",
            "## 5. Read/write audit summary",
            "",
            (
                f"- Declared read/write entries: {_md_code(str(len(model.declared_read_set)))} / "
                f"{_md_code(str(len(model.declared_write_set)))}."
            ),
            (
                f"- Actual read/write entries: {_md_code(str(model.metrics.actual_read_count))} / "
                f"{_md_code(str(model.metrics.actual_write_count))}."
            ),
            (
                "- Out-of-scope detections: "
                f"{_md_code(str(model.metrics.out_of_scope_detection_count))}."
            ),
            "",
            "## 6. Risk, privacy, evidence, and capability boundary",
            "",
            f"- Declared risk count: {_md_code(str(len(model.risks)))}.",
        ]
    )
    lines.extend(_risk_lines(model.risks))
    lines.extend(
        [
            (
                "- Privacy boundary: credentials, private absolute paths, long logs, and raw "
                "ASR/OCR bodies are forbidden in this control projection."
            ),
            (
                "- Evidence/capability boundary: this Markdown adds no fact beyond the "
                "validated machine HANDOFF."
            ),
            "",
            "## 7. Checkpoint and recovery anchor",
            "",
            f"- Checkpoint: {_md_code(model.checkpoint_id)}.",
            (
                f"- Source event head: {_md_code(model.source_event_head.event_id)} at sequence "
                f"{_md_code(str(model.source_event_head.sequence_no))}, SHA-256 "
                f"{_md_code(model.source_event_head.event_hash)}."
            ),
            (
                "- Pre-registration Registry head count: "
                f"{_md_code(str(len(model.source_registry_heads)))}."
            ),
        ]
    )
    lines.extend(_registry_head_lines(model.source_registry_heads))
    lines.extend(
        [
            "- Machine source: the sibling `handoff.json`.",
            "- Rebuildable files: `HANDOFF.md` and `current_state.json` are not recovery facts.",
            "",
            "## 8. Unique next action",
            "",
        ]
    )
    lines.extend(_next_action_lines(model.next_action))
    lines.extend(
        [
            "",
            "## 9. Minimal execution prompt",
            "",
            _minimal_prompt(model.next_action),
        ]
    )
    return ("\n".join(lines).rstrip("\n") + "\n").encode("utf-8")


def validate_handoff_markdown(
    handoff: HandoffV2 | Mapping[str, Any],
    path: Path,
) -> str:
    """Require exact projection bytes and return the expected Markdown SHA-256."""

    expected = render_handoff_markdown(handoff)
    try:
        observed = path.read_bytes()
    except OSError as exc:
        raise HandoffMarkdownDriftError(
            f"Cannot read HANDOFF Markdown projection: {path}"
        ) from exc
    if observed != expected:
        expected_hash = sha256_bytes(expected)
        observed_hash = sha256_bytes(observed)
        offset = _first_different_byte(expected, observed)
        raise HandoffMarkdownDriftError(
            "HANDOFF Markdown byte drift at offset "
            f"{offset}: expected {expected_hash}, observed {observed_hash}"
        )
    return sha256_bytes(expected)


def write_handoff_markdown(
    publication: HandoffPublication,
    sources: HandoffSources | None = None,
) -> HandoffMarkdownPublication:
    """Atomically replace the rebuildable sibling HANDOFF.md and verify exact bytes."""

    if sources is not None:
        model = _validate_publication(publication, sources)
    else:
        model = _validate_publication_bytes(publication)
    path = publication.path.with_name("HANDOFF.md")
    data = render_handoff_markdown(model)
    try:
        _atomic_replace_bytes(path, data)
        file_hash = validate_handoff_markdown(model, path)
    except HandoffValidationError:
        raise
    except OSError as exc:
        raise HandoffMarkdownDriftError(
            f"Cannot publish HANDOFF Markdown projection: {path}"
        ) from exc
    return HandoffMarkdownPublication(
        path=path,
        file_hash=file_hash,
        size_bytes=len(data),
    )


def _validate_sources(sources: HandoffSources) -> _ValidatedSources:
    manifest = validate_manifest(sources.manifest)
    checkpoint = (
        parse_checkpoint(sources.checkpoint.model_dump(mode="json"))
        if isinstance(sources.checkpoint, ExecutionCheckpoint)
        else parse_checkpoint(sources.checkpoint)
    )
    source_sequence = checkpoint.event_head.sequence_no + 1
    if source_sequence > len(sources.events):
        raise HandoffValidationError(
            "HANDOFF requires checkpoint.created after the terminal event"
        )
    prefix = sources.events[:source_sequence]
    validate_event_stream(prefix, manifest, require_terminal=True)
    events = tuple(
        item if isinstance(item, ExecutionEvent) else parse_execution_event(dict(item))
        for item in prefix
    )
    receipt = events[-1]
    if receipt.event_type != "checkpoint.created":
        raise HandoffValidationError("HANDOFF source head must be checkpoint.created")
    if len(receipt.path_refs) != 1:
        raise HandoffValidationError(
            "checkpoint.created must bind exactly one checkpoint file"
        )
    relative_path = validate_relative_path(
        receipt.path_refs[0].relative_path
    ).as_posix()
    root = sources.repository_root.resolve()
    checkpoint_path = (root / relative_path).resolve()
    if not checkpoint_path.is_relative_to(root):
        raise HandoffValidationError("Checkpoint receipt escapes the repository root")
    publication = CheckpointPublication(
        checkpoint=checkpoint,
        path=checkpoint_path,
        relative_path=relative_path,
        file_hash=receipt.path_refs[0].content_hash,
    )
    checkpoint_sources = CheckpointSources(
        repository_root=root,
        manifest=manifest,
        events=sources.events,
        terminal_state=sources.terminal_state,
        registry_snapshots=sources.registry_snapshots,
        dag_path=sources.dag_path,
        dag_relative_path=checkpoint.dag_ref.relative_path,
    )
    validate_checkpoint_created_event(publication, receipt, checkpoint_sources)
    validate_checkpoint(checkpoint, checkpoint_sources, path=checkpoint_path)
    source_state = build_current_state(
        manifest,
        events,
        sources.registry_snapshots,
        sources.dag_path,
    )
    if (
        source_state.get("event_count") != source_sequence
        or source_state.get("as_of_event_id") != receipt.event_id
        or source_state.get("event_head_hash") != receipt.event_hash
    ):
        raise HandoffValidationError(
            "HANDOFF source state does not bind the checkpoint.created event head"
        )
    source_head = {
        "event_id": receipt.event_id,
        "sequence_no": receipt.sequence_no,
        "event_hash": receipt.event_hash,
        "occurred_at": receipt.occurred_at,
    }
    registry_heads = sorted(
        (snapshot.head() for snapshot in sources.registry_snapshots),
        key=lambda item: (item["registry_scope"], item["relative_path"]),
    )
    return _ValidatedSources(
        manifest=manifest,
        events=events,
        checkpoint=checkpoint,
        source_event_head=source_head,
        registry_heads=registry_heads,
        terminal_state=source_state,
    )


def _handoff_raw(
    sources: _ValidatedSources,
    *,
    handoff_artifact_id: str,
    next_action: Mapping[str, Any],
    created_at: str,
    risks: Sequence[Mapping[str, Any]],
    render_profile_version: str,
    warehouse_projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest = sources.manifest
    checkpoint = sources.checkpoint
    state = sources.terminal_state
    human_requirements = sorted(
        (
            {
                "decision_artifact_id": item["decision_artifact_id"],
                "gate_node_id": item["gate_node_id"],
                "reason": item["summary"],
            }
            for item in state["pending_human_decisions"]
        ),
        key=lambda item: (item["decision_artifact_id"], item["gate_node_id"]),
    )
    validation = state["validation_summary"]
    out_of_scope_count = sum(
        event.event_type == "tool.failed"
        and str(event.payload.get("error_class", "")).split(".")[-1]
        == "ScopeViolationError"
        for event in sources.events
    )
    raw = {
        "handoff_version": (
            "handoff_v2.3.0"
            if manifest.session_manifest_version == "session_manifest_v1.3.0"
            else "handoff_v2.2.0"
            if manifest.session_manifest_version == "session_manifest_v1.2.0"
            else "handoff_v2.1.0"
            if manifest.session_manifest_version == "session_manifest_v1.1.0"
            else "handoff_v2.0.0"
        ),
        "handoff_artifact_id": handoff_artifact_id,
        "handoff_scope": "run" if manifest.task_scope == "run" else "project",
        "project_version": manifest.project_version,
        "storage_version": manifest.storage_version,
        "release_id": manifest.release_id,
        "task_id": manifest.task_id,
        "session_id": manifest.session_id,
        "attempt_no": manifest.attempt_no,
        "run_id": manifest.run_id,
        "stage_id": manifest.stage_id,
        "status": checkpoint.terminal_state,
        "parent_checkpoint_id": checkpoint.parent_checkpoint_id,
        "checkpoint_id": checkpoint.checkpoint_id,
        "agent_profile_version": manifest.agent_profile_version,
        "agent_runtime_version": manifest.agent_runtime_version,
        "workflow_version": manifest.workflow_version,
        "dag_version": manifest.dag_version,
        "schema_versions": list(manifest.schema_versions),
        "prompt_version": manifest.prompt_version,
        "code_ref": manifest.code_ref.model_dump(mode="json"),
        "source_event_head": sources.source_event_head,
        "source_registry_heads": sources.registry_heads,
        "input_artifacts": state["input_artifacts"],
        "output_artifacts": state["output_artifacts"],
        "declared_read_set": [
            item.model_dump(mode="json") for item in manifest.declared_read_set
        ],
        "declared_write_set": [
            item.model_dump(mode="json") for item in manifest.declared_write_set
        ],
        "actual_read_set": state["actual_read_set"],
        "actual_write_set": state["actual_write_set"],
        "invalidated_artifacts": state["invalidated_artifacts"],
        "completed_actions": state["completed_actions"],
        "remaining_actions": state["remaining_actions"],
        "risks": list(risks),
        "human_decisions_required": human_requirements,
        "validation_summary": validation,
        "metrics": {
            "event_count": sources.source_event_head["sequence_no"],
            "actual_read_count": len(state["actual_read_set"]),
            "actual_write_count": len(state["actual_write_set"]),
            "input_artifact_count": len(state["input_artifacts"]),
            "output_artifact_count": len(state["output_artifacts"]),
            "validation_passed_count": validation["passed_count"],
            "validation_failed_count": validation["failed_count"],
            "out_of_scope_detection_count": out_of_scope_count,
            "rebuild_hash_match": True,
        },
        "next_action": dict(next_action),
        "render_profile_version": render_profile_version,
        "created_at": created_at,
        "handoff_hash": "0" * 64,
    }
    if manifest.session_manifest_version == "session_manifest_v1.3.0":
        if warehouse_projection is None:
            raise HandoffValidationError(
                "session_manifest_v1.3.0 requires warehouse projection identity"
            )
        raw["warehouse_projection"] = dict(warehouse_projection)
    elif warehouse_projection is not None:
        raise HandoffValidationError(
            "warehouse projection identity is restricted to HANDOFF v2.3"
        )
    return raw


def _validate_next_action(
    handoff: HandoffV2,
    validated: _ValidatedSources,
    sources: HandoffSources,
) -> None:
    action = handoff.next_action
    decisions = {item.decision_artifact_id for item in handoff.human_decisions_required}
    if isinstance(action, WaitForHumanAction):
        if set(action.decision_artifact_ids) != decisions:
            raise HandoffValidationError(
                "wait_for_human must reference every and only pending decision Artifact"
            )
        return
    if decisions:
        raise HandoffValidationError(
            "A HANDOFF with pending decisions cannot bypass the human gate"
        )
    if isinstance(action, TerminateAction):
        if action.termination_kind == "terminal_failure" and handoff.status != "FAILED":
            raise HandoffValidationError("terminal_failure requires status=FAILED")
        if (
            action.termination_kind == "project_complete"
            and handoff.status != "COMPLETED"
        ):
            raise HandoffValidationError("project_complete requires status=COMPLETED")
        return
    current_stage = int(handoff.stage_id[1:])
    target_stage = (
        action.next_stage
        if isinstance(action, NextStageAction)
        else action.target_stage
    )
    target_number = int(target_stage[1:])
    if isinstance(action, NextStageAction) and target_number != current_stage + 1:
        raise HandoffValidationError(
            "next_stage must identify the single adjacent stage"
        )
    if isinstance(action, ReturnToStageAction) and target_number > current_stage:
        raise HandoffValidationError("return_to_stage cannot point forward")
    dag = load_dag(sources.dag_path)
    if not any(node.stage_id == target_stage for node in dag.nodes):
        raise HandoffValidationError(
            f"next action stage is absent from the fixed DAG: {target_stage}"
        )
    if isinstance(action, NextStageActionV2_1):
        resolver = getattr(dag, "workflow_version_for_stage", None)
        if resolver is None or resolver(target_stage) != action.target_workflow_version:
            raise HandoffValidationError(
                "next action target Workflow does not match the fixed DAG stage mapping"
            )
    for relative in (
        action.workflow_reference,
        action.prompt_reference,
        *action.required_read_paths,
    ):
        validate_relative_path(relative)
    stage_prefix = f"{int(target_stage[1:]):02d}_"
    for field, relative in (
        ("workflow_reference", action.workflow_reference),
        ("prompt_reference", action.prompt_reference),
    ):
        path = PurePosixPath(relative)
        if path.parent.as_posix() != "Optmize/workflows" or not path.name.startswith(
            stage_prefix
        ):
            raise HandoffValidationError(
                f"next action {field} is not the canonical {target_stage} workflow entry"
            )
    artifact_refs = {
        item.artifact_id: item
        for item in (*handoff.input_artifacts, *handoff.output_artifacts)
    }
    unknown_ids = set(action.required_input_artifact_ids) - artifact_refs.keys()
    if unknown_ids:
        raise HandoffValidationError(
            f"next action references unknown input Artifacts: {sorted(unknown_ids)}"
        )
    required_paths = set(action.required_read_paths)
    missing_paths = {
        artifact_refs[artifact_id].relative_path
        for artifact_id in action.required_input_artifact_ids
        if artifact_refs[artifact_id].relative_path not in required_paths
    }
    if missing_paths:
        raise HandoffValidationError(
            f"next action omits required Artifact paths: {sorted(missing_paths)}"
        )
    ready_candidates = [
        candidate
        for candidate in validated.terminal_state["candidate_next_nodes"]
        if candidate["status"] == "ready"
    ]
    target_candidates = [
        candidate
        for candidate in ready_candidates
        if candidate["stage_id"] == target_stage
    ]
    if not target_candidates:
        raise HandoffValidationError(
            f"next action target stage has no ready candidate in current_state: {target_stage}"
        )
    if isinstance(action, NextStageAction) and any(
        int(candidate["stage_id"][1:]) <= current_stage
        for candidate in ready_candidates
    ):
        raise HandoffValidationError(
            "next_stage cannot bypass a ready candidate in the current or an earlier stage"
        )
    nodes = {node.node_id: node for node in dag.nodes}
    selected_types = {
        artifact_refs[artifact_id].artifact_type
        for artifact_id in action.required_input_artifact_ids
    }
    unusable_ids = sorted(
        artifact_id
        for artifact_id in action.required_input_artifact_ids
        if artifact_refs[artifact_id].validation_status != "passed"
        or artifact_refs[artifact_id].lifecycle_state not in {"validated", "frozen"}
    )
    if unusable_ids:
        raise HandoffValidationError(
            f"next action requires invalid or non-frozen Artifacts: {unusable_ids}"
        )
    if not any(
        set(nodes[candidate["node_id"]].required_inputs).issubset(selected_types)
        for candidate in target_candidates
    ):
        raise HandoffValidationError(
            "next action selected Artifacts do not cover any ready target candidate inputs"
        )
    expected_read_paths = _expected_recovery_paths(handoff, validated)
    if set(action.required_read_paths) != expected_read_paths:
        missing = sorted(expected_read_paths - set(action.required_read_paths))
        extra = sorted(set(action.required_read_paths) - expected_read_paths)
        raise HandoffValidationError(
            "next action required read paths are not the exact recovery package: "
            f"missing={missing}, extra={extra}"
        )


def _validate_unique_collections(handoff: HandoffV2) -> None:
    collections: dict[str, Sequence[Any]] = {
        "schema_versions": handoff.schema_versions,
        "source_registry_heads": handoff.source_registry_heads,
        "input_artifacts": handoff.input_artifacts,
        "output_artifacts": handoff.output_artifacts,
        "declared_read_set": handoff.declared_read_set,
        "declared_write_set": handoff.declared_write_set,
        "actual_read_set": handoff.actual_read_set,
        "actual_write_set": handoff.actual_write_set,
        "invalidated_artifacts": handoff.invalidated_artifacts,
        "completed_actions": handoff.completed_actions,
        "remaining_actions": handoff.remaining_actions,
        "risks": handoff.risks,
        "human_decisions_required": handoff.human_decisions_required,
        "validators": handoff.validation_summary.validators,
    }
    for name, values in collections.items():
        if len(values) > MAX_HANDOFF_COLLECTION_ITEMS:
            raise HandoffValidationError(
                f"HANDOFF {name} exceeds {MAX_HANDOFF_COLLECTION_ITEMS} items"
            )
        encoded = [_canonical_item(item) for item in values]
        if len(encoded) != len(set(encoded)):
            raise HandoffValidationError(f"HANDOFF {name} values must be unique")
    keyed = {
        "source_registry_heads": [
            (item.registry_scope, item.relative_path)
            for item in handoff.source_registry_heads
        ],
        "completed_actions": [item.action_key for item in handoff.completed_actions],
        "remaining_actions": [item.action_key for item in handoff.remaining_actions],
        "risks": [item.risk_key for item in handoff.risks],
        "human_decisions_required": [
            (item.decision_artifact_id, item.gate_node_id)
            for item in handoff.human_decisions_required
        ],
    }
    for name, values in keyed.items():
        if len(values) != len(set(values)):
            raise HandoffValidationError(f"HANDOFF {name} identities must be unique")
    if isinstance(handoff.next_action, (NextStageAction, ReturnToStageAction)):
        _assert_strings_unique(
            handoff.next_action.required_input_artifact_ids,
            "next_action.required_input_artifact_ids",
        )
        _assert_strings_unique(
            handoff.next_action.required_read_paths,
            "next_action.required_read_paths",
        )
    elif isinstance(handoff.next_action, WaitForHumanAction):
        _assert_strings_unique(
            handoff.next_action.decision_artifact_ids,
            "next_action.decision_artifact_ids",
        )


def _validate_handoff_paths(handoff: HandoffV2) -> None:
    paths: list[str] = [item.relative_path for item in handoff.source_registry_heads]
    paths.extend(
        item.relative_path
        for collection in (
            handoff.input_artifacts,
            handoff.output_artifacts,
            handoff.actual_read_set,
            handoff.actual_write_set,
            handoff.invalidated_artifacts,
        )
        for item in collection
    )
    for scope in (*handoff.declared_read_set, *handoff.declared_write_set):
        relative = getattr(scope, "relative_path", None)
        if relative is not None:
            paths.append(relative)
    if handoff.code_ref.working_tree_manifest_path is not None:
        paths.append(handoff.code_ref.working_tree_manifest_path)
    if isinstance(handoff.next_action, (NextStageAction, ReturnToStageAction)):
        paths.extend(
            (
                handoff.next_action.workflow_reference,
                handoff.next_action.prompt_reference,
                *handoff.next_action.required_read_paths,
            )
        )
    for value in paths:
        validate_relative_path(value)


def _assert_strings_unique(values: Sequence[str], name: str) -> None:
    if len(values) > MAX_HANDOFF_COLLECTION_ITEMS:
        raise HandoffValidationError(
            f"HANDOFF {name} exceeds {MAX_HANDOFF_COLLECTION_ITEMS} items"
        )
    if len(values) != len(set(values)):
        raise HandoffValidationError(f"HANDOFF {name} values must be unique")


def _expected_recovery_paths(
    handoff: HandoffV2,
    sources: _ValidatedSources,
) -> set[str]:
    """Derive the exact file package needed to validate and rebuild the fixed source."""

    first = sources.events[0]
    receipt = sources.events[-1]
    manifest_path = validate_relative_path(
        str(first.payload["manifest_path"])
    ).as_posix()
    session_root = PurePosixPath(manifest_path).parent
    paths = {
        manifest_path,
        (session_root / "events.jsonl").as_posix(),
        (session_root / "handoff.json").as_posix(),
        validate_relative_path(str(receipt.payload["checkpoint_path"])).as_posix(),
        sources.checkpoint.dag_ref.relative_path,
        *(head.relative_path for head in handoff.source_registry_heads),
    }
    for reference in sources.manifest.bootstrap_refs:
        relative = getattr(reference, "relative_path", None)
        if relative is not None:
            paths.add(validate_relative_path(relative).as_posix())
    if sources.manifest.environment_ref.dependency_manifest_path is not None:
        paths.add(
            validate_relative_path(
                sources.manifest.environment_ref.dependency_manifest_path
            ).as_posix()
        )
    if handoff.handoff_version in {
        "handoff_v2.1.0",
        "handoff_v2.2.0",
        "handoff_v2.3.0",
    }:
        artifact_refs = {
            item.artifact_id: item
            for item in (*handoff.input_artifacts, *handoff.output_artifacts)
        }
        paths.update(
            artifact_refs[artifact_id].relative_path
            for artifact_id in handoff.next_action.required_input_artifact_ids
        )
    else:
        for collection in (
            handoff.input_artifacts,
            handoff.output_artifacts,
            handoff.invalidated_artifacts,
            handoff.actual_read_set,
            handoff.actual_write_set,
        ):
            paths.update(reference.relative_path for reference in collection)
    return paths


def _validate_publication(
    publication: HandoffPublication,
    sources: HandoffSources,
) -> HandoffV2:
    model = validate_handoff(publication.handoff, sources)
    root = sources.repository_root.resolve()
    path = publication.path.resolve()
    if path.name != "handoff.json" or not path.is_relative_to(root):
        raise HandoffValidationError(
            "HANDOFF publication path is outside repository_root"
        )
    relative = path.relative_to(root).as_posix()
    if relative != publication.relative_path:
        raise HandoffValidationError(
            "HANDOFF publication does not bind its repository-relative path"
        )
    if not path.is_file():
        raise HandoffValidationError("Published HANDOFF file is missing")
    observed_hash = sha256_file(path)
    observed_size = path.stat().st_size
    if (
        observed_hash != publication.file_hash
        or observed_size != publication.size_bytes
    ):
        raise HandoffValidationError(
            "Published HANDOFF bytes changed after publication"
        )
    read_back = read_handoff(path)
    if read_back.model_dump(mode="json") != model.model_dump(mode="json"):
        raise HandoffValidationError(
            "Published HANDOFF bytes do not match the bound object"
        )
    return model


def _validate_publication_bytes(publication: HandoffPublication) -> HandoffV2:
    path = publication.path.resolve()
    if path.name != "handoff.json" or not path.is_file():
        raise HandoffValidationError("Published HANDOFF file is missing or misnamed")
    if path.as_posix().endswith("/../handoff.json"):
        raise HandoffValidationError("Published HANDOFF path is not canonical")
    model = read_handoff(path)
    if model.model_dump(mode="json") != publication.handoff.model_dump(mode="json"):
        raise HandoffValidationError(
            "Published HANDOFF object differs from its bound bytes"
        )
    if (
        sha256_file(path) != publication.file_hash
        or path.stat().st_size != publication.size_bytes
    ):
        raise HandoffValidationError(
            "Published HANDOFF bytes changed after publication"
        )
    return model


def _artifact_lines(
    label: str,
    references: Sequence[RegisteredArtifactRef],
) -> list[str]:
    ordered = sorted(
        references, key=lambda item: (item.artifact_id, item.relative_path)
    )
    lines = [
        (
            f"- {label}: {_md_code(item.artifact_id)} ({_md_code(item.artifact_type)}) at "
            f"{_md_code(item.relative_path)}, SHA-256 {_md_code(item.content_hash)}, "
            f"validation {_md_code(item.validation_status)}."
        )
        for item in ordered[:5]
    ]
    if len(ordered) > 5:
        lines.append(
            f"- {label}: {_md_code(str(len(ordered) - 5))} additional items omitted."
        )
    if not lines:
        lines.append(f"- {label}: none.")
    return lines


def _action_lines(
    label: str,
    actions: Sequence[CompletedAction | RemainingAction],
) -> list[str]:
    lines = [
        f"- {label} {_md_code(item.action_key)}: {_md_text(item.summary)}."
        for item in actions[:5]
    ]
    if len(actions) > 5:
        lines.append(
            f"- {label}: {_md_code(str(len(actions) - 5))} additional actions omitted."
        )
    if not lines:
        lines.append(f"- {label}: none.")
    return lines


def _risk_lines(risks: Sequence[Risk]) -> list[str]:
    ordered = sorted(
        risks, key=lambda item: (not item.blocking, item.severity, item.risk_key)
    )
    lines = [
        (
            f"- Risk {_md_code(item.risk_key)} [{_md_code(item.severity)}; "
            f"blocking={_md_code(str(item.blocking).lower())}]: {_md_text(item.summary)}; "
            f"mitigation: {_md_text(item.mitigation)}."
        )
        for item in ordered[:5]
    ]
    if len(ordered) > 5:
        lines.append(
            f"- Risks: {_md_code(str(len(ordered) - 5))} additional items omitted."
        )
    if not lines:
        lines.append("- Risks: none.")
    return lines


def _registry_head_lines(heads: Sequence[RegistryHead]) -> list[str]:
    ordered = sorted(heads, key=lambda item: (item.registry_scope, item.relative_path))
    lines = [
        (
            f"- Registry {_md_code(item.registry_scope)} at {_md_code(item.relative_path)}: "
            f"records {_md_code(str(item.record_count))}, head "
            f"{_md_code(item.head_record_id or 'none')}, prefix SHA-256 "
            f"{_md_code(item.content_hash)}."
        )
        for item in ordered[:5]
    ]
    if len(ordered) > 5:
        lines.append(
            f"- Registries: {_md_code(str(len(ordered) - 5))} additional heads omitted."
        )
    return lines


def _next_action_lines(action: NextAction) -> list[str]:
    if isinstance(action, NextStageAction):
        lines = [
            f"- Action: enter adjacent stage {_md_code(action.next_stage)}.",
            f"- Workflow: {_md_code(action.workflow_reference)}.",
            f"- Prompt reference: {_md_code(action.prompt_reference)}.",
            f"- Reason: {_md_text(action.reason)}.",
        ]
        if isinstance(action, NextStageActionV2_1):
            lines.insert(
                2,
                f"- Target Workflow version: {_md_code(action.target_workflow_version)}.",
            )
            lines.insert(
                3,
                "- Execution authorized: "
                f"{_md_code(str(action.execution_authorized).lower())}.",
            )
        lines.extend(_required_action_lines(action))
        return lines
    if isinstance(action, ReturnToStageAction):
        lines = [
            f"- Action: return to stage {_md_code(action.target_stage)}.",
            f"- Workflow: {_md_code(action.workflow_reference)}.",
            f"- Prompt reference: {_md_code(action.prompt_reference)}.",
            f"- Reason: {_md_text(action.reason)}.",
        ]
        lines.extend(_required_action_lines(action))
        return lines
    if isinstance(action, WaitForHumanAction):
        return [
            "- Action: wait for human decision; do not cross the gate.",
            f"- Decision Artifacts: {_md_values(action.decision_artifact_ids)}.",
            f"- Reason: {_md_text(action.reason)}.",
        ]
    return [
        f"- Action: terminate with {_md_code(action.termination_kind)}.",
        f"- Reason: {_md_text(action.reason)}.",
    ]


def _required_action_lines(action: NextStageAction | ReturnToStageAction) -> list[str]:
    return [
        f"- Required input Artifacts: {_md_values(action.required_input_artifact_ids)}.",
        f"- Required read paths: {_md_values(action.required_read_paths)}.",
    ]


def _minimal_prompt(action: NextAction) -> str:
    prefix = (
        "Validate the sibling `handoff.json`, its checkpoint, source event head, Registry heads, "
        "and only the declared required paths. "
    )
    if isinstance(action, NextStageAction):
        return (
            prefix
            + f"Then execute only stage {_md_code(action.next_stage)} using "
            + f"{_md_values(action.required_input_artifact_ids)}; stop at any gate or validation failure."
        )
    if isinstance(action, ReturnToStageAction):
        return (
            prefix
            + f"Then return only to stage {_md_code(action.target_stage)} using "
            + f"{_md_values(action.required_input_artifact_ids)}; stop at any gate or validation failure."
        )
    if isinstance(action, WaitForHumanAction):
        return (
            prefix
            + "Do not continue execution; wait for exactly these decision Artifacts: "
            + f"{_md_values(action.decision_artifact_ids)}."
        )
    return prefix + "Preserve the terminal evidence and do not start another stage."


def _md_values(values: Sequence[str]) -> str:
    if not values:
        return "none"
    rendered = ", ".join(_md_code(value) for value in values[:5])
    if len(values) > 5:
        rendered += f", and {_md_code(str(len(values) - 5))} more"
    return rendered


def _md_text(value: str) -> str:
    escaped = html.escape(value, quote=True)
    for character in ("\\", "`", "*", "_", "{", "}", "[", "]", "#", "|"):
        escaped = escaped.replace(character, f"\\{character}")
    return escaped


def _md_code(value: str) -> str:
    escaped = html.escape(value, quote=True)
    longest = 0
    current = 0
    for character in escaped:
        if character == "`":
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    fence = "`" * max(1, longest + 1)
    padding = " " if escaped.startswith("`") or escaped.endswith("`") else ""
    return f"{fence}{padding}{escaped}{padding}{fence}"


def _atomic_replace_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _first_different_byte(expected: bytes, observed: bytes) -> int:
    for offset, (left, right) in enumerate(zip(expected, observed, strict=False)):
        if left != right:
            return offset
    return min(len(expected), len(observed))


def _write_path_is_declared(relative_path: str, manifest: SessionManifest) -> bool:
    target = PurePosixPath(relative_path)
    for entry in manifest.declared_write_set:
        raw = entry.model_dump(mode="json")
        if raw["scope_type"] == "artifact":
            continue
        scope_path = validate_relative_path(raw["relative_path"])
        if raw["scope_type"] == "path" and target == scope_path:
            return True
        if raw["scope_type"] == "path_prefix" and (
            target == scope_path
            or raw["recursive"] is True
            and target.is_relative_to(scope_path)
            or raw["recursive"] is False
            and target.parent == scope_path
        ):
            return True
    return False


def _validate_handoff_resource_budget(value: Mapping[str, Any]) -> None:
    """Bound parser work before recursive security and schema validation."""

    node_count = 0
    active_containers: set[int] = set()

    def walk(item: Any, *, depth: int) -> None:
        nonlocal node_count
        node_count += 1
        if node_count > MAX_HANDOFF_NODES:
            raise HandoffValidationError(
                f"HANDOFF exceeds the {MAX_HANDOFF_NODES}-node parsing budget"
            )
        if depth > MAX_HANDOFF_DEPTH:
            raise HandoffValidationError(
                f"HANDOFF exceeds the maximum nesting depth {MAX_HANDOFF_DEPTH}"
            )
        if isinstance(item, Mapping):
            if len(item) > MAX_HANDOFF_COLLECTION_ITEMS:
                raise HandoffValidationError(
                    f"HANDOFF object exceeds {MAX_HANDOFF_COLLECTION_ITEMS} members"
                )
            identity = id(item)
            if identity in active_containers:
                raise HandoffValidationError("HANDOFF contains a cyclic object graph")
            active_containers.add(identity)
            try:
                for key, child in item.items():
                    if not isinstance(key, str):
                        raise HandoffValidationError(
                            "HANDOFF object keys must be strings"
                        )
                    walk(child, depth=depth + 1)
            finally:
                active_containers.remove(identity)
        elif isinstance(item, (list, tuple)):
            if len(item) > MAX_HANDOFF_COLLECTION_ITEMS:
                raise HandoffValidationError(
                    f"HANDOFF array exceeds {MAX_HANDOFF_COLLECTION_ITEMS} items"
                )
            identity = id(item)
            if identity in active_containers:
                raise HandoffValidationError("HANDOFF contains a cyclic object graph")
            active_containers.add(identity)
            try:
                for child in item:
                    walk(child, depth=depth + 1)
            finally:
                active_containers.remove(identity)

    walk(value, depth=0)
    try:
        size = len(canonical_json_bytes(value)) + 1
    except (TypeError, ValueError) as exc:
        raise HandoffValidationError(
            f"HANDOFF is not canonical JSON data: {exc}"
        ) from exc
    if size > MAX_HANDOFF_BYTES:
        raise HandoffValidationError(
            f"HANDOFF exceeds the {MAX_HANDOFF_BYTES}-byte canonical JSON budget"
        )


def _reject_unsafe_handoff_text(value: Any, *, location: str = "handoff") -> None:
    forbidden = {"\r", "\n", "\x00", "\u2028", "\u2029"}
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_unsafe_handoff_text(child, location=f"{location}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _reject_unsafe_handoff_text(child, location=f"{location}[{index}]")
    elif isinstance(value, str) and any(character in value for character in forbidden):
        raise HandoffValidationError(
            f"Unsafe line/control character in HANDOFF text at {location}"
        )


def _dump(value: Any) -> dict[str, Any]:
    if isinstance(value, StrictFrozenModel):
        return value.model_dump(mode="json")
    return dict(value)


def _canonical_item(value: Any) -> str:
    if isinstance(value, StrictFrozenModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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


def _matches(pattern: str, value: str) -> bool:
    import re

    return re.fullmatch(pattern, value) is not None
