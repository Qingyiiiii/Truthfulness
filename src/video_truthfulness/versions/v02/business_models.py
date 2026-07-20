"""Strict Stage 5 V02 business and private-control contracts.

The models in this module deliberately have no media, network, model-runtime, or
Registry side effects.  They are the runtime peers of the Stage 5 JSON Schemas.
"""

from __future__ import annotations

import hashlib
import json
import wave
from datetime import datetime, timezone
from io import BytesIO
from pathlib import PurePosixPath
from typing import Annotated, Any, Iterable, Literal, Mapping, TypeAlias

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from video_truthfulness.core.artifacts.models import (
    ArtifactRecordView,
    ArtifactRecordWire,
    parse_artifact_record,
    to_artifact_record_view,
)
from video_truthfulness.core.execution.hashing import embedded_hash
from video_truthfulness.core.execution.models import (
    ArtifactRef,
    BootstrapRef,
    CodeRef,
    EnvironmentRef,
    ExecutionSchemaError,
    HumanGatePolicy,
    PathRef,
)


SHA256 = r"^[0-9a-f]{64}$"
TASK_ID = r"^task_[0-9a-hjkmnp-tv-z]{26}$"
SESSION_ID = r"^session_[0-9a-hjkmnp-tv-z]{26}$"
RUN_ID = r"^run_[0-9a-hjkmnp-tv-z]{26}$"
ARTIFACT_ID = r"^artifact_[0-9a-hjkmnp-tv-z]{26}$"
RECORD_ID = r"^record_[0-9a-hjkmnp-tv-z]{26}$"
EVENT_ID = r"^event_[0-9a-hjkmnp-tv-z]{26}$"
CHECKPOINT_ID = r"^checkpoint_[0-9a-hjkmnp-tv-z]{26}$"
RECEIPT_ID = r"^receipt_[0-9a-hjkmnp-tv-z]{26}$"
UTC_TIMESTAMP = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z$"
NODE_ID = r"^[a-z][a-z0-9_]*$"

BusinessArtifactType = Literal[
    "acquisition.decision",
    "transcript.path_decision",
    "media.audio",
    "transcript.raw",
    "transcript.normalized",
    "transcript.alignment",
    "ocr.gate_decision",
    "ocr.result",
    "claim.collection",
    "claim.entity_index",
    "claim.atomic_collection",
    "evidence.collection",
    "evidence.entity_index",
    "evidence.merged_collection",
    "verdict.collection",
    "verdict.entity_index",
    "verdict.rebuilt_collection",
    "report.machine",
    "report.rebuilt",
    "source_depth.decision",
    "source_depth.prompt",
    "source_depth.result",
    "source_depth.import_validation",
    "screening.sync_record",
]
OcrGateState = Literal["NOT_APPLICABLE", "REQUIRED_BLOCKED", "EXECUTED"]
SourceDepthBranchState = Literal[
    "NO_DEPTH_COMPLETED",
    "DEPTH_WAITING",
    "DEPTH_CAPTURED_WAITING_G3",
    "DEPTH_IMPORTED_COMPLETED",
    "DEPTH_EXTERNAL_EMPTY_FAILED",
]
SourceDepthControlTerminal = Literal["COMPLETED", "WAITING_FOR_HUMAN", "FAILED"]
SourceDepthControlAction = Literal[
    "next_stage", "wait_for_human", "return_to_stage", "terminate"
]


class Stage5ContractError(ValueError):
    """Raised when a Stage 5 private or business contract is invalid."""


class ManualExternalInputError(Stage5ContractError):
    """Raised for an invalid result-ready signal or manual source binding."""


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class CheckpointRepairFile(StrictModel):
    relative_path: str = Field(min_length=1, max_length=512)
    before_sha256: str = Field(pattern=SHA256)
    after_sha256: str = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)

    @model_validator(mode="after")
    def _changed(self) -> "CheckpointRepairFile":
        if self.before_sha256 == self.after_sha256:
            raise ValueError("checkpoint repair file must record a content change")
        return self


class CheckpointRepairTestEvidence(StrictModel):
    command: str = Field(min_length=1, max_length=2000)
    passed_count: int = Field(ge=1)
    failed_count: Literal[0]
    error_count: Literal[0]
    warning_count: int = Field(ge=0)
    completed_at: str = Field(pattern=UTC_TIMESTAMP)

    @field_validator("completed_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return validate_utc_timestamp(value)


class CheckpointRecoveryReceipt(StrictModel):
    checkpoint_recovery_receipt_version: Literal["checkpoint_recovery_receipt_v1.0.0"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    checkpoint_id: str = Field(pattern=CHECKPOINT_ID)
    plan_checkpoint_created_at: str = Field(pattern=UTC_TIMESTAMP)
    actual_checkpoint_created_at: str = Field(pattern=UTC_TIMESTAMP)
    failure_class: Literal["linux_name_max_on_registry_artifact_path"]
    failed_errno: Literal[36]
    failed_path_kind: Literal["registry_authoritative_media_path"]
    failed_artifact_id: str = Field(pattern=ARTIFACT_ID)
    failed_relative_path: str = Field(min_length=1, max_length=512)
    failure_summary: str = Field(min_length=1, max_length=500)
    source_terminal_event_id: str = Field(pattern=EVENT_ID)
    source_terminal_event_hash: str = Field(pattern=SHA256)
    source_event_count: int = Field(ge=1)
    plan_file_sha256: str = Field(pattern=SHA256)
    working_tree_manifest_file_sha256: str = Field(pattern=SHA256)
    registry_file_sha256: str = Field(pattern=SHA256)
    dag_snapshot_sha256: str = Field(pattern=SHA256)
    media_content_reread: Literal[False]
    registry_mutation: Literal[False]
    repair_files: list[CheckpointRepairFile] = Field(min_length=1)
    test_evidence: list[CheckpointRepairTestEvidence] = Field(min_length=1)
    receipt_hash: str = Field(pattern=SHA256)

    @field_validator("plan_checkpoint_created_at", "actual_checkpoint_created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @field_validator("failed_relative_path")
    @classmethod
    def _failed_path(cls, value: str) -> str:
        return validate_relative_path(value)

    @model_validator(mode="after")
    def _provenance(self) -> "CheckpointRecoveryReceipt":
        planned = datetime.fromisoformat(
            self.plan_checkpoint_created_at.removesuffix("Z") + "+00:00"
        )
        actual = datetime.fromisoformat(
            self.actual_checkpoint_created_at.removesuffix("Z") + "+00:00"
        )
        if actual < planned:
            raise ValueError("checkpoint recovery cannot predate the frozen time")
        _unique(
            "checkpoint repair file paths",
            [item.relative_path for item in self.repair_files],
        )
        if any(
            datetime.fromisoformat(item.completed_at.removesuffix("Z") + "+00:00")
            > actual
            for item in self.test_evidence
        ):
            raise ValueError("checkpoint repair test evidence cannot postdate recovery")
        if self.receipt_hash != embedded_hash(
            self.model_dump(mode="json"), "receipt_hash"
        ):
            raise ValueError("checkpoint recovery receipt_hash mismatch")
        return self


class PreviousCheckpointRecoveryReceipt(StrictModel):
    relative_path: str = Field(min_length=1, max_length=512)
    receipt_version: Literal["checkpoint_recovery_receipt_v1.0.0"]
    file_sha256: str = Field(pattern=SHA256)
    receipt_hash: str = Field(pattern=SHA256)
    actual_checkpoint_created_at: str = Field(pattern=UTC_TIMESTAMP)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)

    @field_validator("actual_checkpoint_created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return validate_utc_timestamp(value)


class FailedCheckpointRecoveryAttempt(StrictModel):
    failed_at: str = Field(pattern=UTC_TIMESTAMP)
    failure_class: Literal["checkpoint_dag_bootstrap_validation_conflict"]
    error_message: Literal["DAG bootstrap ref does not match dag_ref"]
    checkpoint_created: Literal[False]
    checkpoint_event_appended: Literal[False]
    media_content_reread: Literal[False]
    source_event_count: int = Field(ge=1)
    source_terminal_event_id: str = Field(pattern=EVENT_ID)
    source_terminal_event_hash: str = Field(pattern=SHA256)

    @field_validator("failed_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return validate_utc_timestamp(value)


class CheckpointRecoveryReceiptV1_1(CheckpointRecoveryReceipt):
    checkpoint_recovery_receipt_version: Literal["checkpoint_recovery_receipt_v1.1.0"]
    previous_receipt: PreviousCheckpointRecoveryReceipt
    prior_failed_attempt: FailedCheckpointRecoveryAttempt

    @model_validator(mode="after")
    def _successor_order(self) -> "CheckpointRecoveryReceiptV1_1":
        previous = datetime.fromisoformat(
            self.previous_receipt.actual_checkpoint_created_at.removesuffix("Z")
            + "+00:00"
        )
        failed = datetime.fromisoformat(
            self.prior_failed_attempt.failed_at.removesuffix("Z") + "+00:00"
        )
        actual = datetime.fromisoformat(
            self.actual_checkpoint_created_at.removesuffix("Z") + "+00:00"
        )
        if not previous <= failed <= actual:
            raise ValueError(
                "checkpoint recovery successor timestamps are out of order"
            )
        if (
            self.prior_failed_attempt.source_event_count != self.source_event_count
            or self.prior_failed_attempt.source_terminal_event_id
            != self.source_terminal_event_id
            or self.prior_failed_attempt.source_terminal_event_hash
            != self.source_terminal_event_hash
        ):
            raise ValueError(
                "checkpoint recovery successor must preserve the failed attempt Event head"
            )
        return self


def validate_utc_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ValueError("timestamp must be a real UTC instant") from exc
    if not value.endswith("Z") or parsed.tzinfo != timezone.utc:
        raise ValueError("timestamp must use a UTC Z suffix")
    return value


def validate_relative_path(value: str) -> str:
    if not value or value in {".", ".."} or "\\" in value:
        raise ValueError("path must be a non-empty repository-relative POSIX path")
    if value.startswith(("/", "~")) or (len(value) >= 2 and value[1] == ":"):
        raise ValueError("absolute paths are forbidden")
    if any(marker in value for marker in ("*", "?", "$", "%")):
        raise ValueError("wildcards and unresolved variables are forbidden")
    path = PurePosixPath(value)
    if any(part in {"", ".", "..", "latest"} for part in path.parts):
        raise ValueError("escaping, dot, and implicit-latest paths are forbidden")
    return path.as_posix()


def _unique(label: str, values: list[str]) -> list[str]:
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")
    return values


def _json_size(value: Any) -> int:
    return len(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    )


def _validate_bounded_json(value: Any, *, depth: int = 0) -> None:
    if depth > 12:
        raise ValueError("business payload nesting exceeds 12 levels")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise ValueError("business payload object is too wide")
        for key, child in value.items():
            if not isinstance(key, str) or not key or len(key) > 160:
                raise ValueError("business payload keys must be bounded strings")
            _validate_bounded_json(child, depth=depth + 1)
    elif isinstance(value, list):
        if len(value) > 10000:
            raise ValueError("business payload array is too long")
        for child in value:
            _validate_bounded_json(child, depth=depth + 1)
    elif isinstance(value, str) and len(value) > 1_000_000:
        raise ValueError("individual business payload text is too large")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError("business payload must be JSON-compatible")


class FileBinding(StrictModel):
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    size_bytes: int = Field(ge=0)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)


class PlanReadBinding(StrictModel):
    """One exact plan read, either pre-frozen or materialized once after G2."""

    relative_path: str = Field(min_length=1, max_length=512)
    binding_mode: Literal["frozen_hash", "materialize_once"]
    content_hash_algorithm: Literal["sha256"]
    content_hash: str | None = Field(pattern=SHA256)
    size_bytes: int | None = Field(ge=0)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)

    @model_validator(mode="after")
    def _binding_mode(self) -> "PlanReadBinding":
        if self.binding_mode == "frozen_hash":
            if self.content_hash is None or self.size_bytes is None:
                raise ValueError("frozen_hash read requires exact hash and size")
        elif self.content_hash is not None or self.size_bytes is not None:
            raise ValueError("materialize_once read requires null hash and size")
        return self


class ReceiptBoundAccessPolicy(StrictModel):
    content_read: Literal["single_sequential_sha256", "tool_managed"]
    decode_allowed: bool

    @model_validator(mode="after")
    def _single_hash_never_decodes(self) -> "ReceiptBoundAccessPolicy":
        if self.content_read == "single_sequential_sha256" and self.decode_allowed:
            raise ValueError("single sequential SHA-256 access cannot authorize decode")
        return self


class ReceiptBoundArtifactReadBinding(StrictModel):
    """One Registry Artifact joined to the receipt for the opened cache bytes."""

    binding_kind: Literal["receipt_bound_artifact"]
    artifact_ref: ArtifactRef
    receipt_path_ref: PathRef
    required_receipt_version: Literal[
        "input_materialization_v1.0.0",
        "input_materialization_v1.1.0",
    ]
    receipt_semantic_hash: str = Field(pattern=SHA256)
    required_storage_root_ref: Literal["ubuntu_native_materialized_v02"]
    access_policy: ReceiptBoundAccessPolicy

    @model_validator(mode="after")
    def _media_identity(self) -> "ReceiptBoundArtifactReadBinding":
        artifact = self.artifact_ref
        if artifact.artifact_type != "media.video" or artifact.record_id is None:
            raise ValueError("receipt-bound input must be a registered media.video")
        if artifact.validation_status != "passed" or artifact.lifecycle_state not in {
            "validated",
            "frozen",
        }:
            raise ValueError("receipt-bound media must remain passed and validated")
        validate_relative_path(artifact.relative_path)
        validate_relative_path(self.receipt_path_ref.relative_path)
        return self


class WriteBinding(StrictModel):
    """Freeze one exact output path and the only mutation mode allowed for it."""

    relative_path: str = Field(min_length=1, max_length=512)
    write_mode: Literal[
        "create_new",
        "append_only_expected_head",
        "atomic_replace_expected_hash",
    ]
    expected_content_hash: str | None = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)

    @model_validator(mode="after")
    def _mode_binding(self) -> "WriteBinding":
        if self.write_mode == "atomic_replace_expected_hash":
            if self.expected_content_hash is None:
                raise ValueError(
                    "atomic replace requires an exact expected content hash"
                )
        elif self.expected_content_hash is not None:
            raise ValueError("only atomic replace may carry expected_content_hash")
        return self


class RegistryHeadBinding(StrictModel):
    relative_path: str = Field(min_length=1, max_length=512)
    record_count: int = Field(ge=1)
    head_record_id: str = Field(pattern=RECORD_ID)
    head_record_hash: str = Field(pattern=SHA256)
    file_hash: str = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)


class TelemetryPlan(StrictModel):
    config_path: str = Field(min_length=1, max_length=512)
    config_hash: str = Field(pattern=SHA256)
    ledger_path: str = Field(min_length=1, max_length=512)
    summary_path: str = Field(min_length=1, max_length=512)
    required: Literal[True]

    @field_validator("config_path", "ledger_path", "summary_path")
    @classmethod
    def _paths(cls, value: str) -> str:
        return validate_relative_path(value)


class ContractFileRef(StrictModel):
    relative_path: str = Field(min_length=1, max_length=512)
    version: str = Field(min_length=1, max_length=160)
    content_hash: str = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)


class Stage5ContractFiles(StrictModel):
    workflow: ContractFileRef
    dag: ContractFileRef
    prompt: ContractFileRef
    agent_profile: ContractFileRef

    @model_validator(mode="after")
    def _unique_paths(self) -> "Stage5ContractFiles":
        _unique(
            "contract file paths",
            [
                self.workflow.relative_path,
                self.dag.relative_path,
                self.prompt.relative_path,
                self.agent_profile.relative_path,
            ],
        )
        return self


class NodeExecutionPolicy(StrictModel):
    execution_kind: Literal[
        "non_model", "interceptable_model", "manual_external_capture"
    ]
    required_gate: Literal["G0B", "G1A", "G1B", "G2", "G3"]
    expected_artifact_types: list[BusinessArtifactType]
    objective: str = Field(min_length=1, max_length=500)
    agent_profile_version: str = Field(
        pattern=r"^[a-z][a-z0-9_]*_agent_v[0-9]+\.[0-9]+\.[0-9]+$"
    )
    agent_runtime_version: str = Field(min_length=1, max_length=120)
    prompt_version: str = Field(
        pattern=r"^[a-z][a-z0-9_]*_prompt_v[0-9]+\.[0-9]+\.[0-9]+$"
    )
    contract_files: Stage5ContractFiles

    @field_validator("expected_artifact_types")
    @classmethod
    def _artifact_types(cls, values: list[str]) -> list[str]:
        return _unique("expected_artifact_types", values)


class SessionControlPlan(StrictModel):
    parent_checkpoint_id: str | None = Field(
        pattern=r"^checkpoint_[0-9a-hjkmnp-tv-z]{26}$"
    )
    bootstrap_refs: list[BootstrapRef]
    code_ref: CodeRef
    environment_ref: EnvironmentRef
    human_gate_policy: HumanGatePolicy
    session_created_at: str = Field(pattern=UTC_TIMESTAMP)

    @field_validator("session_created_at")
    @classmethod
    def _created_at(cls, value: str) -> str:
        return validate_utc_timestamp(value)


class InputBindingControlFinalizationPlan(StrictModel):
    mode: Literal["input_binding_no_handoff"]
    successor_receipt_path: str = Field(min_length=1, max_length=512)
    dag_source_path: str = Field(min_length=1, max_length=512)
    dag_snapshot_path: str = Field(min_length=1, max_length=512)
    checkpoint_id: str = Field(pattern=r"^checkpoint_[0-9a-hjkmnp-tv-z]{26}$")
    checkpoint_path: str = Field(min_length=1, max_length=512)
    terminal_at: str = Field(pattern=UTC_TIMESTAMP)
    checkpoint_created_at: str = Field(pattern=UTC_TIMESTAMP)

    @field_validator(
        "successor_receipt_path",
        "dag_source_path",
        "dag_snapshot_path",
        "checkpoint_path",
    )
    @classmethod
    def _paths(cls, value: str) -> str:
        return validate_relative_path(value)

    @model_validator(mode="after")
    def _time_order(self) -> "InputBindingControlFinalizationPlan":
        terminal = datetime.fromisoformat(self.terminal_at.removesuffix("Z") + "+00:00")
        checkpoint = datetime.fromisoformat(
            self.checkpoint_created_at.removesuffix("Z") + "+00:00"
        )
        if checkpoint < terminal:
            raise ValueError("checkpoint creation cannot predate the terminal Event")
        return self


class Stage5PublicationPlan(StrictModel):
    result_artifact_id: str = Field(pattern=ARTIFACT_ID)
    result_record_id: str = Field(pattern=RECORD_ID)
    result_ready_receipt_id: str = Field(pattern=RECEIPT_ID)
    materialization_receipt_id: str = Field(pattern=RECEIPT_ID)
    publication_receipt_path: str = Field(min_length=1, max_length=512)
    registration_manifest_path: str = Field(min_length=1, max_length=512)
    registry_path: str = Field(min_length=1, max_length=512)
    dag_source_path: str = Field(min_length=1, max_length=512)
    dag_snapshot_path: str = Field(min_length=1, max_length=512)
    checkpoint_id: str = Field(pattern=r"^checkpoint_[0-9a-hjkmnp-tv-z]{26}$")
    checkpoint_path: str = Field(min_length=1, max_length=512)
    handoff_artifact_id: str = Field(pattern=ARTIFACT_ID)
    handoff_record_id: str = Field(pattern=RECORD_ID)
    handoff_path: str = Field(min_length=1, max_length=512)
    handoff_markdown_path: str = Field(min_length=1, max_length=512)
    recovery_workflow_path: str = Field(
        min_length=1,
        max_length=512,
        pattern=r"^Optmize/workflows/02_[^/]+\.md$",
    )
    recovery_prompt_path: str = Field(
        min_length=1,
        max_length=512,
        pattern=r"^Optmize/workflows/02_[^/]+\.md$",
    )
    result_recorded_at: str = Field(pattern=UTC_TIMESTAMP)
    checkpoint_created_at: str = Field(pattern=UTC_TIMESTAMP)
    handoff_created_at: str = Field(pattern=UTC_TIMESTAMP)
    handoff_recorded_at: str = Field(pattern=UTC_TIMESTAMP)

    @field_validator(
        "publication_receipt_path",
        "registration_manifest_path",
        "registry_path",
        "dag_source_path",
        "dag_snapshot_path",
        "checkpoint_path",
        "handoff_path",
        "handoff_markdown_path",
        "recovery_workflow_path",
        "recovery_prompt_path",
    )
    @classmethod
    def _paths(cls, value: str) -> str:
        return validate_relative_path(value)

    @field_validator(
        "result_recorded_at",
        "checkpoint_created_at",
        "handoff_created_at",
        "handoff_recorded_at",
    )
    @classmethod
    def _times(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @model_validator(mode="after")
    def _identity_and_time_order(self) -> "Stage5PublicationPlan":
        if self.result_artifact_id == self.handoff_artifact_id:
            raise ValueError("result and HANDOFF Artifact IDs must be distinct")
        if self.result_record_id == self.handoff_record_id:
            raise ValueError("result and HANDOFF record IDs must be distinct")
        if self.result_ready_receipt_id == self.materialization_receipt_id:
            raise ValueError("manual input receipt IDs must be distinct")
        ordered = [
            datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
            for value in (
                self.result_recorded_at,
                self.checkpoint_created_at,
                self.handoff_created_at,
                self.handoff_recorded_at,
            )
        ]
        if ordered != sorted(ordered):
            raise ValueError("publication timestamps must be monotonically ordered")
        return self


class ManualInputPolicy(StrictModel):
    source_depth_request_id: str = Field(
        pattern=r"^source_depth_request_[0-9a-hjkmnp-tv-z]{26}$"
    )
    prompt_artifact_id: str = Field(pattern=ARTIFACT_ID)
    target_claim_ids: list[str] = Field(min_length=1)
    prompt_created_at: str = Field(pattern=UTC_TIMESTAMP)
    inbox_directory: str = Field(min_length=1, max_length=512)
    source_input_path: str = Field(min_length=1, max_length=512)
    result_output_path: str = Field(min_length=1, max_length=512)
    result_ready_receipt_path: str = Field(min_length=1, max_length=512)
    materialization_receipt_path: str = Field(min_length=1, max_length=512)
    allowed_extensions: list[Literal[".json", ".md", ".txt"]] = Field(
        min_length=1, max_length=3
    )
    max_size_bytes: Literal[20971520]

    @field_validator(
        "inbox_directory",
        "source_input_path",
        "result_output_path",
        "result_ready_receipt_path",
        "materialization_receipt_path",
    )
    @classmethod
    def _paths(cls, value: str) -> str:
        return validate_relative_path(value)

    @field_validator("prompt_created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @field_validator("allowed_extensions")
    @classmethod
    def _extensions(cls, value: list[str]) -> list[str]:
        return _unique("allowed_extensions", value)

    @field_validator("target_claim_ids")
    @classmethod
    def _target_claims(cls, values: list[str]) -> list[str]:
        import re

        if any(
            re.fullmatch(r"^claim_[0-9a-hjkmnp-tv-z]{26}$", item) is None
            for item in values
        ):
            raise ValueError("target_claim_ids must contain canonical claim IDs")
        return _unique("target_claim_ids", values)

    @model_validator(mode="after")
    def _scope(self) -> "ManualInputPolicy":
        expected = f"source_depth/inbox/{self.prompt_artifact_id}"
        if not self.inbox_directory.endswith(expected):
            raise ValueError("inbox_directory must bind the exact prompt Artifact")
        return self


class Stage5ExecutionPlan(StrictModel):
    plan_version: Literal["stage5_execution_plan_v1.0.0"]
    project_version: Literal["v0.2"]
    storage_version: Literal["V02"]
    release_id: Literal["truthfulness_v0.2_youtube_video"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str = Field(pattern=RUN_ID)
    stage_id: Literal["S01", "S02"]
    node_id: str = Field(pattern=NODE_ID)
    workflow_version: Literal[
        "youtube_truthfulness_workflow_v1.1.0",
        "youtube_truthfulness_workflow_v1.3.0",
    ]
    dag_version: Literal["youtube_truthfulness_dag_v1.2.0"]
    repository_root_ref: Literal["repository"]
    task_directory: str = Field(min_length=1, max_length=512)
    session_directory: str = Field(min_length=1, max_length=512)
    read_paths: list[str]
    read_bindings: list[PlanReadBinding]
    write_paths: list[str]
    write_bindings: list[WriteBinding]
    expected_output_paths: list[str]
    expected_registry_head: RegistryHeadBinding
    granted_gates: list[Literal["G0B", "G1A", "G1B", "G2", "G3"]]
    network_allowed: bool
    real_media_allowed: bool
    telemetry: TelemetryPlan
    node_policy: NodeExecutionPolicy
    session_control: SessionControlPlan
    publication: Stage5PublicationPlan | None
    manual_input: ManualInputPolicy | None
    plan_hash: str = Field(pattern=SHA256)

    @field_validator("task_directory", "session_directory")
    @classmethod
    def _session_path(cls, value: str) -> str:
        return validate_relative_path(value)

    @field_validator("read_paths", "write_paths", "expected_output_paths")
    @classmethod
    def _path_lists(cls, values: list[str], info: Any) -> list[str]:
        normalized = [validate_relative_path(value) for value in values]
        return _unique(str(info.field_name), normalized)

    @field_validator("granted_gates")
    @classmethod
    def _gates(cls, values: list[str]) -> list[str]:
        return _unique("granted_gates", values)

    @model_validator(mode="after")
    def _invariants(self) -> "Stage5ExecutionPlan":
        expected_session = f"{self.task_directory}/sessions/{self.session_id}"
        if self.session_directory != expected_session:
            raise ValueError(
                "session_directory must be the exact task-level Session directory"
            )
        if not self.task_directory.endswith(f"/control/tasks/{self.task_id}"):
            raise ValueError(
                "task_directory must bind the exact task_id under control/tasks"
            )
        if (
            self.stage_id == "S01"
            and self.workflow_version != "youtube_truthfulness_workflow_v1.1.0"
        ):
            raise ValueError("S01 requires workflow v1.1")
        if (
            self.stage_id == "S02"
            and self.workflow_version != "youtube_truthfulness_workflow_v1.3.0"
        ):
            raise ValueError("S02 requires workflow v1.3")
        if self.node_id == "external_depth_action":
            if (
                self.stage_id != "S02"
                or self.node_policy.execution_kind != "manual_external_capture"
            ):
                raise ValueError(
                    "external_depth_action requires the S02 manual capture branch"
                )
            if self.node_policy.required_gate != "G2":
                raise ValueError("external_depth_action node policy must require G2")
            if self.node_policy.expected_artifact_types != ["source_depth.result"]:
                raise ValueError(
                    "external_depth_action must produce only source_depth.result"
                )
            if self.manual_input is None:
                raise ValueError("external_depth_action requires manual_input policy")
            if "G2" not in self.granted_gates:
                raise ValueError("external_depth_action requires granted G2")
            if self.network_allowed or self.real_media_allowed:
                raise ValueError(
                    "manual external input capture cannot authorize project network or real media"
                )
            materialize_reads = [
                item
                for item in self.read_bindings
                if item.binding_mode == "materialize_once"
            ]
            if (
                len(materialize_reads) != 1
                or materialize_reads[0].relative_path
                != self.manual_input.source_input_path
            ):
                raise ValueError(
                    "external_depth_action requires exactly one source_input_path materialize_once read"
                )
            manual_writes = {
                self.manual_input.result_output_path,
                self.manual_input.result_ready_receipt_path,
                self.manual_input.materialization_receipt_path,
            }
            if not manual_writes.issubset(set(self.write_paths)):
                raise ValueError(
                    "manual result and receipt paths must be declared writes"
                )
            if self.publication is None:
                raise ValueError(
                    "external_depth_action requires frozen publication identity"
                )
            if self.manual_input.prompt_artifact_id in {
                self.publication.result_artifact_id,
                self.publication.handoff_artifact_id,
            }:
                raise ValueError(
                    "result/HANDOFF Artifact IDs must differ from the prompt Artifact"
                )
            if self.expected_registry_head.head_record_id in {
                self.publication.result_record_id,
                self.publication.handoff_record_id,
            }:
                raise ValueError(
                    "publication record IDs must differ from the frozen Registry head"
                )
            publication_paths = {
                self.publication.publication_receipt_path,
                self.publication.registration_manifest_path,
                self.publication.registry_path,
                self.publication.dag_snapshot_path,
                self.publication.checkpoint_path,
                self.publication.handoff_path,
                self.publication.handoff_markdown_path,
            }
            if not publication_paths.issubset(set(self.write_paths)):
                raise ValueError(
                    "all publication/control paths must be declared writes"
                )
            if (
                self.publication.registry_path
                != self.expected_registry_head.relative_path
            ):
                raise ValueError(
                    "publication Registry path must equal the frozen expected Registry"
                )
            if self.publication.checkpoint_path != (
                f"{self.task_directory}/checkpoints/{self.publication.checkpoint_id}.json"
            ):
                raise ValueError(
                    "checkpoint path must be task-level and bind its frozen checkpoint ID"
                )
            if self.publication.registration_manifest_path != (
                f"{self.task_directory}/registration_manifests/"
                f"{self.node_id}_{self.publication.result_record_id}.json"
            ):
                raise ValueError(
                    "registration manifest path must bind node and result record IDs"
                )
            if self.publication.dag_snapshot_path != (
                f"{self.task_directory}/dag_snapshots/{self.publication.checkpoint_id}.json"
            ):
                raise ValueError(
                    "DAG snapshot filename must bind the frozen checkpoint ID"
                )
            if self.publication.dag_source_path not in self.read_paths:
                raise ValueError("DAG snapshot source must be an exact declared read")
            if self.publication.recovery_workflow_path not in self.read_paths:
                raise ValueError("recovery Workflow must be an exact declared read")
            if self.publication.recovery_prompt_path not in self.read_paths:
                raise ValueError("recovery Prompt must be an exact declared read")
            if (
                self.publication.handoff_path
                != f"{self.session_directory}/handoff.json"
            ):
                raise ValueError("HANDOFF path must be the Session-local handoff.json")
            if (
                self.publication.handoff_markdown_path
                != f"{self.session_directory}/HANDOFF.md"
            ):
                raise ValueError(
                    "HANDOFF Markdown path must be the Session-local HANDOFF.md"
                )
            if datetime.fromisoformat(
                self.publication.result_recorded_at.removesuffix("Z") + "+00:00"
            ) < datetime.fromisoformat(
                self.session_control.session_created_at.removesuffix("Z") + "+00:00"
            ):
                raise ValueError("result publication cannot predate Session creation")
            if datetime.fromisoformat(
                self.publication.result_recorded_at.removesuffix("Z") + "+00:00"
            ) < datetime.fromisoformat(
                self.manual_input.prompt_created_at.removesuffix("Z") + "+00:00"
            ):
                raise ValueError("result publication cannot predate the manual prompt")
        elif self.manual_input is not None:
            raise ValueError("manual_input is only legal for external_depth_action")
        elif self.node_policy.execution_kind == "manual_external_capture":
            raise ValueError(
                "manual_external_capture is reserved for external_depth_action"
            )
        elif any(
            item.binding_mode == "materialize_once" for item in self.read_bindings
        ):
            raise ValueError(
                "materialize_once read is reserved for external_depth_action"
            )
        if self.node_policy.required_gate not in self.granted_gates:
            raise ValueError("node policy required gate is not granted")
        if (
            self.attempt_no == 1
            and self.session_control.parent_checkpoint_id is not None
        ):
            raise ValueError("attempt 1 cannot bind a parent checkpoint")
        if self.attempt_no > 1 and self.session_control.parent_checkpoint_id is None:
            raise ValueError("retry attempt requires a parent checkpoint")
        gate_policy = self.session_control.human_gate_policy
        expected_gate_nodes = [self.node_id] if gate_policy.approval_required else []
        if gate_policy.gate_node_ids != expected_gate_nodes:
            raise ValueError("human gate policy must bind the exact plan node")
        if self.node_policy.execution_kind == "interceptable_model" and (
            not gate_policy.approval_required
            or not gate_policy.decision_artifact_required
        ):
            raise ValueError(
                "interceptable model plan requires an explicit human decision artifact"
            )
        binding_map = {item.relative_path: item for item in self.read_bindings}
        contracts = self.node_policy.contract_files
        expected_versions = {
            "workflow": self.workflow_version,
            "dag": self.dag_version,
            "prompt": self.node_policy.prompt_version,
            "agent_profile": self.node_policy.agent_profile_version,
        }
        for name, reference in (
            ("workflow", contracts.workflow),
            ("dag", contracts.dag),
            ("prompt", contracts.prompt),
            ("agent_profile", contracts.agent_profile),
        ):
            if reference.version != expected_versions[name]:
                raise ValueError(f"{name} contract version differs from plan identity")
            binding = binding_map.get(reference.relative_path)
            if (
                binding is None
                or binding.binding_mode != "frozen_hash"
                or binding.content_hash != reference.content_hash
            ):
                raise ValueError(
                    f"{name} contract file must match an exact frozen_hash read"
                )
        telemetry_binding = binding_map.get(self.telemetry.config_path)
        if (
            telemetry_binding is None
            or telemetry_binding.binding_mode != "frozen_hash"
            or telemetry_binding.content_hash != self.telemetry.config_hash
        ):
            raise ValueError("telemetry config must match an exact read binding")
        registry_binding = binding_map.get(self.expected_registry_head.relative_path)
        if (
            registry_binding is None
            or registry_binding.binding_mode != "frozen_hash"
            or registry_binding.content_hash != self.expected_registry_head.file_hash
        ):
            raise ValueError(
                "Registry head must match an exact frozen_hash read binding"
            )
        for reference in self.session_control.bootstrap_refs:
            relative = getattr(reference, "relative_path", None)
            if relative is None:
                continue
            binding = binding_map.get(relative)
            if (
                binding is None
                or binding.binding_mode != "frozen_hash"
                or binding.content_hash != reference.content_hash
            ):
                raise ValueError("file bootstrap ref must match an exact read binding")
        for relative, content_hash in (
            (
                self.session_control.code_ref.working_tree_manifest_path,
                self.session_control.code_ref.working_tree_manifest_hash,
            ),
            (
                self.session_control.environment_ref.dependency_manifest_path,
                self.session_control.environment_ref.dependency_manifest_hash,
            ),
        ):
            if relative is not None:
                binding = binding_map.get(relative)
                if (
                    binding is None
                    or binding.binding_mode != "frozen_hash"
                    or binding.content_hash != content_hash
                ):
                    raise ValueError(
                        "code/environment manifest must match an exact read binding"
                    )
        if [item.relative_path for item in self.read_bindings] != self.read_paths:
            raise ValueError(
                "read_bindings must correspond one-to-one with read_paths in order"
            )
        if [item.relative_path for item in self.write_bindings] != self.write_paths:
            raise ValueError(
                "write_bindings must correspond one-to-one with write_paths in order"
            )
        write_binding_map = {item.relative_path: item for item in self.write_bindings}
        registry_write = write_binding_map.get(
            self.expected_registry_head.relative_path
        )
        if self.publication is not None:
            if (
                registry_write is None
                or registry_write.write_mode != "append_only_expected_head"
            ):
                raise ValueError(
                    "publication Registry requires append_only_expected_head mode"
                )
        elif registry_write is not None:
            raise ValueError("non-publication plans cannot declare a Registry write")
        if any(
            item.write_mode == "append_only_expected_head"
            and item.relative_path != self.expected_registry_head.relative_path
            for item in self.write_bindings
        ):
            raise ValueError(
                "only the frozen Registry may use append_only_expected_head"
            )
        if any(path not in self.write_paths for path in self.expected_output_paths):
            raise ValueError("expected outputs must be members of write_paths")
        if (
            self.telemetry.ledger_path not in self.write_paths
            or self.telemetry.summary_path not in self.write_paths
        ):
            raise ValueError("telemetry paths must be declared writes")
        required_control_writes = {
            f"{self.session_directory}/session_manifest.json",
            f"{self.session_directory}/events.jsonl",
            f"{self.session_directory}/observations.jsonl",
        }
        if not required_control_writes.issubset(set(self.write_paths)):
            raise ValueError(
                "Session manifest, Event stream, and observation ledger must be declared writes"
            )
        if self.plan_hash != embedded_hash(self.model_dump(mode="json"), "plan_hash"):
            raise ValueError("plan_hash mismatch")
        return self


class Stage5ExecutionPlanV1_1(Stage5ExecutionPlan):
    """Successor adding receipt-bound media reads and control-only finalization."""

    plan_version: Literal["stage5_execution_plan_v1.1.0"]
    artifact_read_bindings: list[ReceiptBoundArtifactReadBinding]
    control_finalization: InputBindingControlFinalizationPlan | None

    @model_validator(mode="after")
    def _successor_invariants(self) -> "Stage5ExecutionPlanV1_1":
        if not self.artifact_read_bindings:
            raise ValueError(
                "v1.1 plan requires at least one receipt-bound Artifact read"
            )
        artifact_ids = [
            item.artifact_ref.artifact_id for item in self.artifact_read_bindings
        ]
        receipt_paths = [
            item.receipt_path_ref.relative_path for item in self.artifact_read_bindings
        ]
        _unique("receipt-bound Artifact IDs", artifact_ids)
        _unique("receipt-bound receipt paths", receipt_paths)
        read_map = {item.relative_path: item for item in self.read_bindings}
        for binding in self.artifact_read_bindings:
            receipt_read = read_map.get(binding.receipt_path_ref.relative_path)
            if (
                receipt_read is None
                or receipt_read.binding_mode != "frozen_hash"
                or receipt_read.content_hash != binding.receipt_path_ref.content_hash
            ):
                raise ValueError(
                    "receipt PathRef must match one exact frozen_hash repository read"
                )

        control = self.control_finalization
        if self.node_id == "input_binding_control":
            if (
                self.stage_id != "S01"
                or self.node_policy.required_gate != "G1A"
                or self.node_policy.execution_kind != "non_model"
                or self.node_policy.expected_artifact_types
            ):
                raise ValueError(
                    "input_binding_control must be a non-business S01-scoped G1A task"
                )
            if "G1A" not in self.granted_gates or not self.real_media_allowed:
                raise ValueError(
                    "input_binding_control requires explicit G1A real-media grant"
                )
            if self.publication is not None or self.manual_input is not None:
                raise ValueError(
                    "input_binding_control cannot declare business publication or manual input"
                )
            if len(self.artifact_read_bindings) != 1:
                raise ValueError(
                    "input_binding_control requires exactly one media binding"
                )
            media_binding = self.artifact_read_bindings[0]
            if (
                media_binding.required_receipt_version != "input_materialization_v1.0.0"
                or media_binding.access_policy.content_read
                != "single_sequential_sha256"
                or media_binding.access_policy.decode_allowed
            ):
                raise ValueError(
                    "G1A must bind the v1.0 receipt for one sequential hash without decode"
                )
            if control is None:
                raise ValueError(
                    "input_binding_control requires no-HANDOFF finalization"
                )
        elif control is not None:
            raise ValueError(
                "control_finalization is reserved for input_binding_control"
            )

        if control is not None:
            if control.dag_source_path not in self.read_paths:
                raise ValueError("control DAG source must be one exact declared read")
            expected_snapshot = (
                f"{self.task_directory}/dag_snapshots/{control.checkpoint_id}.json"
            )
            expected_checkpoint = (
                f"{self.task_directory}/checkpoints/{control.checkpoint_id}.json"
            )
            if control.dag_snapshot_path != expected_snapshot:
                raise ValueError("control DAG snapshot path must bind checkpoint ID")
            if control.checkpoint_path != expected_checkpoint:
                raise ValueError("control checkpoint path must bind checkpoint ID")
            required_writes = {
                control.successor_receipt_path,
                control.dag_snapshot_path,
                control.checkpoint_path,
            }
            if not required_writes.issubset(set(self.write_paths)):
                raise ValueError(
                    "control finalization paths must be exact declared writes"
                )
            write_map = {item.relative_path: item for item in self.write_bindings}
            if any(
                write_map[path].write_mode != "create_new" for path in required_writes
            ):
                raise ValueError("control finalization outputs must use create_new")
            if any(
                "handoff" in PurePosixPath(path).name.lower()
                for path in self.write_paths
            ):
                raise ValueError(
                    "input-binding control plan cannot declare HANDOFF writes"
                )
        return self


class RegistrationValidationSummary(StrictModel):
    record_count: int = Field(ge=1)
    artifact_count: int = Field(ge=1)
    revision_count: int = Field(ge=0)
    candidate_record_count: Literal[1]


class Stage5RegistrationManifest(StrictModel):
    registration_manifest_version: Literal["stage5_registration_manifest_v1.0.0"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    run_id: str = Field(pattern=RUN_ID)
    node_id: str = Field(pattern=NODE_ID)
    plan_hash: str = Field(pattern=SHA256)
    expected_registry_head: RegistryHeadBinding
    candidate_record: dict[str, Any]
    validation_summary: RegistrationValidationSummary
    manifest_hash: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def _record_and_hash(self) -> "Stage5RegistrationManifest":
        record = parse_artifact_record(self.candidate_record)
        if record.run_id != self.run_id or record.dag_node_id != self.node_id:
            raise ValueError("registration candidate identity differs from manifest")
        if (
            self.validation_summary.record_count
            != self.expected_registry_head.record_count + 1
        ):
            raise ValueError(
                "registration validation count does not extend the frozen head once"
            )
        if self.manifest_hash != embedded_hash(
            self.model_dump(mode="json"), "manifest_hash"
        ):
            raise ValueError("registration manifest_hash mismatch")
        return self


class Stage5PublicationReceipt(StrictModel):
    publication_receipt_version: Literal["stage5_publication_receipt_v1.0.0"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    result_artifact_id: str = Field(pattern=ARTIFACT_ID)
    result_record_id: str = Field(pattern=RECORD_ID)
    candidate_record_hash: str = Field(pattern=SHA256)
    result_relative_path: str = Field(min_length=1, max_length=512)
    result_content_hash: str = Field(pattern=SHA256)
    expected_registry_head: RegistryHeadBinding
    new_registry_head: RegistryHeadBinding
    registration_manifest_path: str = Field(min_length=1, max_length=512)
    registration_manifest_hash: str = Field(pattern=SHA256)
    candidate_validation_status: Literal["passed"]
    recorded_at: str = Field(pattern=UTC_TIMESTAMP)
    receipt_hash: str = Field(pattern=SHA256)

    @field_validator("result_relative_path", "registration_manifest_path")
    @classmethod
    def _paths(cls, value: str) -> str:
        return validate_relative_path(value)

    @field_validator("recorded_at")
    @classmethod
    def _recorded_at(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @model_validator(mode="after")
    def _hash(self) -> "Stage5PublicationReceipt":
        if (
            self.new_registry_head.relative_path
            != self.expected_registry_head.relative_path
            or self.new_registry_head.record_count
            != self.expected_registry_head.record_count + 1
            or self.new_registry_head.head_record_id != self.result_record_id
            or self.new_registry_head.head_record_hash != self.candidate_record_hash
        ):
            raise ValueError(
                "publication receipt Registry head transition is inconsistent"
            )
        if self.receipt_hash != embedded_hash(
            self.model_dump(mode="json"), "receipt_hash"
        ):
            raise ValueError("publication receipt_hash mismatch")
        return self


class ArtifactBinding(StrictModel):
    artifact_id: str = Field(pattern=ARTIFACT_ID)
    record_id: str = Field(pattern=RECORD_ID)
    content_hash: str = Field(pattern=SHA256)


class TimeLocator(StrictModel):
    start_ms: int = Field(ge=0)
    end_ms: int = Field(gt=0)

    @model_validator(mode="after")
    def _ordered(self) -> "TimeLocator":
        if self.end_ms <= self.start_ms:
            raise ValueError("time locator end must be after start")
        return self


class WordTimestamp(StrictModel):
    word: str = Field(min_length=1, max_length=240)
    start_ms: int = Field(ge=0)
    end_ms: int = Field(gt=0)
    probability: float | None = Field(default=None, ge=0, le=1)


class TranscriptSegment(StrictModel):
    segment_id: str = Field(pattern=r"^segment_[0-9a-hjkmnp-tv-z]{26}$")
    locator: TimeLocator
    text: str = Field(min_length=1, max_length=20000)
    words: list[WordTimestamp]
    uncertainty: Literal["low", "medium", "high", "unavailable"]


class NormalizedSegment(StrictModel):
    segment_id: str = Field(pattern=r"^segment_[0-9a-hjkmnp-tv-z]{26}$")
    raw_segment_id: str = Field(pattern=r"^segment_[0-9a-hjkmnp-tv-z]{26}$")
    raw_text: str = Field(min_length=1, max_length=20000)
    normalized_text: str = Field(min_length=1, max_length=20000)
    term_mappings: list[str]
    unresolved_ambiguities: list[str]


class AlignmentEntry(StrictModel):
    raw_segment_id: str = Field(pattern=r"^segment_[0-9a-hjkmnp-tv-z]{26}$")
    normalized_segment_id: str = Field(pattern=r"^segment_[0-9a-hjkmnp-tv-z]{26}$")
    locator: TimeLocator
    ocr_entry_ids: list[str]


class FrameBudget(StrictModel):
    run_max_frames: int = Field(ge=0, le=24)
    trigger_max_frames: int = Field(ge=0, le=3)
    adjacent_min_interval_ms: int = Field(ge=2000)


class OcrEntry(StrictModel):
    ocr_entry_id: str = Field(pattern=r"^ocr_entry_[0-9a-hjkmnp-tv-z]{26}$")
    frame_relative_path: str = Field(min_length=1, max_length=512)
    frame_content_hash: str = Field(pattern=SHA256)
    timestamp_ms: int = Field(ge=0)
    raw_text: str = Field(min_length=1, max_length=20000)
    confidence: float | None = Field(default=None, ge=0, le=1)
    trigger_reason: str = Field(min_length=1, max_length=500)

    @field_validator("frame_relative_path")
    @classmethod
    def _frame_path(cls, value: str) -> str:
        return validate_relative_path(value)


class ClaimItem(StrictModel):
    claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    display_no: int = Field(ge=1)
    raw_context: str = Field(min_length=1, max_length=20000)
    normalized_claim: str = Field(min_length=1, max_length=5000)
    locator: TimeLocator
    namespace: Literal["machine_candidate"]


class AtomicClaimItem(StrictModel):
    claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    parent_claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    split_relation: Literal["atomic_child"]
    atomic_text: str = Field(min_length=1, max_length=5000)
    checkability: Literal["checkable", "context_only", "not_checkable"]
    source_depth_candidate: bool
    locator: TimeLocator


class EntityIndexEntry(StrictModel):
    entity_id: str = Field(min_length=1, max_length=80)
    semantic_hash: str = Field(pattern=SHA256)
    parent_entity_id: str | None = Field(default=None, max_length=80)
    upstream_entity_ids: list[str]
    locator: TimeLocator | None

    @field_validator("upstream_entity_ids")
    @classmethod
    def _unique_upstream(cls, values: list[str]) -> list[str]:
        return _unique("upstream_entity_ids", values)


class EvidenceItem(StrictModel):
    evidence_id: str = Field(pattern=r"^evidence_[0-9a-hjkmnp-tv-z]{26}$")
    claim_ids: list[str] = Field(min_length=1)
    source_type: Literal[
        "official",
        "primary_report",
        "paper",
        "database",
        "high_quality_secondary",
        "other",
    ]
    publisher: str = Field(min_length=1, max_length=300)
    published_date: str | None = Field(
        default=None, pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
    )
    retrieved_at: str = Field(pattern=UTC_TIMESTAMP)
    canonical_url: str | None = Field(
        default=None, pattern=r"^https?://[^\s]+$", max_length=2048
    )
    stable_locator: str | None = Field(default=None, max_length=500)
    excerpt: str = Field(min_length=1, max_length=20000)
    relation: Literal["supports", "refutes", "context", "conflicts", "unresolved"]
    quality: Literal["high", "medium", "low", "clue_only"]

    @field_validator("claim_ids")
    @classmethod
    def _claims(cls, values: list[str]) -> list[str]:
        import re

        if any(
            re.fullmatch(r"^claim_[0-9a-hjkmnp-tv-z]{26}$", item) is None
            for item in values
        ):
            raise ValueError("evidence claim_ids must be canonical")
        return _unique("claim_ids", values)

    @model_validator(mode="after")
    def _locator(self) -> "EvidenceItem":
        if self.canonical_url is None and self.stable_locator is None:
            raise ValueError("evidence requires canonical_url or a stable locator")
        return self


class VerdictItem(StrictModel):
    verdict_id: str = Field(pattern=r"^verdict_[0-9a-hjkmnp-tv-z]{26}$")
    claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    evidence_ids: list[str]
    candidate_verdict: Literal[
        "supported", "refuted", "mixed", "insufficient", "unverifiable"
    ]
    reason: str = Field(min_length=1, max_length=10000)
    uncertainty: Literal["low", "medium", "high"]
    review_status: Literal["machine_pending"]

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, values: list[str]) -> list[str]:
        import re

        if any(
            re.fullmatch(r"^evidence_[0-9a-hjkmnp-tv-z]{26}$", item) is None
            for item in values
        ):
            raise ValueError("verdict evidence_ids must be canonical")
        return _unique("evidence_ids", values)


class ReportClaimRef(StrictModel):
    claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    verdict_id: str = Field(pattern=r"^verdict_[0-9a-hjkmnp-tv-z]{26}$")


class AcquisitionDecisionPayload(StrictModel):
    source_id: str = Field(pattern=r"^youtube_[A-Za-z0-9_-]{11}$")
    selected_existing_node_id: Literal["public_no_cookie_download"]
    selected_media: ArtifactBinding
    redownload_forbidden: Literal[True]
    authorization_source: Literal["G0A_D03_reuse_registered_validated_media"]


class TranscriptPathDecisionPayload(StrictModel):
    selected_path: Literal["audio_asr"]
    subtitle_status: Literal["not_registered", "not_authorized"]
    parent_media_validation: ArtifactBinding
    media_artifact_id: str = Field(pattern=ARTIFACT_ID)


class MediaAudioPayload(StrictModel):
    codec: Literal["pcm_s16le"]
    container_format: Literal["wav"]
    channels: Literal[1]
    sample_rate_hz: Literal[16000]
    duration_ms: int = Field(gt=0)
    parent_media_content_hash: str = Field(pattern=SHA256)
    ffmpeg_content_hash: str = Field(pattern=SHA256)
    extraction_parameters_hash: str = Field(pattern=SHA256)
    output_content_hash: str = Field(pattern=SHA256)


def validate_media_audio_wav_bytes(
    data: bytes,
    payload: MediaAudioPayload | Mapping[str, Any],
) -> MediaAudioPayload:
    """Validate one in-memory canonical ASR WAV against its frozen payload."""

    try:
        declared = (
            payload
            if isinstance(payload, MediaAudioPayload)
            else MediaAudioPayload.model_validate(dict(payload))
        )
    except (ValidationError, TypeError, ValueError) as exc:
        raise Stage5ContractError("invalid media.audio payload") from exc
    if not isinstance(data, bytes):
        raise Stage5ContractError("media.audio WAV input must be immutable bytes")
    if len(data) < 12 or data[:4] != b"RIFF" or data[8:12] != b"WAVE":
        raise Stage5ContractError("media.audio must be a RIFF/WAVE byte stream")
    riff_size = int.from_bytes(data[4:8], byteorder="little", signed=False)
    if riff_size + 8 != len(data):
        raise Stage5ContractError(
            "media.audio RIFF size does not match the byte stream"
        )

    try:
        with wave.open(BytesIO(data), "rb") as reader:
            channels = reader.getnchannels()
            sample_width = reader.getsampwidth()
            sample_rate = reader.getframerate()
            frame_count = reader.getnframes()
            compression = reader.getcomptype()
            pcm_bytes = reader.readframes(frame_count + 1)
    except (EOFError, wave.Error) as exc:
        raise Stage5ContractError(
            "media.audio contains an invalid or truncated WAV"
        ) from exc

    if compression != "NONE" or sample_width != 2:
        raise Stage5ContractError("media.audio must use uncompressed PCM s16le")
    if channels != declared.channels:
        raise Stage5ContractError(
            "media.audio WAV channel count does not match payload"
        )
    if sample_rate != declared.sample_rate_hz:
        raise Stage5ContractError("media.audio WAV sample rate does not match payload")
    expected_pcm_bytes = frame_count * channels * sample_width
    if len(pcm_bytes) != expected_pcm_bytes:
        raise Stage5ContractError("media.audio WAV frame data is truncated")
    duration_delta = abs(
        frame_count * 1000 - declared.duration_ms * declared.sample_rate_hz
    )
    if duration_delta > 1000:
        raise Stage5ContractError(
            "media.audio WAV frame count and declared duration differ by more than one frame"
        )
    observed_hash = hashlib.sha256(data).hexdigest()
    if observed_hash != declared.output_content_hash:
        raise Stage5ContractError("media.audio output_content_hash mismatch")
    return declared


class TranscriptRawPayload(StrictModel):
    parent_audio_artifact_id: str = Field(pattern=ARTIFACT_ID)
    asr_engine: Literal["faster-whisper"]
    asr_engine_version: str = Field(min_length=1, max_length=80)
    asr_model: Literal["large-v3"]
    asr_model_revision: str = Field(min_length=1, max_length=200)
    asr_parameters_hash: str = Field(pattern=SHA256)
    language: Literal["zh"]
    segments: list[TranscriptSegment] = Field(min_length=1)

    @field_validator("segments")
    @classmethod
    def _unique_segments(
        cls, values: list[TranscriptSegment]
    ) -> list[TranscriptSegment]:
        _unique("raw transcript segment IDs", [item.segment_id for item in values])
        return values


class TranscriptNormalizedPayload(StrictModel):
    raw_transcript_artifact_id: str = Field(pattern=ARTIFACT_ID)
    preserves_raw: Literal[True]
    segments: list[NormalizedSegment] = Field(min_length=1)

    @field_validator("segments")
    @classmethod
    def _unique_segments(
        cls, values: list[NormalizedSegment]
    ) -> list[NormalizedSegment]:
        _unique("normalized segment IDs", [item.segment_id for item in values])
        _unique("normalized raw segment IDs", [item.raw_segment_id for item in values])
        return values


class TranscriptAlignmentPayload(StrictModel):
    raw_transcript_artifact_id: str = Field(pattern=ARTIFACT_ID)
    normalized_transcript_artifact_id: str = Field(pattern=ARTIFACT_ID)
    ocr_gate_decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    ocr_result_artifact_id: str | None = Field(default=None, pattern=ARTIFACT_ID)
    complete: Literal[True]
    alignments: list[AlignmentEntry] = Field(min_length=1)

    @field_validator("alignments")
    @classmethod
    def _unique_alignments(cls, values: list[AlignmentEntry]) -> list[AlignmentEntry]:
        _unique("alignment raw segment IDs", [item.raw_segment_id for item in values])
        _unique(
            "alignment normalized segment IDs",
            [item.normalized_segment_id for item in values],
        )
        return values


class OcrGateDecisionPayload(StrictModel):
    gate_state: OcrGateState
    trigger_basis: list[str] = Field(min_length=1)
    input_bindings: list[ArtifactBinding] = Field(min_length=1)
    frame_budget: FrameBudget
    adapter_profile_version: str | None = Field(default=None, max_length=120)

    @model_validator(mode="after")
    def _adapter(self) -> "OcrGateDecisionPayload":
        if self.gate_state == "EXECUTED" and self.adapter_profile_version is None:
            raise ValueError("EXECUTED OCR gate requires adapter_profile_version")
        if self.gate_state != "EXECUTED" and self.adapter_profile_version is not None:
            raise ValueError("non-executed OCR gate cannot claim an adapter profile")
        return self


class OcrResultPayload(StrictModel):
    gate_decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    engine_name: str = Field(min_length=1, max_length=120)
    engine_revision: str = Field(min_length=1, max_length=160)
    profile_version: str = Field(min_length=1, max_length=120)
    entries: list[OcrEntry] = Field(min_length=1)

    @field_validator("entries")
    @classmethod
    def _unique_entries(cls, values: list[OcrEntry]) -> list[OcrEntry]:
        _unique("OCR entry IDs", [item.ocr_entry_id for item in values])
        return values


class ClaimCollectionPayload(StrictModel):
    transcript_artifact_id: str = Field(pattern=ARTIFACT_ID)
    claims: list[ClaimItem] = Field(min_length=1)

    @field_validator("claims")
    @classmethod
    def _unique_claims(cls, values: list[ClaimItem]) -> list[ClaimItem]:
        _unique("claim IDs", [item.claim_id for item in values])
        _unique("claim display numbers", [str(item.display_no) for item in values])
        return values


class AtomicClaimCollectionPayload(StrictModel):
    parent_claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    claims: list[AtomicClaimItem] = Field(min_length=1)

    @field_validator("claims")
    @classmethod
    def _unique_claims(cls, values: list[AtomicClaimItem]) -> list[AtomicClaimItem]:
        _unique("atomic claim IDs", [item.claim_id for item in values])
        if any(item.claim_id == item.parent_claim_id for item in values):
            raise ValueError("atomic claim cannot be its own parent")
        return values


class ClaimEntityIndexPayload(StrictModel):
    container: ArtifactBinding
    index_revision: Literal["initial", "atomic_superseding"]
    supersedes_artifact_id: str | None = Field(default=None, pattern=ARTIFACT_ID)
    entries: list[EntityIndexEntry] = Field(min_length=1)

    @field_validator("entries")
    @classmethod
    def _unique_entries(cls, values: list[EntityIndexEntry]) -> list[EntityIndexEntry]:
        _unique("claim index entity IDs", [item.entity_id for item in values])
        return values

    @model_validator(mode="after")
    def _supersedes(self) -> "ClaimEntityIndexPayload":
        if (
            self.index_revision == "atomic_superseding"
            and self.supersedes_artifact_id is None
        ):
            raise ValueError("atomic claim index must supersede the initial index")
        if self.index_revision == "initial" and self.supersedes_artifact_id is not None:
            raise ValueError("initial claim index cannot supersede another index")
        return self


class EvidenceEntityIndexPayload(StrictModel):
    container: ArtifactBinding
    claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    entries: list[EntityIndexEntry] = Field(min_length=1)

    @field_validator("entries")
    @classmethod
    def _unique_entries(cls, values: list[EntityIndexEntry]) -> list[EntityIndexEntry]:
        _unique("evidence index entity IDs", [item.entity_id for item in values])
        return values


class VerdictEntityIndexPayload(StrictModel):
    container: ArtifactBinding
    claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    evidence_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    entries: list[EntityIndexEntry] = Field(min_length=1)

    @field_validator("entries")
    @classmethod
    def _unique_entries(cls, values: list[EntityIndexEntry]) -> list[EntityIndexEntry]:
        _unique("verdict index entity IDs", [item.entity_id for item in values])
        return values


class EvidenceCollectionPayload(StrictModel):
    claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    evidence: list[EvidenceItem] = Field(min_length=1)

    @field_validator("evidence")
    @classmethod
    def _unique_evidence(cls, values: list[EvidenceItem]) -> list[EvidenceItem]:
        _unique("evidence IDs", [item.evidence_id for item in values])
        return values


class VerdictCollectionPayload(StrictModel):
    claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    evidence_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    verdicts: list[VerdictItem] = Field(min_length=1)

    @field_validator("verdicts")
    @classmethod
    def _unique_verdicts(cls, values: list[VerdictItem]) -> list[VerdictItem]:
        _unique("verdict IDs", [item.verdict_id for item in values])
        _unique("verdict claim IDs", [item.claim_id for item in values])
        return values


class MachineReportPayload(StrictModel):
    input_bindings: list[ArtifactBinding] = Field(min_length=2)
    summary: str = Field(min_length=1, max_length=20000)
    claim_refs: list[ReportClaimRef] = Field(min_length=1)
    deterministic_template_version: str = Field(min_length=1, max_length=120)

    @field_validator("claim_refs")
    @classmethod
    def _unique_claim_refs(cls, values: list[ReportClaimRef]) -> list[ReportClaimRef]:
        _unique("report claim IDs", [item.claim_id for item in values])
        _unique("report verdict IDs", [item.verdict_id for item in values])
        return values


class SourceDepthTarget(StrictModel):
    claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    gap: str = Field(min_length=1, max_length=2000)
    preferred_source_types: list[
        Literal["official", "primary_report", "paper", "database"]
    ] = Field(min_length=1)


class SourceDepthDecisionPayload(StrictModel):
    route: Literal["no_depth", "depth"]
    targets: list[SourceDepthTarget]

    @model_validator(mode="after")
    def _route(self) -> "SourceDepthDecisionPayload":
        if self.route == "depth" and not self.targets:
            raise ValueError("depth route requires at least one target claim")
        if self.route == "no_depth" and self.targets:
            raise ValueError("no_depth route cannot contain targets")
        return self


class SourceDepthPromptPayload(StrictModel):
    source_depth_request_id: str = Field(
        pattern=r"^source_depth_request_[0-9a-hjkmnp-tv-z]{26}$"
    )
    target_claims: list[SourceDepthTarget] = Field(min_length=1)
    bounded_context: list[str]
    current_evidence_ids: list[str]
    return_contract_version: Literal["source_depth_manual_return_v1.0.0"]
    require_canonical_urls: Literal[True]


class VisibleModelIdentity(StrictModel):
    value: str | None = Field(default=None, max_length=160)
    status: Literal["reported", "unavailable"]
    source: Literal["ui_label", "not_exposed"]

    @model_validator(mode="after")
    def _identity(self) -> "VisibleModelIdentity":
        if self.status == "unavailable":
            if self.value is not None or self.source != "not_exposed":
                raise ValueError("unavailable visible model requires null/not_exposed")
        elif self.value is None or self.source != "ui_label":
            raise ValueError("reported visible model requires value/ui_label")
        return self


class SourceDepthResultPayload(StrictModel):
    capture_mode: Literal["manual_gemini_web"]
    source_depth_request_id: str = Field(
        pattern=r"^source_depth_request_[0-9a-hjkmnp-tv-z]{26}$"
    )
    prompt_artifact_id: str = Field(pattern=ARTIFACT_ID)
    source_file: FileBinding
    visible_model: VisibleModelIdentity
    received_at: str = Field(pattern=UTC_TIMESTAMP)
    raw_content_hash: str = Field(pattern=SHA256)
    raw_content: str | dict[str, Any] | list[Any]

    @field_validator("received_at")
    @classmethod
    def _received(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @field_validator("raw_content")
    @classmethod
    def _raw(cls, value: Any) -> Any:
        _validate_bounded_json(value)
        return value


class ImportSourceRecord(StrictModel):
    claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    classification: Literal[
        "supports", "refutes", "context", "conflict", "unresolved", "rejected"
    ]
    canonical_url: str | None = Field(
        default=None, pattern=r"^https?://[^\s]+$", max_length=2048
    )
    excerpt_verified: bool
    lead_status: Literal[
        "evidence", "clue_only", "no_evidence", "source_blocked", "pending"
    ]
    rejection_reason: str | None = Field(default=None, max_length=500)


class SourceDepthImportValidationPayload(StrictModel):
    source_depth_result_artifact_id: str = Field(pattern=ARTIFACT_ID)
    mapped_claim_ids: list[str] = Field(min_length=1)
    deduplicated_document_count: int = Field(ge=0)
    sources: list[ImportSourceRecord] = Field(min_length=1)
    conflicts: list[str]
    rejected_count: int = Field(ge=0)

    @model_validator(mode="after")
    def _source_mapping(self) -> "SourceDepthImportValidationPayload":
        mapped = set(_unique("mapped claim IDs", self.mapped_claim_ids))
        source_claims = {item.claim_id for item in self.sources}
        if not source_claims.issubset(mapped):
            raise ValueError("source-depth source record references an unmapped claim")
        rejected = sum(item.classification == "rejected" for item in self.sources)
        if rejected != self.rejected_count:
            raise ValueError("rejected_count does not match rejected source records")
        return self


class EvidenceMergedCollectionPayload(StrictModel):
    base_evidence: ArtifactBinding
    import_validation_artifact_id: str = Field(pattern=ARTIFACT_ID)
    preserved_evidence_ids: list[str]
    added_evidence: list[EvidenceItem]
    diff_summary: str = Field(min_length=1, max_length=5000)

    @model_validator(mode="after")
    def _evidence_identity(self) -> "EvidenceMergedCollectionPayload":
        preserved = set(_unique("preserved evidence IDs", self.preserved_evidence_ids))
        added_ids = [item.evidence_id for item in self.added_evidence]
        added = set(_unique("added evidence IDs", added_ids))
        if preserved & added:
            raise ValueError("added evidence cannot duplicate preserved evidence")
        return self


class VerdictRebuiltCollectionPayload(StrictModel):
    base_verdict: ArtifactBinding
    merged_evidence_artifact_id: str = Field(pattern=ARTIFACT_ID)
    verdicts: list[VerdictItem] = Field(min_length=1)
    before_after_summary: str = Field(min_length=1, max_length=5000)

    @field_validator("verdicts")
    @classmethod
    def _unique_verdicts(cls, values: list[VerdictItem]) -> list[VerdictItem]:
        _unique("rebuilt verdict IDs", [item.verdict_id for item in values])
        _unique("rebuilt verdict claim IDs", [item.claim_id for item in values])
        return values


class RebuiltReportPayload(StrictModel):
    input_bindings: list[ArtifactBinding] = Field(min_length=2)
    summary: str = Field(min_length=1, max_length=20000)
    claim_refs: list[ReportClaimRef] = Field(min_length=1)
    deterministic_template_version: str = Field(min_length=1, max_length=120)
    before_after_summary: str = Field(min_length=1, max_length=5000)

    @field_validator("claim_refs")
    @classmethod
    def _unique_claim_refs(cls, values: list[ReportClaimRef]) -> list[ReportClaimRef]:
        _unique("rebuilt report claim IDs", [item.claim_id for item in values])
        _unique("rebuilt report verdict IDs", [item.verdict_id for item in values])
        return values


class ScreeningSyncPayload(StrictModel):
    selected_report: ArtifactBinding
    selected_report_kind: Literal["machine", "rebuilt"]
    claim_ids: list[str] = Field(min_length=1)
    source_depth_terminal: Literal["NO_DEPTH", "IMPORTED"]
    next_stage: Literal["S03"]
    execution_authorized: Literal[False]

    @field_validator("claim_ids")
    @classmethod
    def _unique_claims(cls, values: list[str]) -> list[str]:
        return _unique("screening claim IDs", values)

    @model_validator(mode="after")
    def _route_report_kind(self) -> "ScreeningSyncPayload":
        if (
            self.source_depth_terminal == "NO_DEPTH"
            and self.selected_report_kind != "machine"
        ):
            raise ValueError("NO_DEPTH screening sync requires the machine report")
        if (
            self.source_depth_terminal == "IMPORTED"
            and self.selected_report_kind != "rebuilt"
        ):
            raise ValueError("IMPORTED screening sync requires the rebuilt report")
        return self


class BusinessArtifactBase(StrictModel):
    artifact_schema_version: Literal["v02_business_artifact_v1.0.0"]
    artifact_id: str = Field(pattern=ARTIFACT_ID)
    artifact_type: BusinessArtifactType
    run_id: str = Field(pattern=RUN_ID)
    stage_id: Literal["S01", "S02"]
    dag_node_id: str = Field(pattern=NODE_ID)
    upstream_artifact_ids: list[str]
    created_at: str = Field(pattern=UTC_TIMESTAMP)
    artifact_hash: str = Field(pattern=SHA256)

    @field_validator("upstream_artifact_ids")
    @classmethod
    def _upstream(cls, values: list[str]) -> list[str]:
        import re

        if any(re.fullmatch(ARTIFACT_ID, value) is None for value in values):
            raise ValueError("invalid upstream Artifact ID")
        return _unique("upstream_artifact_ids", values)

    @field_validator("created_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @model_validator(mode="after")
    def _hash_and_upstream(self) -> "BusinessArtifactBase":
        payload_size = _json_size(self.model_dump(mode="json").get("payload"))
        if payload_size > 20 * 1024 * 1024:
            raise ValueError("business payload exceeds 20 MiB")
        if self.artifact_hash != embedded_hash(
            self.model_dump(mode="json"), "artifact_hash"
        ):
            raise ValueError("artifact_hash mismatch")
        return self


class AcquisitionDecisionArtifact(BusinessArtifactBase):
    artifact_type: Literal["acquisition.decision"]
    payload: AcquisitionDecisionPayload


class TranscriptPathDecisionArtifact(BusinessArtifactBase):
    artifact_type: Literal["transcript.path_decision"]
    payload: TranscriptPathDecisionPayload


class MediaAudioArtifact(BusinessArtifactBase):
    artifact_type: Literal["media.audio"]
    payload: MediaAudioPayload


class TranscriptRawArtifact(BusinessArtifactBase):
    artifact_type: Literal["transcript.raw"]
    payload: TranscriptRawPayload


class TranscriptNormalizedArtifact(BusinessArtifactBase):
    artifact_type: Literal["transcript.normalized"]
    payload: TranscriptNormalizedPayload


class TranscriptAlignmentArtifact(BusinessArtifactBase):
    artifact_type: Literal["transcript.alignment"]
    payload: TranscriptAlignmentPayload


class OcrGateDecisionArtifact(BusinessArtifactBase):
    artifact_type: Literal["ocr.gate_decision"]
    payload: OcrGateDecisionPayload


class OcrResultArtifact(BusinessArtifactBase):
    artifact_type: Literal["ocr.result"]
    payload: OcrResultPayload


class ClaimCollectionArtifact(BusinessArtifactBase):
    artifact_type: Literal["claim.collection"]
    payload: ClaimCollectionPayload


class AtomicClaimCollectionArtifact(BusinessArtifactBase):
    artifact_type: Literal["claim.atomic_collection"]
    payload: AtomicClaimCollectionPayload


class ClaimEntityIndexArtifact(BusinessArtifactBase):
    artifact_type: Literal["claim.entity_index"]
    payload: ClaimEntityIndexPayload


class EvidenceEntityIndexArtifact(BusinessArtifactBase):
    artifact_type: Literal["evidence.entity_index"]
    payload: EvidenceEntityIndexPayload


class VerdictEntityIndexArtifact(BusinessArtifactBase):
    artifact_type: Literal["verdict.entity_index"]
    payload: VerdictEntityIndexPayload


class EvidenceCollectionArtifact(BusinessArtifactBase):
    artifact_type: Literal["evidence.collection"]
    payload: EvidenceCollectionPayload


class VerdictCollectionArtifact(BusinessArtifactBase):
    artifact_type: Literal["verdict.collection"]
    payload: VerdictCollectionPayload


class MachineReportArtifact(BusinessArtifactBase):
    artifact_type: Literal["report.machine"]
    payload: MachineReportPayload


class SourceDepthDecisionArtifact(BusinessArtifactBase):
    artifact_type: Literal["source_depth.decision"]
    payload: SourceDepthDecisionPayload


class SourceDepthPromptArtifact(BusinessArtifactBase):
    artifact_type: Literal["source_depth.prompt"]
    payload: SourceDepthPromptPayload


class SourceDepthResultArtifact(BusinessArtifactBase):
    artifact_type: Literal["source_depth.result"]
    payload: SourceDepthResultPayload


class SourceDepthImportValidationArtifact(BusinessArtifactBase):
    artifact_type: Literal["source_depth.import_validation"]
    payload: SourceDepthImportValidationPayload


class EvidenceMergedCollectionArtifact(BusinessArtifactBase):
    artifact_type: Literal["evidence.merged_collection"]
    payload: EvidenceMergedCollectionPayload


class VerdictRebuiltCollectionArtifact(BusinessArtifactBase):
    artifact_type: Literal["verdict.rebuilt_collection"]
    payload: VerdictRebuiltCollectionPayload


class RebuiltReportArtifact(BusinessArtifactBase):
    artifact_type: Literal["report.rebuilt"]
    payload: RebuiltReportPayload


class ScreeningSyncArtifact(BusinessArtifactBase):
    artifact_type: Literal["screening.sync_record"]
    payload: ScreeningSyncPayload


V02BusinessArtifact: TypeAlias = (
    AcquisitionDecisionArtifact
    | TranscriptPathDecisionArtifact
    | MediaAudioArtifact
    | TranscriptRawArtifact
    | TranscriptNormalizedArtifact
    | TranscriptAlignmentArtifact
    | OcrGateDecisionArtifact
    | OcrResultArtifact
    | ClaimCollectionArtifact
    | AtomicClaimCollectionArtifact
    | ClaimEntityIndexArtifact
    | EvidenceEntityIndexArtifact
    | VerdictEntityIndexArtifact
    | EvidenceCollectionArtifact
    | VerdictCollectionArtifact
    | MachineReportArtifact
    | SourceDepthDecisionArtifact
    | SourceDepthPromptArtifact
    | SourceDepthResultArtifact
    | SourceDepthImportValidationArtifact
    | EvidenceMergedCollectionArtifact
    | VerdictRebuiltCollectionArtifact
    | RebuiltReportArtifact
    | ScreeningSyncArtifact
)


class OcrBranchValidation(StrictModel):
    """Deterministic result of validating the current OCR branch for one run."""

    run_id: str = Field(pattern=RUN_ID)
    gate_decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    gate_state: OcrGateState
    result_artifact_id: str | None = Field(default=None, pattern=ARTIFACT_ID)
    alignment_artifact_ids: list[str]
    alignment_allowed: bool

    @field_validator("alignment_artifact_ids")
    @classmethod
    def _alignment_ids(cls, values: list[str]) -> list[str]:
        import re

        if any(re.fullmatch(ARTIFACT_ID, value) is None for value in values):
            raise ValueError("invalid transcript alignment Artifact ID")
        return _unique("transcript alignment Artifact IDs", values)


class SourceDepthBranchValidation(StrictModel):
    """Unique source-depth branch state derived from business/control facts."""

    run_id: str = Field(pattern=RUN_ID)
    state: SourceDepthBranchState
    control_terminal: SourceDepthControlTerminal
    control_action: SourceDepthControlAction
    target_stage: Literal["S02", "S03"] | None
    decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    prompt_artifact_id: str | None = Field(default=None, pattern=ARTIFACT_ID)
    result_artifact_id: str | None = Field(default=None, pattern=ARTIFACT_ID)
    sync_artifact_id: str | None = Field(default=None, pattern=ARTIFACT_ID)


BUSINESS_ARTIFACT_MODELS: dict[str, type[BusinessArtifactBase]] = {
    "acquisition.decision": AcquisitionDecisionArtifact,
    "transcript.path_decision": TranscriptPathDecisionArtifact,
    "media.audio": MediaAudioArtifact,
    "transcript.raw": TranscriptRawArtifact,
    "transcript.normalized": TranscriptNormalizedArtifact,
    "transcript.alignment": TranscriptAlignmentArtifact,
    "ocr.gate_decision": OcrGateDecisionArtifact,
    "ocr.result": OcrResultArtifact,
    "claim.collection": ClaimCollectionArtifact,
    "claim.atomic_collection": AtomicClaimCollectionArtifact,
    "claim.entity_index": ClaimEntityIndexArtifact,
    "evidence.entity_index": EvidenceEntityIndexArtifact,
    "verdict.entity_index": VerdictEntityIndexArtifact,
    "evidence.collection": EvidenceCollectionArtifact,
    "verdict.collection": VerdictCollectionArtifact,
    "report.machine": MachineReportArtifact,
    "source_depth.decision": SourceDepthDecisionArtifact,
    "source_depth.prompt": SourceDepthPromptArtifact,
    "source_depth.result": SourceDepthResultArtifact,
    "source_depth.import_validation": SourceDepthImportValidationArtifact,
    "evidence.merged_collection": EvidenceMergedCollectionArtifact,
    "verdict.rebuilt_collection": VerdictRebuiltCollectionArtifact,
    "report.rebuilt": RebuiltReportArtifact,
    "screening.sync_record": ScreeningSyncArtifact,
}


# ---------------------------------------------------------------------------
# V02 business Artifact v1.2: lossless Claim text and frozen label taxonomy.
#
# These models are additive successors.  The v1.0 classes and mapping above
# remain unchanged so already-published Artifacts keep their exact contract.
# ---------------------------------------------------------------------------

TRUTHFULNESS_TAXONOMY_VERSION = "truthfulness_taxonomy_v02.1.0"
CLAIM_INLINE_UTF8_LIMIT = 262_144
ATOMIC_CLAIM_WARNING_CHARS = 5_000

SPLIT_STATUS_CODES = ("resolved_atomic", "needs_human_split")
CHECKABILITY_CODES = ("checkable", "context_only", "not_checkable")
MACHINE_VERDICT_CODES = (
    "supported",
    "refuted",
    "mixed",
    "insufficient",
    "unverifiable",
)
HUMAN_GOLD_CODES = (
    "gold_supports",
    "gold_partially_supports",
    "gold_refutes",
    "gold_misleading",
    "gold_missing_context",
    "gold_insufficient_evidence",
    "gold_uncheckable",
)
SOURCE_KIND_CODES = (
    "official",
    "primary_report",
    "paper",
    "database",
    "high_quality_secondary",
    "other",
)
SOURCE_ROLE_CODES = ("primary_source", "secondary_source")
ACCESS_STATUS_CODES = (
    "accessible",
    "source_blocked",
    "not_found",
    "access_error",
)
EVIDENCE_USE_STATUS_CODES = ("evidence", "clue_only", "rejected")
EVIDENCE_STRENGTH_CODES = ("high", "medium", "low")
EVIDENCE_RELATION_CODES = (
    "supports",
    "refutes",
    "context",
    "conflicts",
    "unresolved",
)
EVIDENCE_AVAILABILITY_CODES = ("pending", "has_evidence", "no_evidence")

SplitStatusV1_2 = Literal["resolved_atomic", "needs_human_split"]
CheckabilityV1_2 = Literal["checkable", "context_only", "not_checkable"]
MachineVerdictV1_2 = Literal[
    "supported", "refuted", "mixed", "insufficient", "unverifiable"
]
HumanGoldV1_2 = Literal[
    "gold_supports",
    "gold_partially_supports",
    "gold_refutes",
    "gold_misleading",
    "gold_missing_context",
    "gold_insufficient_evidence",
    "gold_uncheckable",
]
SourceKindV1_2 = Literal[
    "official",
    "primary_report",
    "paper",
    "database",
    "high_quality_secondary",
    "other",
]
SourceRoleV1_2 = Literal["primary_source", "secondary_source"]
AccessStatusV1_2 = Literal[
    "accessible", "source_blocked", "not_found", "access_error"
]
EvidenceUseStatusV1_2 = Literal["evidence", "clue_only", "rejected"]
EvidenceStrengthV1_2 = Literal["high", "medium", "low"]
EvidenceRelationV1_2 = Literal[
    "supports", "refutes", "context", "conflicts", "unresolved"
]
EvidenceAvailabilityStatusV1_2 = Literal[
    "pending", "has_evidence", "no_evidence"
]

PARENT_CLAIM_REVISION_ID = r"^parent_claim_revision_[0-9a-hjkmnp-tv-z]{26}$"
ATOMIC_CLAIM_REVISION_ID = r"^atomic_claim_revision_[0-9a-hjkmnp-tv-z]{26}$"
SPLIT_REVISION_ID = r"^claim_split_revision_[0-9a-hjkmnp-tv-z]{26}$"
SPLIT_MEMBER_ID = r"^split_member_[0-9a-hjkmnp-tv-z]{26}$"
TEXT_CHUNK_ID = r"^claim_text_chunk_[0-9a-hjkmnp-tv-z]{26}$"
CLAIM_DEPENDENCY_ID = r"^claim_dependency_[0-9a-hjkmnp-tv-z]{26}$"
RETRIEVAL_BATCH_ID = r"^retrieval_batch_[0-9a-hjkmnp-tv-z]{26}$"
RETRIEVAL_ATTEMPT_ID = r"^retrieval_attempt_[0-9a-hjkmnp-tv-z]{26}$"
EVIDENCE_REVISION_ID = r"^evidence_revision_[0-9a-hjkmnp-tv-z]{26}$"
EVIDENCE_LINK_ID = r"^evidence_link_[0-9a-hjkmnp-tv-z]{26}$"
ASSESSMENT_REVISION_ID = r"^assessment_revision_[0-9a-hjkmnp-tv-z]{26}$"
GOLD_REVISION_ID = r"^gold_revision_[0-9a-hjkmnp-tv-z]{26}$"
INCLUSION_REVISION_ID = r"^inclusion_revision_[0-9a-hjkmnp-tv-z]{26}$"
GOLD_BATCH_ID = r"^gold_batch_[0-9a-hjkmnp-tv-z]{26}$"


def _validate_taxonomy_version(value: str, expected: str) -> str:
    if value != expected:
        raise ValueError(f"taxonomy_version must equal {expected}")
    return value


class ClaimTextChunkV1_2(StrictModel):
    chunk_id: str = Field(pattern=TEXT_CHUNK_ID)
    owner_kind: Literal["parent_claim_revision", "atomic_claim_revision"]
    owner_revision_id: str = Field(
        pattern=r"^(?:parent_claim_revision|atomic_claim_revision)_[0-9a-hjkmnp-tv-z]{26}$"
    )
    chunk_index: int = Field(ge=0)
    byte_start: int = Field(ge=0)
    byte_end_exclusive: int = Field(gt=0)
    text: str = Field(min_length=1)
    chunk_sha256: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def _byte_identity(self) -> "ClaimTextChunkV1_2":
        encoded = self.text.encode("utf-8")
        if self.byte_end_exclusive <= self.byte_start:
            raise ValueError("claim text chunk byte range must be non-empty")
        if len(encoded) != self.byte_end_exclusive - self.byte_start:
            raise ValueError("claim text chunk byte range does not match UTF-8 bytes")
        if hashlib.sha256(encoded).hexdigest() != self.chunk_sha256:
            raise ValueError("claim text chunk hash mismatch")
        expected_prefix = (
            "parent_claim_revision_"
            if self.owner_kind == "parent_claim_revision"
            else "atomic_claim_revision_"
        )
        if not self.owner_revision_id.startswith(expected_prefix):
            raise ValueError("claim text chunk owner kind/revision mismatch")
        return self


class ClaimTextStorageV1_2(StrictModel):
    text_char_count: int = Field(ge=1)
    text_utf8_byte_count: int = Field(ge=1)
    text_sha256: str = Field(pattern=SHA256)
    inline_text: str | None = Field(default=None, min_length=1)
    chunks: list[ClaimTextChunkV1_2]

    @model_validator(mode="after")
    def _lossless_storage(self) -> "ClaimTextStorageV1_2":
        if (self.inline_text is None) == (len(self.chunks) == 0):
            raise ValueError("claim text requires inline/chunk XOR storage")
        if self.inline_text is not None:
            text = self.inline_text
            encoded = text.encode("utf-8")
            if len(encoded) > CLAIM_INLINE_UTF8_LIMIT:
                raise ValueError("claim text above 262144 UTF-8 bytes must be chunked")
        else:
            if self.text_utf8_byte_count <= CLAIM_INLINE_UTF8_LIMIT:
                raise ValueError("chunked claim text must exceed the inline UTF-8 limit")
            _unique("claim text chunk IDs", [item.chunk_id for item in self.chunks])
            ordered = sorted(self.chunks, key=lambda item: item.chunk_index)
            if [item.chunk_index for item in ordered] != list(range(len(ordered))):
                raise ValueError("claim text chunk indexes must be contiguous from zero")
            expected_start = 0
            for item in ordered:
                if item.byte_start != expected_start:
                    raise ValueError("claim text chunk byte ranges must be contiguous")
                expected_start = item.byte_end_exclusive
            text = "".join(item.text for item in ordered)
            encoded = text.encode("utf-8")
            if expected_start != len(encoded):
                raise ValueError("claim text chunk final byte range is inconsistent")
        if len(text) != self.text_char_count:
            raise ValueError("claim text_char_count mismatch")
        if len(encoded) != self.text_utf8_byte_count:
            raise ValueError("claim text_utf8_byte_count mismatch")
        if hashlib.sha256(encoded).hexdigest() != self.text_sha256:
            raise ValueError("claim text_sha256 mismatch")
        return self

    def reassemble(self) -> str:
        if self.inline_text is not None:
            return self.inline_text
        return "".join(item.text for item in sorted(self.chunks, key=lambda x: x.chunk_index))


class ParentClaimRevisionV1_2(StrictModel):
    parent_claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    parent_revision_id: str = Field(pattern=PARENT_CLAIM_REVISION_ID)
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None, pattern=PARENT_CLAIM_REVISION_ID
    )
    display_no: int = Field(ge=1)
    text: ClaimTextStorageV1_2
    normalized_text: str | None = Field(default=None, min_length=1)
    preview: str | None = Field(default=None, min_length=1, max_length=500)
    source_spans: list[TimeLocator] = Field(min_length=1)
    taxonomy_version: str
    writer_role: Literal["claim_extractor", "authorized_human"]

    @field_validator("taxonomy_version")
    @classmethod
    def _taxonomy(cls, value: str) -> str:
        return _validate_taxonomy_version(value, TRUTHFULNESS_TAXONOMY_VERSION)

    @model_validator(mode="after")
    def _revision_and_chunks(self) -> "ParentClaimRevisionV1_2":
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise ValueError("only the first parent Claim revision omits supersedes")
        if self.supersedes_revision_id == self.parent_revision_id:
            raise ValueError("parent Claim revision cannot supersede itself")
        if any(
            item.owner_kind != "parent_claim_revision"
            or item.owner_revision_id != self.parent_revision_id
            for item in self.text.chunks
        ):
            raise ValueError("parent Claim chunks must bind this parent revision")
        return self


class AtomicClaimRevisionV1_2(StrictModel):
    atomic_claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    parent_claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None, pattern=ATOMIC_CLAIM_REVISION_ID
    )
    split_revision_id: str = Field(pattern=SPLIT_REVISION_ID)
    text: ClaimTextStorageV1_2
    checkability: CheckabilityV1_2
    quality_warnings: list[Literal["atomic_text_over_5000_chars"]]
    machine_verdict_eligible: bool
    taxonomy_version: str
    writer_role: Literal["claim_splitter", "authorized_human"]

    @field_validator("taxonomy_version")
    @classmethod
    def _taxonomy(cls, value: str) -> str:
        return _validate_taxonomy_version(value, TRUTHFULNESS_TAXONOMY_VERSION)

    @field_validator("quality_warnings")
    @classmethod
    def _warnings(cls, values: list[str]) -> list[str]:
        return _unique("atomic Claim quality warnings", values)

    @model_validator(mode="after")
    def _atomic_identity(self) -> "AtomicClaimRevisionV1_2":
        if self.atomic_claim_id == self.parent_claim_id:
            raise ValueError("atomic Claim cannot be its own parent")
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise ValueError("only the first atomic Claim revision omits supersedes")
        if self.supersedes_revision_id == self.atomic_revision_id:
            raise ValueError("atomic Claim revision cannot supersede itself")
        if any(
            item.owner_kind != "atomic_claim_revision"
            or item.owner_revision_id != self.atomic_revision_id
            for item in self.text.chunks
        ):
            raise ValueError("atomic Claim chunks must bind this atomic revision")
        over_limit = self.text.text_char_count > ATOMIC_CLAIM_WARNING_CHARS
        warned = "atomic_text_over_5000_chars" in self.quality_warnings
        if over_limit != warned:
            raise ValueError("atomic Claim over 5000 characters requires its warning")
        return self


class SplitSetMemberV1_2(StrictModel):
    member_id: str = Field(pattern=SPLIT_MEMBER_ID)
    atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    ordinal: int = Field(ge=0)


class ClaimSplitSetRevisionV1_2(StrictModel):
    split_revision_id: str = Field(pattern=SPLIT_REVISION_ID)
    parent_claim_id: str = Field(pattern=r"^claim_[0-9a-hjkmnp-tv-z]{26}$")
    parent_revision_id: str = Field(pattern=PARENT_CLAIM_REVISION_ID)
    revision_no: int = Field(ge=1)
    supersedes_split_revision_id: str | None = Field(
        default=None, pattern=SPLIT_REVISION_ID
    )
    split_status: SplitStatusV1_2
    failure_reason: str | None = Field(default=None, min_length=1, max_length=5000)
    members: list[SplitSetMemberV1_2]
    coverage_reviewed: bool
    taxonomy_version: str
    writer_role: Literal["claim_splitter", "authorized_human"]

    @field_validator("taxonomy_version")
    @classmethod
    def _taxonomy(cls, value: str) -> str:
        return _validate_taxonomy_version(value, TRUTHFULNESS_TAXONOMY_VERSION)

    @model_validator(mode="after")
    def _split_state(self) -> "ClaimSplitSetRevisionV1_2":
        if (self.revision_no == 1) != (self.supersedes_split_revision_id is None):
            raise ValueError("only the first split revision omits supersedes")
        if self.supersedes_split_revision_id == self.split_revision_id:
            raise ValueError("split revision cannot supersede itself")
        _unique("split member IDs", [item.member_id for item in self.members])
        _unique(
            "split member atomic revisions",
            [item.atomic_revision_id for item in self.members],
        )
        if [item.ordinal for item in self.members] != list(range(len(self.members))):
            raise ValueError("split member ordinals must be contiguous from zero")
        if self.split_status == "resolved_atomic":
            if not self.members:
                raise ValueError("resolved_atomic split requires at least one child")
            if self.failure_reason is not None or not self.coverage_reviewed:
                raise ValueError(
                    "resolved_atomic split requires reviewed coverage and no failure reason"
                )
        elif self.failure_reason is None or self.coverage_reviewed:
            raise ValueError(
                "needs_human_split requires a failure reason and unreviewed coverage"
            )
        return self


class ClaimDependencyV1_2(StrictModel):
    dependency_id: str = Field(pattern=CLAIM_DEPENDENCY_ID)
    from_atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    to_atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    dependency_type: Literal[
        "qualifies",
        "conditions",
        "scope_of",
        "compares_with",
        "same_assertion_bundle",
    ]
    writer_role: Literal["claim_splitter", "authorized_human"]

    @model_validator(mode="after")
    def _no_self_loop(self) -> "ClaimDependencyV1_2":
        if self.from_atomic_revision_id == self.to_atomic_revision_id:
            raise ValueError("Claim dependency self-loop is forbidden")
        return self


class ClaimCollectionPayloadV1_2(StrictModel):
    transcript_artifact_id: str = Field(pattern=ARTIFACT_ID)
    parent_revisions: list[ParentClaimRevisionV1_2] = Field(min_length=1)

    @field_validator("parent_revisions")
    @classmethod
    def _parents(
        cls, values: list[ParentClaimRevisionV1_2]
    ) -> list[ParentClaimRevisionV1_2]:
        _unique("parent Claim IDs", [item.parent_claim_id for item in values])
        _unique("parent Claim revision IDs", [item.parent_revision_id for item in values])
        _unique("parent Claim display numbers", [str(item.display_no) for item in values])
        return values


class AtomicClaimCollectionPayloadV1_2(StrictModel):
    parent_claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    split_sets: list[ClaimSplitSetRevisionV1_2] = Field(min_length=1)
    atomic_revisions: list[AtomicClaimRevisionV1_2]
    dependencies: list[ClaimDependencyV1_2]
    run_gate_state: Literal["READY_FOR_S02", "WAITING_FOR_HUMAN"]

    @model_validator(mode="after")
    def _closed_split_graph(self) -> "AtomicClaimCollectionPayloadV1_2":
        _unique(
            "split revision IDs", [item.split_revision_id for item in self.split_sets]
        )
        _unique(
            "split parent Claim IDs", [item.parent_claim_id for item in self.split_sets]
        )
        atomic_by_revision = {
            item.atomic_revision_id: item for item in self.atomic_revisions
        }
        if len(atomic_by_revision) != len(self.atomic_revisions):
            raise ValueError("atomic Claim revision IDs must be unique")
        _unique(
            "atomic Claim IDs", [item.atomic_claim_id for item in self.atomic_revisions]
        )
        member_revision_ids: list[str] = []
        split_by_id = {item.split_revision_id: item for item in self.split_sets}
        for split in self.split_sets:
            for member in split.members:
                atomic = atomic_by_revision.get(member.atomic_revision_id)
                if atomic is None:
                    raise ValueError("split member must reference an atomic revision")
                if (
                    atomic.split_revision_id != split.split_revision_id
                    or atomic.parent_claim_id != split.parent_claim_id
                ):
                    raise ValueError("split member/atomic parent binding mismatch")
                expected_eligible = (
                    split.split_status == "resolved_atomic"
                    and atomic.checkability != "context_only"
                )
                if atomic.machine_verdict_eligible != expected_eligible:
                    raise ValueError(
                        "atomic machine verdict eligibility conflicts with split/checkability"
                    )
                member_revision_ids.append(member.atomic_revision_id)
        _unique("atomic revisions across split sets", member_revision_ids)
        if set(member_revision_ids) != set(atomic_by_revision):
            raise ValueError("every atomic revision must belong to exactly one split set")
        for atomic in self.atomic_revisions:
            if atomic.split_revision_id not in split_by_id:
                raise ValueError("atomic revision references an unknown split revision")
        _unique("Claim dependency IDs", [item.dependency_id for item in self.dependencies])
        relation_keys: list[str] = []
        for dependency in self.dependencies:
            if (
                dependency.from_atomic_revision_id not in atomic_by_revision
                or dependency.to_atomic_revision_id not in atomic_by_revision
            ):
                raise ValueError("Claim dependency must reference atomic revisions in payload")
            left = dependency.from_atomic_revision_id
            right = dependency.to_atomic_revision_id
            if dependency.dependency_type == "compares_with" and right < left:
                left, right = right, left
            relation_keys.append(f"{left}|{right}|{dependency.dependency_type}")
        _unique("Claim dependency triples", relation_keys)
        incident = {
            revision_id
            for item in self.dependencies
            for revision_id in (
                item.from_atomic_revision_id,
                item.to_atomic_revision_id,
            )
        }
        if any(
            item.checkability == "context_only"
            and item.atomic_revision_id not in incident
            for item in self.atomic_revisions
        ):
            raise ValueError("context_only atomic Claim requires a dependency")
        unresolved = any(
            item.split_status == "needs_human_split" for item in self.split_sets
        )
        expected_gate = "WAITING_FOR_HUMAN" if unresolved else "READY_FOR_S02"
        if self.run_gate_state != expected_gate:
            raise ValueError("run gate state must reflect unresolved Claim splits")
        return self


class EvidenceRevisionV1_2(StrictModel):
    evidence_id: str = Field(pattern=r"^evidence_[0-9a-hjkmnp-tv-z]{26}$")
    evidence_revision_id: str = Field(pattern=EVIDENCE_REVISION_ID)
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None, pattern=EVIDENCE_REVISION_ID
    )
    source_kind: SourceKindV1_2
    publisher: str = Field(min_length=1, max_length=300)
    published_date: str | None = Field(
        default=None, pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
    )
    retrieved_at: str = Field(pattern=UTC_TIMESTAMP)
    canonical_url: str | None = Field(
        default=None, pattern=r"^https?://[^\s]+$", max_length=2048
    )
    stable_locator: str | None = Field(default=None, min_length=1, max_length=500)
    excerpt: str = Field(min_length=1, max_length=20000)
    taxonomy_version: str
    writer_role: Literal["machine_evidence_writer", "authorized_human"]

    @field_validator("taxonomy_version")
    @classmethod
    def _taxonomy(cls, value: str) -> str:
        return _validate_taxonomy_version(value, TRUTHFULNESS_TAXONOMY_VERSION)

    @field_validator("retrieved_at")
    @classmethod
    def _retrieved(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @model_validator(mode="after")
    def _revision_and_locator(self) -> "EvidenceRevisionV1_2":
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise ValueError("only the first Evidence revision omits supersedes")
        if self.supersedes_revision_id == self.evidence_revision_id:
            raise ValueError("Evidence revision cannot supersede itself")
        if self.canonical_url is None and self.stable_locator is None:
            raise ValueError("Evidence requires canonical_url or stable_locator")
        return self


class RetrievalAttemptV1_2(StrictModel):
    retrieval_attempt_id: str = Field(pattern=RETRIEVAL_ATTEMPT_ID)
    retrieval_batch_id: str = Field(pattern=RETRIEVAL_BATCH_ID)
    attempted_locator: str = Field(min_length=1, max_length=2048)
    access_status: AccessStatusV1_2
    evidence_revision_id: str | None = Field(
        default=None, pattern=EVIDENCE_REVISION_ID
    )
    attempted_at: str = Field(pattern=UTC_TIMESTAMP)
    writer_role: Literal["retriever"]

    @field_validator("attempted_at")
    @classmethod
    def _attempted(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @model_validator(mode="after")
    def _accessible_binding(self) -> "RetrievalAttemptV1_2":
        if self.access_status == "accessible" and self.evidence_revision_id is None:
            raise ValueError("accessible retrieval attempt requires Evidence revision")
        if self.access_status != "accessible" and self.evidence_revision_id is not None:
            raise ValueError("non-accessible retrieval attempt cannot bind Evidence bytes")
        return self


class ClaimEvidenceLinkV1_2(StrictModel):
    evidence_link_id: str = Field(pattern=EVIDENCE_LINK_ID)
    target_kind: Literal[
        "atomic_claim_revision", "parent_claim_revision"
    ] = "atomic_claim_revision"
    atomic_revision_id: str | None = Field(
        default=None, pattern=ATOMIC_CLAIM_REVISION_ID
    )
    parent_revision_id: str | None = Field(
        default=None, pattern=PARENT_CLAIM_REVISION_ID
    )
    evidence_revision_id: str = Field(pattern=EVIDENCE_REVISION_ID)
    source_role: SourceRoleV1_2 | None
    use_status: EvidenceUseStatusV1_2
    evidence_strength: EvidenceStrengthV1_2 | None
    evidence_relation: EvidenceRelationV1_2 | None
    rejection_reason: str | None = Field(default=None, min_length=1, max_length=2000)
    taxonomy_version: str
    writer_role: Literal["machine_evidence_writer", "authorized_human"]

    @field_validator("taxonomy_version")
    @classmethod
    def _taxonomy(cls, value: str) -> str:
        return _validate_taxonomy_version(value, TRUTHFULNESS_TAXONOMY_VERSION)

    @model_validator(mode="after")
    def _orthogonal_axes(self) -> "ClaimEvidenceLinkV1_2":
        atomic = self.atomic_revision_id is not None
        parent = self.parent_revision_id is not None
        if atomic == parent:
            raise ValueError("Evidence link requires atomic/parent target XOR")
        if self.target_kind == "atomic_claim_revision" and not atomic:
            raise ValueError("atomic Evidence link requires atomic revision target")
        if self.target_kind == "parent_claim_revision" and not parent:
            raise ValueError("parent Evidence link requires parent revision target")
        if self.use_status == "evidence":
            if (
                self.source_role is None
                or self.evidence_strength is None
                or self.evidence_relation is None
            ):
                raise ValueError(
                    "formal evidence requires role, strength, and relation"
                )
            if self.rejection_reason is not None:
                raise ValueError("formal evidence cannot carry a rejection reason")
        elif self.use_status == "clue_only":
            if self.source_role is None or self.evidence_relation is None:
                raise ValueError("clue_only requires source role and relation")
            if self.evidence_strength is not None:
                raise ValueError("clue_only cannot claim Evidence strength")
            if self.rejection_reason is not None:
                raise ValueError("clue_only cannot carry a rejection reason")
        else:
            if self.evidence_strength is not None:
                raise ValueError("rejected Evidence cannot claim strength")
            if self.rejection_reason is None:
                raise ValueError("rejected Evidence requires rejection_reason")
        return self


class EvidenceAvailabilityV1_2(StrictModel):
    atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    retrieval_batch_id: str = Field(pattern=RETRIEVAL_BATCH_ID)
    availability: EvidenceAvailabilityStatusV1_2
    batch_closed: bool
    formal_evidence_link_ids: list[str]
    clue_link_ids: list[str]
    writer_role: Literal["retrieval_batch_closer"]

    @field_validator("formal_evidence_link_ids", "clue_link_ids")
    @classmethod
    def _link_ids(cls, values: list[str]) -> list[str]:
        import re

        if any(re.fullmatch(EVIDENCE_LINK_ID, value) is None for value in values):
            raise ValueError("availability link IDs must be canonical")
        return _unique("availability Evidence link IDs", values)

    @model_validator(mode="after")
    def _closed_semantics(self) -> "EvidenceAvailabilityV1_2":
        if set(self.formal_evidence_link_ids) & set(self.clue_link_ids):
            raise ValueError("one Evidence link cannot be both formal and clue_only")
        if self.availability == "pending":
            if self.batch_closed or self.formal_evidence_link_ids:
                raise ValueError("pending availability requires an open batch")
        elif self.availability == "has_evidence":
            if not self.batch_closed or not self.formal_evidence_link_ids:
                raise ValueError(
                    "has_evidence requires a closed batch and formal Evidence"
                )
        elif not self.batch_closed or self.formal_evidence_link_ids:
            raise ValueError(
                "no_evidence requires a closed batch without formal Evidence"
            )
        return self


class EvidenceCollectionPayloadV1_2(StrictModel):
    atomic_claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    retrieval_batch_id: str = Field(pattern=RETRIEVAL_BATCH_ID)
    evidence_revisions: list[EvidenceRevisionV1_2]
    retrieval_attempts: list[RetrievalAttemptV1_2]
    links: list[ClaimEvidenceLinkV1_2]
    availability: list[EvidenceAvailabilityV1_2] = Field(min_length=1)

    @model_validator(mode="after")
    def _evidence_graph(self) -> "EvidenceCollectionPayloadV1_2":
        evidence_ids = [item.evidence_revision_id for item in self.evidence_revisions]
        _unique("Evidence revision IDs", evidence_ids)
        evidence_set = set(evidence_ids)
        _unique(
            "retrieval attempt IDs",
            [item.retrieval_attempt_id for item in self.retrieval_attempts],
        )
        if any(
            item.retrieval_batch_id != self.retrieval_batch_id
            for item in self.retrieval_attempts
        ):
            raise ValueError("retrieval attempt must bind the payload retrieval batch")
        if any(
            item.evidence_revision_id is not None
            and item.evidence_revision_id not in evidence_set
            for item in self.retrieval_attempts
        ):
            raise ValueError("retrieval attempt references unknown Evidence revision")
        link_by_id = {item.evidence_link_id: item for item in self.links}
        if len(link_by_id) != len(self.links):
            raise ValueError("Evidence link IDs must be unique")
        if any(item.evidence_revision_id not in evidence_set for item in self.links):
            raise ValueError("Evidence link references unknown Evidence revision")
        availability_keys: list[str] = []
        for item in self.availability:
            if item.retrieval_batch_id != self.retrieval_batch_id:
                raise ValueError("availability must bind the payload retrieval batch")
            availability_keys.append(
                f"{item.atomic_revision_id}|{item.retrieval_batch_id}"
            )
            for link_id in item.formal_evidence_link_ids:
                link = link_by_id.get(link_id)
                if (
                    link is None
                    or link.target_kind != "atomic_claim_revision"
                    or link.atomic_revision_id != item.atomic_revision_id
                    or link.use_status != "evidence"
                ):
                    raise ValueError("availability formal link classification mismatch")
            for link_id in item.clue_link_ids:
                link = link_by_id.get(link_id)
                if (
                    link is None
                    or link.target_kind != "atomic_claim_revision"
                    or link.atomic_revision_id != item.atomic_revision_id
                    or link.use_status != "clue_only"
                ):
                    raise ValueError("availability clue link classification mismatch")
            actual_formal = {
                link.evidence_link_id
                for link in self.links
                if link.target_kind == "atomic_claim_revision"
                and link.atomic_revision_id == item.atomic_revision_id
                and link.use_status == "evidence"
            }
            if actual_formal != set(item.formal_evidence_link_ids):
                raise ValueError("availability must enumerate every formal Evidence link")
        _unique("Claim/retrieval availability keys", availability_keys)
        return self


class MachineClaimAssessmentV1_2(StrictModel):
    assessment_revision_id: str = Field(pattern=ASSESSMENT_REVISION_ID)
    atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    claim_checkability: CheckabilityV1_2
    evidence_link_ids: list[str]
    candidate_verdict: MachineVerdictV1_2
    reason: str = Field(min_length=1, max_length=10000)
    uncertainty: Literal["low", "medium", "high"]
    model_version: str = Field(min_length=1, max_length=160)
    prompt_version: str = Field(min_length=1, max_length=160)
    config_hash: str = Field(pattern=SHA256)
    review_status: Literal["machine_pending"]
    writer_role: Literal["machine_assessor"]

    @field_validator("evidence_link_ids")
    @classmethod
    def _evidence_links(cls, values: list[str]) -> list[str]:
        import re

        if any(re.fullmatch(EVIDENCE_LINK_ID, value) is None for value in values):
            raise ValueError("machine assessment Evidence link ID is invalid")
        return _unique("machine assessment Evidence link IDs", values)

    @model_validator(mode="after")
    def _checkability_matrix(self) -> "MachineClaimAssessmentV1_2":
        if self.claim_checkability == "context_only":
            raise ValueError("context_only Claim cannot receive a truth verdict")
        if (
            self.claim_checkability == "not_checkable"
            and self.candidate_verdict != "unverifiable"
        ):
            raise ValueError("not_checkable Claim requires machine unverifiable")
        return self


class HumanGoldLabelV1_2(StrictModel):
    gold_revision_id: str = Field(pattern=GOLD_REVISION_ID)
    target_kind: Literal["atomic_claim_revision", "parent_claim_revision"]
    target_revision_id: str = Field(
        pattern=r"^(?:atomic_claim_revision|parent_claim_revision)_[0-9a-hjkmnp-tv-z]{26}$"
    )
    annotation_scope: Literal["atomic_truth", "parent_context"]
    claim_checkability: CheckabilityV1_2 | None
    gold_label: HumanGoldV1_2
    reason: str = Field(min_length=1, max_length=10000)
    evidence_link_ids: list[str]
    supported_scope: str | None = Field(default=None, min_length=1, max_length=5000)
    unsupported_scope: str | None = Field(default=None, min_length=1, max_length=5000)
    misleading_mechanism: str | None = Field(
        default=None, min_length=1, max_length=5000
    )
    missing_context: str | None = Field(default=None, min_length=1, max_length=5000)
    retrieval_batch_id: str | None = Field(default=None, pattern=RETRIEVAL_BATCH_ID)
    taxonomy_version: str
    approval_status: Literal["approved"]
    writer_role: Literal["authorized_human"]

    @field_validator("taxonomy_version")
    @classmethod
    def _taxonomy(cls, value: str) -> str:
        return _validate_taxonomy_version(value, TRUTHFULNESS_TAXONOMY_VERSION)

    @field_validator("evidence_link_ids")
    @classmethod
    def _evidence_links(cls, values: list[str]) -> list[str]:
        import re

        if any(re.fullmatch(EVIDENCE_LINK_ID, value) is None for value in values):
            raise ValueError("Gold Evidence link ID is invalid")
        return _unique("Gold Evidence link IDs", values)

    @model_validator(mode="after")
    def _gold_matrix(self) -> "HumanGoldLabelV1_2":
        atomic = self.target_kind == "atomic_claim_revision"
        expected_prefix = (
            "atomic_claim_revision_" if atomic else "parent_claim_revision_"
        )
        if not self.target_revision_id.startswith(expected_prefix):
            raise ValueError("Gold target kind/revision mismatch")
        if atomic:
            if self.annotation_scope != "atomic_truth" or self.claim_checkability is None:
                raise ValueError("atomic Gold requires atomic_truth and checkability")
            if self.claim_checkability == "context_only":
                raise ValueError("context_only Claim cannot receive Gold")
            if (
                self.claim_checkability == "not_checkable"
                and self.gold_label != "gold_uncheckable"
            ):
                raise ValueError("not_checkable Claim only permits gold_uncheckable")
            if (
                self.claim_checkability == "checkable"
                and self.gold_label == "gold_uncheckable"
            ):
                raise ValueError("checkable Claim cannot receive gold_uncheckable")
        else:
            if (
                self.annotation_scope != "parent_context"
                or self.claim_checkability is not None
                or self.gold_label
                not in {"gold_misleading", "gold_missing_context"}
            ):
                raise ValueError(
                    "parent Gold is limited to independent context annotations"
                )
            if not self.evidence_link_ids:
                raise ValueError("parent context Gold requires independent Evidence")
        partial_fields = self.supported_scope is not None or self.unsupported_scope is not None
        if self.gold_label == "gold_partially_supports":
            if self.supported_scope is None or self.unsupported_scope is None:
                raise ValueError("gold_partially_supports requires both scope fields")
        elif partial_fields:
            raise ValueError("scope fields are reserved for gold_partially_supports")
        if (self.gold_label == "gold_misleading") != (
            self.misleading_mechanism is not None
        ):
            raise ValueError("gold_misleading requires its mechanism only")
        if (self.gold_label == "gold_missing_context") != (
            self.missing_context is not None
        ):
            raise ValueError("gold_missing_context requires missing_context only")
        if (self.gold_label == "gold_insufficient_evidence") != (
            self.retrieval_batch_id is not None
        ):
            raise ValueError(
                "gold_insufficient_evidence requires a closed retrieval batch only"
            )
        if self.gold_label in {"gold_supports", "gold_refutes"} and not self.evidence_link_ids:
            raise ValueError(f"{self.gold_label} requires formal Evidence")
        return self


class MachineInclusionRecommendationV1_2(StrictModel):
    inclusion_revision_id: str = Field(pattern=INCLUSION_REVISION_ID)
    atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    recommended_status: Literal["included", "excluded", "pending"]
    reason: str = Field(min_length=1, max_length=5000)
    writer_role: Literal["machine_recommender"]


class HumanClaimInclusionDecisionV1_2(StrictModel):
    inclusion_revision_id: str = Field(pattern=INCLUSION_REVISION_ID)
    atomic_revision_id: str = Field(pattern=ATOMIC_CLAIM_REVISION_ID)
    status: Literal["included", "excluded", "pending"]
    reason: str | None = Field(default=None, min_length=1, max_length=5000)
    approved_gold_batch_id: str | None = Field(default=None, pattern=GOLD_BATCH_ID)
    training_eligible: bool
    evaluation_eligible: bool
    writer_role: Literal["authorized_human"]

    @model_validator(mode="after")
    def _dataset_gate(self) -> "HumanClaimInclusionDecisionV1_2":
        if self.status == "excluded" and self.reason is None:
            raise ValueError("excluded inclusion decision requires a reason")
        if self.status == "pending" and self.approved_gold_batch_id is not None:
            raise ValueError("pending inclusion decision cannot bind approved Gold")
        if self.status != "included" and (
            self.training_eligible or self.evaluation_eligible
        ):
            raise ValueError("pending/excluded Claims are not train/eval eligible")
        if (self.training_eligible or self.evaluation_eligible) and (
            self.approved_gold_batch_id is None
        ):
            raise ValueError("dataset eligibility requires an approved Gold batch")
        return self


class VerdictCollectionPayloadV1_2(StrictModel):
    atomic_claim_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    evidence_collection_artifact_id: str = Field(pattern=ARTIFACT_ID)
    machine_assessments: list[MachineClaimAssessmentV1_2]
    inclusion_recommendations: list[MachineInclusionRecommendationV1_2]

    @model_validator(mode="after")
    def _machine_only(self) -> "VerdictCollectionPayloadV1_2":
        _unique(
            "machine assessment revision IDs",
            [item.assessment_revision_id for item in self.machine_assessments],
        )
        _unique(
            "machine assessment atomic revisions",
            [item.atomic_revision_id for item in self.machine_assessments],
        )
        _unique(
            "machine inclusion revision IDs",
            [item.inclusion_revision_id for item in self.inclusion_recommendations],
        )
        _unique(
            "machine inclusion atomic revisions",
            [item.atomic_revision_id for item in self.inclusion_recommendations],
        )
        return self


class MachineReportPayloadV1_1(MachineReportPayload):
    screening_state: Literal["complete", "partial", "blocked"]
    candidate_verdict: MachineVerdictV1_2
    evidence_quality: Literal["high", "medium", "low", "none"]
    needs_split: bool
    needs_source_depth: bool
    review_status: Literal["machine_pending"]


BUSINESS_ARTIFACT_V1_1_PAYLOAD_MODELS: dict[str, type[StrictModel]] = {
    "acquisition.decision": AcquisitionDecisionPayload,
    "transcript.path_decision": TranscriptPathDecisionPayload,
    "media.audio": MediaAudioPayload,
    "transcript.raw": TranscriptRawPayload,
    "transcript.normalized": TranscriptNormalizedPayload,
    "transcript.alignment": TranscriptAlignmentPayload,
    "ocr.gate_decision": OcrGateDecisionPayload,
    "ocr.result": OcrResultPayload,
    "claim.collection": ClaimCollectionPayload,
    "claim.atomic_collection": AtomicClaimCollectionPayload,
    "claim.entity_index": ClaimEntityIndexPayload,
    "evidence.entity_index": EvidenceEntityIndexPayload,
    "verdict.entity_index": VerdictEntityIndexPayload,
    "evidence.collection": EvidenceCollectionPayload,
    "verdict.collection": VerdictCollectionPayload,
    "report.machine": MachineReportPayloadV1_1,
    "source_depth.decision": SourceDepthDecisionPayload,
    "source_depth.prompt": SourceDepthPromptPayload,
    "source_depth.result": SourceDepthResultPayload,
    "source_depth.import_validation": SourceDepthImportValidationPayload,
    "evidence.merged_collection": EvidenceMergedCollectionPayload,
    "verdict.rebuilt_collection": VerdictRebuiltCollectionPayload,
    "report.rebuilt": RebuiltReportPayload,
    "screening.sync_record": ScreeningSyncPayload,
}


class BusinessArtifactV1_1(BusinessArtifactBase):
    """Strict compatibility reader for the already-published v1.1 Schema."""

    artifact_schema_version: Literal["v02_business_artifact_v1.1.0"]
    payload: Any

    @model_validator(mode="before")
    @classmethod
    def _typed_payload(cls, value: Any) -> Any:
        if not isinstance(value, Mapping):
            return value
        raw = dict(value)
        model = BUSINESS_ARTIFACT_V1_1_PAYLOAD_MODELS.get(str(raw.get("artifact_type")))
        if model is None:
            raise ValueError("unsupported v1.1 business Artifact type")
        raw["payload"] = model.model_validate(raw.get("payload"))
        return raw


class WarehouseExportBatchPayloadV1_2(StrictModel):
    export_id: str = Field(pattern=r"^export_[0-9a-hjkmnp-tv-z]{26}$")
    run_id: str = Field(pattern=RUN_ID)
    storage_root_ref: Literal["ubuntu_v02_claim_warehouse"]
    manifest_relative_path: str = Field(min_length=1, max_length=512)
    manifest_hash: str = Field(pattern=SHA256)
    rows_relative_path: str = Field(min_length=1, max_length=512)
    rows_hash: str = Field(pattern=SHA256)
    logical_hash: str = Field(pattern=SHA256)
    row_count: int = Field(ge=1)
    row_counts: dict[str, int] = Field(min_length=1)
    schema_versions: dict[str, str] = Field(min_length=1)
    taxonomy_versions: dict[
        Literal["label_taxonomy_version"],
        Literal["truthfulness_taxonomy_v02.1.0"],
    ] = Field(min_length=1, max_length=1)
    exporter_versions: dict[str, str] = Field(min_length=1)
    projection_status: Literal["pending"]

    @field_validator("manifest_relative_path", "rows_relative_path")
    @classmethod
    def _export_paths(cls, value: str) -> str:
        return validate_relative_path(value)

    @field_validator("row_counts")
    @classmethod
    def _row_counts(cls, values: dict[str, int]) -> dict[str, int]:
        import re

        if any(re.fullmatch(r"^[a-z][a-z0-9_]*$", key) is None for key in values):
            raise ValueError("warehouse row-count keys must be canonical table codes")
        if any(value <= 0 for value in values.values()):
            raise ValueError("warehouse row counts must be positive")
        return values

    @field_validator("schema_versions", "exporter_versions")
    @classmethod
    def _version_maps(cls, values: dict[str, str]) -> dict[str, str]:
        import re

        if any(re.fullmatch(r"^[a-z][a-z0-9_.-]*$", key) is None for key in values):
            raise ValueError("warehouse version-map key is not canonical")
        if any(not isinstance(value, str) or not value.strip() for value in values.values()):
            raise ValueError("warehouse version-map values must be non-empty strings")
        return values

    @model_validator(mode="after")
    def _package_identity(self) -> "WarehouseExportBatchPayloadV1_2":
        if self.row_count != sum(self.row_counts.values()):
            raise ValueError("warehouse row_count must equal sum(row_counts)")
        if self.manifest_relative_path == self.rows_relative_path:
            raise ValueError("warehouse manifest and rows paths must be distinct")
        if PurePosixPath(self.manifest_relative_path).name != "manifest.json":
            raise ValueError("warehouse manifest path must end in manifest.json")
        if PurePosixPath(self.rows_relative_path).name != "rows.jsonl":
            raise ValueError("warehouse rows path must end in rows.jsonl")
        if (
            PurePosixPath(self.manifest_relative_path).parent
            != PurePosixPath(self.rows_relative_path).parent
        ):
            raise ValueError("warehouse manifest and rows must share one package directory")
        return self


BusinessArtifactTypeV1_2 = Literal[
    "acquisition.decision",
    "transcript.path_decision",
    "media.audio",
    "transcript.raw",
    "transcript.normalized",
    "transcript.alignment",
    "ocr.gate_decision",
    "ocr.result",
    "claim.collection",
    "claim.entity_index",
    "claim.atomic_collection",
    "evidence.collection",
    "evidence.entity_index",
    "evidence.merged_collection",
    "verdict.collection",
    "verdict.entity_index",
    "verdict.rebuilt_collection",
    "report.machine",
    "report.rebuilt",
    "source_depth.decision",
    "source_depth.prompt",
    "source_depth.result",
    "source_depth.import_validation",
    "screening.sync_record",
    "warehouse.export_batch",
]


class BusinessArtifactBaseV1_2(BusinessArtifactBase):
    artifact_schema_version: Literal["v02_business_artifact_v1.2.0"]
    artifact_type: BusinessArtifactTypeV1_2


class AcquisitionDecisionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["acquisition.decision"]
    payload: AcquisitionDecisionPayload


class TranscriptPathDecisionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["transcript.path_decision"]
    payload: TranscriptPathDecisionPayload


class MediaAudioArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["media.audio"]
    payload: MediaAudioPayload


class TranscriptRawArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["transcript.raw"]
    payload: TranscriptRawPayload


class TranscriptNormalizedArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["transcript.normalized"]
    payload: TranscriptNormalizedPayload


class TranscriptAlignmentArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["transcript.alignment"]
    payload: TranscriptAlignmentPayload


class OcrGateDecisionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["ocr.gate_decision"]
    payload: OcrGateDecisionPayload


class OcrResultArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["ocr.result"]
    payload: OcrResultPayload


class ClaimCollectionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["claim.collection"]
    payload: ClaimCollectionPayloadV1_2


class AtomicClaimCollectionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["claim.atomic_collection"]
    payload: AtomicClaimCollectionPayloadV1_2


class ClaimEntityIndexArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["claim.entity_index"]
    payload: ClaimEntityIndexPayload


class EvidenceEntityIndexArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["evidence.entity_index"]
    payload: EvidenceEntityIndexPayload


class VerdictEntityIndexArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["verdict.entity_index"]
    payload: VerdictEntityIndexPayload


class EvidenceCollectionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["evidence.collection"]
    payload: EvidenceCollectionPayloadV1_2


class VerdictCollectionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["verdict.collection"]
    payload: VerdictCollectionPayloadV1_2


class MachineReportArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["report.machine"]
    payload: MachineReportPayloadV1_1


class SourceDepthDecisionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["source_depth.decision"]
    payload: SourceDepthDecisionPayload


class SourceDepthPromptArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["source_depth.prompt"]
    payload: SourceDepthPromptPayload


class SourceDepthResultArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["source_depth.result"]
    payload: SourceDepthResultPayload


class SourceDepthImportValidationArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["source_depth.import_validation"]
    payload: SourceDepthImportValidationPayload


class EvidenceMergedCollectionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["evidence.merged_collection"]
    payload: EvidenceMergedCollectionPayload


class VerdictRebuiltCollectionArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["verdict.rebuilt_collection"]
    payload: VerdictRebuiltCollectionPayload


class RebuiltReportArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["report.rebuilt"]
    payload: RebuiltReportPayload


class ScreeningSyncArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["screening.sync_record"]
    payload: ScreeningSyncPayload


class WarehouseExportBatchArtifactV1_2(BusinessArtifactBaseV1_2):
    artifact_type: Literal["warehouse.export_batch"]
    payload: WarehouseExportBatchPayloadV1_2

    @model_validator(mode="after")
    def _warehouse_envelope(self) -> "WarehouseExportBatchArtifactV1_2":
        if self.run_id != self.payload.run_id:
            raise ValueError("warehouse export payload/envelope run_id mismatch")
        if self.stage_id != "S01" or self.dag_node_id != "warehouse_export":
            raise ValueError("warehouse.export_batch must be produced by S01 warehouse_export")
        if not self.upstream_artifact_ids:
            raise ValueError("warehouse.export_batch requires exact upstream Artifacts")
        return self


V02BusinessArtifactV1_2: TypeAlias = (
    AcquisitionDecisionArtifactV1_2
    | TranscriptPathDecisionArtifactV1_2
    | MediaAudioArtifactV1_2
    | TranscriptRawArtifactV1_2
    | TranscriptNormalizedArtifactV1_2
    | TranscriptAlignmentArtifactV1_2
    | OcrGateDecisionArtifactV1_2
    | OcrResultArtifactV1_2
    | ClaimCollectionArtifactV1_2
    | AtomicClaimCollectionArtifactV1_2
    | ClaimEntityIndexArtifactV1_2
    | EvidenceEntityIndexArtifactV1_2
    | VerdictEntityIndexArtifactV1_2
    | EvidenceCollectionArtifactV1_2
    | VerdictCollectionArtifactV1_2
    | MachineReportArtifactV1_2
    | SourceDepthDecisionArtifactV1_2
    | SourceDepthPromptArtifactV1_2
    | SourceDepthResultArtifactV1_2
    | SourceDepthImportValidationArtifactV1_2
    | EvidenceMergedCollectionArtifactV1_2
    | VerdictRebuiltCollectionArtifactV1_2
    | RebuiltReportArtifactV1_2
    | ScreeningSyncArtifactV1_2
    | WarehouseExportBatchArtifactV1_2
)


BUSINESS_ARTIFACT_V1_2_MODELS: dict[str, type[BusinessArtifactBaseV1_2]] = {
    "acquisition.decision": AcquisitionDecisionArtifactV1_2,
    "transcript.path_decision": TranscriptPathDecisionArtifactV1_2,
    "media.audio": MediaAudioArtifactV1_2,
    "transcript.raw": TranscriptRawArtifactV1_2,
    "transcript.normalized": TranscriptNormalizedArtifactV1_2,
    "transcript.alignment": TranscriptAlignmentArtifactV1_2,
    "ocr.gate_decision": OcrGateDecisionArtifactV1_2,
    "ocr.result": OcrResultArtifactV1_2,
    "claim.collection": ClaimCollectionArtifactV1_2,
    "claim.atomic_collection": AtomicClaimCollectionArtifactV1_2,
    "claim.entity_index": ClaimEntityIndexArtifactV1_2,
    "evidence.entity_index": EvidenceEntityIndexArtifactV1_2,
    "verdict.entity_index": VerdictEntityIndexArtifactV1_2,
    "evidence.collection": EvidenceCollectionArtifactV1_2,
    "verdict.collection": VerdictCollectionArtifactV1_2,
    "report.machine": MachineReportArtifactV1_2,
    "source_depth.decision": SourceDepthDecisionArtifactV1_2,
    "source_depth.prompt": SourceDepthPromptArtifactV1_2,
    "source_depth.result": SourceDepthResultArtifactV1_2,
    "source_depth.import_validation": SourceDepthImportValidationArtifactV1_2,
    "evidence.merged_collection": EvidenceMergedCollectionArtifactV1_2,
    "verdict.rebuilt_collection": VerdictRebuiltCollectionArtifactV1_2,
    "report.rebuilt": RebuiltReportArtifactV1_2,
    "screening.sync_record": ScreeningSyncArtifactV1_2,
    "warehouse.export_batch": WarehouseExportBatchArtifactV1_2,
}


PAYLOAD_MODELS: dict[str, type[StrictModel]] = {
    "acquisition.decision": AcquisitionDecisionPayload,
    "transcript.path_decision": TranscriptPathDecisionPayload,
    "media.audio": MediaAudioPayload,
    "transcript.raw": TranscriptRawPayload,
    "transcript.normalized": TranscriptNormalizedPayload,
    "transcript.alignment": TranscriptAlignmentPayload,
    "ocr.gate_decision": OcrGateDecisionPayload,
    "ocr.result": OcrResultPayload,
    "claim.collection": ClaimCollectionPayloadV1_2,
    "claim.atomic_collection": AtomicClaimCollectionPayloadV1_2,
    "claim.entity_index": ClaimEntityIndexPayload,
    "evidence.entity_index": EvidenceEntityIndexPayload,
    "verdict.entity_index": VerdictEntityIndexPayload,
    "evidence.collection": EvidenceCollectionPayloadV1_2,
    "verdict.collection": VerdictCollectionPayloadV1_2,
    "report.machine": MachineReportPayloadV1_1,
    "source_depth.decision": SourceDepthDecisionPayload,
    "source_depth.prompt": SourceDepthPromptPayload,
    "source_depth.result": SourceDepthResultPayload,
    "source_depth.import_validation": SourceDepthImportValidationPayload,
    "evidence.merged_collection": EvidenceMergedCollectionPayload,
    "verdict.rebuilt_collection": VerdictRebuiltCollectionPayload,
    "report.rebuilt": RebuiltReportPayload,
    "screening.sync_record": ScreeningSyncPayload,
    "warehouse.export_batch": WarehouseExportBatchPayloadV1_2,
}


class FileStatSnapshot(StrictModel):
    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    size_bytes: int = Field(ge=0)
    mtime_ns: int = Field(ge=0)
    mode: int = Field(ge=0)


_ReviewerId = Annotated[
    str,
    Field(pattern=r"^[a-z][a-z0-9_.-]{2,159}$", min_length=3, max_length=160),
]
_ReviewerRole = Annotated[
    str,
    Field(pattern=r"^[a-z][a-z0-9_]{2,79}$", min_length=3, max_length=80),
]


class NonProductModelGatePolicyV1(StrictModel):
    """Frozen authorization policy dispatched before a business-model call."""

    policy_version: Literal["non_product_model_gate_policy_v1.0.0"]
    receipt_version: Literal["non_product_domain_review_receipt_v1.0.0"]
    authorized_reviewer_ids: frozenset[_ReviewerId] = Field(
        min_length=1,
        strict=False,
    )
    authorized_reviewer_roles: frozenset[_ReviewerRole] = Field(
        min_length=1,
        strict=False,
    )


class NonProductDomainReviewReceiptV1(StrictModel):
    """Immutable human decision required before any business-model call."""

    receipt_version: Literal["non_product_domain_review_receipt_v1.0.0"]
    receipt_id: str = Field(pattern=RECEIPT_ID)
    source_id: str = Field(pattern=r"^youtube_[A-Za-z0-9_-]{11}$")
    input_artifact_id: str = Field(pattern=ARTIFACT_ID)
    input_content_hash_algorithm: Literal["sha256"]
    input_content_hash: str = Field(pattern=SHA256)
    decision: Literal[
        "non_product_verified",
        "product_rejected",
        "product_mixed_rejected",
        "unconfirmed_rejected",
    ]
    reviewer_id: _ReviewerId
    reviewer_role: _ReviewerRole
    reviewed_at: str = Field(pattern=UTC_TIMESTAMP)
    review_scope: Literal["entire_source_material"]
    review_reason: str = Field(min_length=1, max_length=1000)
    receipt_hash: str = Field(pattern=SHA256)

    @field_validator("reviewed_at")
    @classmethod
    def _reviewed_at(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @model_validator(mode="after")
    def _self_hash(self) -> "NonProductDomainReviewReceiptV1":
        if self.receipt_hash != embedded_hash(
            self.model_dump(mode="json"), "receipt_hash"
        ):
            raise ValueError("non-product domain review receipt_hash mismatch")
        return self


def seal_non_product_domain_review_receipt(
    draft: Mapping[str, Any],
) -> NonProductDomainReviewReceiptV1:
    """Seal one create-new receipt draft without accepting a caller-supplied hash."""

    raw = dict(draft)
    supplied = raw.get("receipt_hash")
    if supplied not in {None, "0" * 64}:
        raise Stage5ContractError(
            "non-product domain review receipt draft cannot supply a sealed hash"
        )
    raw["receipt_hash"] = "0" * 64
    raw["receipt_hash"] = embedded_hash(raw, "receipt_hash")
    return parse_non_product_domain_review_receipt(raw)


class ManualExternalInputReceipt(StrictModel):
    receipt_version: Literal["manual_external_input_receipt_v1.0.0"]
    receipt_kind: Literal["result_ready", "materialization"]
    receipt_id: str = Field(pattern=RECEIPT_ID)
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str = Field(pattern=RUN_ID)
    source_depth_request_id: str = Field(
        pattern=r"^source_depth_request_[0-9a-hjkmnp-tv-z]{26}$"
    )
    prompt_artifact_id: str = Field(pattern=ARTIFACT_ID)
    source_relative_path: str = Field(min_length=1, max_length=512)
    user_signal_at: str = Field(pattern=UTC_TIMESTAMP)
    signal_semantic_hash: str = Field(pattern=SHA256)
    result_ready: Literal[True]
    permission: Literal["capture_only"]
    stat_before: FileStatSnapshot | None
    stat_after: FileStatSnapshot | None
    content_hash: str | None = Field(pattern=SHA256)
    size_bytes: int | None = Field(ge=0)
    media_type: Literal["application/json", "text/markdown", "text/plain"] | None
    receipt_hash: str = Field(pattern=SHA256)

    @field_validator("source_relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return validate_relative_path(value)

    @field_validator("user_signal_at")
    @classmethod
    def _timestamp(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @model_validator(mode="after")
    def _kind_fields(self) -> "ManualExternalInputReceipt":
        materialized = (
            self.stat_before,
            self.stat_after,
            self.content_hash,
            self.size_bytes,
            self.media_type,
        )
        if self.receipt_kind == "result_ready" and any(
            value is not None for value in materialized
        ):
            raise ValueError("result_ready receipt cannot claim file bytes")
        if self.receipt_kind == "materialization" and any(
            value is None for value in materialized
        ):
            raise ValueError("materialization receipt requires stable file evidence")
        if self.receipt_kind == "materialization":
            if self.stat_before != self.stat_after:
                raise ValueError(
                    "materialization receipt requires stable stat-before/stat-after identity"
                )
            if (
                self.stat_after is not None
                and self.size_bytes != self.stat_after.size_bytes
            ):
                raise ValueError(
                    "materialization receipt size does not match stable file stat"
                )
        if self.receipt_hash != embedded_hash(
            self.model_dump(mode="json"), "receipt_hash"
        ):
            raise ValueError("receipt_hash mismatch")
        return self


class ObservationMetric(StrictModel):
    value: int | float | None
    status: Literal["measured", "estimated", "unavailable", "not_applicable"]
    source: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=80)

    @model_validator(mode="after")
    def _null_semantics(self) -> "ObservationMetric":
        if self.status in {"unavailable", "not_applicable"} and self.value is not None:
            raise ValueError("unavailable/not_applicable metric value must be null")
        if self.status in {"measured", "estimated"} and self.value is None:
            raise ValueError("measured/estimated metric requires a value")
        return self


class Stage5Observation(StrictModel):
    observation_version: Literal["stage5_observation_v1.0.0"]
    observation_type: Literal["observation.recorded", "observation.closed"]
    observation_id: str = Field(pattern=EVENT_ID)
    sequence_no: int = Field(ge=1)
    recorded_at: str = Field(pattern=UTC_TIMESTAMP)
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str = Field(pattern=RUN_ID)
    stage_id: Literal["S01", "S02"]
    node_id: str | None = Field(pattern=NODE_ID)
    actor_role: str | None = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=80)
    tool_name: str | None = Field(pattern=r"^[a-z][a-z0-9_.-]*$", max_length=120)
    tool_profile: FileBinding | None
    exit_code: int | None
    accelerator_peak_memory_bytes: ObservationMetric | None
    retry_parent_session_id: str | None = Field(pattern=SESSION_ID)
    rework_observation_ids: list[str]
    supersedes_observation_ids: list[str]
    started_at: str | None = Field(pattern=UTC_TIMESTAMP)
    finished_at: str | None = Field(pattern=UTC_TIMESTAMP)
    active_elapsed_ms: ObservationMetric | None
    input_files: list[FileBinding]
    output_files: list[FileBinding]
    model_event_ids: list[str]
    model_summary: FileBinding | None
    external_cost: ObservationMetric | None
    failure_class: str | None = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    invalid_read_count: int = Field(ge=0)
    scope: Literal["contract_observed"]
    previous_record_hash: str | None = Field(pattern=SHA256)
    record_hash: str = Field(pattern=SHA256)

    @field_validator("recorded_at")
    @classmethod
    def _recorded_at(cls, value: str) -> str:
        return validate_utc_timestamp(value)

    @field_validator("started_at", "finished_at")
    @classmethod
    def _optional_times(cls, value: str | None) -> str | None:
        return validate_utc_timestamp(value) if value is not None else None

    @field_validator("model_event_ids")
    @classmethod
    def _events(cls, values: list[str]) -> list[str]:
        for value in values:
            if not __import__("re").fullmatch(EVENT_ID, value):
                raise ValueError("invalid model event ID")
        return _unique("model_event_ids", values)

    @field_validator("rework_observation_ids", "supersedes_observation_ids")
    @classmethod
    def _observation_ids(cls, values: list[str]) -> list[str]:
        for value in values:
            if not __import__("re").fullmatch(EVENT_ID, value):
                raise ValueError("invalid observation relation ID")
        return _unique("observation relation IDs", values)

    @model_validator(mode="after")
    def _type_fields(self) -> "Stage5Observation":
        active = (
            self.node_id,
            self.actor_role,
            self.tool_name,
            self.started_at,
            self.finished_at,
        )
        if self.observation_type == "observation.recorded" and any(
            value is None for value in active
        ):
            raise ValueError(
                "recorded observation requires node/actor/tool/start/finish"
            )
        if self.observation_type == "observation.recorded":
            if self.active_elapsed_ms is None:
                raise ValueError("recorded observation requires active_elapsed_ms")
            if self.started_at is not None and self.finished_at is not None:
                start = datetime.fromisoformat(
                    self.started_at.removesuffix("Z") + "+00:00"
                )
                finish = datetime.fromisoformat(
                    self.finished_at.removesuffix("Z") + "+00:00"
                )
                if finish < start:
                    raise ValueError("observation finish cannot predate start")
        else:
            if any(
                value is not None
                for value in (*active, self.active_elapsed_ms, self.external_cost)
            ):
                raise ValueError("closed observation cannot claim active work")
            if (
                self.input_files
                or self.output_files
                or self.failure_class is not None
                or self.tool_profile is not None
                or self.exit_code is not None
                or self.accelerator_peak_memory_bytes is not None
                or self.retry_parent_session_id is not None
                or self.rework_observation_ids
                or self.supersedes_observation_ids
            ):
                raise ValueError("closed observation cannot claim node I/O or failure")
            if self.model_summary is None:
                raise ValueError(
                    "closed observation requires the frozen model summary binding"
                )
        _unique(
            "observation input paths", [item.relative_path for item in self.input_files]
        )
        _unique(
            "observation output paths",
            [item.relative_path for item in self.output_files],
        )
        if self.record_hash != embedded_hash(
            self.model_dump(mode="json"), "record_hash"
        ):
            raise ValueError("observation record_hash mismatch")
        return self


def _parse(kind: str, model: type[StrictModel], raw: Mapping[str, Any]) -> Any:
    try:
        return model.model_validate(dict(raw))
    except ValidationError as exc:
        first = exc.errors(include_url=False)[0]
        location = "/".join(str(part) for part in first["loc"]) or "<root>"
        raise ExecutionSchemaError(
            f"Invalid {kind} contract at {location}: {first['msg']}"
        ) from exc


def parse_stage5_execution_plan(
    raw: Mapping[str, Any],
) -> Stage5ExecutionPlan | Stage5ExecutionPlanV1_1:
    version = raw.get("plan_version")
    model: type[Stage5ExecutionPlan] = (
        Stage5ExecutionPlanV1_1
        if version == "stage5_execution_plan_v1.1.0"
        else Stage5ExecutionPlan
    )
    return _parse("Stage 5 execution plan", model, raw)


def parse_checkpoint_recovery_receipt(
    raw: Mapping[str, Any],
) -> CheckpointRecoveryReceipt | CheckpointRecoveryReceiptV1_1:
    model: type[CheckpointRecoveryReceipt] = (
        CheckpointRecoveryReceiptV1_1
        if raw.get("checkpoint_recovery_receipt_version")
        == "checkpoint_recovery_receipt_v1.1.0"
        else CheckpointRecoveryReceipt
    )
    return _parse("checkpoint recovery receipt", model, raw)


def parse_stage5_registration_manifest(
    raw: Mapping[str, Any],
) -> Stage5RegistrationManifest:
    return _parse("Stage 5 registration manifest", Stage5RegistrationManifest, raw)


def parse_stage5_publication_receipt(
    raw: Mapping[str, Any],
) -> Stage5PublicationReceipt:
    return _parse("Stage 5 publication receipt", Stage5PublicationReceipt, raw)


def parse_v02_business_artifact(
    raw: Mapping[str, Any],
) -> V02BusinessArtifact | BusinessArtifactV1_1 | V02BusinessArtifactV1_2:
    version = raw.get("artifact_schema_version")
    artifact_type = raw.get("artifact_type")
    if version == "v02_business_artifact_v1.2.0":
        model: type[StrictModel] | None = BUSINESS_ARTIFACT_V1_2_MODELS.get(
            str(artifact_type)
        )
    elif version == "v02_business_artifact_v1.1.0":
        model = (
            BusinessArtifactV1_1
            if str(artifact_type) in BUSINESS_ARTIFACT_V1_1_PAYLOAD_MODELS
            else None
        )
    elif version == "v02_business_artifact_v1.0.0":
        model = BUSINESS_ARTIFACT_MODELS.get(str(artifact_type))
    else:
        raise ExecutionSchemaError(
            "Invalid V02 business Artifact contract: unsupported "
            f"artifact_schema_version {version!r}"
        )
    if model is None:
        raise ExecutionSchemaError(
            f"Invalid V02 business Artifact contract: unsupported artifact_type {artifact_type!r}"
        )
    return _parse("V02 business Artifact", model, raw)


def validate_ocr_branch_contract(
    *,
    run_id: str,
    artifacts: Iterable[Mapping[str, Any] | V02BusinessArtifact],
    registry_records: Iterable[
        Mapping[str, Any] | ArtifactRecordWire | ArtifactRecordView
    ],
) -> OcrBranchValidation:
    """Validate OCR gate/result/alignment semantics without I/O or state mutation.

    Registry records determine which Artifacts remain current after explicit
    ``supersedes`` relations. Business payloads provide the three-state gate and
    the exact result/alignment bindings that the type-only DAG projection cannot
    inspect.
    """

    branch_types = frozenset(
        {"ocr.gate_decision", "ocr.result", "transcript.alignment"}
    )
    artifacts_by_id: dict[str, V02BusinessArtifact] = {}
    for raw_artifact in artifacts:
        artifact = (
            raw_artifact
            if isinstance(raw_artifact, BusinessArtifactBase)
            else parse_v02_business_artifact(raw_artifact)
        )
        if artifact.artifact_type not in branch_types:
            continue
        if artifact.run_id != run_id:
            raise Stage5ContractError(
                "OCR branch business Artifacts must belong to the requested run"
            )
        if artifact.artifact_id in artifacts_by_id:
            raise Stage5ContractError(
                f"duplicate OCR branch business Artifact payload: {artifact.artifact_id}"
            )
        artifacts_by_id[artifact.artifact_id] = artifact

    latest_records: dict[str, ArtifactRecordView] = {}
    for raw_record in registry_records:
        record = (
            raw_record
            if isinstance(raw_record, ArtifactRecordView)
            else to_artifact_record_view(parse_artifact_record(raw_record))
        )
        if record.run_id != run_id:
            if record.artifact_type in branch_types:
                raise Stage5ContractError(
                    "OCR branch Registry records must belong to the requested run"
                )
            continue
        previous = latest_records.get(record.artifact_id)
        if previous is not None and record.record_revision <= previous.record_revision:
            raise Stage5ContractError(
                "OCR branch Registry history must use strictly increasing revisions"
            )
        latest_records[record.artifact_id] = record

    relevant_records = {
        artifact_id: record
        for artifact_id, record in latest_records.items()
        if record.artifact_type in branch_types
    }
    for artifact_id, artifact in artifacts_by_id.items():
        record = relevant_records.get(artifact_id)
        if record is None:
            raise Stage5ContractError(
                f"OCR branch business Artifact has no Registry record: {artifact_id}"
            )
        if (
            record.artifact_type != artifact.artifact_type
            or record.stage_id != artifact.stage_id
            or record.dag_node_id != artifact.dag_node_id
        ):
            raise Stage5ContractError(
                f"OCR branch Registry/payload identity mismatch: {artifact_id}"
            )

    superseded_ids = {
        superseded_id
        for record in latest_records.values()
        for superseded_id in record.supersedes
    }

    def is_current_valid(record: ArtifactRecordView) -> bool:
        return (
            record.artifact_id not in superseded_ids
            and record.lifecycle_state in {"validated", "frozen"}
            and record.validation_status == "passed"
        )

    current_records = [
        record for record in relevant_records.values() if is_current_valid(record)
    ]
    for record in current_records:
        if record.artifact_id not in artifacts_by_id:
            raise Stage5ContractError(
                "current OCR branch Registry record is missing its validated "
                f"business Artifact payload: {record.artifact_id}"
            )

    def current_artifacts(artifact_type: str) -> list[V02BusinessArtifact]:
        return [
            artifacts_by_id[record.artifact_id]
            for record in sorted(current_records, key=lambda item: item.artifact_id)
            if record.artifact_type == artifact_type
        ]

    gates = current_artifacts("ocr.gate_decision")
    if len(gates) != 1:
        raise Stage5ContractError(
            "OCR branch requires exactly one current valid gate decision"
        )
    gate = gates[0]
    if not isinstance(gate, OcrGateDecisionArtifact):
        raise Stage5ContractError(
            "current OCR gate payload has the wrong business type"
        )

    results = current_artifacts("ocr.result")
    if any(not isinstance(result, OcrResultArtifact) for result in results):
        raise Stage5ContractError(
            "current OCR result payload has the wrong business type"
        )
    alignments = current_artifacts("transcript.alignment")
    if any(
        not isinstance(alignment, TranscriptAlignmentArtifact)
        for alignment in alignments
    ):
        raise Stage5ContractError(
            "current transcript alignment payload has the wrong business type"
        )
    if len(alignments) > 1:
        raise Stage5ContractError(
            "OCR branch permits at most one current transcript alignment"
        )

    gate_id = gate.artifact_id
    gate_state = gate.payload.gate_state
    result_id: str | None = None
    alignment_allowed = gate_state != "REQUIRED_BLOCKED"

    if gate_state == "NOT_APPLICABLE":
        if results:
            raise Stage5ContractError(
                "NOT_APPLICABLE OCR gate forbids current OCR results"
            )
    elif gate_state == "REQUIRED_BLOCKED":
        if results:
            raise Stage5ContractError(
                "REQUIRED_BLOCKED OCR gate forbids current OCR results"
            )
        if alignments:
            raise Stage5ContractError(
                "REQUIRED_BLOCKED OCR gate forbids transcript alignment"
            )
    else:
        if len(results) != 1:
            raise Stage5ContractError(
                "EXECUTED OCR gate requires exactly one current valid OCR result"
            )
        result = results[0]
        if not isinstance(result, OcrResultArtifact):
            raise Stage5ContractError(
                "current OCR result payload has the wrong business type"
            )
        if result.payload.gate_decision_artifact_id != gate_id:
            raise Stage5ContractError(
                "OCR result must bind the current OCR gate decision"
            )
        result_id = result.artifact_id

    for alignment in alignments:
        if not isinstance(alignment, TranscriptAlignmentArtifact):
            raise Stage5ContractError(
                "current transcript alignment payload has the wrong business type"
            )
        if alignment.payload.ocr_gate_decision_artifact_id != gate_id:
            raise Stage5ContractError(
                "transcript alignment must bind the current OCR gate decision"
            )
        if alignment.payload.ocr_result_artifact_id != result_id:
            expected = "null" if result_id is None else "the current OCR result"
            raise Stage5ContractError(
                f"transcript alignment OCR result reference must bind {expected}"
            )

    return OcrBranchValidation(
        run_id=run_id,
        gate_decision_artifact_id=gate_id,
        gate_state=gate_state,
        result_artifact_id=result_id,
        alignment_artifact_ids=[alignment.artifact_id for alignment in alignments],
        alignment_allowed=alignment_allowed,
    )


def validate_source_depth_branch_contract(
    *,
    artifacts: Iterable[Mapping[str, Any] | V02BusinessArtifact],
    control_terminal: SourceDepthControlTerminal,
    control_action: SourceDepthControlAction,
    target_stage: Literal["S02", "S03"] | None,
) -> SourceDepthBranchValidation:
    """Derive exactly one legal source-depth state from validated business facts."""

    relevant_types = frozenset(
        {
            "report.machine",
            "evidence.collection",
            "verdict.collection",
            "source_depth.decision",
            "source_depth.prompt",
            "source_depth.result",
            "source_depth.import_validation",
            "evidence.merged_collection",
            "verdict.rebuilt_collection",
            "report.rebuilt",
            "screening.sync_record",
        }
    )
    by_type: dict[str, V02BusinessArtifact] = {}
    artifact_ids: set[str] = set()
    for raw_artifact in artifacts:
        artifact = (
            raw_artifact
            if isinstance(raw_artifact, BusinessArtifactBase)
            else parse_v02_business_artifact(raw_artifact)
        )
        if artifact.artifact_type not in relevant_types:
            continue
        if artifact.artifact_id in artifact_ids:
            raise Stage5ContractError(
                f"duplicate source-depth Artifact ID: {artifact.artifact_id}"
            )
        if artifact.artifact_type in by_type:
            raise Stage5ContractError(
                "source-depth branch permits exactly one current Artifact of type "
                f"{artifact.artifact_type}"
            )
        artifact_ids.add(artifact.artifact_id)
        by_type[artifact.artifact_type] = artifact

    decisions = by_type.get("source_depth.decision")
    if not isinstance(decisions, SourceDepthDecisionArtifact):
        raise Stage5ContractError(
            "source-depth branch requires exactly one validated decision"
        )
    decision = decisions
    run_ids = {artifact.run_id for artifact in by_type.values()}
    if run_ids != {decision.run_id}:
        raise Stage5ContractError(
            "all source-depth branch Artifacts must belong to one run"
        )

    def require(artifact_type: str, model: type[BusinessArtifactBase]):
        artifact = by_type.get(artifact_type)
        if artifact is None:
            raise Stage5ContractError(
                f"source-depth branch is missing required {artifact_type} Artifact"
            )
        if not isinstance(artifact, model):
            raise Stage5ContractError(
                f"source-depth branch has invalid {artifact_type} business type"
            )
        return artifact

    def forbid(*artifact_types: str) -> None:
        present = sorted(set(artifact_types).intersection(by_type))
        if present:
            raise Stage5ContractError(
                f"source-depth branch contains forbidden Artifacts: {present}"
            )

    def require_upstreams(
        artifact: BusinessArtifactBase, required_ids: Iterable[str]
    ) -> None:
        missing = sorted(set(required_ids) - set(artifact.upstream_artifact_ids))
        if missing:
            raise Stage5ContractError(
                f"{artifact.artifact_type} is missing required upstream Artifacts: {missing}"
            )

    machine_report = require("report.machine", MachineReportArtifact)
    require_upstreams(decision, [machine_report.artifact_id])

    control_tuple = (control_terminal, control_action, target_stage)
    if decision.payload.route == "no_depth":
        if control_tuple != ("COMPLETED", "next_stage", "S03"):
            raise Stage5ContractError(
                "no-depth branch requires COMPLETED/next_stage/S03 control"
            )
        forbid(
            "source_depth.prompt",
            "source_depth.result",
            "source_depth.import_validation",
            "evidence.merged_collection",
            "verdict.rebuilt_collection",
            "report.rebuilt",
        )
        sync = require("screening.sync_record", ScreeningSyncArtifact)
        if (
            sync.payload.source_depth_terminal != "NO_DEPTH"
            or sync.payload.selected_report_kind != "machine"
            or sync.payload.selected_report.artifact_id != machine_report.artifact_id
        ):
            raise Stage5ContractError(
                "no-depth sync must select the exact machine report"
            )
        require_upstreams(sync, [decision.artifact_id, machine_report.artifact_id])
        return SourceDepthBranchValidation(
            run_id=decision.run_id,
            state="NO_DEPTH_COMPLETED",
            control_terminal=control_terminal,
            control_action=control_action,
            target_stage=target_stage,
            decision_artifact_id=decision.artifact_id,
            prompt_artifact_id=None,
            result_artifact_id=None,
            sync_artifact_id=sync.artifact_id,
        )

    prompt = require("source_depth.prompt", SourceDepthPromptArtifact)
    require_upstreams(prompt, [decision.artifact_id])
    if prompt.payload.target_claims != decision.payload.targets:
        raise Stage5ContractError(
            "source-depth prompt targets must exactly match the depth decision"
        )

    state_by_control: dict[tuple[str, str, str | None], SourceDepthBranchState] = {
        ("WAITING_FOR_HUMAN", "wait_for_human", None): "DEPTH_WAITING",
        ("COMPLETED", "return_to_stage", "S02"): "DEPTH_CAPTURED_WAITING_G3",
        ("COMPLETED", "next_stage", "S03"): "DEPTH_IMPORTED_COMPLETED",
        ("FAILED", "terminate", None): "DEPTH_EXTERNAL_EMPTY_FAILED",
    }
    state = state_by_control.get(control_tuple)
    if state is None:
        raise Stage5ContractError(
            "depth branch terminal/action/target_stage does not identify a legal state"
        )

    if state in {"DEPTH_WAITING", "DEPTH_EXTERNAL_EMPTY_FAILED"}:
        forbid(
            "source_depth.result",
            "source_depth.import_validation",
            "evidence.merged_collection",
            "verdict.rebuilt_collection",
            "report.rebuilt",
            "screening.sync_record",
        )
        return SourceDepthBranchValidation(
            run_id=decision.run_id,
            state=state,
            control_terminal=control_terminal,
            control_action=control_action,
            target_stage=target_stage,
            decision_artifact_id=decision.artifact_id,
            prompt_artifact_id=prompt.artifact_id,
            result_artifact_id=None,
            sync_artifact_id=None,
        )

    result = require("source_depth.result", SourceDepthResultArtifact)
    if (
        result.payload.prompt_artifact_id != prompt.artifact_id
        or result.payload.source_depth_request_id
        != prompt.payload.source_depth_request_id
    ):
        raise Stage5ContractError(
            "source-depth result must bind the exact prompt and request"
        )
    require_upstreams(result, [prompt.artifact_id])

    if state == "DEPTH_CAPTURED_WAITING_G3":
        forbid(
            "source_depth.import_validation",
            "evidence.merged_collection",
            "verdict.rebuilt_collection",
            "report.rebuilt",
            "screening.sync_record",
        )
        return SourceDepthBranchValidation(
            run_id=decision.run_id,
            state=state,
            control_terminal=control_terminal,
            control_action=control_action,
            target_stage=target_stage,
            decision_artifact_id=decision.artifact_id,
            prompt_artifact_id=prompt.artifact_id,
            result_artifact_id=result.artifact_id,
            sync_artifact_id=None,
        )

    import_validation = require(
        "source_depth.import_validation", SourceDepthImportValidationArtifact
    )
    base_evidence = require("evidence.collection", EvidenceCollectionArtifact)
    merged_evidence = require(
        "evidence.merged_collection", EvidenceMergedCollectionArtifact
    )
    base_verdict = require("verdict.collection", VerdictCollectionArtifact)
    rebuilt_verdict = require(
        "verdict.rebuilt_collection", VerdictRebuiltCollectionArtifact
    )
    rebuilt_report = require("report.rebuilt", RebuiltReportArtifact)
    sync = require("screening.sync_record", ScreeningSyncArtifact)

    target_claim_ids = {target.claim_id for target in decision.payload.targets}
    if (
        import_validation.payload.source_depth_result_artifact_id != result.artifact_id
        or set(import_validation.payload.mapped_claim_ids) != target_claim_ids
    ):
        raise Stage5ContractError(
            "source-depth import must bind the result and every target claim"
        )
    require_upstreams(import_validation, [result.artifact_id])
    if (
        merged_evidence.payload.base_evidence.artifact_id != base_evidence.artifact_id
        or merged_evidence.payload.import_validation_artifact_id
        != import_validation.artifact_id
    ):
        raise Stage5ContractError(
            "merged evidence must bind the base evidence and import validation"
        )
    require_upstreams(
        merged_evidence, [base_evidence.artifact_id, import_validation.artifact_id]
    )
    if (
        rebuilt_verdict.payload.base_verdict.artifact_id != base_verdict.artifact_id
        or rebuilt_verdict.payload.merged_evidence_artifact_id
        != merged_evidence.artifact_id
    ):
        raise Stage5ContractError(
            "rebuilt verdict must bind the base verdict and merged evidence"
        )
    require_upstreams(
        rebuilt_verdict, [base_verdict.artifact_id, merged_evidence.artifact_id]
    )
    report_input_ids = {
        binding.artifact_id for binding in rebuilt_report.payload.input_bindings
    }
    if rebuilt_verdict.artifact_id not in report_input_ids:
        raise Stage5ContractError(
            "rebuilt report must bind the rebuilt verdict in payload inputs"
        )
    require_upstreams(rebuilt_report, [rebuilt_verdict.artifact_id])
    if (
        sync.payload.source_depth_terminal != "IMPORTED"
        or sync.payload.selected_report_kind != "rebuilt"
        or sync.payload.selected_report.artifact_id != rebuilt_report.artifact_id
        or not target_claim_ids.issubset(set(sync.payload.claim_ids))
    ):
        raise Stage5ContractError(
            "imported sync must select the rebuilt report and all target claims"
        )
    require_upstreams(sync, [decision.artifact_id, rebuilt_report.artifact_id])
    return SourceDepthBranchValidation(
        run_id=decision.run_id,
        state=state,
        control_terminal=control_terminal,
        control_action=control_action,
        target_stage=target_stage,
        decision_artifact_id=decision.artifact_id,
        prompt_artifact_id=prompt.artifact_id,
        result_artifact_id=result.artifact_id,
        sync_artifact_id=sync.artifact_id,
    )


def parse_non_product_domain_review_receipt(
    raw: Mapping[str, Any],
) -> NonProductDomainReviewReceiptV1:
    return _parse(
        "non-product domain review receipt",
        NonProductDomainReviewReceiptV1,
        raw,
    )


def parse_non_product_model_gate_policy(
    raw: Mapping[str, Any],
) -> NonProductModelGatePolicyV1:
    return _parse(
        "non-product model gate policy",
        NonProductModelGatePolicyV1,
        raw,
    )


def parse_manual_external_input_receipt(
    raw: Mapping[str, Any],
) -> ManualExternalInputReceipt:
    return _parse("manual external input receipt", ManualExternalInputReceipt, raw)


def parse_stage5_observation(raw: Mapping[str, Any]) -> Stage5Observation:
    return _parse("Stage 5 observation", Stage5Observation, raw)
