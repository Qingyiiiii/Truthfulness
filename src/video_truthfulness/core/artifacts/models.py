"""Strict data models for Artifact Registry and logical DAG control data."""

from __future__ import annotations

import secrets
import time
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Literal, Mapping, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"
_SENSITIVE_KEYS = {"api_key", "authorization", "cookie", "password", "secret", "token"}
_SENSITIVE_MARKERS = ("authorization:", "bearer ", "cookie:")
StorageRootRef: TypeAlias = Literal["repository", "ubuntu_v02_claim_warehouse"]


def _encode_crockford(value: int, length: int) -> str:
    chars = ["0"] * length
    for index in range(length - 1, -1, -1):
        chars[index] = _CROCKFORD[value & 31]
        value >>= 5
    return "".join(chars)


def new_typed_ulid(prefix: str, timestamp_ms: int | None = None) -> str:
    """Create a lowercase Crockford ULID with an explicit semantic prefix."""

    if not prefix or not prefix.replace("_", "").isalnum() or prefix.lower() != prefix:
        raise ValueError("ID prefix must be lowercase alphanumeric/underscore text.")
    timestamp = int(time.time() * 1000) if timestamp_ms is None else timestamp_ms
    if timestamp < 0 or timestamp >= 1 << 48:
        raise ValueError("ULID timestamp is outside the 48-bit range.")
    value = (timestamp << 80) | secrets.randbits(80)
    return f"{prefix}_{_encode_crockford(value, 26)}"


class UpstreamEntityRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_id: str = Field(min_length=1)
    entity_type: str = Field(min_length=1)
    container_artifact_id: str = Field(pattern=r"^artifact_[0-9a-hjkmnp-tv-z]{26}$")


class _ArtifactRecordBase(BaseModel):
    """Fields and invariants shared by every Registry wire version and view."""

    model_config = ConfigDict(extra="forbid")

    record_id: str = Field(pattern=r"^record_[0-9a-hjkmnp-tv-z]{26}$")
    record_revision: int = Field(ge=1)
    recorded_at: datetime
    previous_record_id: str | None = Field(
        default=None, pattern=r"^record_[0-9a-hjkmnp-tv-z]{26}$"
    )
    previous_record_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    artifact_id: str = Field(pattern=r"^artifact_[0-9a-hjkmnp-tv-z]{26}$")
    artifact_type: str = Field(pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
    logical_name: str = Field(min_length=1, max_length=160)
    container_kind: Literal[
        "file", "directory", "jsonl_container", "entity_index", "decision", "package"
    ]
    project_version: str = Field(pattern=r"^v[0-9]+\.[0-9]+$")
    storage_version: Literal["V01", "V02"]
    source_platform: Literal["youtube", "bilibili"] | None = None
    source_id: str | None = None
    run_id: str | None = Field(default=None, pattern=r"^run_[0-9a-hjkmnp-tv-z]{26}$")
    batch_id: str | None = Field(
        default=None, pattern=r"^batch_[0-9a-hjkmnp-tv-z]{26}$"
    )
    dataset_build_id: str | None = Field(
        default=None, pattern=r"^dataset_build_[0-9a-hjkmnp-tv-z]{26}$"
    )
    dataset_version: str | None = None
    stage_id: str | None = Field(default=None, pattern=r"^S0[1-9]$")
    dag_node_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]*$")

    relative_path: str
    storage_scope: Literal["run", "cross_run"]
    media_type: str = Field(min_length=1, max_length=120)
    size_bytes: int = Field(ge=0)
    content_hash_algorithm: Literal["sha256"] = "sha256"
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    semantic_hash_algorithm: Literal["sha256"] | None = None
    semantic_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    entity_index_artifact_id: str | None = Field(
        default=None, pattern=r"^artifact_[0-9a-hjkmnp-tv-z]{26}$"
    )

    producer_type: Literal[
        "external_source",
        "human",
        "agent",
        "workflow",
        "migration",
        "projection_builder",
    ]
    writer_agent_id: str | None = None
    workflow_id: str | None = None
    workflow_version: str | None = None
    schema_versions: list[str] = Field(default_factory=list)
    prompt_id: str | None = None
    prompt_version: str | None = None
    dag_version: str | None = None
    code_commit: str | None = Field(default=None, pattern=r"^[0-9a-f]{40}$")
    tool_versions: dict[str, str] = Field(default_factory=dict)
    parameters_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    upstream_artifact_ids: list[str] = Field(default_factory=list)
    upstream_entity_refs: list[UpstreamEntityRef] = Field(default_factory=list)
    input_fingerprint: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    authority_level: Literal[
        "raw_source", "human_authoritative", "machine_derived", "projection", "cache"
    ]
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
    validation_status: Literal["not_validated", "passed", "failed", "partial"]
    validation_artifact_ids: list[str] = Field(default_factory=list)
    privacy_class: Literal[
        "private_raw",
        "private_derived",
        "restricted_human",
        "public_synthetic",
        "public_aggregate",
    ]
    access_scope: Literal["local_private", "project_private", "public"]
    retention_policy: str = Field(min_length=1, max_length=160)
    created_at: datetime
    validated_at: datetime | None = None
    frozen_at: datetime | None = None
    archived_at: datetime | None = None
    supersedes: list[str] = Field(default_factory=list)
    change_reason: str | None = Field(default=None, max_length=500)
    metadata_revision_reason: str | None = Field(default=None, max_length=500)

    @field_validator("relative_path")
    @classmethod
    def _relative_posix_path(cls, value: str) -> str:
        if (
            "\\" in value
            or value.startswith("/")
            or (len(value) > 1 and value[1] == ":")
        ):
            raise ValueError(
                "relative_path must be a storage-root-relative POSIX path; "
                "this extends the repository-relative POSIX path rule."
            )
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or not value.strip():
            raise ValueError(
                "relative_path cannot be absolute, blank, or escape its storage scope."
            )
        return value

    @field_validator("source_id")
    @classmethod
    def _source_id_matches_platform(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value.startswith("youtube_") and len(value.removeprefix("youtube_")) == 11:
            return value
        if (
            value.startswith("bilibili_BV")
            and len(value.removeprefix("bilibili_")) == 12
        ):
            return value
        raise ValueError("source_id does not match a supported canonical platform ID.")

    @field_validator("upstream_artifact_ids", "validation_artifact_ids", "supersedes")
    @classmethod
    def _unique_artifact_ids(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("Artifact ID lists must be unique.")
        for value in values:
            if not value.startswith("artifact_") or len(value) != 35:
                raise ValueError(f"Invalid artifact ID: {value}")
        return values

    @model_validator(mode="after")
    def _scope_and_secret_boundary(self) -> "_ArtifactRecordBase":
        if self.storage_scope == "run" and self.run_id is None:
            raise ValueError("run-scoped records require run_id.")
        if self.storage_scope == "cross_run" and not any(
            (
                self.batch_id,
                self.dataset_build_id,
                self.dataset_version,
                getattr(self, "experiment_id", None),
                getattr(self, "exp_id", None),
            )
        ):
            raise ValueError(
                "cross-run records require an explicit cross-run identity field."
            )
        if self.semantic_hash is not None and self.semantic_hash_algorithm is None:
            raise ValueError("semantic_hash requires semantic_hash_algorithm.")
        if self.record_revision == 1 and (
            self.previous_record_id or self.previous_record_hash
        ):
            raise ValueError("Revision 1 cannot point to a previous record.")
        if self.record_revision > 1 and not (
            self.previous_record_id and self.previous_record_hash
        ):
            raise ValueError(
                "Metadata revisions require previous record identity and hash."
            )
        payload = self.model_dump(mode="json")
        _reject_sensitive_material(payload)
        return self


class ArtifactRecord(_ArtifactRecordBase):
    """Historical ``artifact_record_v1.0.0`` wire model; never normalized before hashing."""

    registry_schema_version: Literal["artifact_record_v1.0.0"] = (
        "artifact_record_v1.0.0"
    )
    release_version: str | None = None
    experiment_id: str | None = Field(
        default=None, pattern=r"^experiment_[0-9a-hjkmnp-tv-z]{26}$"
    )
    agent_version: str | None = None


class ArtifactRecordV1_1(_ArtifactRecordBase):
    """Canonical ``artifact_record_v1.1.0`` wire model for new Registry records."""

    registry_schema_version: Literal["artifact_record_v1.1.0"] = (
        "artifact_record_v1.1.0"
    )
    release_id: Literal["truthfulness_v0.2_youtube_video"] = (
        "truthfulness_v0.2_youtube_video"
    )
    exp_id: str | None = Field(default=None, pattern=r"^exp_[0-9a-hjkmnp-tv-z]{26}$")
    agent_profile_version: str | None = Field(
        default=None,
        pattern=r"^[a-z][a-z0-9_]*_agent_v[0-9]+\.[0-9]+\.[0-9]+$",
    )
    agent_runtime_version: str | None = Field(
        default=None, min_length=1, max_length=120
    )


class ArtifactRecordV1_2(_ArtifactRecordBase):
    """Canonical v1.2 wire model with an explicit logical storage-root identity."""

    registry_schema_version: Literal["artifact_record_v1.2.0"] = (
        "artifact_record_v1.2.0"
    )
    release_id: Literal["truthfulness_v0.2_youtube_video"] = (
        "truthfulness_v0.2_youtube_video"
    )
    exp_id: str | None = Field(default=None, pattern=r"^exp_[0-9a-hjkmnp-tv-z]{26}$")
    agent_profile_version: str | None = Field(
        default=None,
        pattern=r"^[a-z][a-z0-9_]*_agent_v[0-9]+\.[0-9]+\.[0-9]+$",
    )
    agent_runtime_version: str | None = Field(
        default=None, min_length=1, max_length=120
    )
    storage_root_ref: StorageRootRef


ArtifactRecordWire: TypeAlias = ArtifactRecord | ArtifactRecordV1_1 | ArtifactRecordV1_2


class ArtifactRecordView(_ArtifactRecordBase):
    """Version-neutral consumer view created only after wire validation succeeds."""

    source_registry_schema_version: Literal[
        "artifact_record_v1.0.0",
        "artifact_record_v1.1.0",
        "artifact_record_v1.2.0",
    ]
    release_id: str | None = None
    exp_id: str | None = Field(default=None, pattern=r"^exp_[0-9a-hjkmnp-tv-z]{26}$")
    agent_profile_version: str | None = Field(
        default=None,
        pattern=r"^[a-z][a-z0-9_]*_agent_v[0-9]+\.[0-9]+\.[0-9]+$",
    )
    agent_runtime_version: str | None = Field(
        default=None, min_length=1, max_length=120
    )
    legacy_experiment_id: str | None = Field(
        default=None,
        pattern=r"^experiment_[0-9a-hjkmnp-tv-z]{26}$",
    )
    storage_root_ref: StorageRootRef = "repository"


def parse_artifact_record(
    value: Mapping[str, Any] | ArtifactRecordWire,
) -> ArtifactRecordWire:
    """Dispatch one Registry wire payload without changing version-specific field names."""

    if isinstance(value, (ArtifactRecord, ArtifactRecordV1_1, ArtifactRecordV1_2)):
        return value
    version = value.get("registry_schema_version")
    if version == "artifact_record_v1.0.0":
        return ArtifactRecord.model_validate(value)
    if version == "artifact_record_v1.1.0":
        return ArtifactRecordV1_1.model_validate(value)
    if version == "artifact_record_v1.2.0":
        return ArtifactRecordV1_2.model_validate(value)
    if version is None:
        raise ValueError("Registry record is missing registry_schema_version.")
    raise ValueError(f"Unsupported registry_schema_version: {version}")


def to_artifact_record_view(record: ArtifactRecordWire) -> ArtifactRecordView:
    """Map an already validated wire record to the canonical read model."""

    payload = record.model_dump(mode="json")
    payload.pop("registry_schema_version")
    if isinstance(record, ArtifactRecord):
        release_id = payload.pop("release_version")
        legacy_experiment_id = payload.pop("experiment_id")
        exp_id = (
            f"exp_{legacy_experiment_id.removeprefix('experiment_')}"
            if legacy_experiment_id is not None
            else None
        )
        agent_runtime_version = payload.pop("agent_version")
        payload.update(
            {
                "source_registry_schema_version": record.registry_schema_version,
                "release_id": release_id,
                "exp_id": exp_id,
                "agent_profile_version": None,
                "agent_runtime_version": agent_runtime_version,
                "legacy_experiment_id": legacy_experiment_id,
                "storage_root_ref": "repository",
            }
        )
    else:
        payload.update(
            {
                "source_registry_schema_version": record.registry_schema_version,
                "legacy_experiment_id": None,
                "storage_root_ref": getattr(
                    record, "storage_root_ref", "repository"
                ),
            }
        )
    return ArtifactRecordView.model_validate(payload)


def _reject_sensitive_material(value: Any, key: str | None = None) -> None:
    if key is not None and key.lower().replace("-", "_") in _SENSITIVE_KEYS:
        raise ValueError(f"Credential-bearing field is forbidden: {key}")
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            _reject_sensitive_material(child_value, str(child_key))
    elif isinstance(value, list):
        for child in value:
            _reject_sensitive_material(child)
    elif isinstance(value, str):
        lowered = value.lower()
        if any(marker in lowered for marker in _SENSITIVE_MARKERS):
            raise ValueError(
                "Credential-like material is forbidden in Registry records."
            )


class EntityLocator(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["json_pointer", "jsonl_line", "time_range", "text_anchor"]
    value: str = Field(min_length=1)


class EntityIndexEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_id: str = Field(min_length=1)
    entity_type: Literal["transcript_segment", "claim", "evidence", "verdict"]
    semantic_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    container_artifact_id: str = Field(pattern=r"^artifact_[0-9a-hjkmnp-tv-z]{26}$")
    upstream_artifact_ids: list[str] = Field(default_factory=list)
    upstream_entity_refs: list[UpstreamEntityRef] = Field(default_factory=list)
    source_locator: EntityLocator


class EntityIndexDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entity_index_schema_version: Literal["entity_index_v1.0.0"] = "entity_index_v1.0.0"
    container_artifact_id: str = Field(pattern=r"^artifact_[0-9a-hjkmnp-tv-z]{26}$")
    created_at: datetime
    entries: list[EntityIndexEntry]

    @model_validator(mode="after")
    def _unique_entities(self) -> "EntityIndexDocument":
        keys = [(entry.entity_type, entry.entity_id) for entry in self.entries]
        if len(keys) != len(set(keys)):
            raise ValueError("Entity index contains duplicate entity identities.")
        if any(
            entry.container_artifact_id != self.container_artifact_id
            for entry in self.entries
        ):
            raise ValueError(
                "Every entity entry must reference the document container Artifact."
            )
        return self


class RetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(ge=1, le=10)
    time_budget_seconds: int | None = Field(default=None, ge=1)
    cost_budget_usd: float | None = Field(default=None, ge=0)


class DAGNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    stage_id: str = Field(pattern=r"^S0[1-9]$")
    name: str = Field(min_length=1)
    node_type: Literal[
        "transform",
        "validation",
        "decision",
        "gate",
        "aggregate",
        "human_action",
        "external_action",
        "terminal",
    ]
    workflow_ref: str
    required_inputs: list[str] = Field(default_factory=list)
    optional_inputs: list[str] = Field(default_factory=list)
    declared_outputs: list[str] = Field(default_factory=list)
    required_outputs: list[str] | None = None
    conditional_outputs: list[str] = Field(default_factory=list)
    entry_condition: str = Field(min_length=1)
    success_condition: str = Field(min_length=1)
    manual_gate: bool
    retry_policy: RetryPolicy
    fallback_nodes: list[str] = Field(default_factory=list)
    failure_terminal: str | None = None
    invalidation_rules: list[str] = Field(default_factory=list)
    next_nodes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _output_contract(self) -> "DAGNode":
        declared = set(self.declared_outputs)
        required = set(self.required_outputs or [])
        conditional = set(self.conditional_outputs)
        if required & conditional:
            raise ValueError("required_outputs and conditional_outputs must be disjoint")
        if not required.issubset(declared) or not conditional.issubset(declared):
            raise ValueError("required/conditional outputs must be declared outputs")
        if self.required_outputs is not None and required | conditional != declared:
            raise ValueError(
                "explicit required/conditional outputs must partition declared_outputs"
            )
        return self


WorkflowVersion: TypeAlias = Literal[
    "youtube_truthfulness_workflow_v1.0.0",
    "youtube_truthfulness_workflow_v1.1.0",
    "youtube_truthfulness_workflow_v1.2.0",
    "youtube_truthfulness_workflow_v1.3.0",
]
StageID: TypeAlias = Literal[
    "S01", "S02", "S03", "S04", "S05", "S06", "S07", "S08", "S09"
]


class DAGDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dag_id: Literal["youtube_truthfulness_dag"]
    dag_version: Literal[
        "youtube_truthfulness_dag_v1.0.0",
        "youtube_truthfulness_dag_v1.1.0",
        "youtube_truthfulness_dag_v1.2.0",
        "youtube_truthfulness_dag_v1.3.0",
        "youtube_truthfulness_dag_v1.4.0",
    ]
    project_version: Literal["v0.2"]
    release_id: Literal["truthfulness_v0.2_youtube_video"]
    source_platform: Literal["youtube"]
    workflow_version: WorkflowVersion
    workflow_versions: dict[StageID, WorkflowVersion] | None = None
    entry_nodes: list[str]
    terminal_nodes: list[str]
    nodes: list[DAGNode]

    @model_validator(mode="after")
    def _unique_node_ids(self) -> "DAGDefinition":
        node_ids = [node.node_id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("DAG node IDs must be unique.")
        expected_pair: dict[str, WorkflowVersion] = {
            "youtube_truthfulness_dag_v1.0.0": "youtube_truthfulness_workflow_v1.0.0",
            "youtube_truthfulness_dag_v1.1.0": "youtube_truthfulness_workflow_v1.1.0",
            "youtube_truthfulness_dag_v1.2.0": "youtube_truthfulness_workflow_v1.3.0",
            "youtube_truthfulness_dag_v1.3.0": "youtube_truthfulness_workflow_v1.3.0",
            "youtube_truthfulness_dag_v1.4.0": "youtube_truthfulness_workflow_v1.3.0",
        }
        if self.workflow_version != expected_pair[self.dag_version]:
            raise ValueError(
                "DAG and workflow versions must use the same compatibility generation."
            )
        if self.dag_version in {
            "youtube_truthfulness_dag_v1.0.0",
            "youtube_truthfulness_dag_v1.1.0",
        }:
            if self.workflow_versions is not None:
                raise ValueError(
                    "DAG v1.0/v1.1 cannot declare stage-scoped workflow_versions."
                )
            expected_refs = {
                node.node_id: f"{node.stage_id}@{self.workflow_version}"
                for node in self.nodes
            }
        else:
            expected_stage_versions: dict[StageID, WorkflowVersion] = {
                "S01": (
                    "youtube_truthfulness_workflow_v1.3.0"
                    if self.dag_version == "youtube_truthfulness_dag_v1.4.0"
                    else (
                        "youtube_truthfulness_workflow_v1.2.0"
                        if self.dag_version == "youtube_truthfulness_dag_v1.3.0"
                        else "youtube_truthfulness_workflow_v1.1.0"
                    )
                ),
                "S02": "youtube_truthfulness_workflow_v1.3.0",
                "S03": "youtube_truthfulness_workflow_v1.1.0",
                "S04": "youtube_truthfulness_workflow_v1.1.0",
                "S05": "youtube_truthfulness_workflow_v1.1.0",
                "S06": "youtube_truthfulness_workflow_v1.1.0",
                "S07": "youtube_truthfulness_workflow_v1.1.0",
                "S08": "youtube_truthfulness_workflow_v1.1.0",
                "S09": "youtube_truthfulness_workflow_v1.1.0",
            }
            if self.workflow_versions != expected_stage_versions:
                raise ValueError(
                    f"{self.dag_version} must declare the authorized stage-scoped workflow_versions."
                )
            if self.dag_version in {
                "youtube_truthfulness_dag_v1.3.0",
                "youtube_truthfulness_dag_v1.4.0",
            }:
                implicit_nodes = [
                    node.node_id for node in self.nodes if node.required_outputs is None
                ]
                if implicit_nodes:
                    raise ValueError(
                        "DAG v1.3+ requires explicit required/conditional outputs: "
                        f"{implicit_nodes}"
                    )
            expected_refs = {
                node.node_id: f"{node.stage_id}@{expected_stage_versions[node.stage_id]}"
                for node in self.nodes
            }
        mismatched_refs = [
            node.node_id
            for node in self.nodes
            if node.workflow_ref != expected_refs[node.node_id]
        ]
        if mismatched_refs:
            raise ValueError(
                f"DAG nodes use a mismatched workflow_ref version: {mismatched_refs}"
            )
        return self

    def workflow_version_for_stage(self, stage_id: StageID) -> WorkflowVersion:
        """Return the exact Workflow generation authorized for one DAG stage."""

        if self.workflow_versions is None:
            return self.workflow_version
        return self.workflow_versions[stage_id]
