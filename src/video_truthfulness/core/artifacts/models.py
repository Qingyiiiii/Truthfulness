"""Strict data models for Artifact Registry and logical DAG control data."""

from __future__ import annotations

import secrets
import time
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"
_SENSITIVE_KEYS = {"api_key", "authorization", "cookie", "password", "secret", "token"}
_SENSITIVE_MARKERS = ("authorization:", "bearer ", "cookie:")


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


class ArtifactRecord(BaseModel):
    """One complete append-only metadata snapshot for an Artifact."""

    model_config = ConfigDict(extra="forbid")

    record_id: str = Field(pattern=r"^record_[0-9a-hjkmnp-tv-z]{26}$")
    registry_schema_version: Literal["artifact_record_v1.0.0"] = "artifact_record_v1.0.0"
    record_revision: int = Field(ge=1)
    recorded_at: datetime
    previous_record_id: str | None = Field(default=None, pattern=r"^record_[0-9a-hjkmnp-tv-z]{26}$")
    previous_record_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    artifact_id: str = Field(pattern=r"^artifact_[0-9a-hjkmnp-tv-z]{26}$")
    artifact_type: str = Field(pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
    logical_name: str = Field(min_length=1, max_length=160)
    container_kind: Literal["file", "directory", "jsonl_container", "entity_index", "decision", "package"]
    project_version: str = Field(pattern=r"^v[0-9]+\.[0-9]+$")
    storage_version: Literal["V01", "V02"]
    release_version: str | None = None
    source_platform: Literal["youtube", "bilibili"] | None = None
    source_id: str | None = None
    run_id: str | None = Field(default=None, pattern=r"^run_[0-9a-hjkmnp-tv-z]{26}$")
    batch_id: str | None = Field(default=None, pattern=r"^batch_[0-9a-hjkmnp-tv-z]{26}$")
    dataset_build_id: str | None = Field(default=None, pattern=r"^dataset_build_[0-9a-hjkmnp-tv-z]{26}$")
    dataset_version: str | None = None
    experiment_id: str | None = Field(default=None, pattern=r"^experiment_[0-9a-hjkmnp-tv-z]{26}$")
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
    entity_index_artifact_id: str | None = Field(default=None, pattern=r"^artifact_[0-9a-hjkmnp-tv-z]{26}$")

    producer_type: Literal["external_source", "human", "agent", "workflow", "migration", "projection_builder"]
    writer_agent_id: str | None = None
    agent_version: str | None = None
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

    authority_level: Literal["raw_source", "human_authoritative", "machine_derived", "projection", "cache"]
    lifecycle_state: Literal["created", "validated", "frozen", "stale", "superseded", "invalid", "archived", "purged"]
    validation_status: Literal["not_validated", "passed", "failed", "partial"]
    validation_artifact_ids: list[str] = Field(default_factory=list)
    privacy_class: Literal["private_raw", "private_derived", "restricted_human", "public_synthetic", "public_aggregate"]
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
        if "\\" in value or value.startswith("/") or (len(value) > 1 and value[1] == ":"):
            raise ValueError("relative_path must be a repository-relative POSIX path.")
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts or not value.strip():
            raise ValueError("relative_path cannot be absolute, blank, or escape its storage scope.")
        return value

    @field_validator("source_id")
    @classmethod
    def _source_id_matches_platform(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value.startswith("youtube_") and len(value.removeprefix("youtube_")) == 11:
            return value
        if value.startswith("bilibili_BV") and len(value.removeprefix("bilibili_")) == 12:
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
    def _scope_and_secret_boundary(self) -> "ArtifactRecord":
        if self.storage_scope == "run" and self.run_id is None:
            raise ValueError("run-scoped records require run_id.")
        if self.storage_scope == "cross_run" and not any(
            (self.batch_id, self.dataset_build_id, self.dataset_version, self.experiment_id)
        ):
            raise ValueError("cross-run records require an explicit cross-run identity field.")
        if self.semantic_hash is not None and self.semantic_hash_algorithm is None:
            raise ValueError("semantic_hash requires semantic_hash_algorithm.")
        if self.record_revision == 1 and (self.previous_record_id or self.previous_record_hash):
            raise ValueError("Revision 1 cannot point to a previous record.")
        if self.record_revision > 1 and not (self.previous_record_id and self.previous_record_hash):
            raise ValueError("Metadata revisions require previous record identity and hash.")
        payload = self.model_dump(mode="json")
        _reject_sensitive_material(payload)
        return self


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
            raise ValueError("Credential-like material is forbidden in Registry records.")


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
        if any(entry.container_artifact_id != self.container_artifact_id for entry in self.entries):
            raise ValueError("Every entity entry must reference the document container Artifact.")
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
    node_type: Literal["transform", "validation", "decision", "gate", "aggregate", "human_action", "external_action", "terminal"]
    workflow_ref: str
    required_inputs: list[str] = Field(default_factory=list)
    optional_inputs: list[str] = Field(default_factory=list)
    declared_outputs: list[str] = Field(default_factory=list)
    entry_condition: str = Field(min_length=1)
    success_condition: str = Field(min_length=1)
    manual_gate: bool
    retry_policy: RetryPolicy
    fallback_nodes: list[str] = Field(default_factory=list)
    failure_terminal: str | None = None
    invalidation_rules: list[str] = Field(default_factory=list)
    next_nodes: list[str] = Field(default_factory=list)


class DAGDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dag_id: Literal["youtube_truthfulness_dag"]
    dag_version: Literal["youtube_truthfulness_dag_v1.0.0"]
    project_version: Literal["v0.2"]
    release_id: Literal["truthfulness_v0.2_youtube_video"]
    source_platform: Literal["youtube"]
    workflow_version: Literal["youtube_truthfulness_workflow_v1.0.0"]
    entry_nodes: list[str]
    terminal_nodes: list[str]
    nodes: list[DAGNode]

    @model_validator(mode="after")
    def _unique_node_ids(self) -> "DAGDefinition":
        node_ids = [node.node_id for node in self.nodes]
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("DAG node IDs must be unique.")
        return self
