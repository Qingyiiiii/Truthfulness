"""Strict contracts for the V02 Claim warehouse projection.

The immutable JSON export packages and append-only Registry remain authoritative.
Objects in this module describe deterministic projection inputs and loader facts;
they deliberately do not define the S01 business Artifact envelope.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, ClassVar, Iterable, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .business_models import (
    AtomicClaimRevisionV1_2,
    ClaimDependencyV1_2,
    ClaimEvidenceLinkV1_2,
    ClaimSplitSetRevisionV1_2,
    ClaimTextChunkV1_2,
    EvidenceAvailabilityV1_2,
    EvidenceRevisionV1_2,
    HumanClaimInclusionDecisionV1_2,
    HumanGoldLabelV1_2,
    MachineClaimAssessmentV1_2,
    MachineInclusionRecommendationV1_2,
    ParentClaimRevisionV1_2,
    RetrievalAttemptV1_2,
    SplitSetMemberV1_2,
)


DATABASE_SCHEMA_VERSION = "truthfulness_db_v02.1.0"
LABEL_TAXONOMY_VERSION = "truthfulness_taxonomy_v02.1.0"
WAREHOUSE_EXPORT_SCHEMA_VERSION = "claim_warehouse_export_v1.0.0"
WAREHOUSE_PROJECTION_VERSION = "claim_warehouse_projection_v1.0.0"
WAREHOUSE_ROW_SCHEMA_VERSION = "claim_warehouse_row_v1.0.0"
WAREHOUSE_LOAD_PLAN_SCHEMA_VERSION = "claim_warehouse_load_plan_v1.0.0"
WAREHOUSE_LOAD_BATCH_SCHEMA_VERSION = "claim_warehouse_load_batch_v1.0.0"
WAREHOUSE_LOAD_RECEIPT_SCHEMA_VERSION = "claim_warehouse_load_receipt_v1.0.0"
WAREHOUSE_PROJECTION_ATTEMPT_SCHEMA_VERSION = (
    "claim_warehouse_projection_attempt_v1.0.0"
)
WAREHOUSE_PROJECTION_SESSION_SCHEMA_VERSION = (
    "claim_warehouse_projection_session_v1.0.0"
)
WAREHOUSE_PROJECTION_JOURNAL_SCHEMA_VERSION = (
    "claim_warehouse_projection_journal_entry_v1.0.0"
)
WAREHOUSE_PROJECTION_CHECKPOINT_SCHEMA_VERSION = (
    "claim_warehouse_projection_checkpoint_v1.0.0"
)
WAREHOUSE_MIGRATION_RESULT_SCHEMA_VERSION = "claim_warehouse_migration_result_v1.0.0"
WAREHOUSE_MIGRATION_ROLLBACK_SCHEMA_VERSION = "claim_warehouse_migration_rollback_v1.0.0"
CLAIM_WAREHOUSE_STORAGE_ROOT_REF = "ubuntu_v02_claim_warehouse"

SHA256_PATTERN = r"^[0-9a-f]{64}$"
UTC_TIMESTAMP_PATTERN = (
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]+)?Z$"
)
TYPED_ULID_PATTERN = r"^[a-z][a-z0-9_]*_[0-9a-hjkmnp-tv-z]{26}$"
EXPORT_ID_PATTERN = r"^export_[0-9a-hjkmnp-tv-z]{26}$"
LOAD_PLAN_ID_PATTERN = r"^load_plan_[0-9a-hjkmnp-tv-z]{26}$"
LOAD_BATCH_ID_PATTERN = r"^load_batch_[0-9a-hjkmnp-tv-z]{26}$"
LOAD_RECEIPT_ID_PATTERN = r"^load_receipt_[0-9a-hjkmnp-tv-z]{26}$"
ATTEMPT_ID_PATTERN = r"^attempt_[0-9a-hjkmnp-tv-z]{26}$"
SESSION_ID_PATTERN = r"^session_[0-9a-hjkmnp-tv-z]{26}$"
CHECKPOINT_ID_PATTERN = r"^checkpoint_[0-9a-hjkmnp-tv-z]{26}$"
ARTIFACT_ID_PATTERN = r"^artifact_[0-9a-hjkmnp-tv-z]{26}$"
RECORD_ID_PATTERN = r"^record_[0-9a-hjkmnp-tv-z]{26}$"
RUN_ID_PATTERN = r"^run_[0-9a-hjkmnp-tv-z]{26}$"
SOURCE_ID_PATTERN = r"^(?:youtube_[A-Za-z0-9_-]{11}|bilibili_BV[A-Za-z0-9]{10})$"
CLAIM_ID_PATTERN = r"^claim_[0-9a-hjkmnp-tv-z]{26}$"
PARENT_REVISION_ID_PATTERN = r"^parent_claim_revision_[0-9a-hjkmnp-tv-z]{26}$"
ATOMIC_REVISION_ID_PATTERN = r"^atomic_claim_revision_[0-9a-hjkmnp-tv-z]{26}$"
SPLIT_REVISION_ID_PATTERN = r"^claim_split_revision_[0-9a-hjkmnp-tv-z]{26}$"
EVIDENCE_ID_PATTERN = r"^evidence_[0-9a-hjkmnp-tv-z]{26}$"
EVIDENCE_REVISION_ID_PATTERN = r"^evidence_revision_[0-9a-hjkmnp-tv-z]{26}$"
ANNOTATION_TASK_ID_PATTERN = r"^annotation_task_[0-9a-hjkmnp-tv-z]{26}$"
ANNOTATION_ID_PATTERN = r"^annotation_[0-9a-hjkmnp-tv-z]{26}$"

LogicalLayer = Literal[
    "core_provenance",
    "machine_screening",
    "source_depth",
    "human_annotation",
    "analytics_mart",
]
ProjectionStage = Literal[
    "load_plan",
    "export_validate",
    "parquet_staging",
    "parquet_validate",
    "parquet_publish",
    "duckdb_transaction",
    "receipt_publish",
    "registry_append",
]

PROJECTION_STAGE_ORDER: tuple[ProjectionStage, ...] = (
    "load_plan",
    "export_validate",
    "parquet_staging",
    "parquet_validate",
    "parquet_publish",
    "duckdb_transaction",
    "receipt_publish",
    "registry_append",
)

LOGICAL_LAYER_ORDER: dict[str, int] = {
    "core_provenance": 0,
    "machine_screening": 1,
    "source_depth": 2,
    "human_annotation": 3,
    "analytics_mart": 4,
}

ALLOWED_TABLE_CODES = frozenset(
    {
        "source_media",
        "run",
        "claim_collection",
        "source_artifact_ref",
        "source_segment",
        "parent_claim",
        "parent_claim_revision",
        "parent_claim_text_chunk",
        "claim_source_span",
        "claim_split_set_revision",
        "atomic_claim",
        "atomic_claim_revision",
        "atomic_claim_text_chunk",
        "split_set_member",
        "claim_dependency",
        "claim_replacement_edge",
        "evidence_item",
        "evidence_revision",
        "claim_evidence_link",
        "retrieval_batch",
        "retrieval_attempt",
        "evidence_availability",
        "machine_assessment_batch",
        "machine_claim_assessment",
        "source_depth_assessment",
        "machine_inclusion_recommendation",
        "human_claim_inclusion_decision",
        "annotation_task",
        "human_annotation",
        "adjudication",
        "gold_label",
        "taxonomy_version_registry",
        "warehouse_import_batch",
        "audit_change_history",
    }
)

CONTROL_LEDGER_TABLE_CODES = frozenset(
    {
        "warehouse_export",
        "export_publication_journal",
        "warehouse_load_plan",
        "warehouse_load_batch",
        "warehouse_loaded_export",
        "warehouse_projection_attempt",
        "warehouse_load_receipt",
        "warehouse_watermark",
    }
)

WAREHOUSE_ENTITY_CODES = ALLOWED_TABLE_CODES | CONTROL_LEDGER_TABLE_CODES
if len(ALLOWED_TABLE_CODES) != 34 or len(CONTROL_LEDGER_TABLE_CODES) != 8:
    raise AssertionError("the frozen warehouse catalog must contain exactly 34 + 8 entities")
if len(WAREHOUSE_ENTITY_CODES) != 42:
    raise AssertionError("the frozen warehouse catalog must contain exactly 42 entities")

ROW_STABLE_REVISION_TABLE_CODES = frozenset({"source_media"})

EXACT_SCALE_COUNTS: dict[str, int] = {
    "distinct_source_ids": 501,
    "temporary_synthetic_contract_runs": 501,
    "s01_machine_export_packages": 501,
    "parent_claims": 2_004,
    "atomic_claims": 5_010,
    "evidence_items": 7_515,
    "claim_evidence_links": 10_020,
    "initial_machine_verdicts": 5_010,
    "source_depth_synthetic_exports": 167,
    "rebuilt_verdicts": 1_670,
    "human_annotation_synthetic_exports": 251,
    "human_annotation_revisions": 2_510,
    "gold_label_revisions": 2_510,
    "total_export_packages": 919,
    "load_batches": 10,
}

_CROCKFORD = "0123456789abcdefghjkmnpqrstvwxyz"
_SENSITIVE_KEYS = {"api_key", "authorization", "cookie", "password", "secret", "token"}
_PATH_KEYS = {
    "path",
    "relative_path",
    "manifest_relative_path",
    "rows_relative_path",
    "artifact_relative_path",
}


class WarehouseContractError(ValueError):
    """Raised when immutable warehouse input violates a frozen contract."""


class WarehouseConflictError(WarehouseContractError):
    """Raised when an immutable identity is reused with different bytes."""


class WarehouseDependencyUnavailable(RuntimeError):
    """Raised when an explicitly required optional analytical dependency is absent."""


def canonical_json_bytes(value: Any) -> bytes:
    """Return strict canonical UTF-8 JSON without a trailing line feed."""

    _validate_json_value(value)
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def embedded_hash(value: Mapping[str, Any], hash_field: str) -> str:
    payload = dict(value)
    if hash_field not in payload:
        raise WarehouseContractError(f"missing embedded hash field: {hash_field}")
    payload.pop(hash_field)
    return sha256_bytes(canonical_json_bytes(payload))


def deterministic_typed_id(prefix: str, seed: str) -> str:
    """Create a deterministic 26-character Crockford identifier for fixtures/plans."""

    if not re.fullmatch(r"[a-z][a-z0-9_]*", prefix):
        raise WarehouseContractError("typed ID prefix must be lowercase snake case")
    value = int.from_bytes(hashlib.sha256(seed.encode("utf-8")).digest(), "big")
    value &= (1 << 130) - 1
    chars = ["0"] * 26
    for index in range(25, -1, -1):
        chars[index] = _CROCKFORD[value & 31]
        value >>= 5
    return f"{prefix}_{''.join(chars)}"


def compute_export_idempotency_key(
    *,
    run_id: str,
    source_registry_head_record_id: str | None,
    ordered_input_artifact_record_ids: Sequence[str],
    taxonomy_versions: Mapping[str, str],
    exporter_versions: Mapping[str, str],
) -> str:
    """Hash exactly the frozen S01 export replay identity inputs."""

    return sha256_bytes(
        canonical_json_bytes(
            {
                "run_id": run_id,
                "source_registry_head_record_id": source_registry_head_record_id,
                "ordered_input_artifact_record_ids": list(
                    ordered_input_artifact_record_ids
                ),
                "warehouse_export_schema_version": WAREHOUSE_EXPORT_SCHEMA_VERSION,
                "taxonomy_versions": dict(sorted(taxonomy_versions.items())),
                "exporter_versions": dict(sorted(exporter_versions.items())),
            }
        )
    )


def _validate_utc(value: str) -> str:
    if re.fullmatch(UTC_TIMESTAMP_PATTERN, value) is None:
        raise WarehouseContractError("timestamp must be RFC 3339 UTC with Z suffix")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise WarehouseContractError("timestamp must be a real UTC instant") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise WarehouseContractError("timestamp must use UTC")
    return value


def _validate_relative_posix(value: str) -> str:
    if not value or not value.strip() or "\\" in value or value.startswith("/"):
        raise WarehouseContractError("path must be a non-empty relative POSIX path")
    if len(value) > 1 and value[1] == ":":
        raise WarehouseContractError("Windows drive paths are forbidden")
    if value.startswith("//"):
        raise WarehouseContractError("UNC paths are forbidden")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path == PurePosixPath("."):
        raise WarehouseContractError("path cannot escape its logical storage root")
    return value


def _validate_json_value(value: Any, *, key: str | None = None) -> None:
    if key is not None and key.lower().replace("-", "_") in _SENSITIVE_KEYS:
        raise WarehouseContractError(f"credential-bearing field is forbidden: {key}")
    if value is None or isinstance(value, (str, bool, int)):
        if key is not None and key.lower() in _PATH_KEYS and isinstance(value, str):
            _validate_relative_posix(value)
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise WarehouseContractError("NaN and Infinity are forbidden")
        return
    if isinstance(value, Mapping):
        for child_key, child_value in value.items():
            if not isinstance(child_key, str):
                raise WarehouseContractError("JSON object keys must be strings")
            _validate_json_value(child_value, key=child_key)
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            _validate_json_value(child)
        return
    raise WarehouseContractError(
        f"value is not strict JSON-compatible: {type(value).__name__}"
    )


class StrictWarehouseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    _hash_field: ClassVar[str | None] = None

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class ExternalStorageRef(StrictWarehouseModel):
    storage_root_ref: str = Field(pattern=r"^[a-z][a-z0-9_]{2,63}$")
    relative_path: str = Field(min_length=1, max_length=1024)

    @field_validator("relative_path")
    @classmethod
    def _safe_relative_path(cls, value: str) -> str:
        return _validate_relative_posix(value)


class RegistryPrefix(StrictWarehouseModel):
    record_count: int = Field(ge=0)
    prefix_hash: str = Field(pattern=SHA256_PATTERN)
    head_record_id: str | None = Field(default=None, pattern=RECORD_ID_PATTERN)
    head_record_hash: str | None = Field(default=None, pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def _complete_prefix(self) -> "RegistryPrefix":
        populated = self.head_record_id is not None and self.head_record_hash is not None
        if self.record_count == 0 and (
            self.head_record_id is not None or self.head_record_hash is not None
        ):
            raise WarehouseContractError("empty Registry prefix cannot have a head")
        if self.record_count == 0 and self.prefix_hash != sha256_bytes(b""):
            raise WarehouseContractError("empty Registry prefix hash must bind zero bytes")
        if self.record_count > 0 and not populated:
            raise WarehouseContractError("non-empty Registry prefix requires ID and hash")
        return self


class InputArtifactRef(StrictWarehouseModel):
    artifact_id: str = Field(pattern=ARTIFACT_ID_PATTERN)
    record_id: str = Field(pattern=RECORD_ID_PATTERN)
    artifact_type: str = Field(pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
    content_hash: str = Field(pattern=SHA256_PATTERN)


class WarehouseTableData(StrictWarehouseModel):
    """Base for strict, table-specific analytical row payloads."""


class SourceMediaData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    platform: Literal["youtube", "bilibili"]
    platform_source_key: str = Field(min_length=1, max_length=160)
    media_kind: Literal["video"] = "video"
    synthetic: bool

    @model_validator(mode="after")
    def _platform_matches_source(self) -> "SourceMediaData":
        if self.platform == "youtube" and not self.source_id.startswith("youtube_"):
            raise WarehouseContractError("YouTube source_id/platform mismatch")
        if self.platform == "bilibili" and not self.source_id.startswith("bilibili_"):
            raise WarehouseContractError("Bilibili source_id/platform mismatch")
        return self


class RunData(WarehouseTableData):
    run_id: str = Field(pattern=RUN_ID_PATTERN)
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    execution_plan_id: str = Field(
        pattern=r"^execution_plan_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_registry_prefix: RegistryPrefix
    run_created_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    execution_scope: str = Field(pattern=r"^[a-z][a-z0-9_]{2,95}$")
    synthetic: bool

    @field_validator("run_created_at")
    @classmethod
    def _utc(cls, value: str) -> str:
        return _validate_utc(value)


class SourceArtifactRefData(WarehouseTableData):
    source_artifact_ref_id: str = Field(
        pattern=r"^source_artifact_ref_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    referenced_artifact_id: str = Field(pattern=ARTIFACT_ID_PATTERN)
    referenced_record_id: str = Field(pattern=RECORD_ID_PATTERN)
    artifact_type: str = Field(pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
    content_hash: str = Field(pattern=SHA256_PATTERN)
    storage_root_ref: str = Field(pattern=r"^[a-z][a-z0-9_]{2,63}$")
    relative_path: str = Field(min_length=1, max_length=1024)

    @field_validator("relative_path")
    @classmethod
    def _relative(cls, value: str) -> str:
        return _validate_relative_posix(value)


class ParentClaimData(WarehouseTableData):
    parent_claim_id: str = Field(pattern=CLAIM_ID_PATTERN)
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    display_no: int = Field(ge=1)


class ParentClaimRevisionData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    revision: ParentClaimRevisionV1_2


class ClaimTextChunkData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    chunk: ClaimTextChunkV1_2


class ClaimSplitSetRevisionData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    split_set: ClaimSplitSetRevisionV1_2


class AtomicClaimData(WarehouseTableData):
    atomic_claim_id: str = Field(pattern=CLAIM_ID_PATTERN)
    parent_claim_id: str = Field(pattern=CLAIM_ID_PATTERN)
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)

    @model_validator(mode="after")
    def _not_parent(self) -> "AtomicClaimData":
        if self.atomic_claim_id == self.parent_claim_id:
            raise WarehouseContractError("atomic Claim cannot equal its parent")
        return self


class AtomicClaimRevisionData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    revision: AtomicClaimRevisionV1_2


class SplitSetMemberData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    parent_claim_id: str = Field(pattern=CLAIM_ID_PATTERN)
    split_revision_id: str = Field(pattern=SPLIT_REVISION_ID_PATTERN)
    writer_role: Literal["claim_splitter", "authorized_human"]
    member: SplitSetMemberV1_2


class EvidenceItemData(WarehouseTableData):
    evidence_id: str = Field(pattern=EVIDENCE_ID_PATTERN)
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    canonical_locator_hash: str = Field(pattern=SHA256_PATTERN)


class EvidenceRevisionData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    revision: EvidenceRevisionV1_2


class ClaimEvidenceLinkData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    link: ClaimEvidenceLinkV1_2


class RetrievalBatchData(WarehouseTableData):
    retrieval_batch_id: str = Field(
        pattern=r"^retrieval_batch_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    run_id: str = Field(pattern=RUN_ID_PATTERN)
    phase: Literal["initial_machine", "source_depth"]
    query_hash: str = Field(pattern=SHA256_PATTERN)
    config_hash: str = Field(pattern=SHA256_PATTERN)
    batch_closed: Literal[True]
    writer_role: Literal["retrieval_batch_closer"]


class MachineAssessmentBatchData(WarehouseTableData):
    assessment_batch_id: str = Field(
        pattern=r"^machine_assessment_batch_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    phase: Literal["initial_machine", "source_depth"]
    model_version: str = Field(min_length=1, max_length=160)
    prompt_version: str = Field(min_length=1, max_length=160)
    config_hash: str = Field(pattern=SHA256_PATTERN)


class MachineClaimAssessmentData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    phase: Literal["initial_machine"] = "initial_machine"
    label_namespace: Literal["machine_candidate"] = "machine_candidate"
    assessment_batch_id: str = Field(
        pattern=r"^machine_assessment_batch_[0-9a-hjkmnp-tv-z]{26}$"
    )
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None, pattern=r"^assessment_revision_[0-9a-hjkmnp-tv-z]{26}$"
    )
    assessment: MachineClaimAssessmentV1_2

    @model_validator(mode="after")
    def _revision(self) -> "MachineClaimAssessmentData":
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise WarehouseContractError("machine assessment revision/supersedes mismatch")
        return self


class SourceDepthAssessmentData(WarehouseTableData):
    assessment_revision_id: str = Field(
        pattern=r"^source_depth_assessment_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    atomic_revision_id: str = Field(pattern=ATOMIC_REVISION_ID_PATTERN)
    retrieval_batch_id: str = Field(
        pattern=r"^retrieval_batch_[0-9a-hjkmnp-tv-z]{26}$"
    )
    base_assessment_revision_id: str = Field(
        pattern=r"^assessment_revision_[0-9a-hjkmnp-tv-z]{26}$"
    )
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None,
        pattern=r"^source_depth_assessment_[0-9a-hjkmnp-tv-z]{26}$",
    )
    label_namespace: Literal["machine_candidate"] = "machine_candidate"
    rebuilt_verdict: Literal[
        "supported", "refuted", "mixed", "insufficient", "unverifiable"
    ]
    reason: str = Field(min_length=1, max_length=10_000)
    uncertainty: Literal["low", "medium", "high"]
    writer_role: Literal["source_depth_assessor"]

    @model_validator(mode="after")
    def _revision(self) -> "SourceDepthAssessmentData":
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise WarehouseContractError("source-depth revision/supersedes mismatch")
        return self


class AnnotationTaskData(WarehouseTableData):
    annotation_task_id: str = Field(pattern=ANNOTATION_TASK_ID_PATTERN)
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    target_kind: Literal[
        "atomic_claim_revision", "parent_claim_revision"
    ] = "atomic_claim_revision"
    atomic_revision_id: str | None = Field(
        default=None, pattern=ATOMIC_REVISION_ID_PATTERN
    )
    parent_revision_id: str | None = Field(
        default=None, pattern=PARENT_REVISION_ID_PATTERN
    )
    state: Literal["approved"]
    synthetic: bool

    @model_validator(mode="after")
    def _target_xor(self) -> "AnnotationTaskData":
        atomic = self.atomic_revision_id is not None
        parent = self.parent_revision_id is not None
        if atomic == parent:
            raise WarehouseContractError(
                "annotation task requires atomic/parent target XOR"
            )
        if self.target_kind == "atomic_claim_revision" and not atomic:
            raise WarehouseContractError("atomic annotation task target is missing")
        if self.target_kind == "parent_claim_revision" and not parent:
            raise WarehouseContractError("parent annotation task target is missing")
        return self

    @property
    def target_revision_id(self) -> str:
        return str(self.atomic_revision_id or self.parent_revision_id)


GoldCode = Literal[
    "gold_supports",
    "gold_refutes",
    "gold_partially_supports",
    "gold_misleading",
    "gold_missing_context",
    "gold_insufficient_evidence",
    "gold_uncheckable",
]


class HumanAnnotationData(WarehouseTableData):
    annotation_id: str = Field(pattern=ANNOTATION_ID_PATTERN)
    annotation_task_id: str = Field(pattern=ANNOTATION_TASK_ID_PATTERN)
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    target_kind: Literal[
        "atomic_claim_revision", "parent_claim_revision"
    ] = "atomic_claim_revision"
    atomic_revision_id: str | None = Field(
        default=None, pattern=ATOMIC_REVISION_ID_PATTERN
    )
    parent_revision_id: str | None = Field(
        default=None, pattern=PARENT_REVISION_ID_PATTERN
    )
    label_namespace: Literal["human_canonical"] = "human_canonical"
    verdict: GoldCode
    reason: str = Field(min_length=1, max_length=10_000)
    evidence_link_ids: list[str]
    revision_no: int = Field(ge=1)
    supersedes_annotation_id: str | None = Field(
        default=None, pattern=ANNOTATION_ID_PATTERN
    )
    synthetic: bool
    writer_role: Literal["authorized_human"]

    @field_validator("evidence_link_ids")
    @classmethod
    def _unique_links(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise WarehouseContractError("human annotation Evidence links must be unique")
        return values

    @model_validator(mode="after")
    def _revision(self) -> "HumanAnnotationData":
        atomic = self.atomic_revision_id is not None
        parent = self.parent_revision_id is not None
        if atomic == parent:
            raise WarehouseContractError(
                "human annotation requires atomic/parent target XOR"
            )
        if self.target_kind == "atomic_claim_revision" and not atomic:
            raise WarehouseContractError("atomic human annotation target is missing")
        if self.target_kind == "parent_claim_revision" and not parent:
            raise WarehouseContractError("parent human annotation target is missing")
        if parent and self.verdict not in {"gold_misleading", "gold_missing_context"}:
            raise WarehouseContractError(
                "parent context annotation only permits misleading/missing-context"
            )
        if (self.revision_no == 1) != (self.supersedes_annotation_id is None):
            raise WarehouseContractError("human annotation revision/supersedes mismatch")
        return self

    @property
    def target_revision_id(self) -> str:
        return str(self.atomic_revision_id or self.parent_revision_id)


class GoldLabelData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    annotation_id: str = Field(pattern=ANNOTATION_ID_PATTERN)
    label_namespace: Literal["human_canonical"] = "human_canonical"
    revision_no: int = Field(ge=1)
    supersedes_gold_revision_id: str | None = Field(
        default=None, pattern=r"^gold_revision_[0-9a-hjkmnp-tv-z]{26}$"
    )
    gold: HumanGoldLabelV1_2
    synthetic: bool

    @model_validator(mode="after")
    def _revision(self) -> "GoldLabelData":
        if (self.revision_no == 1) != (self.supersedes_gold_revision_id is None):
            raise WarehouseContractError("Gold revision/supersedes mismatch")
        return self


class ClaimCollectionData(WarehouseTableData):
    claim_collection_id: str = Field(
        pattern=r"^claim_collection_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    run_id: str = Field(pattern=RUN_ID_PATTERN)
    collection_revision_no: int = Field(ge=1)
    supersedes_collection_id: str | None = Field(
        default=None, pattern=r"^claim_collection_[0-9a-hjkmnp-tv-z]{26}$"
    )
    transcript_artifact_id: str = Field(pattern=ARTIFACT_ID_PATTERN)
    writer_role: Literal["claim_extractor"]

    @model_validator(mode="after")
    def _revision(self) -> "ClaimCollectionData":
        if (self.collection_revision_no == 1) != (
            self.supersedes_collection_id is None
        ):
            raise WarehouseContractError("Claim collection revision/supersedes mismatch")
        return self


class SourceSegmentData(WarehouseTableData):
    source_segment_id: str = Field(
        pattern=r"^source_segment_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    source_artifact_ref_id: str = Field(
        pattern=r"^source_artifact_ref_[0-9a-hjkmnp-tv-z]{26}$"
    )
    segment_kind: Literal["transcript", "ocr"]
    start_ms: int = Field(ge=0)
    end_ms: int = Field(gt=0)
    text_sha256: str = Field(pattern=SHA256_PATTERN)
    writer_role: Literal["source_segmenter"]

    @model_validator(mode="after")
    def _range(self) -> "SourceSegmentData":
        if self.end_ms <= self.start_ms:
            raise WarehouseContractError("source segment end_ms must follow start_ms")
        return self


class ClaimSourceSpanData(WarehouseTableData):
    claim_source_span_id: str = Field(
        pattern=r"^claim_source_span_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    claim_revision_kind: Literal["parent_claim_revision", "atomic_claim_revision"]
    claim_revision_id: str = Field(
        pattern=r"^(?:parent_claim_revision|atomic_claim_revision)_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_segment_id: str = Field(
        pattern=r"^source_segment_[0-9a-hjkmnp-tv-z]{26}$"
    )
    start_ms: int = Field(ge=0)
    end_ms: int = Field(gt=0)
    writer_role: Literal["claim_extractor", "claim_splitter", "authorized_human"]

    @model_validator(mode="after")
    def _kind_and_range(self) -> "ClaimSourceSpanData":
        if not self.claim_revision_id.startswith(f"{self.claim_revision_kind}_"):
            raise WarehouseContractError("Claim source span kind/revision mismatch")
        if self.end_ms <= self.start_ms:
            raise WarehouseContractError("Claim source span end_ms must follow start_ms")
        return self


class ClaimDependencyData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    dependency: ClaimDependencyV1_2


class ClaimReplacementEdgeData(WarehouseTableData):
    replacement_edge_id: str = Field(
        pattern=r"^claim_replacement_edge_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    old_claim_id: str = Field(pattern=CLAIM_ID_PATTERN)
    new_claim_id: str = Field(pattern=CLAIM_ID_PATTERN)
    replacement_type: Literal["semantic_identity_change"]
    reason: str = Field(min_length=1, max_length=5000)
    writer_role: Literal["machine_reconciler", "authorized_human"]

    @model_validator(mode="after")
    def _no_self_edge(self) -> "ClaimReplacementEdgeData":
        if self.old_claim_id == self.new_claim_id:
            raise WarehouseContractError("Claim replacement self-edge is forbidden")
        return self


class RetrievalAttemptData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    attempt: RetrievalAttemptV1_2


class EvidenceAvailabilityData(WarehouseTableData):
    availability_assessment_id: str = Field(
        pattern=r"^evidence_availability_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    assessment: EvidenceAvailabilityV1_2


class MachineInclusionRecommendationData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None, pattern=r"^inclusion_revision_[0-9a-hjkmnp-tv-z]{26}$"
    )
    recommendation: MachineInclusionRecommendationV1_2

    @model_validator(mode="after")
    def _revision(self) -> "MachineInclusionRecommendationData":
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise WarehouseContractError("machine inclusion revision/supersedes mismatch")
        return self


class HumanClaimInclusionDecisionData(WarehouseTableData):
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None, pattern=r"^inclusion_revision_[0-9a-hjkmnp-tv-z]{26}$"
    )
    decision: HumanClaimInclusionDecisionV1_2

    @model_validator(mode="after")
    def _revision(self) -> "HumanClaimInclusionDecisionData":
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise WarehouseContractError("human inclusion revision/supersedes mismatch")
        return self


class AdjudicationData(WarehouseTableData):
    adjudication_revision_id: str = Field(
        pattern=r"^adjudication_revision_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    atomic_revision_id: str = Field(pattern=ATOMIC_REVISION_ID_PATTERN)
    annotation_ids: list[str] = Field(min_length=2)
    verdict: GoldCode
    reason: str = Field(min_length=1, max_length=10000)
    revision_no: int = Field(ge=1)
    supersedes_revision_id: str | None = Field(
        default=None, pattern=r"^adjudication_revision_[0-9a-hjkmnp-tv-z]{26}$"
    )
    writer_role: Literal["authorized_adjudicator"]

    @field_validator("annotation_ids")
    @classmethod
    def _annotations(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)) or any(
            re.fullmatch(ANNOTATION_ID_PATTERN, value) is None for value in values
        ):
            raise WarehouseContractError("adjudication annotation IDs must be unique")
        return values

    @model_validator(mode="after")
    def _revision(self) -> "AdjudicationData":
        if (self.revision_no == 1) != (self.supersedes_revision_id is None):
            raise WarehouseContractError("adjudication revision/supersedes mismatch")
        return self


class TaxonomyVersionRegistryData(WarehouseTableData):
    taxonomy_name: str = Field(pattern=r"^[a-z][a-z0-9_]{2,95}$")
    taxonomy_version: str = Field(min_length=1, max_length=160)
    effective_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    taxonomy_hash: str = Field(pattern=SHA256_PATTERN)
    writer_role: Literal["schema_publisher"]

    @field_validator("effective_at")
    @classmethod
    def _utc(cls, value: str) -> str:
        return _validate_utc(value)


class WarehouseImportBatchData(WarehouseTableData):
    warehouse_import_batch_id: str = Field(
        pattern=r"^warehouse_import_batch_[0-9a-hjkmnp-tv-z]{26}$"
    )
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    source_manifest_hash: str = Field(pattern=SHA256_PATTERN)
    source_schema_version: str = Field(min_length=1, max_length=160)
    status: Literal["succeeded"]
    synthetic: Literal[True]
    writer_role: Literal["future_migrator"]


class AuditChangeHistoryData(WarehouseTableData):
    audit_event_id: str = Field(pattern=r"^audit_event_[0-9a-hjkmnp-tv-z]{26}$")
    source_id: str = Field(pattern=SOURCE_ID_PATTERN)
    entity_table_code: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    entity_primary_key: str = Field(min_length=1, max_length=512)
    action: Literal["created", "superseded", "deactivated"]
    actor_role: str = Field(pattern=r"^[a-z][a-z0-9_]{1,63}$")
    occurred_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    previous_row_hash: str | None = Field(default=None, pattern=SHA256_PATTERN)
    new_row_hash: str | None = Field(default=None, pattern=SHA256_PATTERN)
    writer_role: Literal["audit_writer"]

    @field_validator("occurred_at")
    @classmethod
    def _utc(cls, value: str) -> str:
        return _validate_utc(value)

    @model_validator(mode="after")
    def _hash_matrix(self) -> "AuditChangeHistoryData":
        if self.entity_table_code not in ALLOWED_TABLE_CODES - {"audit_change_history"}:
            raise WarehouseContractError("audit target must be an exportable entity")
        if self.action == "created" and (
            self.previous_row_hash is not None or self.new_row_hash is None
        ):
            raise WarehouseContractError("created audit event requires only new_row_hash")
        if self.action == "superseded" and (
            self.previous_row_hash is None or self.new_row_hash is None
        ):
            raise WarehouseContractError("superseded audit event requires both hashes")
        if self.action == "deactivated" and (
            self.previous_row_hash is None or self.new_row_hash is not None
        ):
            raise WarehouseContractError("deactivated audit event requires only previous hash")
        return self


TABLE_DATA_MODELS: dict[str, type[WarehouseTableData]] = {
    "source_media": SourceMediaData,
    "run": RunData,
    "claim_collection": ClaimCollectionData,
    "source_artifact_ref": SourceArtifactRefData,
    "source_segment": SourceSegmentData,
    "parent_claim": ParentClaimData,
    "parent_claim_revision": ParentClaimRevisionData,
    "parent_claim_text_chunk": ClaimTextChunkData,
    "claim_source_span": ClaimSourceSpanData,
    "claim_split_set_revision": ClaimSplitSetRevisionData,
    "atomic_claim": AtomicClaimData,
    "atomic_claim_revision": AtomicClaimRevisionData,
    "atomic_claim_text_chunk": ClaimTextChunkData,
    "split_set_member": SplitSetMemberData,
    "claim_dependency": ClaimDependencyData,
    "claim_replacement_edge": ClaimReplacementEdgeData,
    "evidence_item": EvidenceItemData,
    "evidence_revision": EvidenceRevisionData,
    "claim_evidence_link": ClaimEvidenceLinkData,
    "retrieval_batch": RetrievalBatchData,
    "retrieval_attempt": RetrievalAttemptData,
    "evidence_availability": EvidenceAvailabilityData,
    "machine_assessment_batch": MachineAssessmentBatchData,
    "machine_claim_assessment": MachineClaimAssessmentData,
    "source_depth_assessment": SourceDepthAssessmentData,
    "machine_inclusion_recommendation": MachineInclusionRecommendationData,
    "human_claim_inclusion_decision": HumanClaimInclusionDecisionData,
    "annotation_task": AnnotationTaskData,
    "human_annotation": HumanAnnotationData,
    "adjudication": AdjudicationData,
    "gold_label": GoldLabelData,
    "taxonomy_version_registry": TaxonomyVersionRegistryData,
    "warehouse_import_batch": WarehouseImportBatchData,
    "audit_change_history": AuditChangeHistoryData,
}

if set(TABLE_DATA_MODELS) != set(ALLOWED_TABLE_CODES):
    raise AssertionError("every exportable warehouse entity requires a strict data model")


def _data_primary_key(table_code: str, data: WarehouseTableData) -> str:
    if isinstance(data, SourceMediaData):
        return data.source_id
    if isinstance(data, RunData):
        return data.run_id
    if isinstance(data, ClaimCollectionData):
        return data.claim_collection_id
    if isinstance(data, SourceArtifactRefData):
        return data.source_artifact_ref_id
    if isinstance(data, SourceSegmentData):
        return data.source_segment_id
    if isinstance(data, ParentClaimData):
        return data.parent_claim_id
    if isinstance(data, ParentClaimRevisionData):
        return data.revision.parent_revision_id
    if isinstance(data, ClaimTextChunkData):
        return data.chunk.chunk_id
    if isinstance(data, ClaimSourceSpanData):
        return data.claim_source_span_id
    if isinstance(data, ClaimSplitSetRevisionData):
        return data.split_set.split_revision_id
    if isinstance(data, AtomicClaimData):
        return data.atomic_claim_id
    if isinstance(data, AtomicClaimRevisionData):
        return data.revision.atomic_revision_id
    if isinstance(data, SplitSetMemberData):
        return data.member.member_id
    if isinstance(data, ClaimDependencyData):
        return data.dependency.dependency_id
    if isinstance(data, ClaimReplacementEdgeData):
        return data.replacement_edge_id
    if isinstance(data, EvidenceItemData):
        return data.evidence_id
    if isinstance(data, EvidenceRevisionData):
        return data.revision.evidence_revision_id
    if isinstance(data, ClaimEvidenceLinkData):
        return data.link.evidence_link_id
    if isinstance(data, RetrievalAttemptData):
        return data.attempt.retrieval_attempt_id
    if isinstance(data, EvidenceAvailabilityData):
        return data.availability_assessment_id
    if isinstance(data, MachineAssessmentBatchData):
        return data.assessment_batch_id
    if isinstance(data, MachineClaimAssessmentData):
        return data.assessment.assessment_revision_id
    if isinstance(data, SourceDepthAssessmentData):
        return data.assessment_revision_id
    if isinstance(data, MachineInclusionRecommendationData):
        return data.recommendation.inclusion_revision_id
    if isinstance(data, HumanClaimInclusionDecisionData):
        return data.decision.inclusion_revision_id
    if isinstance(data, RetrievalBatchData):
        return data.retrieval_batch_id
    if isinstance(data, AnnotationTaskData):
        return data.annotation_task_id
    if isinstance(data, HumanAnnotationData):
        return data.annotation_id
    if isinstance(data, AdjudicationData):
        return data.adjudication_revision_id
    if isinstance(data, GoldLabelData):
        return data.gold.gold_revision_id
    if isinstance(data, TaxonomyVersionRegistryData):
        return f"{data.taxonomy_name}@{data.taxonomy_version}"
    if isinstance(data, WarehouseImportBatchData):
        return data.warehouse_import_batch_id
    if isinstance(data, AuditChangeHistoryData):
        return data.audit_event_id
    raise WarehouseContractError(f"unsupported table data model: {table_code}")


def parse_warehouse_table_data(
    table_code: str, value: Mapping[str, Any]
) -> WarehouseTableData:
    model = TABLE_DATA_MODELS.get(table_code)
    if model is None:
        raise WarehouseContractError(
            f"table_code has no implemented fail-closed data contract: {table_code}"
        )
    try:
        return model.model_validate(value)
    except Exception as exc:
        raise WarehouseContractError(
            f"invalid {table_code} warehouse data: {exc}"
        ) from exc


def _validate_row_projection_contract(
    *,
    table_code: str,
    data: WarehouseTableData,
    canonical_primary_key: str,
    revision_no: int,
    logical_layer: str,
    writer_role: str,
    run_id: str,
) -> None:
    if canonical_primary_key != _data_primary_key(table_code, data):
        raise WarehouseContractError(
            f"{table_code} canonical_primary_key does not match typed data identity"
        )
    data_revision_no: int | None = None
    inner_writer: str | None = None
    if isinstance(data, RunData) and data.run_id != run_id:
        raise WarehouseContractError("run row data/run_id mismatch")
    if isinstance(data, ClaimCollectionData):
        data_revision_no = data.collection_revision_no
        inner_writer = data.writer_role
        if data.run_id != run_id:
            raise WarehouseContractError("Claim collection data/run_id mismatch")
    elif isinstance(data, SourceSegmentData):
        inner_writer = data.writer_role
    elif isinstance(data, ClaimSourceSpanData):
        inner_writer = data.writer_role
    elif isinstance(data, ClaimDependencyData):
        inner_writer = data.dependency.writer_role
    elif isinstance(data, ClaimReplacementEdgeData):
        inner_writer = data.writer_role
    elif isinstance(data, ParentClaimRevisionData):
        data_revision_no = data.revision.revision_no
        inner_writer = data.revision.writer_role
    elif isinstance(data, ClaimSplitSetRevisionData):
        data_revision_no = data.split_set.revision_no
        inner_writer = data.split_set.writer_role
    elif isinstance(data, AtomicClaimRevisionData):
        data_revision_no = data.revision.revision_no
        inner_writer = data.revision.writer_role
    elif isinstance(data, SplitSetMemberData):
        inner_writer = data.writer_role
    elif isinstance(data, EvidenceRevisionData):
        data_revision_no = data.revision.revision_no
        inner_writer = data.revision.writer_role
    elif isinstance(data, ClaimEvidenceLinkData):
        inner_writer = data.link.writer_role
    elif isinstance(data, RetrievalAttemptData):
        inner_writer = data.attempt.writer_role
    elif isinstance(data, EvidenceAvailabilityData):
        inner_writer = data.assessment.writer_role
    elif isinstance(data, SourceDepthAssessmentData):
        data_revision_no = data.revision_no
        inner_writer = data.writer_role
    elif isinstance(data, MachineClaimAssessmentData):
        data_revision_no = data.revision_no
    elif isinstance(data, MachineInclusionRecommendationData):
        data_revision_no = data.revision_no
        inner_writer = data.recommendation.writer_role
    elif isinstance(data, HumanClaimInclusionDecisionData):
        data_revision_no = data.revision_no
        inner_writer = data.decision.writer_role
    elif isinstance(data, HumanAnnotationData):
        data_revision_no = data.revision_no
        inner_writer = data.writer_role
    elif isinstance(data, GoldLabelData):
        data_revision_no = data.revision_no
        inner_writer = data.gold.writer_role
    elif isinstance(data, AdjudicationData):
        data_revision_no = data.revision_no
        inner_writer = data.writer_role
    elif isinstance(
        data,
        TaxonomyVersionRegistryData | WarehouseImportBatchData | AuditChangeHistoryData,
    ):
        inner_writer = data.writer_role
    if data_revision_no is not None and data_revision_no != revision_no:
        raise WarehouseContractError(f"{table_code} row/data revision_no mismatch")
    if inner_writer is not None and writer_role != inner_writer:
        raise WarehouseContractError(f"{table_code} row/data writer_role mismatch")

    core_tables = {
        "source_media",
        "run",
        "claim_collection",
        "source_artifact_ref",
        "source_segment",
        "parent_claim",
        "parent_claim_revision",
        "parent_claim_text_chunk",
        "claim_source_span",
        "claim_split_set_revision",
        "atomic_claim",
        "atomic_claim_revision",
        "atomic_claim_text_chunk",
        "split_set_member",
        "claim_dependency",
        "claim_replacement_edge",
        "taxonomy_version_registry",
        "warehouse_import_batch",
        "audit_change_history",
    }
    machine_tables: set[str] = set()
    human_tables = {
        "annotation_task",
        "human_annotation",
        "adjudication",
        "gold_label",
    }
    if table_code in core_tables and logical_layer != "core_provenance":
        raise WarehouseContractError(f"{table_code} must be core_provenance")
    if table_code in machine_tables and logical_layer != "machine_screening":
        raise WarehouseContractError(f"{table_code} must be machine_screening")
    if table_code in human_tables and logical_layer != "human_annotation":
        raise WarehouseContractError(f"{table_code} must be human_annotation")

    fixed_writers: dict[str, set[str]] = {
        "source_media": {"s01_acquisition_writer"},
        "run": {"stage5_coordinator"},
        "source_artifact_ref": {"artifact_publisher"},
        "claim_collection": {"claim_extractor"},
        "source_segment": {"source_segmenter"},
        "parent_claim": {"claim_extractor", "authorized_human"},
        "parent_claim_text_chunk": {"claim_extractor", "authorized_human"},
        "atomic_claim": {"claim_splitter", "authorized_human"},
        "atomic_claim_text_chunk": {"claim_splitter", "authorized_human"},
        "claim_replacement_edge": {"machine_reconciler", "authorized_human"},
        "evidence_item": {"machine_evidence_writer", "authorized_human"},
        "machine_assessment_batch": {"machine_assessor", "source_depth_assessor"},
        "annotation_task": {"annotation_coordinator"},
        "adjudication": {"authorized_adjudicator"},
        "taxonomy_version_registry": {"schema_publisher"},
        "warehouse_import_batch": {"future_migrator"},
        "audit_change_history": {"audit_writer"},
    }
    if table_code in fixed_writers and writer_role not in fixed_writers[table_code]:
        raise WarehouseContractError(f"{table_code} writer_role is not authorized")

    if isinstance(data, MachineAssessmentBatchData):
        expected_layer = (
            "machine_screening" if data.phase == "initial_machine" else "source_depth"
        )
        expected_writer = (
            "machine_assessor"
            if data.phase == "initial_machine"
            else "source_depth_assessor"
        )
        if logical_layer != expected_layer or writer_role != expected_writer:
            raise WarehouseContractError("machine batch phase/layer/writer mismatch")
    if isinstance(data, EvidenceItemData):
        expected_layers = (
            {"human_annotation"}
            if writer_role == "authorized_human"
            else {"machine_screening", "source_depth"}
        )
        if logical_layer not in expected_layers:
            raise WarehouseContractError("Evidence item writer/layer mismatch")
    if isinstance(data, EvidenceRevisionData | ClaimEvidenceLinkData):
        evidence_writer = (
            data.revision.writer_role
            if isinstance(data, EvidenceRevisionData)
            else data.link.writer_role
        )
        expected_layers = (
            {"human_annotation"}
            if evidence_writer == "authorized_human"
            else {"machine_screening", "source_depth"}
        )
        if logical_layer not in expected_layers or writer_role != evidence_writer:
            raise WarehouseContractError("Evidence revision/link writer/layer mismatch")
    if isinstance(data, MachineClaimAssessmentData):
        if logical_layer != "machine_screening" or writer_role != "machine_assessor":
            raise WarehouseContractError("machine assessment phase/layer/writer mismatch")
    if isinstance(data, SourceDepthAssessmentData):
        if logical_layer != "source_depth" or writer_role != "source_depth_assessor":
            raise WarehouseContractError("source-depth assessment layer/writer mismatch")
    if isinstance(data, RetrievalBatchData):
        expected_layer = (
            "machine_screening" if data.phase == "initial_machine" else "source_depth"
        )
        if logical_layer != expected_layer or writer_role != data.writer_role:
            raise WarehouseContractError("retrieval batch phase/layer/writer mismatch")
    if isinstance(data, RetrievalAttemptData):
        if logical_layer not in {"machine_screening", "source_depth"}:
            raise WarehouseContractError("retrieval attempt must be machine/source-depth")
    if isinstance(data, EvidenceAvailabilityData):
        if logical_layer not in {"machine_screening", "source_depth"}:
            raise WarehouseContractError("Evidence availability layer mismatch")
    if isinstance(data, MachineInclusionRecommendationData):
        if logical_layer != "machine_screening":
            raise WarehouseContractError("machine inclusion must be machine_screening")
    if isinstance(data, HumanClaimInclusionDecisionData):
        if logical_layer != "human_annotation":
            raise WarehouseContractError("human inclusion must be human_annotation")


class WarehouseRow(StrictWarehouseModel):
    row_schema_version: Literal["claim_warehouse_row_v1.0.0"] = (
        WAREHOUSE_ROW_SCHEMA_VERSION
    )
    logical_layer: LogicalLayer
    table_code: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    canonical_primary_key: str = Field(min_length=1, max_length=512)
    revision_no: int = Field(ge=1)
    is_active: bool
    effective_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    run_id: str = Field(pattern=RUN_ID_PATTERN)
    artifact_id: str = Field(pattern=ARTIFACT_ID_PATTERN)
    artifact_record_id: str = Field(pattern=RECORD_ID_PATTERN)
    artifact_content_hash: str = Field(pattern=SHA256_PATTERN)
    created_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    writer_role: str = Field(pattern=r"^[a-z][a-z0-9_]{1,63}$")
    schema_versions: dict[str, str]
    taxonomy_versions: dict[str, str]
    data: dict[str, Any]
    row_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("effective_at", "created_at")
    @classmethod
    def _utc_timestamp(cls, value: str) -> str:
        return _validate_utc(value)

    @model_validator(mode="after")
    def _row_invariants(self) -> "WarehouseRow":
        if self.table_code not in ALLOWED_TABLE_CODES:
            raise WarehouseContractError(f"unknown table_code: {self.table_code}")
        _validate_json_value(self.data)
        typed_data = parse_warehouse_table_data(self.table_code, self.data)
        _validate_row_projection_contract(
            table_code=self.table_code,
            data=typed_data,
            canonical_primary_key=self.canonical_primary_key,
            revision_no=self.revision_no,
            logical_layer=self.logical_layer,
            writer_role=self.writer_role,
            run_id=self.run_id,
        )
        if not self.schema_versions or not all(self.schema_versions.values()):
            raise WarehouseContractError("schema_versions must be explicit")
        if not self.taxonomy_versions or not all(self.taxonomy_versions.values()):
            raise WarehouseContractError("taxonomy_versions must be explicit")
        expected = embedded_hash(self.model_dump(mode="json"), "row_hash")
        if self.row_hash != expected:
            raise WarehouseContractError(
                f"row_hash mismatch: expected {expected}, observed {self.row_hash}"
            )
        return self

    @classmethod
    def build(cls, **values: Any) -> "WarehouseRow":
        payload = dict(values)
        payload.setdefault("row_schema_version", WAREHOUSE_ROW_SCHEMA_VERSION)
        payload["row_hash"] = "0" * 64
        payload["row_hash"] = embedded_hash(payload, "row_hash")
        return cls.model_validate(payload)


def _row_data_source_id(data: WarehouseTableData) -> str | None:
    value = getattr(data, "source_id", None)
    return value if isinstance(value, str) else None


def validate_plan_relations(
    export_rows: Iterable[Iterable[WarehouseRow]],
    *,
    existing_rows: Iterable[WarehouseRow] = (),
) -> None:
    """Validate typed PK/FK/UNIQUE and stage boundaries before Parquet creation."""

    existing_materialized = list(existing_rows)
    new_rows = [row for group in export_rows for row in group]
    combined: dict[tuple[str, str, int], WarehouseRow] = {}
    for row in existing_materialized:
        key = (row.table_code, row.canonical_primary_key, row.revision_no)
        prior = combined.get(key)
        if prior is not None:
            detail = "hash conflict" if prior.row_hash != row.row_hash else "duplicate"
            raise WarehouseConflictError(
                "existing logical warehouse revision "
                f"{detail}: {row.table_code}/{row.canonical_primary_key}/r{row.revision_no}"
            )
        combined[key] = row
    for row in new_rows:
        key = (row.table_code, row.canonical_primary_key, row.revision_no)
        prior = combined.get(key)
        if prior is not None:
            detail = "hash conflict" if prior.row_hash != row.row_hash else "duplicate"
            raise WarehouseConflictError(
                "logical warehouse revision "
                f"{detail}: {row.table_code}/{row.canonical_primary_key}/r{row.revision_no}"
            )
        combined[key] = row
    for table_code in ROW_STABLE_REVISION_TABLE_CODES:
        revisions_by_primary_key: dict[str, set[int]] = {}
        for row in combined.values():
            if row.table_code == table_code:
                revisions_by_primary_key.setdefault(
                    row.canonical_primary_key, set()
                ).add(row.revision_no)
        for primary_key, revisions in revisions_by_primary_key.items():
            if revisions != set(range(1, max(revisions) + 1)):
                raise WarehouseContractError(
                    "stable warehouse entity revision chain is not contiguous: "
                    f"{table_code}/{primary_key}"
                )
    typed: dict[tuple[str, str, int], WarehouseTableData] = {
        key: parse_warehouse_table_data(row.table_code, row.data)
        for key, row in combined.items()
    }
    row_by_table_pk: dict[str, dict[str, WarehouseRow]] = {}
    by_table: dict[str, dict[str, WarehouseTableData]] = {}
    for (table_code, primary_key, revision_no), data in typed.items():
        selected = row_by_table_pk.setdefault(table_code, {}).get(primary_key)
        if selected is None or revision_no > selected.revision_no:
            row = combined[(table_code, primary_key, revision_no)]
            row_by_table_pk[table_code][primary_key] = row
            by_table.setdefault(table_code, {})[primary_key] = data

    def require(table_code: str, primary_key: str) -> WarehouseTableData:
        value = by_table.get(table_code, {}).get(primary_key)
        selected = row_by_table_pk.get(table_code, {}).get(primary_key)
        if value is None or selected is None or not selected.is_active:
            raise WarehouseContractError(
                f"missing or inactive warehouse FK {table_code}/{primary_key}"
            )
        return value

    def same_source(owner: WarehouseTableData, target: WarehouseTableData) -> None:
        owner_source = _row_data_source_id(owner)
        target_source = _row_data_source_id(target)
        if owner_source is not None and target_source is not None and owner_source != target_source:
            raise WarehouseContractError("cross-source warehouse FK is forbidden")

    source_keys: set[tuple[str, str]] = set()
    for data in by_table.get("source_media", {}).values():
        assert isinstance(data, SourceMediaData)
        key = (data.platform, data.platform_source_key)
        if key in source_keys:
            raise WarehouseContractError("duplicate platform/source key")
        source_keys.add(key)

    for row in new_rows:
        data = typed[(row.table_code, row.canonical_primary_key, row.revision_no)]
        source_id = _row_data_source_id(data)
        if source_id is not None and not isinstance(data, SourceMediaData):
            source = require("source_media", source_id)
            same_source(data, source)
        run = require("run", row.run_id)
        assert isinstance(run, RunData)
        if source_id is not None and run.source_id != source_id:
            raise WarehouseContractError("row source_id differs from authoritative run")

        if isinstance(data, SourceArtifactRefData):
            same_source(data, require("source_media", data.source_id))
        elif isinstance(data, ClaimCollectionData):
            if data.run_id != row.run_id:
                raise WarehouseContractError("Claim collection run mismatch")
            _validate_revision_predecessor(
                data.collection_revision_no,
                data.supersedes_collection_id,
                data.run_id,
                by_table.get("claim_collection", {}),
                lambda item: item.run_id,
                lambda item: item.collection_revision_no,
            )
        elif isinstance(data, SourceSegmentData):
            artifact_ref = require("source_artifact_ref", data.source_artifact_ref_id)
            same_source(data, artifact_ref)
        elif isinstance(data, ParentClaimData):
            pass
        elif isinstance(data, ParentClaimRevisionData):
            parent = require("parent_claim", data.revision.parent_claim_id)
            same_source(data, parent)
            _validate_revision_predecessor(
                data.revision.revision_no,
                data.revision.supersedes_revision_id,
                data.revision.parent_claim_id,
                by_table.get("parent_claim_revision", {}),
                lambda item: item.revision.parent_claim_id,
                lambda item: item.revision.revision_no,
            )
            expected_chunks = {
                item.chunk_id: item for item in data.revision.text.chunks
            }
            observed_chunks = {
                item.chunk.chunk_id: item.chunk
                for item in by_table.get("parent_claim_text_chunk", {}).values()
                if isinstance(item, ClaimTextChunkData)
                and item.chunk.owner_revision_id == data.revision.parent_revision_id
            }
            if expected_chunks != observed_chunks:
                raise WarehouseContractError("parent Claim chunk projection mismatch")
        elif isinstance(data, ClaimSplitSetRevisionData):
            parent_revision = require(
                "parent_claim_revision", data.split_set.parent_revision_id
            )
            assert isinstance(parent_revision, ParentClaimRevisionData)
            if parent_revision.revision.parent_claim_id != data.split_set.parent_claim_id:
                raise WarehouseContractError("split set parent binding mismatch")
            same_source(data, parent_revision)
            expected_members = {
                item.member_id: item for item in data.split_set.members
            }
            observed_members = {
                item.member.member_id: item.member
                for item in by_table.get("split_set_member", {}).values()
                if isinstance(item, SplitSetMemberData)
                and item.split_revision_id == data.split_set.split_revision_id
            }
            if expected_members != observed_members:
                raise WarehouseContractError("split member projection mismatch")
        elif isinstance(data, ClaimSourceSpanData):
            segment = require("source_segment", data.source_segment_id)
            claim = require(data.claim_revision_kind, data.claim_revision_id)
            same_source(data, segment)
            same_source(data, claim)
        elif isinstance(data, AtomicClaimData):
            parent = require("parent_claim", data.parent_claim_id)
            same_source(data, parent)
        elif isinstance(data, AtomicClaimRevisionData):
            atomic = require("atomic_claim", data.revision.atomic_claim_id)
            split = require("claim_split_set_revision", data.revision.split_revision_id)
            assert isinstance(atomic, AtomicClaimData)
            assert isinstance(split, ClaimSplitSetRevisionData)
            if (
                atomic.parent_claim_id != data.revision.parent_claim_id
                or split.split_set.parent_claim_id != data.revision.parent_claim_id
            ):
                raise WarehouseContractError("atomic Claim parent/split mismatch")
            same_source(data, atomic)
            same_source(data, split)
            _validate_revision_predecessor(
                data.revision.revision_no,
                data.revision.supersedes_revision_id,
                data.revision.atomic_claim_id,
                by_table.get("atomic_claim_revision", {}),
                lambda item: item.revision.atomic_claim_id,
                lambda item: item.revision.revision_no,
            )
            expected_chunks = {
                item.chunk_id: item for item in data.revision.text.chunks
            }
            observed_chunks = {
                item.chunk.chunk_id: item.chunk
                for item in by_table.get("atomic_claim_text_chunk", {}).values()
                if isinstance(item, ClaimTextChunkData)
                and item.chunk.owner_revision_id == data.revision.atomic_revision_id
            }
            if expected_chunks != observed_chunks:
                raise WarehouseContractError("atomic Claim chunk projection mismatch")
        elif isinstance(data, SplitSetMemberData):
            split = require("claim_split_set_revision", data.split_revision_id)
            atomic = require("atomic_claim_revision", data.member.atomic_revision_id)
            assert isinstance(split, ClaimSplitSetRevisionData)
            assert isinstance(atomic, AtomicClaimRevisionData)
            if (
                split.split_set.parent_claim_id != data.parent_claim_id
                or atomic.revision.parent_claim_id != data.parent_claim_id
                or atomic.revision.split_revision_id != data.split_revision_id
            ):
                raise WarehouseContractError("split member parent mismatch")
            same_source(data, split)
            same_source(data, atomic)
        elif isinstance(data, ClaimDependencyData):
            same_source(
                data,
                require(
                    "atomic_claim_revision",
                    data.dependency.from_atomic_revision_id,
                ),
            )
            same_source(
                data,
                require(
                    "atomic_claim_revision",
                    data.dependency.to_atomic_revision_id,
                ),
            )
        elif isinstance(data, ClaimReplacementEdgeData):
            old = by_table.get("parent_claim", {}).get(data.old_claim_id) or by_table.get(
                "atomic_claim", {}
            ).get(data.old_claim_id)
            new = by_table.get("parent_claim", {}).get(data.new_claim_id) or by_table.get(
                "atomic_claim", {}
            ).get(data.new_claim_id)
            if old is None or new is None:
                raise WarehouseContractError("Claim replacement target is missing")
            same_source(data, old)
            same_source(data, new)
        elif isinstance(data, EvidenceRevisionData):
            evidence = require("evidence_item", data.revision.evidence_id)
            same_source(data, evidence)
            _validate_revision_predecessor(
                data.revision.revision_no,
                data.revision.supersedes_revision_id,
                data.revision.evidence_id,
                by_table.get("evidence_revision", {}),
                lambda item: item.revision.evidence_id,
                lambda item: item.revision.revision_no,
            )
        elif isinstance(data, ClaimEvidenceLinkData):
            if data.link.target_kind == "atomic_claim_revision":
                target = require(
                    "atomic_claim_revision", str(data.link.atomic_revision_id)
                )
            else:
                target = require(
                    "parent_claim_revision", str(data.link.parent_revision_id)
                )
            evidence = require("evidence_revision", data.link.evidence_revision_id)
            same_source(data, target)
            same_source(data, evidence)
        elif isinstance(data, RetrievalBatchData):
            if data.run_id != row.run_id:
                raise WarehouseContractError("retrieval batch run mismatch")
        elif isinstance(data, RetrievalAttemptData):
            retrieval = require("retrieval_batch", data.attempt.retrieval_batch_id)
            same_source(data, retrieval)
            retrieval_row = row_by_table_pk["retrieval_batch"][
                data.attempt.retrieval_batch_id
            ]
            if row.logical_layer != retrieval_row.logical_layer:
                raise WarehouseContractError(
                    "retrieval attempt must use its batch logical layer"
                )
            if data.attempt.evidence_revision_id is not None:
                same_source(
                    data,
                    require("evidence_revision", data.attempt.evidence_revision_id),
                )
        elif isinstance(data, EvidenceAvailabilityData):
            assessment = data.assessment
            same_source(
                data,
                require("atomic_claim_revision", assessment.atomic_revision_id),
            )
            same_source(data, require("retrieval_batch", assessment.retrieval_batch_id))
            retrieval_row = row_by_table_pk["retrieval_batch"][
                assessment.retrieval_batch_id
            ]
            if row.logical_layer != retrieval_row.logical_layer:
                raise WarehouseContractError(
                    "Evidence availability must use its batch logical layer"
                )
            for link_id in [
                *assessment.formal_evidence_link_ids,
                *assessment.clue_link_ids,
            ]:
                link = require("claim_evidence_link", link_id)
                assert isinstance(link, ClaimEvidenceLinkData)
                if (
                    link.link.target_kind != "atomic_claim_revision"
                    or link.link.atomic_revision_id != assessment.atomic_revision_id
                ):
                    raise WarehouseContractError("Evidence availability link Claim mismatch")
                same_source(data, link)
        elif isinstance(data, MachineAssessmentBatchData):
            pass
        elif isinstance(data, MachineClaimAssessmentData):
            batch = require("machine_assessment_batch", data.assessment_batch_id)
            atomic = require("atomic_claim_revision", data.assessment.atomic_revision_id)
            same_source(data, batch)
            same_source(data, atomic)
            assert isinstance(atomic, AtomicClaimRevisionData)
            if data.assessment.claim_checkability != atomic.revision.checkability:
                raise WarehouseContractError(
                    "machine assessment Claim checkability mismatch"
                )
            for link_id in data.assessment.evidence_link_ids:
                link = require("claim_evidence_link", link_id)
                assert isinstance(link, ClaimEvidenceLinkData)
                if (
                    link.link.target_kind != "atomic_claim_revision"
                    or link.link.atomic_revision_id
                    != data.assessment.atomic_revision_id
                ):
                    raise WarehouseContractError("assessment Evidence link Claim mismatch")
            _validate_revision_predecessor(
                data.revision_no,
                data.supersedes_revision_id,
                data.assessment.atomic_revision_id,
                by_table.get("machine_claim_assessment", {}),
                lambda item: item.assessment.atomic_revision_id,
                lambda item: item.revision_no,
            )
        elif isinstance(data, SourceDepthAssessmentData):
            retrieval = require("retrieval_batch", data.retrieval_batch_id)
            atomic = require("atomic_claim_revision", data.atomic_revision_id)
            base = require("machine_claim_assessment", data.base_assessment_revision_id)
            same_source(data, retrieval)
            same_source(data, atomic)
            same_source(data, base)
            _validate_revision_predecessor(
                data.revision_no,
                data.supersedes_revision_id,
                data.atomic_revision_id,
                by_table.get("source_depth_assessment", {}),
                lambda item: item.atomic_revision_id,
                lambda item: item.revision_no,
            )
        elif isinstance(data, MachineInclusionRecommendationData):
            same_source(
                data,
                require(
                    "atomic_claim_revision",
                    data.recommendation.atomic_revision_id,
                ),
            )
            _validate_revision_predecessor(
                data.revision_no,
                data.supersedes_revision_id,
                data.recommendation.atomic_revision_id,
                by_table.get("machine_inclusion_recommendation", {}),
                lambda item: item.recommendation.atomic_revision_id,
                lambda item: item.revision_no,
            )
        elif isinstance(data, HumanClaimInclusionDecisionData):
            same_source(
                data,
                require("atomic_claim_revision", data.decision.atomic_revision_id),
            )
            _validate_revision_predecessor(
                data.revision_no,
                data.supersedes_revision_id,
                data.decision.atomic_revision_id,
                by_table.get("human_claim_inclusion_decision", {}),
                lambda item: item.decision.atomic_revision_id,
                lambda item: item.revision_no,
            )
        elif isinstance(data, AnnotationTaskData):
            same_source(data, require(data.target_kind, data.target_revision_id))
        elif isinstance(data, HumanAnnotationData):
            task = require("annotation_task", data.annotation_task_id)
            target = require(data.target_kind, data.target_revision_id)
            assert isinstance(task, AnnotationTaskData)
            if (
                task.target_kind != data.target_kind
                or task.target_revision_id != data.target_revision_id
            ):
                raise WarehouseContractError("annotation task/Claim mismatch")
            same_source(data, task)
            same_source(data, target)
            for link_id in data.evidence_link_ids:
                link = require("claim_evidence_link", link_id)
                assert isinstance(link, ClaimEvidenceLinkData)
                link_target_id = str(
                    link.link.atomic_revision_id or link.link.parent_revision_id
                )
                if (
                    link.link.target_kind != data.target_kind
                    or link_target_id != data.target_revision_id
                ):
                    raise WarehouseContractError(
                        "human annotation Evidence link Claim mismatch"
                    )
                same_source(data, link)
            _validate_revision_predecessor(
                data.revision_no,
                data.supersedes_annotation_id,
                data.annotation_task_id,
                by_table.get("human_annotation", {}),
                lambda item: item.annotation_task_id,
                lambda item: item.revision_no,
            )
        elif isinstance(data, AdjudicationData):
            same_source(data, require("atomic_claim_revision", data.atomic_revision_id))
            for annotation_id in data.annotation_ids:
                annotation = require("human_annotation", annotation_id)
                assert isinstance(annotation, HumanAnnotationData)
                if annotation.atomic_revision_id != data.atomic_revision_id:
                    raise WarehouseContractError("adjudication annotation target mismatch")
                same_source(data, annotation)
            _validate_revision_predecessor(
                data.revision_no,
                data.supersedes_revision_id,
                data.atomic_revision_id,
                by_table.get("adjudication", {}),
                lambda item: item.atomic_revision_id,
                lambda item: item.revision_no,
            )
        elif isinstance(data, GoldLabelData):
            annotation = require("human_annotation", data.annotation_id)
            assert isinstance(annotation, HumanAnnotationData)
            if (
                data.gold.target_kind != annotation.target_kind
                or data.gold.target_revision_id != annotation.target_revision_id
                or data.gold.gold_label != annotation.verdict
                or set(data.gold.evidence_link_ids)
                != set(annotation.evidence_link_ids)
            ):
                raise WarehouseContractError(
                    "Gold/annotation target, verdict, or Evidence mismatch"
                )
            same_source(data, annotation)
            target = require(data.gold.target_kind, data.gold.target_revision_id)
            same_source(data, target)
            if data.gold.target_kind == "atomic_claim_revision":
                assert isinstance(target, AtomicClaimRevisionData)
                if data.gold.claim_checkability != target.revision.checkability:
                    raise WarehouseContractError("Gold Claim checkability mismatch")
            formal_relations: set[str] = set()
            for link_id in data.gold.evidence_link_ids:
                link = require("claim_evidence_link", link_id)
                assert isinstance(link, ClaimEvidenceLinkData)
                link_target_id = str(
                    link.link.atomic_revision_id or link.link.parent_revision_id
                )
                if (
                    link.link.target_kind != data.gold.target_kind
                    or link_target_id != data.gold.target_revision_id
                ):
                    raise WarehouseContractError("Gold Evidence link Claim mismatch")
                if link.link.use_status != "evidence":
                    raise WarehouseContractError(
                        "Gold may bind only formal Evidence links"
                    )
                formal_relations.add(str(link.link.evidence_relation))
                same_source(data, link)
            if (
                data.gold.gold_label == "gold_supports"
                and "supports" not in formal_relations
            ):
                raise WarehouseContractError(
                    "gold_supports requires formal supports Evidence"
                )
            if (
                data.gold.gold_label == "gold_refutes"
                and "refutes" not in formal_relations
            ):
                raise WarehouseContractError(
                    "gold_refutes requires formal refutes Evidence"
                )
            if data.gold.retrieval_batch_id is not None:
                retrieval = require("retrieval_batch", data.gold.retrieval_batch_id)
                same_source(data, retrieval)
                availability = [
                    item
                    for primary_key, item in by_table.get(
                        "evidence_availability", {}
                    ).items()
                    if isinstance(item, EvidenceAvailabilityData)
                    and row_by_table_pk["evidence_availability"][
                        primary_key
                    ].is_active
                    and item.assessment.atomic_revision_id
                    == data.gold.target_revision_id
                    and item.assessment.retrieval_batch_id
                    == data.gold.retrieval_batch_id
                    and item.assessment.batch_closed
                    and item.assessment.availability == "no_evidence"
                ]
                if len(availability) != 1:
                    raise WarehouseContractError(
                        "gold_insufficient_evidence requires one closed no_evidence assessment"
                    )
            _validate_revision_predecessor(
                data.revision_no,
                data.supersedes_gold_revision_id,
                (
                    data.gold.target_kind,
                    data.gold.target_revision_id,
                    data.gold.annotation_scope,
                    data.label_namespace,
                ),
                by_table.get("gold_label", {}),
                lambda item: (
                    item.gold.target_kind,
                    item.gold.target_revision_id,
                    item.gold.annotation_scope,
                    item.label_namespace,
                ),
                lambda item: item.revision_no,
            )
        elif isinstance(data, WarehouseImportBatchData):
            pass
        elif isinstance(data, TaxonomyVersionRegistryData):
            pass
        elif isinstance(data, AuditChangeHistoryData):
            target = by_table.get(data.entity_table_code, {}).get(data.entity_primary_key)
            if target is None:
                raise WarehouseContractError("audit target row is missing")
            same_source(data, target)

    _validate_unique_graph_constraints(by_table, row_by_table_pk, new_rows)


def validate_export_relations(
    rows: Iterable[WarehouseRow], *, existing_rows: Iterable[WarehouseRow] = ()
) -> None:
    validate_plan_relations([tuple(rows)], existing_rows=existing_rows)


def _validate_revision_predecessor(
    revision_no: int,
    supersedes_id: str | None,
    stable_id: str,
    revisions: Mapping[str, WarehouseTableData],
    stable_getter: Any,
    revision_getter: Any,
) -> None:
    if revision_no == 1:
        return
    predecessor = revisions.get(str(supersedes_id))
    if predecessor is None:
        raise WarehouseContractError("revision supersedes target is missing")
    if stable_getter(predecessor) != stable_id or revision_getter(predecessor) != revision_no - 1:
        raise WarehouseContractError("revision supersedes chain is not contiguous")


def _validate_unique_graph_constraints(
    by_table: Mapping[str, Mapping[str, WarehouseTableData]],
    row_by_table_pk: Mapping[str, Mapping[str, WarehouseRow]],
    new_rows: Sequence[WarehouseRow],
) -> None:
    def unique(label: str, values: Iterable[Any]) -> None:
        materialized = list(values)
        if len(materialized) != len(set(materialized)):
            raise WarehouseContractError(f"duplicate warehouse unique key: {label}")

    def current_values(
        table_code: str, supersedes_getter: Any
    ) -> list[WarehouseTableData]:
        values = list(by_table.get(table_code, {}).values())
        superseded = {
            value
            for item in values
            if (value := supersedes_getter(item)) is not None
        }
        return [
            item
            for primary_key, item in by_table.get(table_code, {}).items()
            if primary_key not in superseded
            and row_by_table_pk.get(table_code, {}).get(primary_key) is not None
            and row_by_table_pk[table_code][primary_key].is_active
        ]

    unique(
        "parent source/display",
        (
            (item.source_id, item.display_no)
            for item in by_table.get("parent_claim", {}).values()
            if isinstance(item, ParentClaimData)
        ),
    )
    unique(
        "Claim collection run/revision",
        (
            (item.run_id, item.collection_revision_no)
            for item in by_table.get("claim_collection", {}).values()
            if isinstance(item, ClaimCollectionData)
        ),
    )
    unique(
        "source segment locator",
        (
            (item.source_id, item.start_ms, item.end_ms, item.segment_kind)
            for item in by_table.get("source_segment", {}).values()
            if isinstance(item, SourceSegmentData)
        ),
    )
    unique(
        "source Artifact/content",
        (
            (item.referenced_artifact_id, item.content_hash)
            for item in by_table.get("source_artifact_ref", {}).values()
            if isinstance(item, SourceArtifactRefData)
        ),
    )
    unique(
        "Claim dependency triple",
        (
            (
                item.dependency.from_atomic_revision_id,
                item.dependency.to_atomic_revision_id,
                item.dependency.dependency_type,
            )
            for item in by_table.get("claim_dependency", {}).values()
            if isinstance(item, ClaimDependencyData)
        ),
    )
    unique(
        "Claim replacement edge",
        (
            (item.old_claim_id, item.new_claim_id, item.replacement_type)
            for item in by_table.get("claim_replacement_edge", {}).values()
            if isinstance(item, ClaimReplacementEdgeData)
        ),
    )
    unique(
        "Evidence availability Claim/batch",
        (
            (item.assessment.atomic_revision_id, item.assessment.retrieval_batch_id)
            for item in by_table.get("evidence_availability", {}).values()
            if isinstance(item, EvidenceAvailabilityData)
        ),
    )
    unique(
        "warehouse import source manifest",
        (
            item.source_manifest_hash
            for item in by_table.get("warehouse_import_batch", {}).values()
            if isinstance(item, WarehouseImportBatchData)
        ),
    )
    unique(
        "machine phase/Claim/revision",
        (
            (item.phase, item.assessment.atomic_revision_id, item.revision_no)
            for item in by_table.get("machine_claim_assessment", {}).values()
            if isinstance(item, MachineClaimAssessmentData)
        ),
    )

    current_machine = [
        item
        for item in current_values(
            "machine_claim_assessment", lambda item: item.supersedes_revision_id
        )
        if isinstance(item, MachineClaimAssessmentData)
    ]
    current_gold = [
        item
        for item in current_values(
            "gold_label", lambda item: item.supersedes_gold_revision_id
        )
        if isinstance(item, GoldLabelData)
    ]
    unique(
        "current Gold target/taxonomy",
        (
            (
                item.gold.target_kind,
                item.gold.target_revision_id,
                item.gold.taxonomy_version,
            )
            for item in current_gold
        ),
    )

    new_active_atomic_revision_ids = {
        row.canonical_primary_key
        for row in new_rows
        if row.table_code == "atomic_claim_revision" and row.is_active
    }
    for revision_id in new_active_atomic_revision_ids:
        atomic = by_table.get("atomic_claim_revision", {}).get(revision_id)
        assert isinstance(atomic, AtomicClaimRevisionData)
        dependencies = [
            item
            for primary_key, item in by_table.get("claim_dependency", {}).items()
            if isinstance(item, ClaimDependencyData)
            and row_by_table_pk["claim_dependency"][primary_key].is_active
            and revision_id
            in {
                item.dependency.from_atomic_revision_id,
                item.dependency.to_atomic_revision_id,
            }
        ]
        machine = [
            item
            for item in current_machine
            if item.assessment.atomic_revision_id == revision_id
        ]
        gold = [
            item
            for item in current_gold
            if item.gold.target_kind == "atomic_claim_revision"
            and item.gold.target_revision_id == revision_id
        ]
        if atomic.revision.checkability == "context_only":
            if not dependencies:
                raise WarehouseContractError(
                    "context_only Claim requires at least one dependency"
                )
            if machine or gold:
                raise WarehouseContractError(
                    "context_only Claim forbids machine verdict and Gold"
                )
        elif not machine:
            raise WarehouseContractError(
                "non-context atomic Claim requires a machine verdict in the same "
                "load-plan closure"
            )
        if atomic.revision.checkability == "not_checkable":
            if any(
                item.assessment.candidate_verdict != "unverifiable"
                for item in machine
            ) or any(item.gold.gold_label != "gold_uncheckable" for item in gold):
                raise WarehouseContractError(
                    "not_checkable Claim only permits unverifiable/uncheckable labels"
                )

    new_parent_revisions = {
        row.canonical_primary_key
        for row in new_rows
        if row.table_code == "parent_claim_revision"
    }
    for revision_id in new_parent_revisions:
        split_sets = [
            item
            for item in by_table.get("claim_split_set_revision", {}).values()
            if isinstance(item, ClaimSplitSetRevisionData)
            and item.split_set.parent_revision_id == revision_id
        ]
        if not split_sets:
            raise WarehouseContractError("every parent Claim revision requires a split set")
        if any(not item.split_set.members for item in split_sets):
            raise WarehouseContractError("every parent Claim requires at least one child")
    gold_counts: dict[str, int] = {}
    for item in current_values(
        "gold_label", lambda item: item.supersedes_gold_revision_id
    ):
        assert isinstance(item, GoldLabelData)
        gold_counts[item.annotation_id] = gold_counts.get(item.annotation_id, 0) + 1
    new_annotations = {
        row.canonical_primary_key
        for row in new_rows
        if row.table_code == "human_annotation"
    }
    if any(gold_counts.get(annotation_id) != 1 for annotation_id in new_annotations):
        raise WarehouseContractError(
            "each approved human annotation requires exactly one Gold revision"
        )
    _validate_all_revision_chains(by_table)


def _validate_all_revision_chains(
    by_table: Mapping[str, Mapping[str, WarehouseTableData]],
) -> None:
    specs: tuple[tuple[str, Any, Any, Any], ...] = (
        (
            "claim_collection",
            lambda item: item.run_id,
            lambda item: item.collection_revision_no,
            lambda item: item.supersedes_collection_id,
        ),
        (
            "parent_claim_revision",
            lambda item: item.revision.parent_claim_id,
            lambda item: item.revision.revision_no,
            lambda item: item.revision.supersedes_revision_id,
        ),
        (
            "claim_split_set_revision",
            lambda item: item.split_set.parent_claim_id,
            lambda item: item.split_set.revision_no,
            lambda item: item.split_set.supersedes_split_revision_id,
        ),
        (
            "atomic_claim_revision",
            lambda item: item.revision.atomic_claim_id,
            lambda item: item.revision.revision_no,
            lambda item: item.revision.supersedes_revision_id,
        ),
        (
            "evidence_revision",
            lambda item: item.revision.evidence_id,
            lambda item: item.revision.revision_no,
            lambda item: item.revision.supersedes_revision_id,
        ),
        (
            "machine_claim_assessment",
            lambda item: (item.phase, item.assessment.atomic_revision_id),
            lambda item: item.revision_no,
            lambda item: item.supersedes_revision_id,
        ),
        (
            "source_depth_assessment",
            lambda item: (item.atomic_revision_id, item.label_namespace),
            lambda item: item.revision_no,
            lambda item: item.supersedes_revision_id,
        ),
        (
            "machine_inclusion_recommendation",
            lambda item: item.recommendation.atomic_revision_id,
            lambda item: item.revision_no,
            lambda item: item.supersedes_revision_id,
        ),
        (
            "human_claim_inclusion_decision",
            lambda item: item.decision.atomic_revision_id,
            lambda item: item.revision_no,
            lambda item: item.supersedes_revision_id,
        ),
        (
            "human_annotation",
            lambda item: item.annotation_task_id,
            lambda item: item.revision_no,
            lambda item: item.supersedes_annotation_id,
        ),
        (
            "adjudication",
            lambda item: item.atomic_revision_id,
            lambda item: item.revision_no,
            lambda item: item.supersedes_revision_id,
        ),
        (
            "gold_label",
            lambda item: (
                item.gold.target_kind,
                item.gold.target_revision_id,
                item.gold.annotation_scope,
                item.label_namespace,
            ),
            lambda item: item.revision_no,
            lambda item: item.supersedes_gold_revision_id,
        ),
    )
    for table_code, stable_getter, revision_getter, supersedes_getter in specs:
        values = by_table.get(table_code, {})
        if not values:
            continue
        groups: dict[Any, list[tuple[str, WarehouseTableData]]] = {}
        successors: dict[str, int] = {}
        for primary_key, item in values.items():
            groups.setdefault(stable_getter(item), []).append((primary_key, item))
            supersedes = supersedes_getter(item)
            if supersedes is not None:
                successors[supersedes] = successors.get(supersedes, 0) + 1
        if any(count > 1 for count in successors.values()):
            raise WarehouseContractError(f"{table_code} revision chain forks")
        for stable_id, revisions in groups.items():
            revision_numbers = [revision_getter(item) for _, item in revisions]
            if len(revision_numbers) != len(set(revision_numbers)):
                raise WarehouseContractError(
                    f"duplicate {table_code} stable/revision key"
                )
            if sorted(revision_numbers) != list(range(1, max(revision_numbers) + 1)):
                raise WarehouseContractError(f"{table_code} revision chain has gaps")
            superseded_ids = {
                supersedes_getter(item)
                for _, item in revisions
                if supersedes_getter(item) is not None
            }
            heads = [primary_key for primary_key, _ in revisions if primary_key not in superseded_ids]
            if len(heads) != 1:
                raise WarehouseContractError(
                    f"{table_code} stable entity requires exactly one chain head"
                )
            for primary_key, item in revisions:
                revision_no = revision_getter(item)
                supersedes = supersedes_getter(item)
                if revision_no == 1:
                    if supersedes is not None:
                        raise WarehouseContractError(
                            f"{table_code} first revision cannot supersede"
                        )
                    continue
                predecessor = values.get(str(supersedes))
                if (
                    predecessor is None
                    or stable_getter(predecessor) != stable_id
                    or revision_getter(predecessor) != revision_no - 1
                ):
                    raise WarehouseContractError(
                        f"{table_code} revision predecessor is not contiguous"
                    )


class WarehouseExportManifestV1(StrictWarehouseModel):
    manifest_schema_version: Literal["claim_warehouse_export_v1.0.0"] = (
        WAREHOUSE_EXPORT_SCHEMA_VERSION
    )
    export_id: str = Field(pattern=EXPORT_ID_PATTERN)
    run_id: str = Field(pattern=RUN_ID_PATTERN)
    run_created_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    created_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    export_idempotency_key: str = Field(pattern=SHA256_PATTERN)
    storage_root_ref: Literal["ubuntu_v02_claim_warehouse"]
    manifest_relative_path: str = Field(min_length=1, max_length=1024)
    rows_relative_path: str = Field(min_length=1, max_length=1024)
    rows_hash: str = Field(pattern=SHA256_PATTERN)
    logical_hash: str = Field(pattern=SHA256_PATTERN)
    row_count: int = Field(ge=1)
    row_counts: dict[str, int]
    source_registry_prefix: RegistryPrefix
    input_artifacts: list[InputArtifactRef] = Field(min_length=1)
    schema_versions: dict[str, str]
    taxonomy_versions: dict[str, str]
    exporter_versions: dict[str, str]

    @field_validator("run_created_at", "created_at")
    @classmethod
    def _utc_timestamp(cls, value: str) -> str:
        return _validate_utc(value)

    @field_validator("manifest_relative_path", "rows_relative_path")
    @classmethod
    def _safe_paths(cls, value: str) -> str:
        return _validate_relative_posix(value)

    @model_validator(mode="after")
    def _manifest_invariants(self) -> "WarehouseExportManifestV1":
        expected_package = PurePosixPath("exports") / self.export_id
        if PurePosixPath(self.manifest_relative_path) != expected_package / "manifest.json":
            raise WarehouseContractError(
                "manifest_relative_path must be exports/<export_id>/manifest.json"
            )
        if PurePosixPath(self.rows_relative_path) != expected_package / "rows.jsonl":
            raise WarehouseContractError(
                "rows_relative_path must be exports/<export_id>/rows.jsonl"
            )
        if sum(self.row_counts.values()) != self.row_count:
            raise WarehouseContractError("row_counts must sum to row_count")
        if any(value <= 0 for value in self.row_counts.values()):
            raise WarehouseContractError("row_counts values must be positive")
        identities = [(item.artifact_id, item.record_id) for item in self.input_artifacts]
        if identities != sorted(identities) or len(identities) != len(set(identities)):
            raise WarehouseContractError("input_artifacts must be unique and sorted")
        expected_idempotency_key = compute_export_idempotency_key(
            run_id=self.run_id,
            source_registry_head_record_id=self.source_registry_prefix.head_record_id,
            ordered_input_artifact_record_ids=[
                item.record_id for item in self.input_artifacts
            ],
            taxonomy_versions=self.taxonomy_versions,
            exporter_versions=self.exporter_versions,
        )
        if self.export_idempotency_key != expected_idempotency_key:
            raise WarehouseContractError("export_idempotency_key mismatch")
        return self


class WarehouseExportBinding(StrictWarehouseModel):
    export_id: str = Field(pattern=EXPORT_ID_PATTERN)
    export_idempotency_key: str = Field(pattern=SHA256_PATTERN)
    source_run_id: str = Field(pattern=RUN_ID_PATTERN)
    source_registry_ref: ExternalStorageRef
    logical_layer: LogicalLayer
    storage_root_ref: Literal["ubuntu_v02_claim_warehouse"]
    manifest_relative_path: str = Field(min_length=1, max_length=1024)
    manifest_hash: str = Field(pattern=SHA256_PATTERN)
    rows_hash: str = Field(pattern=SHA256_PATTERN)
    logical_hash: str = Field(pattern=SHA256_PATTERN)
    row_count: int = Field(ge=1)

    @field_validator("manifest_relative_path")
    @classmethod
    def _safe_manifest_path(cls, value: str) -> str:
        value = _validate_relative_posix(value)
        if PurePosixPath(value).parts[:1] != ("exports",) or not value.endswith(
            "/manifest.json"
        ):
            raise WarehouseContractError(
                "manifest binding must use exports/<export_id>/manifest.json"
            )
        return value

    @model_validator(mode="after")
    def _binding_path_matches_export(self) -> "WarehouseExportBinding":
        expected = PurePosixPath("exports") / self.export_id / "manifest.json"
        if PurePosixPath(self.manifest_relative_path) != expected:
            raise WarehouseContractError("manifest binding path/export_id mismatch")
        expected_registry = (
            PurePosixPath("runs")
            / "V02"
            / self.source_run_id
            / "artifact_registry.jsonl"
        )
        if PurePosixPath(self.source_registry_ref.relative_path) != expected_registry:
            raise WarehouseContractError(
                "source Registry path must bind runs/V02/<run_id>/artifact_registry.jsonl"
            )
        return self


class WarehouseLoadPlanV1(StrictWarehouseModel):
    load_plan_schema_version: Literal["claim_warehouse_load_plan_v1.0.0"] = (
        WAREHOUSE_LOAD_PLAN_SCHEMA_VERSION
    )
    load_plan_id: str = Field(pattern=LOAD_PLAN_ID_PATTERN)
    created_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    storage_root_ref: Literal["ubuntu_v02_claim_warehouse"]
    plan_relative_path: str = Field(min_length=1, max_length=1024)
    warehouse_projection_version: Literal["claim_warehouse_projection_v1.0.0"] = (
        WAREHOUSE_PROJECTION_VERSION
    )
    exports: list[WarehouseExportBinding] = Field(min_length=1, max_length=100)
    ordered_export_set_hash: str = Field(pattern=SHA256_PATTERN)
    plan_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("created_at")
    @classmethod
    def _utc_timestamp(cls, value: str) -> str:
        return _validate_utc(value)

    @field_validator("plan_relative_path")
    @classmethod
    def _safe_plan_path(cls, value: str) -> str:
        return _validate_relative_posix(value)

    @model_validator(mode="after")
    def _plan_invariants(self) -> "WarehouseLoadPlanV1":
        if self.plan_relative_path != f"receipts/{self.load_plan_id}.json":
            raise WarehouseContractError(
                "plan_relative_path must be receipts/<load_plan_id>.json"
            )
        observed = [
            (LOGICAL_LAYER_ORDER[item.logical_layer], item.export_id)
            for item in self.exports
        ]
        if observed != sorted(observed):
            raise WarehouseContractError("load plan exports must use canonical global order")
        export_ids = [item.export_id for item in self.exports]
        if len(export_ids) != len(set(export_ids)):
            raise WarehouseContractError("load plan cannot contain duplicate export IDs")
        idempotency_keys = [item.export_idempotency_key for item in self.exports]
        if len(idempotency_keys) != len(set(idempotency_keys)):
            raise WarehouseContractError(
                "load plan cannot contain duplicate export idempotency keys"
            )
        expected_set_hash = sha256_bytes(
            canonical_json_bytes(
                [
                    {
                        "export_id": item.export_id,
                        "export_idempotency_key": item.export_idempotency_key,
                        "source_run_id": item.source_run_id,
                        "source_registry_ref": item.source_registry_ref.model_dump(mode="json"),
                        "logical_hash": item.logical_hash,
                        "manifest_hash": item.manifest_hash,
                    }
                    for item in self.exports
                ]
            )
        )
        if self.ordered_export_set_hash != expected_set_hash:
            raise WarehouseContractError("ordered_export_set_hash mismatch")
        expected_plan_hash = embedded_hash(self.model_dump(mode="json"), "plan_hash")
        if self.plan_hash != expected_plan_hash:
            raise WarehouseContractError("plan_hash mismatch")
        return self

    @classmethod
    def build(cls, **values: Any) -> "WarehouseLoadPlanV1":
        payload = dict(values)
        payload.setdefault("load_plan_schema_version", WAREHOUSE_LOAD_PLAN_SCHEMA_VERSION)
        payload.setdefault("warehouse_projection_version", WAREHOUSE_PROJECTION_VERSION)
        export_values = [
            item.model_dump(mode="json")
            if isinstance(item, WarehouseExportBinding)
            else dict(item)
            for item in payload["exports"]
        ]
        export_values.sort(
            key=lambda item: (LOGICAL_LAYER_ORDER[item["logical_layer"]], item["export_id"])
        )
        payload["exports"] = export_values
        payload["ordered_export_set_hash"] = sha256_bytes(
            canonical_json_bytes(
                [
                    {
                        "export_id": item["export_id"],
                        "export_idempotency_key": item["export_idempotency_key"],
                        "source_run_id": item["source_run_id"],
                        "source_registry_ref": (
                            item["source_registry_ref"].model_dump(mode="json")
                            if isinstance(item["source_registry_ref"], ExternalStorageRef)
                            else item["source_registry_ref"]
                        ),
                        "logical_hash": item["logical_hash"],
                        "manifest_hash": item["manifest_hash"],
                    }
                    for item in export_values
                ]
            )
        )
        payload["plan_hash"] = "0" * 64
        payload["plan_hash"] = embedded_hash(payload, "plan_hash")
        return cls.model_validate(payload)


class ParquetFileDescriptor(StrictWarehouseModel):
    logical_layer: LogicalLayer
    table_code: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    export_id: str = Field(pattern=EXPORT_ID_PATTERN)
    relative_path: str = Field(min_length=1, max_length=1024)
    size_bytes: int = Field(ge=1)
    file_hash: str = Field(pattern=SHA256_PATTERN)
    row_count: int = Field(ge=1)
    row_logical_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("relative_path")
    @classmethod
    def _safe_relative_path(cls, value: str) -> str:
        return _validate_relative_posix(value)

    @model_validator(mode="after")
    def _partition_identity(self) -> "ParquetFileDescriptor":
        parts = PurePosixPath(self.relative_path).parts
        expected_prefix = (
            "parquet",
            f"logical_layer={self.logical_layer}",
            f"table_code={self.table_code}",
            f"schema_version={DATABASE_SCHEMA_VERSION}",
        )
        if (
            len(parts) != 7
            or parts[:4] != expected_prefix
            or re.fullmatch(r"run_date=[0-9]{4}-[0-9]{2}-[0-9]{2}", parts[4])
            is None
            or parts[5] != f"export_id={self.export_id}"
            or parts[6] != "part-00000.parquet"
        ):
            raise WarehouseContractError(
                "Parquet path must match the frozen logical partition identity"
            )
        return self


class WarehouseLoadBatchV1(StrictWarehouseModel):
    load_batch_schema_version: Literal["claim_warehouse_load_batch_v1.0.0"] = (
        WAREHOUSE_LOAD_BATCH_SCHEMA_VERSION
    )
    load_batch_id: str = Field(pattern=LOAD_BATCH_ID_PATTERN)
    load_plan_id: str = Field(pattern=LOAD_PLAN_ID_PATTERN)
    load_plan_hash: str = Field(pattern=SHA256_PATTERN)
    started_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    completed_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    status: Literal["succeeded"] = "succeeded"
    export_count: int = Field(ge=1, le=100)
    row_count: int = Field(ge=1)
    logical_hash: str = Field(pattern=SHA256_PATTERN)
    batch_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("started_at", "completed_at")
    @classmethod
    def _utc_timestamp(cls, value: str) -> str:
        return _validate_utc(value)

    @model_validator(mode="after")
    def _batch_hash(self) -> "WarehouseLoadBatchV1":
        if self.completed_at < self.started_at:
            raise WarehouseContractError("completed_at cannot precede started_at")
        if self.batch_hash != embedded_hash(self.model_dump(mode="json"), "batch_hash"):
            raise WarehouseContractError("batch_hash mismatch")
        return self

    @classmethod
    def build(cls, **values: Any) -> "WarehouseLoadBatchV1":
        payload = dict(values)
        payload.setdefault("load_batch_schema_version", WAREHOUSE_LOAD_BATCH_SCHEMA_VERSION)
        payload.setdefault("status", "succeeded")
        payload["batch_hash"] = "0" * 64
        payload["batch_hash"] = embedded_hash(payload, "batch_hash")
        return cls.model_validate(payload)


class ProjectionJournalEntry(StrictWarehouseModel):
    sequence_no: int = Field(ge=1, le=8)
    stage: ProjectionStage
    completed_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)

    @field_validator("completed_at")
    @classmethod
    def _utc_timestamp(cls, value: str) -> str:
        return _validate_utc(value)


class WarehouseProjectionAttemptV1(StrictWarehouseModel):
    projection_attempt_schema_version: Literal[
        "claim_warehouse_projection_attempt_v1.0.0"
    ] = WAREHOUSE_PROJECTION_ATTEMPT_SCHEMA_VERSION
    attempt_id: str = Field(pattern=ATTEMPT_ID_PATTERN)
    load_plan_id: str = Field(pattern=LOAD_PLAN_ID_PATTERN)
    load_plan_hash: str = Field(pattern=SHA256_PATTERN)
    attempt_no: int = Field(ge=1)
    status: Literal["succeeded", "failed"]
    last_completed_stage: ProjectionStage
    phase_history: list[ProjectionJournalEntry] = Field(min_length=1, max_length=8)
    started_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    completed_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    error_code: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]{2,95}$")
    error_message: str | None = Field(default=None, max_length=1000)
    receipt_id: str | None = Field(default=None, pattern=LOAD_RECEIPT_ID_PATTERN)
    receipt_hash: str | None = Field(default=None, pattern=SHA256_PATTERN)
    attempt_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("started_at", "completed_at")
    @classmethod
    def _utc_timestamp(cls, value: str) -> str:
        return _validate_utc(value)

    @model_validator(mode="after")
    def _attempt_invariants(self) -> "WarehouseProjectionAttemptV1":
        if self.completed_at < self.started_at:
            raise WarehouseContractError("completed_at cannot precede started_at")
        if self.status == "failed":
            if self.error_code is None or self.error_message is None:
                raise WarehouseContractError("failed attempt requires error details")
            if self.receipt_id is not None or self.receipt_hash is not None:
                raise WarehouseContractError("failed attempt cannot bind success receipt")
        elif (
            self.error_code is not None
            or self.error_message is not None
            or self.receipt_id is None
            or self.receipt_hash is None
        ):
            raise WarehouseContractError(
                "succeeded attempt requires receipt and forbids error details"
            )
        sequences = [entry.sequence_no for entry in self.phase_history]
        stages = [entry.stage for entry in self.phase_history]
        if sequences != list(range(1, len(self.phase_history) + 1)):
            raise WarehouseContractError("projection journal sequence must be contiguous")
        if stages != list(PROJECTION_STAGE_ORDER[: len(stages)]):
            raise WarehouseContractError(
                "projection journal must be an exact prefix of the eight frozen stages"
            )
        if stages[-1] != self.last_completed_stage:
            raise WarehouseContractError(
                "last_completed_stage must match the projection journal tail"
            )
        if self.status == "succeeded" and tuple(stages) != PROJECTION_STAGE_ORDER:
            raise WarehouseContractError(
                "succeeded projection attempt requires all eight frozen stages"
            )
        if self.attempt_hash != embedded_hash(
            self.model_dump(mode="json"), "attempt_hash"
        ):
            raise WarehouseContractError("attempt_hash mismatch")
        return self

    @classmethod
    def build(cls, **values: Any) -> "WarehouseProjectionAttemptV1":
        payload = dict(values)
        payload.setdefault(
            "projection_attempt_schema_version",
            WAREHOUSE_PROJECTION_ATTEMPT_SCHEMA_VERSION,
        )
        if "phase_history" not in payload:
            last_stage = payload["last_completed_stage"]
            last_index = PROJECTION_STAGE_ORDER.index(last_stage)
            payload["phase_history"] = [
                {
                    "sequence_no": index + 1,
                    "stage": stage,
                    "completed_at": payload["completed_at"],
                }
                for index, stage in enumerate(
                    PROJECTION_STAGE_ORDER[: last_index + 1]
                )
            ]
        payload["attempt_hash"] = "0" * 64
        payload["attempt_hash"] = embedded_hash(payload, "attempt_hash")
        return cls.model_validate(payload)


class WarehouseLoadReceiptV1(StrictWarehouseModel):
    load_receipt_schema_version: Literal["claim_warehouse_load_receipt_v1.0.0"] = (
        WAREHOUSE_LOAD_RECEIPT_SCHEMA_VERSION
    )
    receipt_id: str = Field(pattern=LOAD_RECEIPT_ID_PATTERN)
    receipt_relative_path: str = Field(min_length=1, max_length=1024)
    load_batch: WarehouseLoadBatchV1
    exports: list[WarehouseExportBinding] = Field(min_length=1, max_length=100)
    parquet_manifest: list[ParquetFileDescriptor] = Field(min_length=1)
    row_counts: dict[str, int]
    watermark: dict[str, str]
    dependency_versions: dict[str, str]
    duckdb_transaction_marker: str = Field(pattern=SHA256_PATTERN)
    receipt_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("receipt_relative_path")
    @classmethod
    def _safe_receipt_path(cls, value: str) -> str:
        return _validate_relative_posix(value)

    @model_validator(mode="after")
    def _receipt_invariants(self) -> "WarehouseLoadReceiptV1":
        if self.receipt_relative_path != f"receipts/{self.receipt_id}.json":
            raise WarehouseContractError(
                "receipt_relative_path must be receipts/<receipt_id>.json"
            )
        if len(self.exports) != self.load_batch.export_count:
            raise WarehouseContractError("receipt export count mismatch")
        if sum(self.row_counts.values()) != self.load_batch.row_count:
            raise WarehouseContractError("receipt row count mismatch")
        if sum(item.row_count for item in self.parquet_manifest) != self.load_batch.row_count:
            raise WarehouseContractError("Parquet manifest row count mismatch")
        expected = embedded_hash(self.model_dump(mode="json"), "receipt_hash")
        if self.receipt_hash != expected:
            raise WarehouseContractError("receipt_hash mismatch")
        return self

    @classmethod
    def build(cls, **values: Any) -> "WarehouseLoadReceiptV1":
        payload = dict(values)
        payload.setdefault("load_receipt_schema_version", WAREHOUSE_LOAD_RECEIPT_SCHEMA_VERSION)
        if isinstance(payload.get("load_batch"), WarehouseLoadBatchV1):
            payload["load_batch"] = payload["load_batch"].model_dump(mode="json")
        payload["exports"] = [
            item.model_dump(mode="json")
            if isinstance(item, WarehouseExportBinding)
            else dict(item)
            for item in payload["exports"]
        ]
        payload["parquet_manifest"] = [
            item.model_dump(mode="json")
            if isinstance(item, ParquetFileDescriptor)
            else dict(item)
            for item in payload["parquet_manifest"]
        ]
        payload["receipt_hash"] = "0" * 64
        payload["receipt_hash"] = embedded_hash(payload, "receipt_hash")
        return cls.model_validate(payload)


class WarehouseMigrationResultV1(StrictWarehouseModel):
    migration_result_schema_version: Literal[
        "claim_warehouse_migration_result_v1.0.0"
    ] = WAREHOUSE_MIGRATION_RESULT_SCHEMA_VERSION
    migration_batch_id: str = Field(
        pattern=r"^migration_batch_[0-9a-hjkmnp-tv-z]{26}$"
    )
    created_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    source_schema_version: str = Field(pattern=r"^truthfulness_db_v[0-9]+\.[0-9]+\.[0-9]+$")
    target_schema_version: Literal["truthfulness_db_v02.1.0"] = DATABASE_SCHEMA_VERSION
    legacy_file_hash: str = Field(pattern=SHA256_PATTERN)
    ordered_source_receipt_hashes: list[str] = Field(min_length=1)
    source_row_count: int = Field(ge=1)
    rebuilt_logical_hash: str = Field(pattern=SHA256_PATTERN)
    successor_ref: ExternalStorageRef
    successor_file_hash: str = Field(pattern=SHA256_PATTERN)
    result_relative_path: str = Field(min_length=1, max_length=1024)
    result_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("created_at")
    @classmethod
    def _utc(cls, value: str) -> str:
        return _validate_utc(value)

    @field_validator("result_relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _validate_relative_posix(value)

    @model_validator(mode="after")
    def _invariants(self) -> "WarehouseMigrationResultV1":
        if not self.source_schema_version.startswith("truthfulness_db_v02.0."):
            raise WarehouseContractError("unsupported legacy warehouse schema major/minor")
        if self.successor_ref.storage_root_ref != CLAIM_WAREHOUSE_STORAGE_ROOT_REF:
            raise WarehouseContractError("migration successor must use Claim warehouse root")
        if len(self.ordered_source_receipt_hashes) != len(
            set(self.ordered_source_receipt_hashes)
        ) or any(
            re.fullmatch(SHA256_PATTERN, value) is None
            for value in self.ordered_source_receipt_hashes
        ):
            raise WarehouseContractError("migration receipt hashes must be unique SHA-256")
        if self.result_relative_path != f"receipts/{self.migration_batch_id}.json":
            raise WarehouseContractError("migration result path/batch mismatch")
        if self.result_hash != embedded_hash(
            self.model_dump(mode="json"), "result_hash"
        ):
            raise WarehouseContractError("migration result hash mismatch")
        return self

    @classmethod
    def build(cls, **values: Any) -> "WarehouseMigrationResultV1":
        payload = dict(values)
        payload.setdefault(
            "migration_result_schema_version",
            WAREHOUSE_MIGRATION_RESULT_SCHEMA_VERSION,
        )
        if isinstance(payload.get("successor_ref"), ExternalStorageRef):
            payload["successor_ref"] = payload["successor_ref"].model_dump(mode="json")
        payload["result_hash"] = "0" * 64
        payload["result_hash"] = embedded_hash(payload, "result_hash")
        return cls.model_validate(payload)


class WarehouseMigrationRollbackV1(StrictWarehouseModel):
    migration_rollback_schema_version: Literal[
        "claim_warehouse_migration_rollback_v1.0.0"
    ] = WAREHOUSE_MIGRATION_ROLLBACK_SCHEMA_VERSION
    rollback_id: str = Field(pattern=r"^rollback_[0-9a-hjkmnp-tv-z]{26}$")
    migration_batch_id: str = Field(pattern=r"^migration_batch_[0-9a-hjkmnp-tv-z]{26}$")
    migration_result_hash: str = Field(pattern=SHA256_PATTERN)
    rolled_back_at: str = Field(pattern=UTC_TIMESTAMP_PATTERN)
    legacy_file_hash: str = Field(pattern=SHA256_PATTERN)
    archived_successor_ref: ExternalStorageRef
    archived_successor_hash: str = Field(pattern=SHA256_PATTERN)
    rollback_relative_path: str = Field(min_length=1, max_length=1024)
    rollback_hash: str = Field(pattern=SHA256_PATTERN)

    @field_validator("rolled_back_at")
    @classmethod
    def _utc(cls, value: str) -> str:
        return _validate_utc(value)

    @field_validator("rollback_relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _validate_relative_posix(value)

    @model_validator(mode="after")
    def _invariants(self) -> "WarehouseMigrationRollbackV1":
        if self.archived_successor_ref.storage_root_ref != CLAIM_WAREHOUSE_STORAGE_ROOT_REF:
            raise WarehouseContractError("rollback archive must use Claim warehouse root")
        if self.rollback_relative_path != f"receipts/{self.rollback_id}.json":
            raise WarehouseContractError("rollback receipt path/ID mismatch")
        if self.rollback_hash != embedded_hash(
            self.model_dump(mode="json"), "rollback_hash"
        ):
            raise WarehouseContractError("migration rollback hash mismatch")
        return self

    @classmethod
    def build(cls, **values: Any) -> "WarehouseMigrationRollbackV1":
        payload = dict(values)
        payload.setdefault(
            "migration_rollback_schema_version",
            WAREHOUSE_MIGRATION_ROLLBACK_SCHEMA_VERSION,
        )
        if isinstance(payload.get("archived_successor_ref"), ExternalStorageRef):
            payload["archived_successor_ref"] = payload[
                "archived_successor_ref"
            ].model_dump(mode="json")
        payload["rollback_hash"] = "0" * 64
        payload["rollback_hash"] = embedded_hash(payload, "rollback_hash")
        return cls.model_validate(payload)


def validate_exact_scale_counts(observed: Mapping[str, int]) -> None:
    """Fail closed unless every frozen 501-scale count matches exactly."""

    missing = sorted(set(EXACT_SCALE_COUNTS) - set(observed))
    extra = sorted(set(observed) - set(EXACT_SCALE_COUNTS))
    mismatched = {
        key: (EXACT_SCALE_COUNTS[key], observed.get(key))
        for key in EXACT_SCALE_COUNTS
        if observed.get(key) != EXACT_SCALE_COUNTS[key]
    }
    if missing or extra or mismatched:
        raise WarehouseContractError(
            f"501 scale mismatch: missing={missing}, extra={extra}, values={mismatched}"
        )


def canonical_export_sort_key(binding: WarehouseExportBinding) -> tuple[int, str]:
    return LOGICAL_LAYER_ORDER[binding.logical_layer], binding.export_id


def split_export_bindings(
    bindings: Sequence[WarehouseExportBinding], *, max_exports: int = 100
) -> list[list[WarehouseExportBinding]]:
    """Globally sort exports across layers, then split into deterministic batches."""

    if not 1 <= max_exports <= 100:
        raise WarehouseContractError("max_exports must be between 1 and 100")
    ordered = sorted(bindings, key=canonical_export_sort_key)
    export_ids = [item.export_id for item in ordered]
    if len(export_ids) != len(set(export_ids)):
        raise WarehouseContractError("duplicate export IDs are forbidden")
    return [ordered[index : index + max_exports] for index in range(0, len(ordered), max_exports)]


def build_exact_scale_bindings(
    *, storage_root_ref: str = "ubuntu_v02_claim_warehouse"
) -> tuple[WarehouseExportBinding, ...]:
    """Build the frozen 919 synthetic export identities without any real data."""

    groups = (
        ("core_provenance", "s01", EXACT_SCALE_COUNTS["s01_machine_export_packages"]),
        ("source_depth", "depth", EXACT_SCALE_COUNTS["source_depth_synthetic_exports"]),
        (
            "human_annotation",
            "human",
            EXACT_SCALE_COUNTS["human_annotation_synthetic_exports"],
        ),
    )
    bindings: list[WarehouseExportBinding] = []
    for logical_layer, group, count in groups:
        for index in range(count):
            seed = f"501-scale:{group}:{index:04d}"
            export_id = deterministic_typed_id("export", seed)
            manifest_hash = sha256_bytes(f"manifest:{seed}".encode("utf-8"))
            rows_hash = sha256_bytes(f"rows:{seed}".encode("utf-8"))
            bindings.append(
                WarehouseExportBinding(
                    export_id=export_id,
                    export_idempotency_key=sha256_bytes(
                        f"idempotency:{seed}".encode("utf-8")
                    ),
                    source_run_id=deterministic_typed_id("run", seed),
                    source_registry_ref=ExternalStorageRef(
                        storage_root_ref="repository",
                        relative_path=(
                            "runs/V02/"
                            f"{deterministic_typed_id('run', seed)}/artifact_registry.jsonl"
                        ),
                    ),
                    logical_layer=logical_layer,
                    storage_root_ref=storage_root_ref,
                    manifest_relative_path=(
                        f"exports/{export_id}/manifest.json"
                    ),
                    manifest_hash=manifest_hash,
                    rows_hash=rows_hash,
                    logical_hash=rows_hash,
                    row_count=1,
                )
            )
    if len(bindings) != EXACT_SCALE_COUNTS["total_export_packages"]:
        raise AssertionError("internal 501-scale export count drift")
    return tuple(bindings)


__all__ = [
    "ALLOWED_TABLE_CODES",
    "DATABASE_SCHEMA_VERSION",
    "EXACT_SCALE_COUNTS",
    "ExternalStorageRef",
    "InputArtifactRef",
    "LABEL_TAXONOMY_VERSION",
    "LOGICAL_LAYER_ORDER",
    "LogicalLayer",
    "ParquetFileDescriptor",
    "PROJECTION_STAGE_ORDER",
    "ProjectionStage",
    "ProjectionJournalEntry",
    "ROW_STABLE_REVISION_TABLE_CODES",
    "RegistryPrefix",
    "WAREHOUSE_EXPORT_SCHEMA_VERSION",
    "WAREHOUSE_LOAD_BATCH_SCHEMA_VERSION",
    "WAREHOUSE_LOAD_PLAN_SCHEMA_VERSION",
    "WAREHOUSE_LOAD_RECEIPT_SCHEMA_VERSION",
    "WAREHOUSE_PROJECTION_ATTEMPT_SCHEMA_VERSION",
    "WAREHOUSE_PROJECTION_VERSION",
    "WAREHOUSE_ROW_SCHEMA_VERSION",
    "WarehouseConflictError",
    "WarehouseContractError",
    "WarehouseDependencyUnavailable",
    "WarehouseExportBinding",
    "WarehouseExportManifestV1",
    "WarehouseLoadBatchV1",
    "WarehouseLoadPlanV1",
    "WarehouseLoadReceiptV1",
    "WarehouseMigrationResultV1",
    "WarehouseMigrationRollbackV1",
    "WarehouseProjectionAttemptV1",
    "WarehouseRow",
    "WAREHOUSE_ENTITY_CODES",
    "CONTROL_LEDGER_TABLE_CODES",
    "TABLE_DATA_MODELS",
    "build_exact_scale_bindings",
    "canonical_export_sort_key",
    "canonical_json_bytes",
    "compute_export_idempotency_key",
    "deterministic_typed_id",
    "embedded_hash",
    "sha256_bytes",
    "split_export_bindings",
    "validate_exact_scale_counts",
]
