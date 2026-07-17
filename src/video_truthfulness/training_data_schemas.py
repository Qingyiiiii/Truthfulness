"""Strict schemas for the v0.1.1 training-data quality pack.

These models live beside, rather than inside, the annotation schemas.  Machine
screening and human gold labels remain upstream contracts; this module defines
quality, lineage, SFT, synthetic-data, and preference-pair derivatives.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StrictTrainingDataModel(BaseModel):
    """Reject silent schema drift in generated training-data artifacts."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class GateStatus(str, Enum):
    """Task-specific quality-gate outcome."""

    PASS = "pass"
    QUARANTINE = "quarantine"
    REJECT = "reject"


class OriginType(str, Enum):
    """Where a record came from."""

    HUMAN_GOLD = "human_gold"
    MACHINE_CANDIDATE = "machine_candidate"
    SYNTHETIC = "synthetic"
    DERIVED = "derived"


class UsageScope(str, Enum):
    """Allowed publication/use boundary for a source snapshot."""

    PRIVATE_ONLY = "private_only"
    PUBLIC_SYNTHETIC = "public_synthetic"
    APPROVED_PUBLIC = "approved_public"
    UNKNOWN = "unknown"


class ReviewStatus(str, Enum):
    """Human review state for synthetic preference data."""

    PENDING = "pending"
    REVIEWED = "reviewed"


class ReviewDecision(str, Enum):
    """Allowed single-human pilot decisions."""

    ACCEPT = "accept"
    EDIT = "edit"
    REJECT = "reject"


class ChatMessage(StrictTrainingDataModel):
    """One portable SFT chat message."""

    role: str
    content: str


class TaskGateResult(StrictTrainingDataModel):
    """Explain why a record may or may not enter one task."""

    status: GateStatus
    reasons: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class QualityRecord(StrictTrainingDataModel):
    """Canonical quality/lineage view of one upstream annotation record."""

    schema_version: str = "quality_record_v1"
    pipeline_version: str
    output_dataset_version: str
    source_dataset_version: str
    source_schema_version: str
    record_id: str
    source_record_id: str
    source_id: str
    run_id: str
    claim_id: str
    parent_claim_id: str
    origin: OriginType
    usage_scope: UsageScope
    claim_text: str
    normalized_text: str
    raw_context: str = ""
    status: str
    evidence_quality: str = ""
    domain: str = ""
    claim_type: str = ""
    checkability: str = ""
    noise_patterns: list[str] = Field(default_factory=list)
    recommended_action: str = ""
    include_decision: str = ""
    training_use: str = ""
    input_content_hash: str
    normalized_content_hash: str
    language: str
    pii_flags: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)
    duplicate_cluster_id: str | None = None
    split_group_id: str
    split: str
    evidence_completeness: float = Field(ge=0.0, le=1.0)
    gate_status_by_task: dict[str, TaskGateResult]
    generated_at: str


class SyntheticExample(StrictTrainingDataModel):
    """One controlled, traceable hard negative."""

    schema_version: str = "synthetic_example_v1"
    pipeline_version: str
    output_dataset_version: str
    synthetic_id: str
    parent_record_id: str
    parent_dataset_version: str
    mutation_type: str
    mutation_parameters: dict[str, Any] = Field(default_factory=dict)
    before_text: str
    after_text: str
    generator_type: str
    generator_version: str
    prompt_version: str
    generation_seed: int
    generated_at: str
    verifier_result: str
    verifier_reasons: list[str] = Field(default_factory=list)
    human_review_status: ReviewStatus = ReviewStatus.PENDING
    split: str
    origin: OriginType = OriginType.SYNTHETIC
    content_hash: str


class SFTExample(StrictTrainingDataModel):
    """Portable, task-scoped instruction/chat derivative."""

    schema_version: str = "sft_example_v1"
    pipeline_version: str
    output_dataset_version: str
    sft_example_id: str
    source_record_id: str
    source_dataset_version: str
    task_name: str
    messages: list[ChatMessage]
    target_schema_version: str
    split: str
    origin: OriginType = OriginType.DERIVED
    quality_gate_version: str
    eligibility_reason: str
    content_hash: str


class PreferencePair(StrictTrainingDataModel):
    """A DPO-ready schema record; not evidence of completed RLHF."""

    schema_version: str = "preference_pair_v1"
    pipeline_version: str
    output_dataset_version: str
    pair_id: str
    source_record_id: str
    synthetic_id: str
    prompt: str
    chosen: str
    rejected: str
    rejection_reason: str
    mutation_type: str
    split: str
    review_status: ReviewStatus = ReviewStatus.PENDING
    review_decision: ReviewDecision | None = None
    review_reason: str = ""
    reviewed_at: str | None = None
    reviewer_type: str = "single_human"
    final_chosen: str = ""
    quality_flags: list[str] = Field(default_factory=list)
    content_hash: str


SCHEMA_MODELS: dict[str, type[StrictTrainingDataModel]] = {
    "quality_record_v1": QualityRecord,
    "synthetic_example_v1": SyntheticExample,
    "sft_example_v1": SFTExample,
    "preference_pair_v1": PreferencePair,
}
