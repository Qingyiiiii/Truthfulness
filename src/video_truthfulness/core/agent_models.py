"""Strict contracts for the LangGraph evidence-aware query workflow."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class AgentStrictModel(BaseModel):
    """Reject undeclared fields at every agent boundary."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class OutcomeStatus(str, Enum):
    ANSWERED = "answered"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    REFUSED = "refused"
    HUMAN_REVIEW_REQUIRED = "human_review_required"
    TIMEOUT = "timeout"
    FAILED = "failed"


class QueryCategory(str, Enum):
    FACT_CHECK = "fact_check"
    SOURCE_LOOKUP = "source_lookup"
    UNAUTHORIZED = "unauthorized"
    HIGH_RISK_DECISION = "high_risk_decision"
    PROMPT_MANIPULATION = "prompt_manipulation"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class NodeStatus(str, Enum):
    SUCCESS = "success"
    TIMEOUT = "timeout"
    FAILED = "failed"
    SKIPPED = "skipped"


class FailureCode(str, Enum):
    UNAUTHORIZED = "unauthorized"
    POLICY_REFUSAL = "policy_refusal"
    RETRIEVAL_TIMEOUT = "retrieval_timeout"
    GENERATION_TIMEOUT = "generation_timeout"
    CITATION_TIMEOUT = "citation_timeout"
    TOOL_TIMEOUT = "tool_timeout"
    INVALID_CITATION = "invalid_citation"
    MALFORMED_MODEL_OUTPUT = "malformed_model_output"
    INTERNAL_ERROR = "internal_error"


class TokenSource(str, Enum):
    PROVIDER_REPORTED = "provider_reported"
    ESTIMATED = "estimated"
    NO_LLM = "no_llm"
    UNAVAILABLE = "unavailable"


class CostSource(str, Enum):
    LOCAL_ZERO = "local_zero"
    CONFIGURED_RATE = "configured_rate"
    NO_LLM = "no_llm"
    UNAVAILABLE = "unavailable"


class QueryRequest(AgentStrictModel):
    query: str = Field(min_length=3, max_length=2000)
    authorized: bool = True
    top_k: int = Field(default=4, ge=1, le=8)


class QueryClassification(AgentStrictModel):
    category: QueryCategory
    allowed: bool
    risk_level: RiskLevel
    reason: str


class SourceDocument(AgentStrictModel):
    source_id: str
    title: str
    publisher: str
    source_url: str
    source_type: str
    published_at: datetime | None = None
    retrieved_at: datetime
    authorized: bool = True
    content: str = Field(min_length=1)
    conflict_group: str | None = None
    claim_value: str | None = None
    contains_prompt_injection: bool = False


class RetrievedEvidence(AgentStrictModel):
    source: SourceDocument
    score: float = Field(ge=0, le=1)


class EvidenceAssessment(AgentStrictModel):
    sufficient: bool
    conflicting: bool = False
    reason: str


class GeneratedAnswer(AgentStrictModel):
    answer: str
    citation_source_ids: list[str] = Field(default_factory=list)
    refusal_reason: str | None = None
    review_reason: str | None = None


class Citation(AgentStrictModel):
    source_id: str
    page_title: str
    publisher: str
    source_url: str
    quote: str
    retrieved_at: datetime
    score: float = Field(ge=0, le=1)


class CitationValidation(AgentStrictModel):
    valid: bool
    citations: list[Citation] = Field(default_factory=list)
    reason: str


class NodeTrace(AgentStrictModel):
    node: str
    status: NodeStatus
    elapsed_ms: float = Field(ge=0)
    attempts: int = Field(ge=1)
    error_code: FailureCode | None = None
    error_message: str | None = None


class TokenUsage(AgentStrictModel):
    prompt_tokens: int | None = Field(default=None, ge=0)
    completion_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    source: TokenSource = TokenSource.UNAVAILABLE


class CostUsage(AgentStrictModel):
    amount_usd: float | None = Field(default=None, ge=0)
    source: CostSource = CostSource.UNAVAILABLE


class AgentTelemetry(AgentStrictModel):
    total_elapsed_ms: float = Field(ge=0)
    nodes: list[NodeTrace] = Field(default_factory=list)
    tokens: TokenUsage = Field(default_factory=TokenUsage)
    cost: CostUsage = Field(default_factory=CostUsage)
    retries: int = Field(default=0, ge=0)
    timed_out: bool = False


class QueryResponse(AgentStrictModel):
    trace_id: str
    status: OutcomeStatus
    classification: QueryClassification
    answer: str
    citations: list[Citation] = Field(default_factory=list)
    review_task_id: str | None = None
    failure_code: FailureCode | None = None
    failure_message: str | None = None
    telemetry: AgentTelemetry


class HealthResponse(AgentStrictModel):
    status: str
    indexed_sources: int = Field(ge=0)
    embedding_backend: str
    llm_provider: str


class SourceLookupInput(AgentStrictModel):
    source_id: str = Field(min_length=1, max_length=200)


class SourceInfo(AgentStrictModel):
    source_id: str
    title: str
    publisher: str
    source_url: str
    source_type: str
    published_at: datetime | None = None
    retrieved_at: datetime
    authorized: bool


class CreateReviewTaskInput(AgentStrictModel):
    trace_id: str
    query: str = Field(min_length=3, max_length=2000)
    reason: str = Field(min_length=1, max_length=2000)
    evidence_ids: list[str] = Field(default_factory=list)


class ReviewTask(AgentStrictModel):
    task_id: str
    trace_id: str
    query: str
    reason: str
    evidence_ids: list[str]
    status: str
    created_at: datetime


class EvalCase(AgentStrictModel):
    case_id: str
    category: str
    query: str
    authorized: bool = True
    expected_status: OutcomeStatus
    expected_source_ids: list[str] = Field(default_factory=list)
    forbidden_phrases: list[str] = Field(default_factory=list)
    fault_stage: str | None = None


class EvalCaseResult(AgentStrictModel):
    case_id: str
    category: str
    passed: bool
    failures: list[str] = Field(default_factory=list)
    response: QueryResponse


class EvalSummary(AgentStrictModel):
    total: int
    passed: int
    failed: int
    results: list[EvalCaseResult]
