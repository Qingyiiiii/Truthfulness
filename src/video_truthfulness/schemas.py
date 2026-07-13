"""Pydantic schemas shared by the Demo1 pipeline.

The schema layer is the contract between adapters, extractors, evidence
providers, reasoning, reports, and later training data.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    """Base model that rejects silent schema drift."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)


class Platform(str, Enum):
    """Supported input platforms for Demo1 and manual fallback."""

    BILIBILI = "bilibili"
    DOUYIN = "douyin"
    YOUTUBE = "youtube"
    MANUAL = "manual"
    UNKNOWN = "unknown"


class ClaimType(str, Enum):
    """Claim categories used for filtering and reporting."""

    EVENT = "event"
    NUMERIC = "numeric"
    POLICY = "policy"
    QUOTE = "quote"
    CAUSAL = "causal"
    COMPARISON = "comparison"
    OTHER = "other"


class Checkability(str, Enum):
    """Whether a claim can be checked with external evidence."""

    CHECKABLE = "checkable"
    NEEDS_CONTEXT = "needs_context"
    NOT_CHECKABLE = "not_checkable"


class StanceLabel(str, Enum):
    """Author stance labels; these are not truth verdicts."""

    SUPPORT = "support"
    OPPOSE = "oppose"
    SKEPTICAL = "skeptical"
    MARKETING = "marketing"
    SATIRE = "satire"
    NEUTRAL = "neutral"
    MIXED = "mixed"


class AuthorEvidenceKind(str, Enum):
    """Types of evidence shown or claimed by the video author."""

    SCREENSHOT = "screenshot"
    LINK = "link"
    NEWS = "news"
    DATA = "data"
    COMMENT = "comment"
    EXPERIMENT = "experiment"
    PERSONAL_EXPERIENCE = "personal_experience"
    OTHER = "other"


class SourceType(str, Enum):
    """External evidence source categories."""

    OFFICIAL = "official"
    ACADEMIC = "academic"
    NEWS = "news"
    DATABASE = "database"
    SOCIAL = "social"
    UNKNOWN = "unknown"


class EvidenceRelation(str, Enum):
    """Manual or model-assisted relation between evidence and a claim."""

    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class DownloadStatus(str, Enum):
    """Outcome of one compliant media download attempt."""

    SUCCESS = "success"
    MISSING_COMPONENT = "missing_component"
    BLOCKED = "blocked"
    FAILED = "failed"


class VerdictLabel(str, Enum):
    """Conservative verdict labels used by Demo1."""

    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    PARTIALLY_SUPPORTS = "partially_supports"
    MISLEADING = "misleading"
    MISSING_CONTEXT = "missing_context"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    UNCHECKABLE = "uncheckable"


class VideoInput(StrictModel):
    """Original user input and authorization boundary."""

    platform: Platform = Platform.MANUAL
    source_url: str | None = None
    authorized: bool = True
    manual_transcript_path: str | None = None
    notes: str = ""


class VideoMetadata(StrictModel):
    """Metadata collected from a platform or manual fallback."""

    title: str
    platform: Platform = Platform.MANUAL
    source_url: str | None = None
    author: str | None = None
    published_at: datetime | None = None
    retrieved_at: datetime
    duration_seconds: float | None = Field(default=None, ge=0)


class TranscriptSegment(StrictModel):
    """A traceable text segment from subtitle, ASR, page text, or manual input."""

    segment_id: str
    text: str = Field(min_length=1)
    start_seconds: float | None = Field(default=None, ge=0)
    end_seconds: float | None = Field(default=None, ge=0)


class Transcript(StrictModel):
    """Structured transcript used by claim extraction."""

    language: str = "zh"
    source: str = "manual"
    segments: list[TranscriptSegment]

    def full_text(self) -> str:
        """Return all segment text in source order."""

        return "\n".join(segment.text for segment in self.segments)


class Claim(StrictModel):
    """Atomic factual claim extracted from transcript text."""

    claim_id: str
    text: str = Field(min_length=1)
    normalized_text: str
    type: ClaimType = ClaimType.OTHER
    source_segment_ids: list[str] = Field(default_factory=list)
    checkability: Checkability = Checkability.CHECKABLE
    entities: list[str] = Field(default_factory=list)
    time_scope: str | None = None
    location_scope: str | None = None


class Stance(StrictModel):
    """Author stance toward a claim or topic."""

    target: str
    stance: StanceLabel
    evidence_segment_ids: list[str] = Field(default_factory=list)
    reason: str


class AuthorEvidence(StrictModel):
    """Evidence or proof material presented by the video author."""

    author_evidence_id: str
    claim_id: str | None = None
    kind: AuthorEvidenceKind = AuthorEvidenceKind.OTHER
    text: str
    source_segment_ids: list[str] = Field(default_factory=list)
    screenshot_path: str | None = None
    needs_source_trace: bool = True


class SearchQueryPlan(StrictModel):
    """Query plan for a claim."""

    query_id: str
    claim_id: str
    query: str
    priority: int = Field(default=1, ge=1, le=5)
    notes: str = ""


class Evidence(StrictModel):
    """External evidence record tied to exactly one claim."""

    evidence_id: str
    claim_id: str
    search_query: str
    source_url: str
    page_title: str
    publisher: str
    published_at: datetime | None = None
    retrieved_at: datetime
    selected_text: str
    screenshot_path: str | None = None
    source_type: SourceType = SourceType.UNKNOWN
    relation_to_claim: EvidenceRelation = EvidenceRelation.UNKNOWN
    notes: str = ""


class MediaAsset(StrictModel):
    """Downloaded or manually imported media file metadata."""

    asset_id: str
    platform: Platform
    title: str
    source_url: str
    media_path: str | None = None
    filename: str
    status: DownloadStatus
    created_at: datetime
    downloader: str = "manual"
    error_summary: str = ""


class DownloadAttempt(StrictModel):
    """One strategy attempt in a multi-strategy download run."""

    strategy_name: str
    status: DownloadStatus
    started_at: datetime
    ended_at: datetime
    command: list[str] = Field(default_factory=list)
    media_path: str | None = None
    filename: str
    error_summary: str = ""
    blocked_or_denied: bool = False


class DownloadRun(StrictModel):
    """All download attempts for one video URL."""

    run_id: str
    platform: Platform
    title: str
    source_url: str
    created_at: datetime
    run_dir: str
    attempts: list[DownloadAttempt] = Field(default_factory=list)
    final_status: DownloadStatus
    final_media_path: str | None = None
    fallback_required: bool = True


class PageFallbackArtifact(StrictModel):
    """Browser fallback artifacts when direct media download fails."""

    page_url: str
    page_title: str
    captured_at: datetime
    page_text_path: str
    screenshot_path: str | None = None
    keyframe_screenshot_paths: list[str] = Field(default_factory=list)
    notes: str = ""


class EvidenceScore(StrictModel):
    """Explanatory quality score for one evidence item."""

    evidence_id: str
    claim_id: str
    relevance: float = Field(ge=0, le=1)
    authority: float = Field(ge=0, le=1)
    completeness: float = Field(ge=0, le=1)
    screenshot: float = Field(ge=0, le=1)
    final_score: float = Field(ge=0, le=1)
    reason: str


class Verdict(StrictModel):
    """Claim-level judgment derived from evidence."""

    claim_id: str
    verdict: VerdictLabel
    confidence: float = Field(ge=0, le=1)
    reason: str
    supporting_evidence_ids: list[str] = Field(default_factory=list)
    contradicting_evidence_ids: list[str] = Field(default_factory=list)
    missing_context: list[str] = Field(default_factory=list)
    review_required: bool = True


class Report(StrictModel):
    """Machine-readable run report."""

    run_id: str
    created_at: datetime
    video_input: VideoInput
    metadata: VideoMetadata
    transcript: Transcript
    claims: list[Claim]
    stance: list[Stance] = Field(default_factory=list)
    author_evidence: list[AuthorEvidence] = Field(default_factory=list)
    search_queries: list[SearchQueryPlan] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)
    evidence_scores: list[EvidenceScore] = Field(default_factory=list)
    verdicts: list[Verdict] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)


class ReviewRecord(StrictModel):
    """Human review record for future training or evaluation."""

    review_id: str
    run_id: str
    claim_id: str
    reviewer_verdict: VerdictLabel | None = None
    reviewer_notes: str = ""
    created_at: datetime
    extra: dict[str, Any] = Field(default_factory=dict)
