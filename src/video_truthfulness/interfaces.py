"""Protocol interfaces for Demo1 extension points."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from video_truthfulness.schemas import (
    AuthorEvidence,
    Claim,
    Evidence,
    EvidenceScore,
    Report,
    SearchQueryPlan,
    Stance,
    Transcript,
    Verdict,
    VideoInput,
    VideoMetadata,
)


class PlatformAdapter(Protocol):
    """Adapter for one video platform."""

    platform_name: str

    def supports(self, source_url: str) -> bool:
        """Return whether this adapter can handle the URL."""

    def fetch_metadata(self, video_input: VideoInput) -> VideoMetadata:
        """Fetch public or user-authorized metadata without bypassing controls."""


class MediaIntake(Protocol):
    """Media acquisition and manual fallback interface."""

    def collect(self, video_input: VideoInput, run_dir: Path) -> Transcript:
        """Return a transcript or raise a clear intake failure."""


class TranscriptBuilder(Protocol):
    """Build a structured transcript from one allowed input source."""

    def build(self, source_path: Path) -> Transcript:
        """Convert local input into a traceable transcript."""


class ClaimExtractor(Protocol):
    """Extract checkable claims from transcript text."""

    def extract(self, transcript: Transcript) -> list[Claim]:
        """Return atomic claims with source segment IDs."""


class StanceAnalyzer(Protocol):
    """Analyze author stance without producing truth verdicts."""

    def analyze(self, transcript: Transcript, claims: list[Claim]) -> list[Stance]:
        """Return stance records tied to claims or topics."""


class AuthorEvidenceExtractor(Protocol):
    """Extract evidence shown by the video author."""

    def extract(self, transcript: Transcript, claims: list[Claim]) -> list[AuthorEvidence]:
        """Return author evidence records that still need verification."""


class SearchProvider(Protocol):
    """Retrieve or accept external evidence candidates."""

    def plan_queries(self, claims: list[Claim]) -> list[SearchQueryPlan]:
        """Build search queries for claims."""

    def collect(self, queries: list[SearchQueryPlan], run_dir: Path) -> list[Evidence]:
        """Collect evidence and save screenshots when required."""


class EvidenceStore(Protocol):
    """Persist evidence records and associated artifacts."""

    def save(self, evidence: list[Evidence], run_dir: Path) -> Path:
        """Write evidence metadata and return the manifest path."""


class EvidenceScorer(Protocol):
    """Score evidence quality without replacing a verdict."""

    def score(self, claims: list[Claim], evidence: list[Evidence]) -> list[EvidenceScore]:
        """Return explanatory scores for each evidence item."""


class ReasoningEngine(Protocol):
    """Align claims and evidence into conservative verdicts."""

    def decide(
        self,
        claims: list[Claim],
        evidence: list[Evidence],
        evidence_scores: list[EvidenceScore],
    ) -> list[Verdict]:
        """Return verdicts that cite existing evidence IDs."""


class ReportGenerator(Protocol):
    """Generate human and machine-readable reports."""

    def write(self, report: Report, run_dir: Path) -> tuple[Path, Path]:
        """Write Markdown and JSON report files."""


class ReviewStore(Protocol):
    """Store human review data."""

    def initialize(self, report: Report, run_dir: Path) -> Path:
        """Create a review JSONL file for future manual decisions."""
