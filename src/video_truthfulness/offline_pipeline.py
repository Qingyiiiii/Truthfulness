"""Offline Demo1 pipeline.

This path intentionally avoids platform downloads, browser search, ASR, OCR, and
LLM calls. It lets the project validate schemas, storage, claim extraction,
evidence scoring, reasoning, and report generation before real video intake.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from video_truthfulness.claims import RuleBasedClaimExtractor
from video_truthfulness.json_io import append_jsonl, read_json, write_json
from video_truthfulness.naming import build_run_id
from video_truthfulness.reasoning import generate_verdicts
from video_truthfulness.reporting import write_report
from video_truthfulness.schemas import (
    Platform,
    Report,
    ReviewRecord,
    Transcript,
    VideoInput,
    VideoMetadata,
)
from video_truthfulness.scoring import score_evidence
from video_truthfulness.search import build_query_plan, load_manual_evidence


@dataclass(frozen=True)
class OfflineRunResult:
    """Paths and report object produced by one offline run."""

    run_dir: Path
    report: Report
    markdown_report_path: Path
    json_report_path: Path


def run_offline_demo(
    transcript_path: Path,
    evidence_path: Path,
    runs_dir: Path = Path("runs"),
    video_title: str = "offline_demo",
    source_url: str | None = None,
) -> OfflineRunResult:
    """Run transcript -> claim -> evidence -> verdict -> report."""

    created_at = datetime.now(timezone.utc)
    run_id = build_run_id(Platform.MANUAL, video_title, created_at)
    run_dir = runs_dir / run_id
    _create_run_directories(run_dir)

    video_input = VideoInput(
        platform=Platform.MANUAL,
        source_url=source_url,
        authorized=True,
        manual_transcript_path=str(transcript_path),
        notes="Offline MVP uses local transcript and local evidence JSON.",
    )
    metadata = VideoMetadata(
        title=video_title,
        platform=Platform.MANUAL,
        source_url=source_url,
        retrieved_at=created_at,
    )
    transcript = _load_transcript(transcript_path)
    evidence = load_manual_evidence(evidence_path)
    claims = RuleBasedClaimExtractor().extract(transcript)
    search_queries = build_query_plan(claims)
    evidence_scores = score_evidence(claims, evidence)
    verdicts = generate_verdicts(claims, evidence, evidence_scores)

    report = Report(
        run_id=run_id,
        created_at=created_at,
        video_input=video_input,
        metadata=metadata,
        transcript=transcript,
        claims=claims,
        search_queries=search_queries,
        evidence=evidence,
        evidence_scores=evidence_scores,
        verdicts=verdicts,
        limitations=[
            "Offline MVP did not download video or audio.",
            "Offline MVP did not capture browser evidence screenshots.",
            "Offline MVP did not call an LLM; claims are rule-extracted.",
        ],
    )

    _write_run_artifacts(run_dir, report)
    markdown_path, json_path = write_report(report, run_dir)
    _initialize_review_file(run_dir, report)
    return OfflineRunResult(run_dir=run_dir, report=report, markdown_report_path=markdown_path, json_report_path=json_path)


def _create_run_directories(run_dir: Path) -> None:
    """Create all run subdirectories expected by Demo1."""

    for child in ("media", "frames", "screenshots"):
        (run_dir / child).mkdir(parents=True, exist_ok=True)


def _load_transcript(path: Path) -> Transcript:
    """Load and validate a transcript JSON file."""

    return Transcript.model_validate(read_json(path))


def _write_run_artifacts(run_dir: Path, report: Report) -> None:
    """Write intermediate artifacts expected by the project plan."""

    write_json(run_dir / "input.json", report.video_input)
    write_json(run_dir / "metadata.json", report.metadata)
    write_json(run_dir / "transcript.json", report.transcript)
    write_json(run_dir / "claims.json", report.claims)
    write_json(run_dir / "stance.json", report.stance)
    write_json(run_dir / "author_evidence.json", report.author_evidence)
    write_json(run_dir / "search_queries.json", report.search_queries)
    write_json(run_dir / "evidence_manifest.json", report.evidence)
    write_json(run_dir / "evidence_scores.json", report.evidence_scores)
    write_json(run_dir / "verdicts.json", report.verdicts)
    append_jsonl(
        run_dir / "run_log.jsonl",
        {
            "stage": "offline_mvp_completed",
            "created_at": report.created_at.isoformat(),
            "claims": len(report.claims),
            "evidence": len(report.evidence),
            "verdicts": len(report.verdicts),
        },
    )


def _initialize_review_file(run_dir: Path, report: Report) -> None:
    """Create empty review records for every claim."""

    for claim in report.claims:
        append_jsonl(
            run_dir / "review.jsonl",
            ReviewRecord(
                review_id=f"review_{claim.claim_id}",
                run_id=report.run_id,
                claim_id=claim.claim_id,
                created_at=report.created_at,
                reviewer_notes="Pending human review.",
            ),
        )
