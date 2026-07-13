"""Markdown and JSON report generation."""

from __future__ import annotations

from pathlib import Path

from video_truthfulness.json_io import write_json
from video_truthfulness.schemas import Report


def write_report(report: Report, run_dir: Path) -> tuple[Path, Path]:
    """Write `report.md` and `report.json` into a run directory."""

    markdown_path = run_dir / "report.md"
    json_path = run_dir / "report.json"
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    write_json(json_path, report)
    return markdown_path, json_path


def render_markdown(report: Report) -> str:
    """Render the report for human review."""

    lines: list[str] = []
    lines.append(f"# Video Truthfulness Report: {report.metadata.title}")
    lines.append("")
    lines.append(f"- Run ID: `{report.run_id}`")
    lines.append(f"- Platform: `{report.metadata.platform.value}`")
    lines.append(f"- Created at: `{report.created_at.isoformat()}`")
    lines.append("")
    lines.append("## Claims")
    for claim in report.claims:
        lines.append(f"- `{claim.claim_id}` {claim.text}")
        lines.append(f"  - Type: `{claim.type.value}`")
        lines.append(f"  - Checkability: `{claim.checkability.value}`")
    lines.append("")
    lines.append("## Evidence")
    if report.evidence:
        for evidence in report.evidence:
            lines.append(f"- `{evidence.evidence_id}` for `{evidence.claim_id}`")
            lines.append(f"  - Source: {evidence.publisher} - {evidence.page_title}")
            lines.append(f"  - URL: {evidence.source_url}")
            lines.append(f"  - Relation: `{evidence.relation_to_claim.value}`")
            lines.append(f"  - Screenshot: `{evidence.screenshot_path or 'missing'}`")
    else:
        lines.append("- No external evidence was provided.")
    lines.append("")
    lines.append("## Verdicts")
    for verdict in report.verdicts:
        lines.append(f"- `{verdict.claim_id}` -> `{verdict.verdict.value}`")
        lines.append(f"  - Confidence: `{verdict.confidence:.2f}`")
        lines.append(f"  - Review required: `{verdict.review_required}`")
        lines.append(f"  - Reason: {verdict.reason}")
    lines.append("")
    lines.append("## Limitations")
    for limitation in report.limitations:
        lines.append(f"- {limitation}")
    lines.append("")
    return "\n".join(lines)
