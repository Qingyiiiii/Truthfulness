"""Evidence artifact storage helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from video_truthfulness.json_io import write_json
from video_truthfulness.naming import safe_filename_part, timestamp_for_filename
from video_truthfulness.schemas import Evidence


def evidence_screenshot_name(evidence: Evidence, moment: datetime | None = None) -> str:
    """Build a deterministic evidence screenshot filename."""

    claim_id = safe_filename_part(evidence.claim_id, 32)
    evidence_id = safe_filename_part(evidence.evidence_id, 32)
    source_type = safe_filename_part(evidence.source_type.value, 24)
    timestamp = timestamp_for_filename(moment)
    return f"{claim_id}_{evidence_id}_{timestamp}_{source_type}.png"


def save_evidence_manifest(run_dir: Path, evidence: list[Evidence]) -> Path:
    """Write `evidence_manifest.json` for a run."""

    path = run_dir / "evidence_manifest.json"
    write_json(path, evidence)
    return path


def ensure_screenshot_dir(run_dir: Path) -> Path:
    """Create and return the run screenshot directory."""

    screenshot_dir = run_dir / "screenshots"
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    return screenshot_dir
