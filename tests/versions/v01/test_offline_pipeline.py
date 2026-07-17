from pathlib import Path

from video_truthfulness.core.schemas import VerdictLabel
from video_truthfulness.versions.v01.offline_pipeline import run_offline_demo


def test_offline_pipeline_writes_report() -> None:
    runs_dir = Path("tmp/test_runs")
    runs_dir.mkdir(parents=True, exist_ok=True)

    result = run_offline_demo(
        transcript_path=Path("examples/offline_demo/transcript.json"),
        evidence_path=Path("examples/offline_demo/evidence.json"),
        runs_dir=runs_dir,
        video_title="offline_demo",
    )

    assert result.run_dir.exists()
    assert (result.run_dir / "claims.json").exists()
    assert (result.run_dir / "evidence_manifest.json").exists()
    assert (result.run_dir / "report.md").exists()
    assert (result.run_dir / "report.json").exists()
    assert (result.run_dir / "review.jsonl").exists()
    assert len(result.report.claims) == 1
    assert result.report.claims[0].claim_id == "claim_001"
    assert result.report.verdicts[0].verdict == VerdictLabel.SUPPORTS
    assert result.report.verdicts[0].review_required is True
