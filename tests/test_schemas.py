from pathlib import Path

from video_truthfulness.json_io import read_json
from video_truthfulness.schemas import Evidence, EvidenceRelation, SourceType, Transcript


def test_transcript_schema_loads_example() -> None:
    transcript = Transcript.model_validate(read_json(Path("examples/offline_demo/transcript.json")))

    assert transcript.language == "zh"
    assert transcript.segments[0].segment_id == "seg_001"
    assert "粮食总产量" in transcript.full_text()


def test_evidence_schema_loads_example() -> None:
    raw = read_json(Path("examples/offline_demo/evidence.json"))
    evidence = Evidence.model_validate(raw["evidence"][0])

    assert evidence.evidence_id == "ev_001"
    assert evidence.source_type == SourceType.OFFICIAL
    assert evidence.relation_to_claim == EvidenceRelation.SUPPORTS
