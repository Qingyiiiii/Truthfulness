from __future__ import annotations

from datetime import datetime, timezone

from video_truthfulness.core.artifacts.invalidation import entity_ref_key, fingerprint_is_stale, propagate_stale
from video_truthfulness.core.artifacts.models import UpstreamEntityRef, to_artifact_record_view
from video_truthfulness.core.artifacts.registry import create_artifact_record


RUN_ID = "run_01j00000000000000000000000"


def _record(
    number: int,
    artifact_type: str,
    *,
    upstream: list[str] | None = None,
    entity_refs: list[UpstreamEntityRef] | None = None,
    fingerprint: str | None = None,
):
    return to_artifact_record_view(create_artifact_record(
        artifact_id=f"artifact_{number:026d}",
        artifact_type=artifact_type,
        logical_name=f"synthetic-{artifact_type}",
        container_kind="jsonl_container",
        project_version="v0.2",
        storage_version="V02",
        source_platform="youtube",
        source_id="youtube_synth3tic01",
        run_id=RUN_ID,
        stage_id="S01",
        dag_node_id="synthetic_node",
        relative_path=f"runs/V02/{RUN_ID}/artifact-{number}.jsonl",
        storage_scope="run",
        media_type="application/x-ndjson",
        size_bytes=number,
        content_hash=f"{number:064x}",
        producer_type="workflow",
        schema_versions=["artifact_record_v1.1.0"],
        tool_versions={"synthetic": "1"},
        upstream_artifact_ids=upstream or [],
        upstream_entity_refs=entity_refs or [],
        input_fingerprint=fingerprint,
        authority_level="machine_derived",
        lifecycle_state="validated",
        validation_status="passed",
        privacy_class="public_synthetic",
        access_scope="public",
        retention_policy="test only",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    ))


def _chain():
    transcript = _record(1, "transcript.normalized")
    segment_ref = UpstreamEntityRef(
        entity_id="segment_001",
        entity_type="transcript_segment",
        container_artifact_id=transcript.artifact_id,
    )
    claim = _record(2, "claim.collection", upstream=[transcript.artifact_id], entity_refs=[segment_ref])
    evidence = _record(3, "evidence.collection", upstream=[claim.artifact_id])
    verdict = _record(4, "verdict.collection", upstream=[claim.artifact_id, evidence.artifact_id])
    report = _record(5, "report.machine", upstream=[verdict.artifact_id])
    unrelated = _record(6, "claim.entity_index")
    return transcript, claim, evidence, verdict, report, unrelated, segment_ref


def test_container_change_propagates_forward_only() -> None:
    transcript, claim, evidence, verdict, report, unrelated, _ = _chain()
    results = propagate_stale(
        [transcript, claim, evidence, verdict, report, unrelated],
        changed_artifact_ids=[transcript.artifact_id],
    )
    assert {result.artifact_id for result in results} == {
        claim.artifact_id,
        evidence.artifact_id,
        verdict.artifact_id,
        report.artifact_id,
    }
    assert transcript.artifact_id not in {result.artifact_id for result in results}
    assert unrelated.artifact_id not in {result.artifact_id for result in results}


def test_entity_change_invalidates_only_referencing_branch_and_downstream() -> None:
    transcript, claim, evidence, verdict, report, unrelated, segment_ref = _chain()
    results = propagate_stale(
        [transcript, claim, evidence, verdict, report, unrelated],
        changed_entity_refs=[entity_ref_key(segment_ref)],
    )
    assert {result.artifact_id for result in results} == {
        claim.artifact_id,
        evidence.artifact_id,
        verdict.artifact_id,
        report.artifact_id,
    }


def test_report_or_verdict_changes_never_propagate_backwards() -> None:
    transcript, claim, evidence, verdict, report, unrelated, _ = _chain()
    records = [transcript, claim, evidence, verdict, report, unrelated]
    assert propagate_stale(records, changed_artifact_ids=[report.artifact_id]) == []
    assert {result.artifact_id for result in propagate_stale(records, changed_artifact_ids=[verdict.artifact_id])} == {
        report.artifact_id
    }


def test_input_fingerprint_mismatch_marks_candidate_stale_without_mutation() -> None:
    record = _record(1, "report.machine", fingerprint="a" * 64)
    assert not fingerprint_is_stale(record, "a" * 64)
    assert fingerprint_is_stale(record, "b" * 64)
    assert record.lifecycle_state == "validated"
