from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from video_truthfulness.core.artifacts.dag import derive_dag_state, explain_node, load_dag, write_dag_state
from video_truthfulness.core.artifacts.models import new_typed_ulid
from video_truthfulness.core.artifacts.registry import create_artifact_record


ROOT = Path(__file__).resolve().parents[3]
RUN_ID = "run_01j00000000000000000000000"


def _record(number: int, artifact_type: str, node_id: str, *, lifecycle: str = "validated"):
    return create_artifact_record(
        artifact_id=f"artifact_{number:026d}",
        artifact_type=artifact_type,
        logical_name=f"synthetic-{artifact_type}",
        container_kind="file",
        project_version="v0.2",
        storage_version="V02",
        source_platform="youtube",
        source_id="youtube_synth3tic01",
        run_id=RUN_ID,
        stage_id="S01",
        dag_node_id=node_id,
        relative_path=f"runs/V02/{RUN_ID}/artifact-{number}.json",
        storage_scope="run",
        media_type="application/json",
        size_bytes=number,
        content_hash=f"{number:064x}",
        producer_type="workflow",
        schema_versions=["artifact_record_v1.0.0"],
        tool_versions={"synthetic": "1"},
        authority_level="machine_derived",
        lifecycle_state=lifecycle,
        validation_status="passed",
        privacy_class="public_synthetic",
        access_scope="public",
        retention_policy="test only",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_minimal_realistic_registry_materializes_only_three_nodes() -> None:
    dag = load_dag(ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1.yaml")
    records = [
        _record(1, "run.identity", "source_identity"),
        _record(2, "media.video", "public_no_cookie_download"),
        _record(3, "media.validation", "media_validation"),
    ]
    state = derive_dag_state(dag, records, run_id=RUN_ID)
    satisfied = {node_id for node_id, node in state["nodes"].items() if node["status"] == "satisfied"}
    assert satisfied == {"source_identity", "public_no_cookie_download", "media_validation"}
    assert state["nodes"]["acquisition_decision"]["status"] == "ready"
    assert state["nodes"]["authorized_cookie_fallback"]["status"] == "blocked"
    assert state["nodes"]["manual_media_input"]["status"] == "manual_gate"
    assert state["nodes"]["subtitle_or_audio_decision"]["status"] == "ready"
    assert state["nodes"]["claim_extract"]["status"] == "blocked"
    assert not any(node["status"] == "failed" for node in state["nodes"].values())

    explanation = explain_node(state, "claim_extract")
    assert explanation["missing_inputs"] == ["transcript.normalized"]
    temp_root = ROOT / ".tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    output = temp_root / f"dag-state-{new_typed_ulid('test')}.json"
    try:
        write_dag_state(output, state)
    finally:
        output.unlink(missing_ok=True)
    rebuilt = derive_dag_state(dag, records, run_id=RUN_ID)
    assert rebuilt["nodes"] == state["nodes"]


def test_same_artifact_type_does_not_satisfy_other_acquisition_branches() -> None:
    dag = load_dag(ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1.yaml")
    state = derive_dag_state(
        dag,
        [_record(1, "run.identity", "source_identity"), _record(2, "media.video", "public_no_cookie_download")],
        run_id=RUN_ID,
    )
    assert state["nodes"]["public_no_cookie_download"]["status"] == "satisfied"
    assert state["nodes"]["authorized_cookie_fallback"]["status"] != "satisfied"
    assert state["nodes"]["manual_media_input"]["status"] != "satisfied"


def test_stale_output_is_explained_as_stale() -> None:
    dag = load_dag(ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1.yaml")
    state = derive_dag_state(dag, [_record(1, "run.identity", "source_identity", lifecycle="stale")], run_id=RUN_ID)
    assert explain_node(state, "source_identity")["status"] == "stale"
