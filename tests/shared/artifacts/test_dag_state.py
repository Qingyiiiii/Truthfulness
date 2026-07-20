from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from video_truthfulness.core.artifacts.dag import (
    derive_dag_state,
    explain_node,
    load_dag,
    write_dag_state,
)
from video_truthfulness.core.artifacts.models import (
    new_typed_ulid,
    to_artifact_record_view,
)
from video_truthfulness.core.artifacts.registry import create_artifact_record


ROOT = Path(__file__).resolve().parents[3]
RUN_ID = "run_01j00000000000000000000000"


def _record(
    number: int,
    artifact_type: str,
    node_id: str,
    *,
    lifecycle: str = "validated",
    validation_status: str = "passed",
):
    return to_artifact_record_view(
        create_artifact_record(
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
            schema_versions=["artifact_record_v1.1.0"],
            tool_versions={"synthetic": "1"},
            authority_level="machine_derived",
            lifecycle_state=lifecycle,
            validation_status=validation_status,
            privacy_class="public_synthetic",
            access_scope="public",
            retention_policy="test only",
            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    )


def test_minimal_realistic_registry_materializes_only_three_nodes(
    tmp_path: Path,
) -> None:
    dag = load_dag(ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1.yaml")
    records = [
        _record(1, "run.identity", "source_identity"),
        _record(2, "media.video", "public_no_cookie_download"),
        _record(3, "media.validation", "media_validation"),
    ]
    state = derive_dag_state(dag, records, run_id=RUN_ID)
    satisfied = {
        node_id
        for node_id, node in state["nodes"].items()
        if node["status"] == "satisfied"
    }
    assert satisfied == {
        "source_identity",
        "public_no_cookie_download",
        "media_validation",
    }
    assert state["nodes"]["acquisition_decision"]["status"] == "ready"
    assert state["nodes"]["authorized_cookie_fallback"]["status"] == "blocked"
    assert state["nodes"]["manual_media_input"]["status"] == "manual_gate"
    assert state["nodes"]["subtitle_or_audio_decision"]["status"] == "ready"
    assert state["nodes"]["claim_extract"]["status"] == "blocked"
    assert not any(node["status"] == "failed" for node in state["nodes"].values())

    explanation = explain_node(state, "claim_extract")
    assert explanation["missing_inputs"] == ["transcript.normalized"]
    output = tmp_path / f"dag-state-{new_typed_ulid('test')}.json"
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
        [
            _record(1, "run.identity", "source_identity"),
            _record(2, "media.video", "public_no_cookie_download"),
        ],
        run_id=RUN_ID,
    )
    assert state["nodes"]["public_no_cookie_download"]["status"] == "satisfied"
    assert state["nodes"]["authorized_cookie_fallback"]["status"] != "satisfied"
    assert state["nodes"]["manual_media_input"]["status"] != "satisfied"


def test_stale_output_is_explained_as_stale() -> None:
    dag = load_dag(ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1.yaml")
    state = derive_dag_state(
        dag,
        [_record(1, "run.identity", "source_identity", lifecycle="stale")],
        run_id=RUN_ID,
    )
    assert explain_node(state, "source_identity")["status"] == "stale"


def test_v12_ocr_gate_materialization_controls_alignment_readiness() -> None:
    dag = load_dag(
        ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1_2.yaml"
    )
    base_records = [
        _record(1, "media.validation", "media_validation"),
        _record(2, "transcript.raw", "asr_transcription"),
    ]

    no_decision = derive_dag_state(dag, base_records, run_id=RUN_ID)
    assert no_decision["nodes"]["optional_ocr"]["status"] == "ready"
    assert no_decision["nodes"]["transcript_normalize_and_align"]["status"] == "blocked"
    assert no_decision["nodes"]["transcript_normalize_and_align"]["missing_inputs"] == [
        "ocr.gate_decision"
    ]

    not_applicable = derive_dag_state(
        dag,
        base_records + [_record(3, "ocr.gate_decision", "optional_ocr")],
        run_id=RUN_ID,
    )
    assert not_applicable["nodes"]["optional_ocr"]["status"] == "satisfied"
    assert (
        not_applicable["nodes"]["transcript_normalize_and_align"]["status"] == "ready"
    )
    assert not_applicable["nodes"]["optional_ocr"]["artifact_ids"] == [
        "artifact_00000000000000000000000003"
    ]

    executed = derive_dag_state(
        dag,
        base_records
        + [
            _record(3, "ocr.gate_decision", "optional_ocr"),
            _record(4, "ocr.result", "optional_ocr"),
        ],
        run_id=RUN_ID,
    )
    assert executed["nodes"]["optional_ocr"]["status"] == "satisfied"
    assert executed["nodes"]["optional_ocr"]["artifact_ids"] == [
        "artifact_00000000000000000000000003",
        "artifact_00000000000000000000000004",
    ]
    assert executed["nodes"]["transcript_normalize_and_align"]["status"] == "ready"


def test_v12_source_depth_routes_converge_at_s03_route_boundary() -> None:
    dag = load_dag(
        ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1_2.yaml"
    )
    assert dag.dag_version == "youtube_truthfulness_dag_v1.2.0"
    assert dag.workflow_version == "youtube_truthfulness_workflow_v1.3.0"

    nodes = {node.node_id: node for node in dag.nodes}
    no_depth_route = (
        "source_depth_decision",
        "screening_sync",
        "screening_pool_update",
    )
    depth_route = (
        "source_depth_decision",
        "source_depth_prompt",
        "external_depth_action",
        "source_depth_import_validation",
        "evidence_merge",
        "verdict_rebuild",
        "report_rebuild",
        "screening_sync",
        "screening_pool_update",
    )

    # This checks declared next-node topology only; payload routing is validated elsewhere.
    routes: list[tuple[str, ...]] = []
    pending = [("source_depth_decision", ("source_depth_decision",))]
    while pending:
        node_id, route = pending.pop()
        if node_id == "screening_pool_update":
            routes.append(route)
            continue
        assert nodes[node_id].stage_id != "S03"
        pending.extend(
            (next_node, (*route, next_node)) for next_node in nodes[node_id].next_nodes
        )

    assert set(routes) == {no_depth_route, depth_route}
    assert set(no_depth_route) & set(depth_route) == {
        "source_depth_decision",
        "screening_sync",
        "screening_pool_update",
    }

    s02_nodes = depth_route[1:-1]
    assert {nodes[node_id].stage_id for node_id in s02_nodes} == {"S02"}
    assert {nodes[node_id].workflow_ref for node_id in s02_nodes} == {
        "S02@youtube_truthfulness_workflow_v1.3.0"
    }
    s03_target = nodes["screening_pool_update"]
    assert s03_target.stage_id == "S03"
    assert s03_target.workflow_ref == "S03@youtube_truthfulness_workflow_v1.1.0"


def test_v13_multi_output_node_requires_every_current_validated_output() -> None:
    dag = load_dag(
        ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1_3.yaml"
    )
    normalized = _record(
        1,
        "transcript.normalized",
        "transcript_normalize_and_align",
    )

    missing = derive_dag_state(dag, [normalized], run_id=RUN_ID)
    assert missing["nodes"]["transcript_normalize_and_align"]["status"] == "incomplete"
    assert missing["nodes"]["transcript_normalize_and_align"]["missing_outputs"] == [
        "transcript.alignment"
    ]

    created = derive_dag_state(
        dag,
        [
            normalized,
            _record(
                2,
                "transcript.alignment",
                "transcript_normalize_and_align",
                lifecycle="created",
                validation_status="not_validated",
            ),
        ],
        run_id=RUN_ID,
    )
    assert created["nodes"]["transcript_normalize_and_align"]["status"] == "incomplete"

    partial = derive_dag_state(
        dag,
        [
            normalized,
            _record(
                3,
                "transcript.alignment",
                "transcript_normalize_and_align",
                validation_status="partial",
            ),
        ],
        run_id=RUN_ID,
    )
    assert partial["nodes"]["transcript_normalize_and_align"]["status"] == "incomplete"

    complete = derive_dag_state(
        dag,
        [
            normalized,
            _record(
                4,
                "transcript.alignment",
                "transcript_normalize_and_align",
                lifecycle="frozen",
            ),
        ],
        run_id=RUN_ID,
    )
    assert complete["nodes"]["transcript_normalize_and_align"]["status"] == "satisfied"


def test_invalid_input_record_does_not_unlock_downstream_node() -> None:
    dag = load_dag(
        ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1_3.yaml"
    )
    state = derive_dag_state(
        dag,
        [
            _record(
                1,
                "transcript.raw",
                "asr_transcription",
                lifecycle="created",
                validation_status="not_validated",
            ),
            _record(2, "ocr.gate_decision", "optional_ocr"),
        ],
        run_id=RUN_ID,
    )
    assert state["nodes"]["transcript_normalize_and_align"]["status"] == "blocked"
    assert state["nodes"]["transcript_normalize_and_align"]["missing_inputs"] == [
        "transcript.raw"
    ]


def test_v13_conditional_gate_payload_is_required_for_fail_closed_projection() -> None:
    dag = load_dag(
        ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1_3.yaml"
    )
    gate = _record(2, "ocr.gate_decision", "optional_ocr")
    unresolved = derive_dag_state(dag, [gate], run_id=RUN_ID)
    assert unresolved["nodes"]["optional_ocr"]["status"] == "invalid"
    resolved = derive_dag_state(
        dag,
        [gate],
        run_id=RUN_ID,
        artifact_payloads={
            gate.artifact_id: {"payload": {"gate_state": "NOT_APPLICABLE"}}
        },
    )
    assert resolved["nodes"]["optional_ocr"]["status"] == "satisfied"
