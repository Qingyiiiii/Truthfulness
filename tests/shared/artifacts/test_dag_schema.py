from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JSONSchemaValidationError
from pydantic import ValidationError

from video_truthfulness.core.artifacts.dag import (
    DAGValidationError,
    load_dag,
    validate_dag,
)
from video_truthfulness.core.artifacts.models import DAGDefinition


ROOT = Path(__file__).resolve().parents[3]
DAG_PATH = ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1.yaml"
DAG_V11_PATH = ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1_1.yaml"
DAG_V12_PATH = ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1_2.yaml"


def test_full_s01_to_s09_dag_matches_schema_and_invariants() -> None:
    raw = json.loads(DAG_PATH.read_text(encoding="utf-8"))
    schema = json.loads(
        (
            ROOT / "schemas" / "dag" / "youtube_truthfulness_dag_v1.schema.json"
        ).read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(raw)

    dag = load_dag(DAG_PATH)
    summary = validate_dag(dag)
    assert summary["node_count"] == 56
    assert summary["stage_count"] == 9
    assert {node.stage_id for node in dag.nodes} == {
        f"S0{number}" for number in range(1, 10)
    }
    assert dag.terminal_nodes == ["wait_for_more_runs", "training_stop", "terminal"]
    assert all(node.retry_policy.max_attempts <= 10 for node in dag.nodes)
    assert {node.node_id for node in dag.nodes if node.manual_gate} >= {
        "authorized_cookie_fallback",
        "human_annotation_gate",
        "dataset_freeze_gate",
        "smoke_gate",
        "capability_claim_gate",
        "memory_promotion_gate",
    }


def test_dag_rejects_dangling_edges_and_structural_cycles() -> None:
    dag = load_dag(DAG_PATH)
    dangling = dag.model_copy(deep=True)
    dangling.nodes[0].next_nodes = ["missing_node"]
    with pytest.raises(DAGValidationError, match="dangling edges"):
        validate_dag(dangling)

    cyclic = dag.model_copy(deep=True)
    cyclic.nodes[0].next_nodes = [cyclic.nodes[0].node_id]
    with pytest.raises(DAGValidationError, match="cycle"):
        validate_dag(cyclic)


def test_v11_dag_matches_schema_and_preserves_v10_topology() -> None:
    v10 = json.loads(DAG_PATH.read_text(encoding="utf-8"))
    v11 = json.loads(DAG_V11_PATH.read_text(encoding="utf-8"))
    schema = json.loads(
        (
            ROOT / "schemas" / "dag" / "youtube_truthfulness_dag_v1_1.schema.json"
        ).read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(v11)
    summary = validate_dag(load_dag(DAG_V11_PATH))
    assert summary == {"node_count": 56, "edge_count": 60, "stage_count": 9}

    def topology(document: dict[str, object]) -> dict[str, object]:
        normalized = json.loads(json.dumps(document))
        normalized.pop("dag_version")
        normalized.pop("workflow_version")
        for node in normalized["nodes"]:
            node["workflow_ref"] = node["workflow_ref"].split("@", maxsplit=1)[0]
        return normalized

    assert topology(v11) == topology(v10)


def test_dag_rejects_mixed_compatibility_generations() -> None:
    raw = json.loads(DAG_V11_PATH.read_text(encoding="utf-8"))
    raw["workflow_version"] = "youtube_truthfulness_workflow_v1.0.0"
    with pytest.raises(ValidationError, match="same compatibility generation"):
        DAGDefinition.model_validate(raw)


def test_v12_dag_uses_exact_stage_scoped_workflow_versions() -> None:
    raw = json.loads(DAG_V12_PATH.read_text(encoding="utf-8"))
    schema = json.loads(
        (
            ROOT / "schemas" / "dag" / "youtube_truthfulness_dag_v1_2.schema.json"
        ).read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(raw)
    dag = load_dag(DAG_V12_PATH)
    assert validate_dag(dag) == {"node_count": 56, "edge_count": 60, "stage_count": 9}
    assert dag.workflow_version == "youtube_truthfulness_workflow_v1.3.0"
    assert (
        dag.workflow_version_for_stage("S01") == "youtube_truthfulness_workflow_v1.1.0"
    )
    assert (
        dag.workflow_version_for_stage("S02") == "youtube_truthfulness_workflow_v1.3.0"
    )
    assert (
        dag.workflow_version_for_stage("S03") == "youtube_truthfulness_workflow_v1.1.0"
    )
    assert {node.workflow_ref for node in dag.nodes if node.stage_id == "S02"} == {
        "S02@youtube_truthfulness_workflow_v1.3.0"
    }
    assert {node.workflow_ref for node in dag.nodes if node.stage_id != "S02"} == {
        f"{stage}@youtube_truthfulness_workflow_v1.1.0"
        for stage in {f"S0{number}" for number in range(1, 10)} - {"S02"}
    }

    by_id = {node.node_id: node for node in dag.nodes}
    assert by_id["screening_sync"].next_nodes == ["screening_pool_update"]
    assert (
        by_id["screening_pool_update"].workflow_ref
        == "S03@youtube_truthfulness_workflow_v1.1.0"
    )
    assert by_id["optional_ocr"].declared_outputs == ["ocr.gate_decision", "ocr.result"]
    assert by_id["transcript_normalize_and_align"].required_inputs == [
        "transcript.raw",
        "ocr.gate_decision",
    ]
    assert by_id["transcript_normalize_and_align"].optional_inputs == ["ocr.result"]


def test_v12_preserves_v11_nodes_edges_and_retry_budgets() -> None:
    v11 = load_dag(DAG_V11_PATH)
    v12 = load_dag(DAG_V12_PATH)

    def control_shape(dag: DAGDefinition) -> dict[str, object]:
        return {
            node.node_id: {
                "stage_id": node.stage_id,
                "node_type": node.node_type,
                "retry_policy": node.retry_policy.model_dump(mode="json"),
                "fallback_nodes": node.fallback_nodes,
                "failure_terminal": node.failure_terminal,
                "next_nodes": node.next_nodes,
            }
            for node in dag.nodes
        }

    assert control_shape(v12) == control_shape(v11)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda raw: raw["workflow_versions"].__setitem__(
                "S02", "youtube_truthfulness_workflow_v1.1.0"
            ),
            "stage-scoped workflow_versions",
        ),
        (
            lambda raw: raw["nodes"][18].__setitem__(
                "workflow_ref", "S02@youtube_truthfulness_workflow_v1.1.0"
            ),
            "mismatched workflow_ref",
        ),
        (
            lambda raw: raw["nodes"][0].__setitem__(
                "workflow_ref", "S01@youtube_truthfulness_workflow_v1.3.0"
            ),
            "mismatched workflow_ref",
        ),
    ],
)
def test_v12_rejects_undeclared_stage_workflow_combinations(
    mutate, message: str
) -> None:
    raw = json.loads(DAG_V12_PATH.read_text(encoding="utf-8"))
    mutate(raw)
    schema = json.loads(
        (
            ROOT / "schemas" / "dag" / "youtube_truthfulness_dag_v1_2.schema.json"
        ).read_text(encoding="utf-8")
    )
    with pytest.raises(JSONSchemaValidationError):
        Draft202012Validator(schema).validate(raw)
    with pytest.raises(ValidationError, match=message):
        DAGDefinition.model_validate(raw)


def test_v11_rejects_stage_scoped_mapping_to_preserve_old_behavior() -> None:
    raw = json.loads(DAG_V11_PATH.read_text(encoding="utf-8"))
    raw["workflow_versions"] = {
        f"S0{number}": "youtube_truthfulness_workflow_v1.1.0" for number in range(1, 10)
    }
    with pytest.raises(ValidationError, match="cannot declare stage-scoped"):
        DAGDefinition.model_validate(raw)
