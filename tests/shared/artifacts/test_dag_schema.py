from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from video_truthfulness.core.artifacts.dag import DAGValidationError, load_dag, validate_dag


ROOT = Path(__file__).resolve().parents[3]
DAG_PATH = ROOT / "configs" / "workflows" / "youtube_truthfulness_dag_v1.yaml"


def test_full_s01_to_s09_dag_matches_schema_and_invariants() -> None:
    raw = json.loads(DAG_PATH.read_text(encoding="utf-8"))
    schema = json.loads((ROOT / "schemas" / "dag" / "youtube_truthfulness_dag_v1.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(raw)

    dag = load_dag(DAG_PATH)
    summary = validate_dag(dag)
    assert summary["node_count"] == 56
    assert summary["stage_count"] == 9
    assert {node.stage_id for node in dag.nodes} == {f"S0{number}" for number in range(1, 10)}
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
