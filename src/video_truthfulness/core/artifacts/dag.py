"""Logical DAG validation, state projection and human-readable explanation."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from video_truthfulness.core.artifacts.models import ArtifactRecord, DAGDefinition


class DAGValidationError(ValueError):
    """Raised when a logical DAG has dangling edges or unsafe topology."""


def load_dag(path: Path) -> DAGDefinition:
    """Load a YAML 1.2 document encoded in its dependency-free JSON subset."""

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DAGValidationError(
            "DAG YAML must use the YAML 1.2 JSON-compatible subset so it can be parsed without unsafe loaders."
        ) from exc
    dag = DAGDefinition.model_validate(raw)
    validate_dag(dag)
    return dag


def validate_dag(dag: DAGDefinition) -> dict[str, int]:
    node_map = {node.node_id: node for node in dag.nodes}
    if not set(dag.entry_nodes).issubset(node_map):
        raise DAGValidationError("DAG contains unknown entry nodes.")
    if not set(dag.terminal_nodes).issubset(node_map):
        raise DAGValidationError("DAG contains unknown terminal nodes.")
    if any(node_map[node_id].node_type != "terminal" for node_id in dag.terminal_nodes):
        raise DAGValidationError("Every terminal_nodes entry must reference a terminal node.")
    stages = {node.stage_id for node in dag.nodes}
    expected_stages = {f"S0{number}" for number in range(1, 10)}
    if stages != expected_stages:
        raise DAGValidationError(f"DAG must cover S01-S09 exactly; got {sorted(stages)}")
    edge_count = 0
    graph: dict[str, list[str]] = {}
    for node in dag.nodes:
        references = node.next_nodes + node.fallback_nodes + ([node.failure_terminal] if node.failure_terminal else [])
        unknown = sorted(set(references) - set(node_map))
        if unknown:
            raise DAGValidationError(f"Node {node.node_id} has dangling edges: {unknown}")
        if node.node_type == "terminal" and node.next_nodes:
            raise DAGValidationError(f"Terminal node {node.node_id} cannot have next_nodes.")
        if node.failure_terminal and node_map[node.failure_terminal].node_type != "terminal":
            raise DAGValidationError(
                f"Node {node.node_id} failure_terminal must reference a terminal node."
            )
        graph[node.node_id] = list(node.next_nodes)
        edge_count += len(node.next_nodes)
        if node.retry_policy.max_attempts < 1:
            raise DAGValidationError(f"Node {node.node_id} has an unbounded retry policy.")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise DAGValidationError(f"Unbounded structural cycle detected at {node_id}.")
        if node_id in visited:
            return
        visiting.add(node_id)
        for child in graph[node_id]:
            visit(child)
        visiting.remove(node_id)
        visited.add(node_id)

    for entry in dag.entry_nodes:
        visit(entry)
    unreachable = sorted(set(node_map) - visited)
    if unreachable:
        raise DAGValidationError(f"DAG contains unreachable nodes: {unreachable}")
    return {"node_count": len(dag.nodes), "edge_count": edge_count, "stage_count": len(stages)}


def derive_dag_state(
    dag: DAGDefinition,
    records: Iterable[ArtifactRecord],
    *,
    run_id: str,
) -> dict[str, Any]:
    latest: dict[str, ArtifactRecord] = {}
    for record in records:
        if record.run_id == run_id:
            latest[record.artifact_id] = record
    by_type: dict[str, list[ArtifactRecord]] = {}
    for record in latest.values():
        by_type.setdefault(record.artifact_type, []).append(record)

    nodes: dict[str, dict[str, Any]] = {}
    for node in dag.nodes:
        outputs = [
            record
            for artifact_type in node.declared_outputs
            for record in by_type.get(artifact_type, [])
            if record.dag_node_id == node.node_id
        ]
        missing_inputs = [artifact_type for artifact_type in node.required_inputs if not by_type.get(artifact_type)]
        if outputs:
            if any(record.lifecycle_state == "stale" for record in outputs):
                status, reason = "stale", "one or more declared outputs are stale"
            elif any(record.lifecycle_state == "invalid" or record.validation_status == "failed" for record in outputs):
                status, reason = "invalid", "one or more declared outputs are invalid"
            else:
                status, reason = "satisfied", "declared output exists and is neither stale nor invalid"
        elif missing_inputs:
            status, reason = "blocked", f"missing required inputs: {', '.join(missing_inputs)}"
        elif node.manual_gate:
            status, reason = "manual_gate", "required inputs exist; explicit human decision is required"
        elif node.node_type == "terminal":
            status, reason = "not_materialized", "terminal condition has not been materialized"
        else:
            status, reason = "ready", "required inputs exist; node has not been executed"
        nodes[node.node_id] = {
            "stage_id": node.stage_id,
            "status": status,
            "reason": reason,
            "artifact_ids": sorted({record.artifact_id for record in outputs}),
            "missing_inputs": missing_inputs,
        }
    return {
        "dag_state_schema_version": "youtube_truthfulness_dag_state_v1.0.0",
        "dag_id": dag.dag_id,
        "dag_version": dag.dag_version,
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "registry_record_count": len(list(latest.values())),
        "nodes": nodes,
    }


def explain_node(state: dict[str, Any], node_id: str) -> dict[str, Any]:
    try:
        node = state["nodes"][node_id]
    except KeyError as exc:
        raise KeyError(f"Unknown DAG node: {node_id}") from exc
    return {"run_id": state["run_id"], "dag_version": state["dag_version"], "node_id": node_id, **node}


def write_dag_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
