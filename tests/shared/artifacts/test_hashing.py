from __future__ import annotations

from pathlib import Path

from video_truthfulness.core.artifacts.hashing import (
    canonical_json_bytes,
    directory_hash,
    directory_manifest,
    input_fingerprint,
    semantic_hash_text,
    sha256_bytes,
    sha256_file,
)


def test_file_directory_and_semantic_hashes_are_deterministic() -> None:
    example = Path(__file__).resolve().parents[3] / "examples" / "artifact_registry" / "synthetic_run"
    manifest = directory_manifest(example)
    paths = [row["relative_path"] for row in manifest]
    assert paths == sorted(paths)
    run_row = next(row for row in manifest if row["relative_path"] == "run.json")
    assert run_row["sha256"] == sha256_file(example / "run.json")
    assert directory_hash(example) == sha256_bytes(canonical_json_bytes(manifest))
    assert semantic_hash_text("line  \r\nnext\r\n") == semantic_hash_text("line\nnext\n")
    assert canonical_json_bytes({"b": 2, "a": 1}) == b'{"a":1,"b":2}'


def test_every_generation_version_participates_in_input_fingerprint() -> None:
    base = {
        "upstream_hashes": ["b" * 64, "a" * 64],
        "upstream_entity_hashes": ["c" * 64],
        "agent_version": "agent-v1",
        "workflow_version": "workflow-v1",
        "schema_versions": ["schema-b", "schema-a"],
        "prompt_version": "prompt-v1",
        "dag_version": "dag-v1",
        "code_version": "commit-v1",
        "tool_versions": {"tool": "1"},
        "parameters_hash": "d" * 64,
    }
    fingerprint = input_fingerprint(**base)
    assert fingerprint == input_fingerprint(**{**base, "upstream_hashes": list(reversed(base["upstream_hashes"]))})

    changes = {
        "agent_version": "agent-v2",
        "workflow_version": "workflow-v2",
        "schema_versions": ["schema-c"],
        "prompt_version": "prompt-v2",
        "dag_version": "dag-v2",
        "code_version": "commit-v2",
        "tool_versions": {"tool": "2"},
        "parameters_hash": "e" * 64,
        "upstream_entity_hashes": ["f" * 64],
    }
    for field, changed in changes.items():
        assert input_fingerprint(**{**base, field: changed}) != fingerprint
