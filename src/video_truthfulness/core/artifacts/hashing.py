"""Deterministic content, semantic, directory and input-fingerprint hashing."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def semantic_hash_text(text: str) -> str:
    normalized = "\n".join(line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"))
    return sha256_bytes(normalized.encode("utf-8"))


def directory_manifest(path: Path) -> list[dict[str, int | str]]:
    root = path.resolve()
    rows: list[dict[str, int | str]] = []
    files = (candidate for candidate in root.rglob("*") if candidate.is_file())
    for file_path in sorted(files, key=lambda candidate: candidate.relative_to(root).as_posix()):
        rows.append(
            {
                "relative_path": file_path.relative_to(root).as_posix(),
                "size_bytes": file_path.stat().st_size,
                "sha256": sha256_file(file_path),
            }
        )
    return rows


def directory_hash(path: Path) -> str:
    return sha256_bytes(canonical_json_bytes(directory_manifest(path)))


def input_fingerprint(
    *,
    upstream_hashes: Iterable[str] = (),
    upstream_entity_hashes: Iterable[str] = (),
    agent_version: str | None = None,
    workflow_version: str | None = None,
    schema_versions: Iterable[str] = (),
    prompt_version: str | None = None,
    dag_version: str | None = None,
    code_version: str | None = None,
    tool_versions: Mapping[str, str] | None = None,
    parameters_hash: str | None = None,
) -> str:
    payload = {
        "upstream_hashes": sorted(upstream_hashes),
        "upstream_entity_hashes": sorted(upstream_entity_hashes),
        "agent_version": agent_version,
        "workflow_version": workflow_version,
        "schema_versions": sorted(schema_versions),
        "prompt_version": prompt_version,
        "dag_version": dag_version,
        "code_version": code_version,
        "tool_versions": dict(sorted((tool_versions or {}).items())),
        "parameters_hash": parameters_hash,
    }
    return sha256_bytes(canonical_json_bytes(payload))


def record_hash(record: Mapping[str, Any]) -> str:
    payload = dict(record)
    payload.pop("record_hash", None)
    return sha256_bytes(canonical_json_bytes(payload))
