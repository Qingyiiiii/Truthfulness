"""Rebuildable SQLite projection for authoritative JSONL Registries."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from video_truthfulness.core.artifacts.models import ArtifactRecord
from video_truthfulness.core.artifacts.registry import AppendOnlyRegistry


def _load_records(registries: Iterable[AppendOnlyRegistry]) -> list[ArtifactRecord]:
    records: list[ArtifactRecord] = []
    seen_record_ids: set[str] = set()
    for registry in registries:
        for record in registry.read_records():
            if record.record_id in seen_record_ids:
                raise ValueError(f"Duplicate record_id across Registry scopes: {record.record_id}")
            seen_record_ids.add(record.record_id)
            records.append(record)
    return records


def rebuild_sqlite_projection(output_path: Path, registries: Iterable[AppendOnlyRegistry]) -> dict[str, int]:
    """Delete/rebuild only the non-authoritative SQLite projection."""

    records = _load_records(registries)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    connection = sqlite3.connect(temporary)
    try:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE registry_records (
                record_id TEXT PRIMARY KEY,
                artifact_id TEXT NOT NULL,
                record_revision INTEGER NOT NULL,
                record_hash TEXT NOT NULL,
                previous_record_id TEXT,
                recorded_at TEXT NOT NULL,
                storage_scope TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                validation_status TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                UNIQUE (artifact_id, record_revision)
            );
            CREATE TABLE artifacts (
                artifact_id TEXT PRIMARY KEY,
                current_record_id TEXT NOT NULL,
                current_revision INTEGER NOT NULL,
                artifact_type TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                run_id TEXT,
                storage_scope TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                validation_status TEXT NOT NULL
            );
            CREATE TABLE upstream_edges (
                artifact_id TEXT NOT NULL,
                upstream_artifact_id TEXT NOT NULL,
                PRIMARY KEY (artifact_id, upstream_artifact_id)
            );
            CREATE TABLE entity_refs (
                artifact_id TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                container_artifact_id TEXT NOT NULL,
                PRIMARY KEY (artifact_id, entity_type, entity_id, container_artifact_id)
            );
            CREATE TABLE validation_edges (
                artifact_id TEXT NOT NULL,
                validation_artifact_id TEXT NOT NULL,
                PRIMARY KEY (artifact_id, validation_artifact_id)
            );
            CREATE TABLE dag_node_artifacts (
                run_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                dag_node_id TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                validation_status TEXT NOT NULL,
                PRIMARY KEY (run_id, dag_node_id, artifact_id)
            );
            """
        )
        latest: dict[str, ArtifactRecord] = {}
        for record in records:
            raw_json = json.dumps(record.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            connection.execute(
                "INSERT INTO registry_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.record_id,
                    record.artifact_id,
                    record.record_revision,
                    record.record_hash,
                    record.previous_record_id,
                    record.recorded_at.isoformat(),
                    record.storage_scope,
                    record.lifecycle_state,
                    record.validation_status,
                    raw_json,
                ),
            )
            latest[record.artifact_id] = record
        for artifact_id, record in sorted(latest.items()):
            connection.execute(
                "INSERT INTO artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    artifact_id,
                    record.record_id,
                    record.record_revision,
                    record.artifact_type,
                    record.relative_path,
                    record.content_hash,
                    record.run_id,
                    record.storage_scope,
                    record.lifecycle_state,
                    record.validation_status,
                ),
            )
            for upstream in sorted(record.upstream_artifact_ids):
                connection.execute("INSERT INTO upstream_edges VALUES (?, ?)", (artifact_id, upstream))
            for entity_ref in sorted(
                record.upstream_entity_refs,
                key=lambda ref: (ref.entity_type, ref.entity_id, ref.container_artifact_id),
            ):
                connection.execute(
                    "INSERT INTO entity_refs VALUES (?, ?, ?, ?)",
                    (artifact_id, entity_ref.entity_id, entity_ref.entity_type, entity_ref.container_artifact_id),
                )
            for validation_id in sorted(record.validation_artifact_ids):
                connection.execute("INSERT INTO validation_edges VALUES (?, ?)", (artifact_id, validation_id))
            if record.run_id and record.stage_id and record.dag_node_id:
                connection.execute(
                    "INSERT INTO dag_node_artifacts VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        record.run_id,
                        record.stage_id,
                        record.dag_node_id,
                        artifact_id,
                        record.lifecycle_state,
                        record.validation_status,
                    ),
                )
        connection.commit()
    finally:
        connection.close()
    output_path.unlink(missing_ok=True)
    temporary.replace(output_path)
    return projection_snapshot(output_path)


def projection_snapshot(path: Path) -> dict[str, int]:
    connection = sqlite3.connect(path)
    try:
        names = (
            "registry_records",
            "artifacts",
            "upstream_edges",
            "entity_refs",
            "validation_edges",
            "dag_node_artifacts",
        )
        return {name: connection.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0] for name in names}
    finally:
        connection.close()


def query_artifact(path: Path, artifact_id: str) -> dict[str, str | int | None] | None:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute("SELECT * FROM artifacts WHERE artifact_id = ?", (artifact_id,)).fetchone()
        return dict(row) if row is not None else None
    finally:
        connection.close()
