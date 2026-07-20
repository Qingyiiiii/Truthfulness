"""Rebuildable SQLite projection for authoritative JSONL Registries."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from video_truthfulness.core.artifacts.models import ArtifactRecordView
from video_truthfulness.core.artifacts.registry import AppendOnlyRegistry, RegistryEntry


def _load_entries(registries: Iterable[AppendOnlyRegistry]) -> list[RegistryEntry]:
    entries: list[RegistryEntry] = []
    seen_record_ids: set[str] = set()
    for registry in registries:
        for entry in registry.read_entries():
            if entry.wire_record.record_id in seen_record_ids:
                raise ValueError(f"Duplicate record_id across Registry scopes: {entry.wire_record.record_id}")
            seen_record_ids.add(entry.wire_record.record_id)
            entries.append(entry)
    return entries


def rebuild_sqlite_projection(output_path: Path, registries: Iterable[AppendOnlyRegistry]) -> dict[str, int]:
    """Delete/rebuild only the non-authoritative SQLite projection."""

    entries = _load_entries(registries)
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
                source_registry_schema_version TEXT NOT NULL,
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
                storage_root_ref TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                run_id TEXT,
                storage_scope TEXT NOT NULL,
                release_id TEXT,
                exp_id TEXT,
                agent_profile_version TEXT,
                agent_runtime_version TEXT,
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
        latest: dict[str, ArtifactRecordView] = {}
        for entry in entries:
            wire_record = entry.wire_record
            record = entry.canonical_view
            raw_json = json.dumps(
                wire_record.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                """
                INSERT INTO registry_records (
                    record_id, artifact_id, record_revision, record_hash, previous_record_id,
                    recorded_at, storage_scope, source_registry_schema_version,
                    lifecycle_state, validation_status, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.record_id,
                    record.artifact_id,
                    record.record_revision,
                    record.record_hash,
                    record.previous_record_id,
                    record.recorded_at.isoformat(),
                    record.storage_scope,
                    record.source_registry_schema_version,
                    record.lifecycle_state,
                    record.validation_status,
                    raw_json,
                ),
            )
            latest[record.artifact_id] = record
        for artifact_id, record in sorted(latest.items()):
            connection.execute(
                """
                INSERT INTO artifacts (
                    artifact_id, current_record_id, current_revision, artifact_type,
                    storage_root_ref, relative_path, content_hash, run_id, storage_scope, release_id,
                    exp_id, agent_profile_version, agent_runtime_version,
                    lifecycle_state, validation_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    artifact_id,
                    record.record_id,
                    record.record_revision,
                    record.artifact_type,
                    record.storage_root_ref,
                    record.relative_path,
                    record.content_hash,
                    record.run_id,
                    record.storage_scope,
                    record.release_id,
                    record.exp_id,
                    record.agent_profile_version,
                    record.agent_runtime_version,
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
