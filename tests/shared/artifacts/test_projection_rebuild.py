from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from video_truthfulness.core.artifacts.projection import query_artifact, rebuild_sqlite_projection
from video_truthfulness.core.artifacts.models import new_typed_ulid
from video_truthfulness.core.artifacts.registry import AppendOnlyRegistry, create_artifact_record


RUN_ID = "run_01j00000000000000000000000"
ROOT = Path(__file__).resolve().parents[3]


def _record(number: int, artifact_type: str, node_id: str, *, upstream: list[str] | None = None):
    return create_artifact_record(
        artifact_id=f"artifact_{number:026d}",
        artifact_type=artifact_type,
        logical_name=f"synthetic-{number}",
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
        upstream_artifact_ids=upstream or [],
        authority_level="machine_derived",
        lifecycle_state="validated",
        validation_status="passed",
        privacy_class="public_synthetic",
        access_scope="public",
        retention_policy="test only",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_sqlite_is_rebuildable_and_cannot_write_back_to_jsonl() -> None:
    temp_root = ROOT / ".tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    suffix = new_typed_ulid("test")
    registry_path = temp_root / f"projection-{suffix}.jsonl"
    cross_path = temp_root / f"missing-cross-{suffix}.jsonl"
    index_path = temp_root / f"projection-{suffix}.sqlite3"
    registry = AppendOnlyRegistry(registry_path, scope="run", expected_run_id=RUN_ID)
    try:
        identity = _record(1, "run.identity", "source_identity")
        media = _record(2, "media.video", "public_no_cookie_download", upstream=[identity.artifact_id])
        registry.append_many([identity, media])
        missing_cross_run = AppendOnlyRegistry(cross_path, scope="cross_run")
        authoritative_bytes = registry.path.read_bytes()

        first = rebuild_sqlite_projection(index_path, [registry, missing_cross_run])
        assert first == {
            "registry_records": 2,
            "artifacts": 2,
            "upstream_edges": 1,
            "entity_refs": 0,
            "validation_edges": 0,
            "dag_node_artifacts": 2,
        }
        baseline_query = query_artifact(index_path, media.artifact_id)
        assert baseline_query is not None
        assert baseline_query["content_hash"] == media.content_hash

        connection = sqlite3.connect(index_path)
        try:
            connection.execute("UPDATE artifacts SET relative_path = 'tampered' WHERE artifact_id = ?", (media.artifact_id,))
            connection.commit()
        finally:
            connection.close()
        assert registry.path.read_bytes() == authoritative_bytes
        assert query_artifact(index_path, media.artifact_id)["relative_path"] == "tampered"

        index_path.unlink()
        second = rebuild_sqlite_projection(index_path, [registry, missing_cross_run])
        assert second == first
        assert query_artifact(index_path, media.artifact_id) == baseline_query
        assert registry.path.read_bytes() == authoritative_bytes
    finally:
        registry_path.unlink(missing_ok=True)
        cross_path.unlink(missing_ok=True)
        index_path.unlink(missing_ok=True)
        index_path.with_suffix(index_path.suffix + ".tmp").unlink(missing_ok=True)
