from __future__ import annotations

import hashlib
import os
from dataclasses import replace
from pathlib import Path

import pytest
import video_truthfulness.versions.v02.warehouse_loader as loader_module
import video_truthfulness.versions.v02.warehouse_projection as projection_module

from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    create_artifact_record,
)

from video_truthfulness.versions.v02.warehouse_export import (
    canonicalize_export,
    write_export_package,
)
from video_truthfulness.versions.v02.warehouse_loader import (
    LoaderAdmissionRejected,
    WarehouseLoader,
    WarehouseSingleWriterLock,
    build_loader_admission_context,
    build_load_plan,
    write_immutable_fact,
)
from video_truthfulness.versions.v02.warehouse_models import (
    DATABASE_SCHEMA_VERSION,
    LABEL_TAXONOMY_VERSION,
    ExternalStorageRef,
    WarehouseConflictError,
    WarehouseDependencyUnavailable,
    WarehouseLoadReceiptV1,
    WarehouseProjectionAttemptV1,
    WarehouseRow,
    deterministic_typed_id,
    sha256_bytes,
)
from video_truthfulness.versions.v02.warehouse_projection import (
    WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1,
    WarehouseMigrationAdmission,
    WarehouseMigrationAdmissionRejected,
    build_parquet_artifacts,
    dependency_status,
    migrate_legacy_projection_side_by_side,
)


NOW = "2026-07-20T00:00:00Z"
DONE = "2026-07-20T00:01:00Z"
SHA = "0" * 64
ROOT_REF = "ubuntu_v02_claim_warehouse"


SOURCE_ID = "youtube_synthldr001"


def _record(
    *,
    artifact_id: str,
    record_id: str,
    artifact_type: str,
    relative_path: str,
    run_id: str,
    content_hash: str,
    size_bytes: int,
    semantic_hash: str | None = None,
    storage_root_ref: str = "repository",
    project_version: str = "v0.2",
    storage_version: str = "V02",
):
    return create_artifact_record(
        registry_schema_version="artifact_record_v1.2.0",
        artifact_id=artifact_id,
        record_id=record_id,
        recorded_at=NOW,
        artifact_type=artifact_type,
        logical_name=relative_path.rsplit("/", 1)[-1],
        container_kind="package" if artifact_type == "warehouse.export_batch" else "file",
        project_version=project_version,
        storage_version=storage_version,
        source_platform="youtube",
        source_id=SOURCE_ID,
        run_id=run_id,
        stage_id="S01",
        dag_node_id="warehouse_export",
        relative_path=relative_path,
        storage_root_ref=storage_root_ref,
        storage_scope="run",
        media_type="application/json",
        size_bytes=size_bytes,
        content_hash=content_hash,
        semantic_hash_algorithm="sha256" if semantic_hash is not None else None,
        semantic_hash=semantic_hash,
        producer_type="workflow",
        schema_versions=["synthetic_loader_v1"],
        tool_versions={"synthetic": "1"},
        authority_level="machine_derived",
        lifecycle_state="validated",
        validation_status="passed",
        privacy_class="public_synthetic",
        access_scope="public",
        retention_policy="test only",
        created_at=NOW,
        validated_at=NOW,
    )


def _export_result(
    seed: str = "loader",
    *,
    source_registry: AppendOnlyRegistry | None = None,
    registry_project_version: str = "v0.2",
    registry_storage_version: str = "V02",
):
    run_id = deterministic_typed_id("run", seed)
    artifact_id = deterministic_typed_id("artifact", seed)
    record_id = deterministic_typed_id("record", seed)
    export_id = deterministic_typed_id("export", seed)
    execution_plan_id = deterministic_typed_id("execution_plan", seed)
    if source_registry is not None:
        source_registry.append(
            _record(
                artifact_id=artifact_id,
                record_id=record_id,
                artifact_type="claim.collection",
                relative_path=f"runs/V02/{run_id}/claim.json",
                run_id=run_id,
                content_hash=SHA,
                size_bytes=1,
                project_version=registry_project_version,
                storage_version=registry_storage_version,
            )
        )
        prefix_bytes = source_registry.path.read_bytes()
        head = source_registry.read_entries()[-1].wire_record
        prefix = {
            "record_count": 1,
            "prefix_hash": sha256_bytes(prefix_bytes),
            "head_record_id": head.record_id,
            "head_record_hash": head.record_hash,
        }
    else:
        prefix = {
            "record_count": 1,
            "prefix_hash": SHA,
            "head_record_id": record_id,
            "head_record_hash": SHA,
        }
    rows = [
        WarehouseRow.build(
            logical_layer="core_provenance",
            table_code="source_media",
            canonical_primary_key=SOURCE_ID,
            revision_no=1,
            is_active=True,
            effective_at=NOW,
            run_id=run_id,
            artifact_id=artifact_id,
            artifact_record_id=record_id,
            artifact_content_hash=SHA,
            created_at=NOW,
            writer_role="s01_acquisition_writer",
            schema_versions={"database": DATABASE_SCHEMA_VERSION},
            taxonomy_versions={"label": LABEL_TAXONOMY_VERSION},
            data={
                "source_id": SOURCE_ID,
                "platform": "youtube",
                "platform_source_key": "synthldr001",
                "media_kind": "video",
                "synthetic": True,
            },
        ),
        WarehouseRow.build(
            logical_layer="core_provenance",
            table_code="run",
            canonical_primary_key=run_id,
            revision_no=1,
            is_active=True,
            effective_at=NOW,
            run_id=run_id,
            artifact_id=artifact_id,
            artifact_record_id=record_id,
            artifact_content_hash=SHA,
            created_at=NOW,
            writer_role="stage5_coordinator",
            schema_versions={"database": DATABASE_SCHEMA_VERSION},
            taxonomy_versions={"label": LABEL_TAXONOMY_VERSION},
            data={
                "run_id": run_id,
                "source_id": SOURCE_ID,
                "execution_plan_id": execution_plan_id,
                "source_registry_prefix": prefix,
                "run_created_at": NOW,
                "execution_scope": "synthetic_contract",
                "synthetic": True,
            },
        ),
    ]
    result = canonicalize_export(
        rows,
        export_id=export_id,
        run_id=run_id,
        source_registry_prefix=prefix,
        input_artifacts=[
            {
                "artifact_id": artifact_id,
                "record_id": record_id,
                "artifact_type": "claim.collection",
                "content_hash": SHA,
            }
        ],
        run_created_at=NOW,
        created_at=NOW,
        storage_ref={
            "storage_root_ref": ROOT_REF,
            "relative_path": f"exports/{export_id}",
        },
    )
    if source_registry is not None:
        source_registry.append(
            _record(
                artifact_id=deterministic_typed_id("artifact", f"{seed}:export"),
                record_id=deterministic_typed_id("record", f"{seed}:export"),
                artifact_type="warehouse.export_batch",
                relative_path=result.manifest.manifest_relative_path,
                run_id=run_id,
                content_hash=result.manifest_hash,
                semantic_hash=result.logical_hash,
                size_bytes=len(result.manifest_bytes),
                storage_root_ref=ROOT_REF,
                project_version=registry_project_version,
                storage_version=registry_storage_version,
            )
        )
        source_registry.append(
            _record(
                artifact_id=deterministic_typed_id("artifact", f"{seed}:handoff"),
                record_id=deterministic_typed_id("record", f"{seed}:handoff"),
                artifact_type="handoff.final",
                relative_path=f"runs/V02/{run_id}/HANDOFF.json",
                run_id=run_id,
                content_hash=hashlib.sha256(b"handoff").hexdigest(),
                size_bytes=7,
                project_version=registry_project_version,
                storage_version=registry_storage_version,
            )
        )
    return result


def _fixture(
    tmp_path: Path,
    *,
    registry_project_version: str = "v0.2",
    registry_storage_version: str = "V02",
):
    root = tmp_path / "warehouse"
    root.mkdir()
    if os.name == "nt":
        root = Path("\\\\?\\" + str(root.resolve()))
    roots = {ROOT_REF: root}
    repository_root = tmp_path / "repository"
    repository_root.mkdir()
    run_id = deterministic_typed_id("run", "loader")
    source_registry = AppendOnlyRegistry(
        repository_root / f"runs/V02/{run_id}/artifact_registry.jsonl",
        scope="run",
        expected_run_id=run_id,
    )
    result = _export_result(
        source_registry=source_registry,
        registry_project_version=registry_project_version,
        registry_storage_version=registry_storage_version,
    )
    write_export_package(result, storage_roots=roots)
    plan = build_load_plan(
        [result.load_binding(logical_layer="core_provenance")],
        created_at=NOW,
        storage_root_ref=ROOT_REF,
    )
    cross_run_registry = AppendOnlyRegistry(
        repository_root / "registries/claim_warehouse.jsonl",
        scope="cross_run",
    )
    loader = WarehouseLoader(
        storage_roots=roots,
        storage_root_ref=ROOT_REF,
        source_registry_roots={"repository": repository_root},
        cross_run_registry=cross_run_registry,
    )
    admission = build_loader_admission_context(
        plan,
        source_registry_roots={"repository": repository_root},
    )
    return root, roots, result, plan, admission, loader, cross_run_registry


def test_parquet_build_is_byte_deterministic() -> None:
    result = _export_result()
    first = build_parquet_artifacts([result])
    second = build_parquet_artifacts([result])
    assert [item.descriptor for item in first] == [item.descriptor for item in second]
    assert [item.data for item in first] == [item.data for item in second]
    assert first[0].data[:4] == b"PAR1"


def test_immutable_plan_write_is_idempotent_and_conflict_safe(tmp_path: Path) -> None:
    root, roots, _, plan, _, _, _ = _fixture(tmp_path)
    ref = ExternalStorageRef(
        storage_root_ref=ROOT_REF, relative_path=plan.plan_relative_path
    )
    first = write_immutable_fact(plan, storage_ref=ref, storage_roots=roots)
    second = write_immutable_fact(plan, storage_ref=ref, storage_roots=roots)
    assert first == second
    target = root / plan.plan_relative_path
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(WarehouseConflictError, match="immutable warehouse fact"):
        write_immutable_fact(plan, storage_ref=ref, storage_roots=roots)


def test_second_writer_lock_is_rejected(tmp_path: Path) -> None:
    lock = tmp_path / "locks/warehouse-loader.lock"
    with WarehouseSingleWriterLock(lock, load_plan_id="plan_one"):
        with pytest.raises(WarehouseConflictError, match="SECOND_WRITER"):
            with WarehouseSingleWriterLock(lock, load_plan_id="plan_two"):
                pass
    assert not lock.exists()


def _tree_metadata_snapshot(root: Path) -> tuple[tuple[str, str, int, int], ...]:
    observed: list[tuple[str, str, int, int]] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        stat_result = path.stat()
        observed.append(
            (
                path.relative_to(root).as_posix(),
                "dir" if path.is_dir() else "file",
                stat_result.st_size,
                stat_result.st_mtime_ns,
            )
        )
    return tuple(observed)


def _replace_admission_path(admission, relative_path: str):
    changed = replace(
        admission.exports[0],
        source_registry_relative_path=relative_path,
    )
    return replace(admission, exports=(changed, *admission.exports[1:]))


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("unknown_policy", "E_LOADER_ADMISSION_POLICY_FORBIDDEN"),
        ("project_over_storage_and_path", "E_PROJECT_VERSION_FORBIDDEN"),
        ("storage_over_path", "E_STORAGE_VERSION_FORBIDDEN"),
        ("v01_path", "E_REGISTRY_PATH_VERSION_FORBIDDEN"),
    ],
)
def test_loader_admission_rejects_before_every_effect_with_zero_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_code: str,
) -> None:
    _, _, _, plan, admission, loader, _ = _fixture(tmp_path)
    v01_path = (
        f"runs/V01/{plan.exports[0].source_run_id}/artifact_registry.jsonl"
    )
    rejected = _replace_admission_path(admission, v01_path)
    if case == "unknown_policy":
        rejected = replace(rejected, policy_version="unknown_loader_policy_v999")
    elif case == "project_over_storage_and_path":
        rejected = replace(
            rejected, project_version="v0.1", storage_version="V01"
        )
    elif case == "storage_over_path":
        rejected = replace(rejected, storage_version="V01")

    forbidden_calls: list[str] = []

    def forbid(name: str):
        def fail(*args, **kwargs):
            forbidden_calls.append(name)
            raise AssertionError(f"admission rejection reached forbidden I/O: {name}")

        return fail

    monkeypatch.setattr(AppendOnlyRegistry, "read_entries", forbid("registry_read"))
    monkeypatch.setattr(loader_module, "read_export_manifest", forbid("manifest_read"))
    monkeypatch.setattr(loader_module, "read_export_package", forbid("rows_read"))
    monkeypatch.setattr(
        loader_module, "read_committed_receipt_payload", forbid("database_read")
    )
    monkeypatch.setattr(
        loader_module, "read_existing_warehouse_rows", forbid("database_rows_read")
    )
    monkeypatch.setattr(loader_module, "write_immutable_fact", forbid("fact_write"))
    monkeypatch.setattr(
        loader_module, "apply_receipt_transaction", forbid("database_write")
    )
    monkeypatch.setattr(
        WarehouseSingleWriterLock, "__enter__", forbid("lock_write")
    )
    monkeypatch.setattr(loader, "_append_fact_publications", forbid("registry_append"))

    before = _tree_metadata_snapshot(tmp_path)
    with pytest.raises(LoaderAdmissionRejected) as raised:
        loader.load(
            plan,
            admission=rejected,
            attempt_no=1,
            started_at=NOW,
            completed_at=DONE,
        )
    assert raised.value.error_code == expected_code
    assert raised.value.proof.payload_read_count == 0
    assert raised.value.proof.database_write_count == 0
    assert raised.value.proof.registry_append_count == 0
    assert forbidden_calls == []
    assert _tree_metadata_snapshot(tmp_path) == before


def test_v01_registry_record_disguised_by_outer_v02_identity_is_rejected_pre_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, _, plan, admission, loader, _ = _fixture(
        tmp_path,
        registry_project_version="v0.1",
        registry_storage_version="V01",
    )
    forged_record = replace(
        admission.exports[0].export_record,
        project_version="v0.2",
        storage_version="V02",
    )
    forged_export = replace(admission.exports[0], export_record=forged_record)
    forged = replace(admission, exports=(forged_export,))

    effects: list[str] = []

    def forbid(name: str):
        def fail(*args, **kwargs):
            effects.append(name)
            raise AssertionError(f"disguised V01 record reached {name}")

        return fail

    monkeypatch.setattr(loader_module, "read_export_manifest", forbid("manifest"))
    monkeypatch.setattr(loader_module, "read_export_package", forbid("rows"))
    monkeypatch.setattr(
        loader_module, "read_committed_receipt_payload", forbid("database_read")
    )
    monkeypatch.setattr(loader_module, "write_immutable_fact", forbid("fact_write"))
    monkeypatch.setattr(
        loader_module, "apply_receipt_transaction", forbid("database_write")
    )
    monkeypatch.setattr(loader, "_append_fact_publications", forbid("registry_append"))

    before = _tree_metadata_snapshot(tmp_path)
    with pytest.raises(LoaderAdmissionRejected) as raised:
        loader.load(
            plan,
            admission=forged,
            attempt_no=1,
            started_at=NOW,
            completed_at=DONE,
        )
    assert raised.value.error_code == "E_PROJECT_VERSION_FORBIDDEN"
    assert raised.value.proof == loader_module.LoaderAdmissionProof()
    assert effects == []
    assert _tree_metadata_snapshot(tmp_path) == before


def test_valid_admission_reaches_payload_spy_only_after_source_registry_control(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, _, plan, admission, loader, _ = _fixture(tmp_path)
    events: list[str] = []
    original_registry_read = AppendOnlyRegistry.read_entries

    def track_registry_read(registry):
        if registry.path.name == "artifact_registry.jsonl":
            events.append("source_registry_control")
        return original_registry_read(registry)

    def stop_at_manifest(*args, **kwargs):
        events.append("manifest_payload")
        raise RuntimeError("synthetic payload spy stop")

    monkeypatch.setattr(AppendOnlyRegistry, "read_entries", track_registry_read)
    monkeypatch.setattr(loader_module, "read_export_manifest", stop_at_manifest)
    with pytest.raises(RuntimeError, match="payload spy stop"):
        loader.load(
            plan,
            admission=admission,
            attempt_no=1,
            started_at=NOW,
            completed_at=DONE,
        )
    assert events.count("manifest_payload") == 1
    assert events.index("source_registry_control") < events.index("manifest_payload")


@pytest.mark.parametrize(
    ("admission", "expected_code"),
    [
        (
            WarehouseMigrationAdmission(
                policy_version="unknown_migration_policy_v999",
                project_version="v0.2",
                storage_version="V02",
                source_schema_version="truthfulness_db_v02.0.0",
                target_schema_version="truthfulness_db_v02.1.0",
            ),
            "E_WAREHOUSE_MIGRATION_POLICY_FORBIDDEN",
        ),
        (
            WarehouseMigrationAdmission(
                policy_version=WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1,
                project_version="v0.1",
                storage_version="V01",
                source_schema_version="truthfulness_db_v01.0.0",
                target_schema_version="truthfulness_db_v01.1.0",
            ),
            "E_PROJECT_VERSION_FORBIDDEN",
        ),
        (
            WarehouseMigrationAdmission(
                policy_version=WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1,
                project_version="v0.2",
                storage_version="V01",
                source_schema_version="truthfulness_db_v02.0.0",
                target_schema_version="truthfulness_db_v02.1.0",
            ),
            "E_STORAGE_VERSION_FORBIDDEN",
        ),
        *[
            (
                WarehouseMigrationAdmission(
                    policy_version=WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1,
                    project_version="v0.2",
                    storage_version="V02",
                    source_schema_version=source,
                    target_schema_version=target,
                ),
                "E_STORAGE_VERSION_FORBIDDEN",
            )
            for source, target in (
                ("truthfulness_db_v01.0.0", "truthfulness_db_v02.1.0"),
                ("truthfulness_db_v03.0.0", "truthfulness_db_v02.1.0"),
                ("unknown", "truthfulness_db_v02.1.0"),
                ("truthfulness_db_v02.0.0", "truthfulness_db_v01.1.0"),
            )
        ],
    ],
)
def test_migration_admission_rejects_before_old_database_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    admission: WarehouseMigrationAdmission,
    expected_code: str,
) -> None:
    calls: list[str] = []

    def forbid_detect(path: Path) -> str:
        calls.append(str(path))
        raise AssertionError("invalid migration admission opened the old database")

    monkeypatch.setattr(projection_module, "detect_warehouse_schema_version", forbid_detect)
    with pytest.raises(WarehouseMigrationAdmissionRejected) as raised:
        migrate_legacy_projection_side_by_side(
            tmp_path / "must-not-open.duckdb",
            tmp_path / "must-not-create.duckdb",
            (),
            admission=admission,
            storage_root_ref=ROOT_REF,
            storage_roots={ROOT_REF: tmp_path},
        )
    assert raised.value.error_code == expected_code
    assert calls == []
    assert not (tmp_path / "must-not-create.duckdb").exists()


def test_migration_actual_schema_must_equal_declared_before_rebuild(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    legacy = tmp_path / "synthetic-control-only.duckdb"
    legacy.write_bytes(b"synthetic control marker only")
    successor = tmp_path / "successor.duckdb"
    admission = WarehouseMigrationAdmission(
        policy_version=WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1,
        project_version="v0.2",
        storage_version="V02",
        source_schema_version="truthfulness_db_v02.0.0",
        target_schema_version="truthfulness_db_v02.1.0",
    )
    monkeypatch.setattr(
        projection_module,
        "detect_warehouse_schema_version",
        lambda path: "truthfulness_db_v02.0.1",
    )

    def forbid_rebuild(*args, **kwargs):
        raise AssertionError("schema mismatch reached successor rebuild")

    monkeypatch.setattr(projection_module, "rebuild_duckdb_projection", forbid_rebuild)
    with pytest.raises(WarehouseConflictError, match="declared migration source"):
        migrate_legacy_projection_side_by_side(
            legacy,
            successor,
            [object()],  # type: ignore[list-item] - rejected before receipt use
            admission=admission,
            storage_root_ref=ROOT_REF,
            storage_roots={ROOT_REF: tmp_path},
        )
    assert not successor.exists()


def test_fault_before_duckdb_writes_immutable_failed_attempt(tmp_path: Path) -> None:
    root, _, _, plan, admission, loader, registry = _fixture(tmp_path)

    def fail(stage: str) -> None:
        if stage == "parquet_validate":
            raise RuntimeError("synthetic injected failure")

    with pytest.raises(RuntimeError, match="synthetic injected failure"):
        loader.load(
            plan,
            admission=admission,
            attempt_no=1,
            started_at=NOW,
            completed_at=DONE,
            fault_hook=fail,
        )
    attempts = list((root / "receipts").glob("attempt_*.json"))
    assert len(attempts) == 1
    attempt = WarehouseProjectionAttemptV1.model_validate_json(attempts[0].read_bytes())
    assert attempt.status == "failed"
    assert attempt.last_completed_stage == "parquet_validate"
    assert attempt.error_code == "WAREHOUSE_LOAD_FAILED"
    assert [item.artifact_type for item in registry.read_records()] == [
        "warehouse.load_plan",
        "warehouse.projection_attempt",
    ]
    assert not (root / "locks/warehouse-loader.lock").exists()


def test_missing_duckdb_is_explicit_and_keeps_projection_pending(tmp_path: Path) -> None:
    if dependency_status()["duckdb"]["available"]:
        pytest.skip("host has DuckDB; full transaction tests cover this branch")
    root, _, _, plan, admission, loader, _ = _fixture(tmp_path)
    with pytest.raises(WarehouseDependencyUnavailable, match="DuckDB 1.5.1"):
        loader.load(
            plan,
            admission=admission,
            attempt_no=1,
            started_at=NOW,
            completed_at=DONE,
        )
    attempt_path = next((root / "receipts").glob("attempt_*.json"))
    attempt = WarehouseProjectionAttemptV1.model_validate_json(attempt_path.read_bytes())
    assert attempt.status == "failed"
    assert attempt.error_code == "WAREHOUSE_DEPENDENCY_UNAVAILABLE"
    assert list((root / "parquet").rglob("*.parquet"))
    assert not list((root / "receipts").glob("load_receipt_*.json"))


@pytest.mark.skipif(
    not dependency_status()["duckdb"]["exact_match"],
    reason="DuckDB 1.5.1 is not installed in this environment",
)
def test_full_load_is_idempotent_and_queryable(tmp_path: Path) -> None:
    root, _, _, plan, admission, loader, registry = _fixture(tmp_path)
    first = loader.load(
        plan,
        admission=admission,
        attempt_no=1,
        started_at=NOW,
        completed_at=DONE,
    )
    second = loader.load(
        plan,
        admission=admission,
        attempt_no=2,
        started_at="2026-07-20T00:02:00Z",
        completed_at="2026-07-20T00:03:00Z",
    )
    assert first.receipt.canonical_bytes() == second.receipt.canonical_bytes()
    assert first.receipt.receipt_hash == second.receipt.receipt_hash
    import duckdb

    connection = duckdb.connect(str(root / "duckdb/truthfulness_v02.duckdb"), read_only=True)
    try:
        assert connection.execute("SELECT count(*) FROM warehouse_rows").fetchone()[0] == 2
        assert connection.execute("SELECT count(*) FROM v_warehouse_projection_lag").fetchone()[0] == 1
    finally:
        connection.close()
    assert [item.artifact_type for item in registry.read_records()] == [
        "warehouse.load_plan",
        "warehouse.load_receipt",
        "warehouse.projection_attempt",
        "warehouse.projection_attempt",
    ]


@pytest.mark.skipif(
    not dependency_status()["duckdb"]["exact_match"],
    reason="DuckDB 1.5.1 is not installed in this environment",
)
def test_crash_after_duckdb_commit_recovers_exact_receipt(tmp_path: Path) -> None:
    root, _, _, plan, admission, loader, registry = _fixture(tmp_path)

    def fail(stage: str) -> None:
        if stage == "duckdb_transaction":
            raise RuntimeError("crash after committed transaction")

    with pytest.raises(RuntimeError, match="after committed"):
        loader.load(
            plan,
            admission=admission,
            attempt_no=1,
            started_at=NOW,
            completed_at=DONE,
            fault_hook=fail,
        )
    assert not list((root / "receipts").glob("load_receipt_*.json"))
    recovered = loader.load(
        plan,
        admission=admission,
        attempt_no=2,
        started_at="2026-07-20T00:02:00Z",
        completed_at="2026-07-20T00:03:00Z",
    )
    receipt_path = root / recovered.receipt.receipt_relative_path
    assert receipt_path.read_bytes() == recovered.receipt.canonical_bytes()
    assert WarehouseLoadReceiptV1.model_validate_json(receipt_path.read_bytes()) == recovered.receipt
    assert [item.artifact_type for item in registry.read_records()] == [
        "warehouse.load_plan",
        "warehouse.projection_attempt",
        "warehouse.load_receipt",
        "warehouse.projection_attempt",
    ]


@pytest.mark.skipif(
    not dependency_status()["duckdb"]["exact_match"],
    reason="DuckDB 1.5.1 is not installed in this environment",
)
def test_success_attempt_file_precedes_registry_and_exact_replay_repairs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, _, _, plan, admission, loader, registry = _fixture(tmp_path)
    original_append = loader._append_fact_publications
    calls = 0

    def fail_after_attempt_file(publications, *, recorded_at):
        nonlocal calls
        calls += 1
        if calls == 2:
            attempt_publication = next(
                item
                for item in publications
                if item.relative_path.startswith("receipts/attempt_")
            )
            attempt_path = root / attempt_publication.relative_path
            assert attempt_path.is_file()
            parsed = WarehouseProjectionAttemptV1.model_validate_json(
                attempt_path.read_bytes()
            )
            assert parsed.status == "succeeded"
            assert sha256_bytes(attempt_path.read_bytes()) == attempt_publication.content_hash
            raise RuntimeError("synthetic Registry outage after immutable attempt")
        return original_append(publications, recorded_at=recorded_at)

    monkeypatch.setattr(loader, "_append_fact_publications", fail_after_attempt_file)
    with pytest.raises(WarehouseConflictError, match="replay the same attempt bytes"):
        loader.load(
            plan,
            admission=admission,
            attempt_no=1,
            started_at=NOW,
            completed_at=DONE,
        )
    assert [item.artifact_type for item in registry.read_records()] == [
        "warehouse.load_plan"
    ]

    monkeypatch.setattr(loader, "_append_fact_publications", original_append)
    recovered = loader.load(
        plan,
        admission=admission,
        attempt_no=1,
        started_at=NOW,
        completed_at=DONE,
    )
    assert recovered.attempt.status == "succeeded"
    assert [item.artifact_type for item in registry.read_records()] == [
        "warehouse.load_plan",
        "warehouse.load_receipt",
        "warehouse.projection_attempt",
    ]
