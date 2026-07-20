"""Deterministic Parquet files and a rebuildable DuckDB analytical projection."""

from __future__ import annotations

import importlib.util
import json
import os
import re
import uuid
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .warehouse_export import (
    WarehouseExportResult,
    resolve_external_storage_ref,
)
from .warehouse_models import (
    DATABASE_SCHEMA_VERSION,
    WAREHOUSE_PROJECTION_VERSION,
    ExternalStorageRef,
    ParquetFileDescriptor,
    WarehouseConflictError,
    WarehouseContractError,
    WarehouseDependencyUnavailable,
    WarehouseLoadReceiptV1,
    WarehouseRow,
    canonical_json_bytes,
    sha256_bytes,
)


PYARROW_REQUIRED_VERSION = "21.0.0"
DUCKDB_REQUIRED_VERSION = "1.5.1"
PARQUET_ROW_GROUP_SIZE = 8_192
PARQUET_COMPRESSION = "zstd"
PARQUET_COMPRESSION_LEVEL = 3
WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1 = (
    "claim_warehouse_migration_admission_v1.0.0"
)


@dataclass(frozen=True, slots=True)
class ParquetBuildArtifact:
    descriptor: ParquetFileDescriptor
    data: bytes


@dataclass(frozen=True, slots=True)
class StagedParquetArtifact:
    artifact: ParquetBuildArtifact
    staged_path: Path


@dataclass(frozen=True, slots=True)
class WarehouseMigrationAdmission:
    """Payload-free declaration required before an old DuckDB file is opened."""

    policy_version: str
    project_version: str
    storage_version: str
    source_schema_version: str
    target_schema_version: str


class WarehouseMigrationAdmissionRejected(WarehouseContractError):
    """Stable fail-closed migration admission error."""

    def __init__(self, error_code: str, message: str) -> None:
        self.error_code = error_code
        super().__init__(f"{error_code}: {message}")


@dataclass(frozen=True, slots=True)
class _WarehouseMigrationAdmissionPolicy:
    policy_version: str
    project_version: str
    storage_version: str
    source_schema_pattern: str
    target_schema_version: str


_WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1 = _WarehouseMigrationAdmissionPolicy(
    policy_version=WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1,
    project_version="v0.2",
    storage_version="V02",
    source_schema_pattern=r"truthfulness_db_v02\.0\.[0-9]+",
    target_schema_version=DATABASE_SCHEMA_VERSION,
)


def _migration_admission_policy(
    policy_version: str,
) -> _WarehouseMigrationAdmissionPolicy:
    """Single fail-closed dispatch point for migration admission generations."""

    if policy_version == WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1:
        return _WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1
    raise WarehouseMigrationAdmissionRejected(
        "E_WAREHOUSE_MIGRATION_POLICY_FORBIDDEN",
        f"unsupported warehouse migration admission policy: {policy_version}",
    )


def dependency_status() -> dict[str, dict[str, str | bool | None]]:
    """Report optional analytical dependencies without importing them eagerly."""

    status: dict[str, dict[str, str | bool | None]] = {}
    for name, required in (
        ("pyarrow", PYARROW_REQUIRED_VERSION),
        ("duckdb", DUCKDB_REQUIRED_VERSION),
    ):
        spec = importlib.util.find_spec(name)
        observed: str | None = None
        if spec is not None:
            module = __import__(name)
            observed = str(getattr(module, "__version__", "unknown"))
        status[name] = {
            "available": spec is not None,
            "required_version": required,
            "observed_version": observed,
            "exact_match": observed == required,
        }
    return status


def require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise WarehouseDependencyUnavailable(
            f"PyArrow {PYARROW_REQUIRED_VERSION} is required for Parquet projection"
        ) from exc
    if pa.__version__ != PYARROW_REQUIRED_VERSION:
        raise WarehouseDependencyUnavailable(
            f"PyArrow exact version mismatch: required {PYARROW_REQUIRED_VERSION}, "
            f"observed {pa.__version__}"
        )
    return pa, pq


def require_duckdb() -> Any:
    try:
        import duckdb
    except ImportError as exc:
        raise WarehouseDependencyUnavailable(
            f"DuckDB {DUCKDB_REQUIRED_VERSION} is required for catalog projection"
        ) from exc
    if duckdb.__version__ != DUCKDB_REQUIRED_VERSION:
        raise WarehouseDependencyUnavailable(
            f"DuckDB exact version mismatch: required {DUCKDB_REQUIRED_VERSION}, "
            f"observed {duckdb.__version__}"
        )
    return duckdb


def build_parquet_artifacts(
    exports: Sequence[WarehouseExportResult],
) -> tuple[ParquetBuildArtifact, ...]:
    """Build deterministic, fixed-schema Parquet bytes grouped by export/table."""

    pa, pq = require_pyarrow()
    schema = _arrow_schema(pa)
    artifacts: list[ParquetBuildArtifact] = []
    seen_exports: dict[str, str] = {}
    for export in sorted(exports, key=lambda item: item.manifest.export_id):
        previous = seen_exports.setdefault(export.manifest.export_id, export.logical_hash)
        if previous != export.logical_hash:
            raise WarehouseConflictError("same export ID has different logical hash")
        grouped: dict[tuple[str, str], list[WarehouseRow]] = defaultdict(list)
        for row in export.rows:
            grouped[(row.logical_layer, row.table_code)].append(row)
        run_date = export.manifest.run_created_at[:10]
        for (logical_layer, table_code), rows in sorted(grouped.items()):
            rows.sort(key=lambda item: item.canonical_primary_key)
            columns = _arrow_columns(export.manifest.export_id, rows)
            table = pa.Table.from_pydict(columns, schema=schema)
            sink = pa.BufferOutputStream()
            pq.write_table(
                table,
                sink,
                version="2.6",
                data_page_version="1.0",
                compression=PARQUET_COMPRESSION,
                compression_level=PARQUET_COMPRESSION_LEVEL,
                use_dictionary=False,
                write_statistics=False,
                row_group_size=PARQUET_ROW_GROUP_SIZE,
                use_byte_stream_split=False,
                coerce_timestamps="us",
                allow_truncated_timestamps=False,
                store_schema=True,
                write_page_index=False,
                write_page_checksum=False,
            )
            data = sink.getvalue().to_pybytes()
            relative_path = (
                f"parquet/logical_layer={logical_layer}/table_code={table_code}/"
                f"schema_version={DATABASE_SCHEMA_VERSION}/run_date={run_date}/"
                f"export_id={export.manifest.export_id}/part-00000.parquet"
            )
            row_logical_hash = sha256_bytes(
                b"".join(row.canonical_bytes() + b"\n" for row in rows)
            )
            descriptor = ParquetFileDescriptor(
                logical_layer=logical_layer,
                table_code=table_code,
                export_id=export.manifest.export_id,
                relative_path=relative_path,
                size_bytes=len(data),
                file_hash=sha256_bytes(data),
                row_count=len(rows),
                row_logical_hash=row_logical_hash,
            )
            artifacts.append(ParquetBuildArtifact(descriptor=descriptor, data=data))
    return tuple(
        sorted(
            artifacts,
            key=lambda item: (
                item.descriptor.logical_layer,
                item.descriptor.table_code,
                item.descriptor.export_id,
            ),
        )
    )


def publish_parquet_artifacts(
    artifacts: Iterable[ParquetBuildArtifact],
    *,
    storage_root_ref: str,
    storage_roots: Mapping[str, Path | str],
) -> tuple[Path, ...]:
    """No-clobber publish; byte-identical partial replay is accepted."""

    published: list[Path] = []
    for artifact in artifacts:
        ref = ExternalStorageRef(
            storage_root_ref=storage_root_ref,
            relative_path=artifact.descriptor.relative_path,
        )
        path = resolve_external_storage_ref(storage_roots, ref)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.is_symlink() or sha256_bytes(path.read_bytes()) != artifact.descriptor.file_hash:
                raise WarehouseConflictError(
                    f"immutable Parquet conflict: {artifact.descriptor.relative_path}"
                )
        else:
            with path.open("xb") as handle:
                handle.write(artifact.data)
                handle.flush()
                os.fsync(handle.fileno())
        verify_parquet_file(path, artifact.descriptor)
        published.append(path)
    return tuple(published)


def stage_parquet_artifacts(
    artifacts: Iterable[ParquetBuildArtifact],
    *,
    load_plan_id: str,
    storage_root_ref: str,
    storage_roots: Mapping[str, Path | str],
) -> tuple[StagedParquetArtifact, ...]:
    """Write create-new Parquet bytes below the plan-owned staging directory."""

    staged: list[StagedParquetArtifact] = []
    for artifact in artifacts:
        relative_path = (
            f"staging/{load_plan_id}/{artifact.descriptor.relative_path}"
        )
        path = resolve_external_storage_ref(
            storage_roots,
            ExternalStorageRef(
                storage_root_ref=storage_root_ref,
                relative_path=relative_path,
            ),
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            if path.is_symlink() or sha256_bytes(path.read_bytes()) != artifact.descriptor.file_hash:
                raise WarehouseConflictError(f"Parquet staging conflict: {relative_path}")
        else:
            with path.open("xb") as handle:
                handle.write(artifact.data)
                handle.flush()
                os.fsync(handle.fileno())
        staged.append(StagedParquetArtifact(artifact=artifact, staged_path=path))
    return tuple(staged)


def validate_staged_parquet_artifacts(
    staged: Iterable[StagedParquetArtifact],
) -> None:
    for item in staged:
        verify_parquet_file(item.staged_path, item.artifact.descriptor)


def publish_staged_parquet_artifacts(
    staged: Iterable[StagedParquetArtifact],
    *,
    load_plan_id: str,
    storage_root_ref: str,
    storage_roots: Mapping[str, Path | str],
) -> tuple[Path, ...]:
    """Atomically publish validated staging files without clobbering final bytes."""

    staged_items = tuple(staged)
    validate_staged_parquet_artifacts(staged_items)
    published: list[Path] = []
    for item in staged_items:
        descriptor = item.artifact.descriptor
        final_path = resolve_external_storage_ref(
            storage_roots,
            ExternalStorageRef(
                storage_root_ref=storage_root_ref,
                relative_path=descriptor.relative_path,
            ),
        )
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            if final_path.is_symlink() or sha256_bytes(final_path.read_bytes()) != descriptor.file_hash:
                raise WarehouseConflictError(
                    f"immutable Parquet conflict: {descriptor.relative_path}"
                )
            item.staged_path.unlink()
        else:
            item.staged_path.rename(final_path)
        verify_parquet_file(final_path, descriptor)
        published.append(final_path)

    staging_root = resolve_external_storage_ref(
        storage_roots,
        ExternalStorageRef(
            storage_root_ref=storage_root_ref,
            relative_path=f"staging/{load_plan_id}",
        ),
    )
    for path in sorted(
        (item for item in staging_root.rglob("*") if item.is_dir()),
        key=lambda item: len(item.parts),
        reverse=True,
    ):
        try:
            path.rmdir()
        except OSError:
            pass
    try:
        staging_root.rmdir()
    except OSError:
        pass
    return tuple(published)


def verify_parquet_file(path: Path, descriptor: ParquetFileDescriptor) -> None:
    _, pq = require_pyarrow()
    if path.is_symlink() or not path.is_file():
        raise WarehouseContractError("Parquet projection must be a regular file")
    data = path.read_bytes()
    if len(data) != descriptor.size_bytes or sha256_bytes(data) != descriptor.file_hash:
        raise WarehouseConflictError("Parquet size/hash mismatch")
    metadata = pq.read_metadata(path)
    if metadata.num_rows != descriptor.row_count:
        raise WarehouseContractError("Parquet row count mismatch")


def verify_receipt_parquet(
    receipt: WarehouseLoadReceiptV1,
    *,
    storage_root_ref: str,
    storage_roots: Mapping[str, Path | str],
) -> tuple[Path, ...]:
    paths: list[Path] = []
    for descriptor in receipt.parquet_manifest:
        path = resolve_external_storage_ref(
            storage_roots,
            ExternalStorageRef(
                storage_root_ref=storage_root_ref,
                relative_path=descriptor.relative_path,
            ),
        )
        verify_parquet_file(path, descriptor)
        paths.append(path)
    return tuple(paths)


def apply_receipt_transaction(
    database_path: Path,
    receipt: WarehouseLoadReceiptV1,
    *,
    parquet_paths: Sequence[Path],
    schema_sql_path: Path | None = None,
) -> None:
    """Commit loaded exports, rows, watermark and canonical receipt atomically."""

    duckdb = require_duckdb()
    if len(parquet_paths) != len(receipt.parquet_manifest):
        raise WarehouseContractError("Parquet path list differs from receipt manifest")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    sql_path = schema_sql_path or _default_schema_sql_path()
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(sql_path.read_text(encoding="utf-8"))
        existing_payload = _read_receipt_payload(connection, receipt.load_batch.load_batch_id)
        if existing_payload is not None:
            if existing_payload != receipt.canonical_bytes():
                raise WarehouseConflictError(
                    "load batch ID already committed with different receipt payload"
                )
            return
        connection.execute("BEGIN TRANSACTION")
        try:
            for binding in receipt.exports:
                existing = connection.execute(
                    "SELECT logical_hash, export_idempotency_key "
                    "FROM warehouse_loaded_export WHERE export_id = ?",
                    [binding.export_id],
                ).fetchone()
                if existing is not None and (
                    existing[0] != binding.logical_hash
                    or existing[1] != binding.export_idempotency_key
                ):
                    raise WarehouseConflictError(
                        "WAREHOUSE_EXPORT_CONFLICT: export ID provenance differs"
                    )
                idempotent = connection.execute(
                    """
                    SELECT export_id, logical_hash, manifest_hash, rows_hash
                    FROM warehouse_loaded_export
                    WHERE export_idempotency_key = ?
                    """,
                    [binding.export_idempotency_key],
                ).fetchone()
                if idempotent is not None and idempotent != (
                    binding.export_id,
                    binding.logical_hash,
                    binding.manifest_hash,
                    binding.rows_hash,
                ):
                    raise WarehouseConflictError(
                        "WAREHOUSE_EXPORT_CONFLICT: idempotency key provenance differs"
                    )
            connection.execute(
                """
                INSERT INTO warehouse_load_plan
                (load_plan_id, plan_hash, export_count, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (load_plan_id) DO NOTHING
                """,
                [
                    receipt.load_batch.load_plan_id,
                    receipt.load_batch.load_plan_hash,
                    receipt.load_batch.export_count,
                    receipt.load_batch.started_at,
                ],
            )
            connection.execute(
                """
                INSERT INTO warehouse_load_batch
                (load_batch_id, load_plan_id, batch_hash, status, started_at,
                 completed_at, export_count, row_count, logical_hash, receipt_payload)
                VALUES (?, ?, ?, 'succeeded', ?, ?, ?, ?, ?, ?)
                """,
                [
                    receipt.load_batch.load_batch_id,
                    receipt.load_batch.load_plan_id,
                    receipt.load_batch.batch_hash,
                    receipt.load_batch.started_at,
                    receipt.load_batch.completed_at,
                    receipt.load_batch.export_count,
                    receipt.load_batch.row_count,
                    receipt.load_batch.logical_hash,
                    receipt.canonical_bytes().decode("utf-8"),
                ],
            )
            for binding in receipt.exports:
                connection.execute(
                    """
                    INSERT INTO warehouse_loaded_export
                    (export_id, export_idempotency_key, logical_hash, manifest_hash,
                     rows_hash, load_batch_id, loaded_at, row_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (export_id) DO NOTHING
                    """,
                    [
                        binding.export_id,
                        binding.export_idempotency_key,
                        binding.logical_hash,
                        binding.manifest_hash,
                        binding.rows_hash,
                        receipt.load_batch.load_batch_id,
                        receipt.load_batch.completed_at,
                        binding.row_count,
                    ],
                )
            for descriptor, parquet_path in zip(
                receipt.parquet_manifest, parquet_paths, strict=True
            ):
                connection.execute(
                    """
                    INSERT INTO warehouse_parquet_file
                    (relative_path, export_id, logical_layer, table_code, file_hash,
                     size_bytes, row_count, row_logical_hash, load_batch_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (relative_path) DO NOTHING
                    """,
                    [
                        descriptor.relative_path,
                        descriptor.export_id,
                        descriptor.logical_layer,
                        descriptor.table_code,
                        descriptor.file_hash,
                        descriptor.size_bytes,
                        descriptor.row_count,
                        descriptor.row_logical_hash,
                        receipt.load_batch.load_batch_id,
                    ],
                )
                connection.execute(
                    """
                    INSERT INTO warehouse_rows
                    SELECT export_id, row_schema_version, logical_layer, table_code,
                           canonical_primary_key, revision_no, is_active, effective_at,
                           run_id, artifact_id, artifact_record_id,
                           artifact_content_hash, created_at, writer_role,
                           schema_versions_json, taxonomy_versions_json, data_json,
                           row_hash
                    FROM read_parquet(?)
                    ON CONFLICT (export_id, logical_layer, table_code,
                                 canonical_primary_key, revision_no) DO NOTHING
                    """,
                    [str(parquet_path)],
                )
            for layer, export_id in sorted(receipt.watermark.items()):
                connection.execute(
                    """
                    INSERT INTO warehouse_watermark
                    (logical_layer, latest_export_id, load_batch_id, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (logical_layer) DO UPDATE SET
                        latest_export_id = excluded.latest_export_id,
                        load_batch_id = excluded.load_batch_id,
                        updated_at = excluded.updated_at
                    """,
                    [
                        layer,
                        export_id,
                        receipt.load_batch.load_batch_id,
                        receipt.load_batch.completed_at,
                    ],
                )
            connection.execute(
                """
                INSERT INTO warehouse_load_receipt
                (receipt_id, receipt_hash, load_batch_id, receipt_relative_path,
                 committed_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    receipt.receipt_id,
                    receipt.receipt_hash,
                    receipt.load_batch.load_batch_id,
                    receipt.receipt_relative_path,
                    receipt.load_batch.completed_at,
                ],
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.close()


def read_committed_receipt_payload(
    database_path: Path, load_batch_id: str, *, schema_sql_path: Path | None = None
) -> bytes | None:
    """Recover the exact receipt bytes persisted inside the DuckDB transaction."""

    if not database_path.exists():
        return None
    duckdb = require_duckdb()
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        row = connection.execute(
            "SELECT receipt_payload FROM warehouse_load_batch WHERE load_batch_id = ?",
            [load_batch_id],
        ).fetchone()
        return None if row is None else str(row[0]).encode("utf-8")
    finally:
        connection.close()


def read_existing_warehouse_rows(database_path: Path) -> tuple[WarehouseRow, ...]:
    """Read committed business rows for cross-batch PK/FK/revision validation."""

    if not database_path.exists():
        return ()
    duckdb = require_duckdb()
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        tables = {
            str(item[0])
            for item in connection.execute("SHOW TABLES").fetchall()
        }
        if "warehouse_rows" not in tables:
            return ()
        records = connection.execute(
            """
            SELECT row_schema_version, logical_layer, table_code,
                   canonical_primary_key, revision_no, is_active, effective_at,
                   run_id, artifact_id, artifact_record_id, artifact_content_hash,
                   created_at, writer_role, schema_versions_json,
                   taxonomy_versions_json, data_json, row_hash
            FROM warehouse_rows
            ORDER BY logical_layer, table_code, canonical_primary_key, revision_no
            """
        ).fetchall()
        rows: list[WarehouseRow] = []
        for record in records:
            rows.append(
                WarehouseRow.model_validate(
                    {
                        "row_schema_version": record[0],
                        "logical_layer": record[1],
                        "table_code": record[2],
                        "canonical_primary_key": record[3],
                        "revision_no": record[4],
                        "is_active": record[5],
                        "effective_at": record[6],
                        "run_id": record[7],
                        "artifact_id": record[8],
                        "artifact_record_id": record[9],
                        "artifact_content_hash": record[10],
                        "created_at": record[11],
                        "writer_role": record[12],
                        "schema_versions": json.loads(str(record[13])),
                        "taxonomy_versions": json.loads(str(record[14])),
                        "data": json.loads(str(record[15])),
                        "row_hash": record[16],
                    }
                )
            )
        return tuple(rows)
    finally:
        connection.close()


def rebuild_duckdb_projection(
    target_path: Path,
    receipts: Sequence[WarehouseLoadReceiptV1],
    *,
    storage_root_ref: str,
    storage_roots: Mapping[str, Path | str],
    replace: bool = False,
) -> Path:
    """Create a new catalog from immutable receipts/Parquet, then atomically publish."""

    require_duckdb()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists() and not replace:
        raise WarehouseConflictError("DuckDB rebuild target already exists")
    staging = target_path.parent / f".{target_path.name}.{uuid.uuid4().hex}.staging"
    try:
        for receipt in receipts:
            paths = verify_receipt_parquet(
                receipt,
                storage_root_ref=storage_root_ref,
                storage_roots=storage_roots,
            )
            apply_receipt_transaction(staging, receipt, parquet_paths=paths)
        if target_path.exists():
            os.replace(staging, target_path)
        else:
            staging.rename(target_path)
    finally:
        if staging.exists():
            staging.unlink()
    return target_path


def detect_warehouse_schema_version(database_path: Path) -> str:
    """Read the catalog version without mutating a legacy or successor file."""

    if database_path.is_symlink() or not database_path.is_file():
        raise WarehouseContractError("warehouse database must be a regular file")
    duckdb = require_duckdb()
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        row = connection.execute(
            """
            SELECT metadata_value FROM warehouse_metadata
            WHERE metadata_key = 'database_schema_version'
            """
        ).fetchone()
    finally:
        connection.close()
    if row is None or not str(row[0]).strip():
        raise WarehouseContractError("warehouse database has no schema-version marker")
    return str(row[0])


def migrate_legacy_projection_side_by_side(
    legacy_path: Path,
    successor_path: Path,
    receipts: Sequence[WarehouseLoadReceiptV1],
    *,
    admission: WarehouseMigrationAdmission,
    storage_root_ref: str,
    storage_roots: Mapping[str, Path | str],
) -> Path:
    """Rebuild a successor beside an untouched old catalog from authoritative facts."""

    _validate_migration_admission(admission)
    if not receipts:
        raise WarehouseContractError("migration requires authoritative load receipts")
    if legacy_path.resolve(strict=True) == successor_path.resolve(strict=False):
        raise WarehouseContractError("legacy and successor database paths must differ")
    if successor_path.exists() or successor_path.is_symlink():
        raise WarehouseConflictError("successor database already exists")
    legacy_version = detect_warehouse_schema_version(legacy_path)
    if legacy_version != admission.source_schema_version:
        raise WarehouseConflictError(
            "declared migration source schema differs from warehouse metadata"
        )
    rebuilt = rebuild_duckdb_projection(
        successor_path,
        receipts,
        storage_root_ref=storage_root_ref,
        storage_roots=storage_roots,
    )
    duckdb = require_duckdb()
    connection = duckdb.connect(str(rebuilt))
    try:
        connection.execute(
            "INSERT INTO warehouse_metadata VALUES (?, ?) ON CONFLICT DO NOTHING",
            ["migration_source_schema_version", legacy_version],
        )
        connection.execute(
            "INSERT INTO warehouse_metadata VALUES (?, ?) ON CONFLICT DO NOTHING",
            ["migration_source_file_hash", sha256_bytes(legacy_path.read_bytes())],
        )
    finally:
        connection.close()
    if detect_warehouse_schema_version(legacy_path) != legacy_version:
        raise WarehouseConflictError("legacy database changed during successor build")
    return rebuilt


def rollback_successor_projection(
    legacy_path: Path,
    successor_path: Path,
    *,
    admission: WarehouseMigrationAdmission,
    archived_successor_path: Path,
) -> Path:
    """Rollback by archiving only the successor and returning the untouched legacy DB."""

    _validate_migration_admission(admission)
    legacy_resolved = legacy_path.resolve(strict=True)
    if legacy_path.is_symlink() or not legacy_path.is_file():
        raise WarehouseContractError("legacy database must be a regular file")
    if successor_path.is_symlink() or not successor_path.is_file():
        raise WarehouseContractError("successor database must be a regular file")
    if archived_successor_path.exists() or archived_successor_path.is_symlink():
        raise WarehouseConflictError("rollback archive target already exists")
    if successor_path.resolve(strict=True) == legacy_resolved:
        raise WarehouseContractError("successor and legacy database paths must differ")
    if detect_warehouse_schema_version(legacy_path) != admission.source_schema_version:
        raise WarehouseConflictError(
            "rollback legacy schema differs from migration admission"
        )
    if detect_warehouse_schema_version(successor_path) != admission.target_schema_version:
        raise WarehouseConflictError(
            "rollback successor schema differs from migration admission"
        )
    archived_successor_path.parent.mkdir(parents=True, exist_ok=True)
    successor_path.rename(archived_successor_path)
    return legacy_resolved


def _validate_migration_admission(admission: WarehouseMigrationAdmission) -> None:
    policy = _migration_admission_policy(admission.policy_version)
    if admission.project_version != policy.project_version:
        raise WarehouseMigrationAdmissionRejected(
            "E_PROJECT_VERSION_FORBIDDEN",
            "migration project_version must be exactly v0.2",
        )
    if admission.storage_version != policy.storage_version:
        raise WarehouseMigrationAdmissionRejected(
            "E_STORAGE_VERSION_FORBIDDEN",
            "migration storage_version must be exactly V02",
        )
    if (
        re.fullmatch(policy.source_schema_pattern, admission.source_schema_version)
        is None
        or admission.target_schema_version != policy.target_schema_version
    ):
        raise WarehouseMigrationAdmissionRejected(
            "E_STORAGE_VERSION_FORBIDDEN",
            "only an audited truthfulness_db_v02.0.x to current V02 migration is allowed",
        )


def _arrow_schema(pa: Any) -> Any:
    metadata = {
        b"database_schema_version": DATABASE_SCHEMA_VERSION.encode("ascii"),
        b"warehouse_projection_version": WAREHOUSE_PROJECTION_VERSION.encode("ascii"),
        b"writer_contract": b"claim_warehouse_parquet_v1",
    }
    return pa.schema(
        [
            pa.field("export_id", pa.string(), nullable=False),
            pa.field("row_schema_version", pa.string(), nullable=False),
            pa.field("logical_layer", pa.string(), nullable=False),
            pa.field("table_code", pa.string(), nullable=False),
            pa.field("canonical_primary_key", pa.string(), nullable=False),
            pa.field("revision_no", pa.int64(), nullable=False),
            pa.field("is_active", pa.bool_(), nullable=False),
            pa.field("effective_at", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("artifact_id", pa.string(), nullable=False),
            pa.field("artifact_record_id", pa.string(), nullable=False),
            pa.field("artifact_content_hash", pa.string(), nullable=False),
            pa.field("created_at", pa.string(), nullable=False),
            pa.field("writer_role", pa.string(), nullable=False),
            pa.field("schema_versions_json", pa.string(), nullable=False),
            pa.field("taxonomy_versions_json", pa.string(), nullable=False),
            pa.field("data_json", pa.string(), nullable=False),
            pa.field("row_hash", pa.string(), nullable=False),
        ],
        metadata=metadata,
    )


def _arrow_columns(export_id: str, rows: Sequence[WarehouseRow]) -> dict[str, list[Any]]:
    return {
        "export_id": [export_id for _ in rows],
        "row_schema_version": [row.row_schema_version for row in rows],
        "logical_layer": [row.logical_layer for row in rows],
        "table_code": [row.table_code for row in rows],
        "canonical_primary_key": [row.canonical_primary_key for row in rows],
        "revision_no": [row.revision_no for row in rows],
        "is_active": [row.is_active for row in rows],
        "effective_at": [row.effective_at for row in rows],
        "run_id": [row.run_id for row in rows],
        "artifact_id": [row.artifact_id for row in rows],
        "artifact_record_id": [row.artifact_record_id for row in rows],
        "artifact_content_hash": [row.artifact_content_hash for row in rows],
        "created_at": [row.created_at for row in rows],
        "writer_role": [row.writer_role for row in rows],
        "schema_versions_json": [
            canonical_json_bytes(row.schema_versions).decode("utf-8") for row in rows
        ],
        "taxonomy_versions_json": [
            canonical_json_bytes(row.taxonomy_versions).decode("utf-8") for row in rows
        ],
        "data_json": [canonical_json_bytes(row.data).decode("utf-8") for row in rows],
        "row_hash": [row.row_hash for row in rows],
    }


def _default_schema_sql_path() -> Path:
    return Path(__file__).resolve().parents[4] / "schemas" / "warehouse" / "claim_warehouse_v1.sql"


def _read_receipt_payload(connection: Any, load_batch_id: str) -> bytes | None:
    row = connection.execute(
        "SELECT receipt_payload FROM warehouse_load_batch WHERE load_batch_id = ?",
        [load_batch_id],
    ).fetchone()
    return None if row is None else str(row[0]).encode("utf-8")


__all__ = [
    "DUCKDB_REQUIRED_VERSION",
    "PARQUET_COMPRESSION",
    "PARQUET_COMPRESSION_LEVEL",
    "PARQUET_ROW_GROUP_SIZE",
    "PYARROW_REQUIRED_VERSION",
    "WAREHOUSE_MIGRATION_ADMISSION_POLICY_V1",
    "ParquetBuildArtifact",
    "StagedParquetArtifact",
    "WarehouseMigrationAdmission",
    "WarehouseMigrationAdmissionRejected",
    "apply_receipt_transaction",
    "build_parquet_artifacts",
    "dependency_status",
    "publish_parquet_artifacts",
    "publish_staged_parquet_artifacts",
    "read_committed_receipt_payload",
    "read_existing_warehouse_rows",
    "rebuild_duckdb_projection",
    "detect_warehouse_schema_version",
    "migrate_legacy_projection_side_by_side",
    "rollback_successor_projection",
    "stage_parquet_artifacts",
    "validate_staged_parquet_artifacts",
    "require_duckdb",
    "require_pyarrow",
    "verify_parquet_file",
    "verify_receipt_parquet",
]
