"""Deterministic Claim warehouse export packages and external-storage safety."""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from .warehouse_models import (
    DATABASE_SCHEMA_VERSION,
    LABEL_TAXONOMY_VERSION,
    WAREHOUSE_EXPORT_SCHEMA_VERSION,
    WAREHOUSE_PROJECTION_VERSION,
    ExternalStorageRef,
    InputArtifactRef,
    RegistryPrefix,
    WarehouseConflictError,
    WarehouseContractError,
    WarehouseExportBinding,
    WarehouseExportManifestV1,
    WarehouseRow,
    compute_export_idempotency_key,
    sha256_bytes,
)


DEFAULT_STORAGE_ROOT_REF = "ubuntu_v02_claim_warehouse"
INLINE_TEXT_MAX_BYTES = 262_144


@dataclass(frozen=True, slots=True)
class ClaimTextChunk:
    chunk_index: int
    byte_start: int
    byte_end: int
    text: str
    text_hash: str

    @property
    def byte_count(self) -> int:
        return self.byte_end - self.byte_start


@dataclass(frozen=True, slots=True)
class WarehouseExportResult:
    manifest: WarehouseExportManifestV1
    manifest_bytes: bytes
    rows_bytes: bytes
    rows: tuple[WarehouseRow, ...]

    @property
    def manifest_hash(self) -> str:
        return sha256_bytes(self.manifest_bytes)

    @property
    def rows_hash(self) -> str:
        return sha256_bytes(self.rows_bytes)

    @property
    def logical_hash(self) -> str:
        return self.manifest.logical_hash

    @property
    def row_counts(self) -> dict[str, int]:
        return dict(self.manifest.row_counts)

    def business_payload(self) -> dict[str, Any]:
        """Return exactly the payload fields owned by the business-model layer."""

        return {
            "export_id": self.manifest.export_id,
            "run_id": self.manifest.run_id,
            "storage_root_ref": self.manifest.storage_root_ref,
            "manifest_relative_path": self.manifest.manifest_relative_path,
            "manifest_hash": self.manifest_hash,
            "rows_relative_path": self.manifest.rows_relative_path,
            "rows_hash": self.manifest.rows_hash,
            "logical_hash": self.manifest.logical_hash,
            "row_count": self.manifest.row_count,
            "row_counts": dict(self.manifest.row_counts),
            "schema_versions": dict(self.manifest.schema_versions),
            "taxonomy_versions": dict(self.manifest.taxonomy_versions),
            "exporter_versions": dict(self.manifest.exporter_versions),
            "projection_status": "pending",
        }

    def load_binding(
        self,
        *,
        logical_layer: str,
        source_registry_ref: ExternalStorageRef | Mapping[str, Any] | None = None,
    ) -> WarehouseExportBinding:
        """Bind this package into an immutable Loader plan."""

        registry_ref = source_registry_ref or ExternalStorageRef(
            storage_root_ref="repository",
            relative_path=(
                f"runs/V02/{self.manifest.run_id}/artifact_registry.jsonl"
            ),
        )

        return WarehouseExportBinding(
            export_id=self.manifest.export_id,
            export_idempotency_key=self.manifest.export_idempotency_key,
            source_run_id=self.manifest.run_id,
            source_registry_ref=registry_ref,
            logical_layer=logical_layer,
            storage_root_ref=self.manifest.storage_root_ref,
            manifest_relative_path=self.manifest.manifest_relative_path,
            manifest_hash=self.manifest_hash,
            rows_hash=self.rows_hash,
            logical_hash=self.logical_hash,
            row_count=self.manifest.row_count,
        )


def text_metrics(text: str) -> dict[str, int | str]:
    encoded = text.encode("utf-8")
    return {
        "text_char_count": len(text),
        "text_utf8_byte_count": len(encoded),
        "text_sha256": sha256_bytes(encoded),
    }


def chunk_utf8_text(
    text: str, *, max_chunk_bytes: int = INLINE_TEXT_MAX_BYTES
) -> tuple[ClaimTextChunk, ...]:
    """Split text on Unicode code-point boundaries without normalization.

    Inline-sized values return an empty tuple. A chunked result always reconstructs
    the exact original UTF-8 byte sequence and uses contiguous zero-based indexes.
    """

    if max_chunk_bytes < 4:
        raise WarehouseContractError("max_chunk_bytes must fit one UTF-8 code point")
    encoded = text.encode("utf-8")
    if len(encoded) <= max_chunk_bytes:
        return ()
    chunks: list[ClaimTextChunk] = []
    current: list[str] = []
    current_bytes = 0
    byte_start = 0
    for character in text:
        character_size = len(character.encode("utf-8"))
        if current and current_bytes + character_size > max_chunk_bytes:
            chunk_text = "".join(current)
            chunk_data = chunk_text.encode("utf-8")
            byte_end = byte_start + len(chunk_data)
            chunks.append(
                ClaimTextChunk(
                    chunk_index=len(chunks),
                    byte_start=byte_start,
                    byte_end=byte_end,
                    text=chunk_text,
                    text_hash=sha256_bytes(chunk_data),
                )
            )
            current = []
            current_bytes = 0
            byte_start = byte_end
        current.append(character)
        current_bytes += character_size
    if current:
        chunk_text = "".join(current)
        chunk_data = chunk_text.encode("utf-8")
        chunks.append(
            ClaimTextChunk(
                chunk_index=len(chunks),
                byte_start=byte_start,
                byte_end=byte_start + len(chunk_data),
                text=chunk_text,
                text_hash=sha256_bytes(chunk_data),
            )
        )
    if b"".join(item.text.encode("utf-8") for item in chunks) != encoded:
        raise AssertionError("internal UTF-8 chunk reconstruction failure")
    return tuple(chunks)


def canonicalize_export(
    rows: Iterable[WarehouseRow | Mapping[str, Any]],
    *,
    export_id: str,
    run_id: str,
    source_registry_prefix: RegistryPrefix | Mapping[str, Any],
    input_artifacts: Iterable[InputArtifactRef | Mapping[str, Any]],
    run_created_at: str,
    created_at: str,
    storage_ref: ExternalStorageRef | Mapping[str, Any],
    schema_versions: Mapping[str, str] | None = None,
    taxonomy_versions: Mapping[str, str] | None = None,
    exporter_versions: Mapping[str, str] | None = None,
) -> WarehouseExportResult:
    """Build byte-deterministic manifest/rows content without writing files."""

    parsed_rows = tuple(
        row if isinstance(row, WarehouseRow) else WarehouseRow.model_validate(row)
        for row in rows
    )
    if not parsed_rows:
        raise WarehouseContractError("warehouse export requires at least one row")
    if any(row.run_id != run_id for row in parsed_rows):
        raise WarehouseContractError("every export row must bind the declared run_id")
    ordered_rows = tuple(
        sorted(
            parsed_rows,
            key=lambda row: (
                row.logical_layer,
                row.table_code,
                row.canonical_primary_key,
                row.revision_no,
            ),
        )
    )
    identities = [
        (
            row.logical_layer,
            row.table_code,
            row.canonical_primary_key,
            row.revision_no,
        )
        for row in ordered_rows
    ]
    if len(identities) != len(set(identities)):
        raise WarehouseContractError("export contains duplicate canonical row identity")
    rows_bytes = b"".join(row.canonical_bytes() + b"\n" for row in ordered_rows)
    rows_hash = sha256_bytes(rows_bytes)
    row_counts = dict(sorted(Counter(row.table_code for row in ordered_rows).items()))

    package_ref = (
        storage_ref
        if isinstance(storage_ref, ExternalStorageRef)
        else ExternalStorageRef.model_validate(storage_ref)
    )
    package_path = PurePosixPath(package_ref.relative_path)
    if package_path != PurePosixPath("exports") / export_id:
        raise WarehouseContractError(
            "export package path must be exports/<export_id>"
        )
    manifest_relative_path = str(package_path / "manifest.json")
    rows_relative_path = str(package_path / "rows.jsonl")
    prefix = (
        source_registry_prefix
        if isinstance(source_registry_prefix, RegistryPrefix)
        else RegistryPrefix.model_validate(source_registry_prefix)
    )
    artifacts = sorted(
        (
            item
            if isinstance(item, InputArtifactRef)
            else InputArtifactRef.model_validate(item)
            for item in input_artifacts
        ),
        key=lambda item: (item.artifact_id, item.record_id),
    )
    if not artifacts:
        raise WarehouseContractError("export requires exact input Artifact bindings")
    effective_taxonomy_versions = dict(
        taxonomy_versions
        or {
            "label_taxonomy_version": LABEL_TAXONOMY_VERSION,
        }
    )
    effective_exporter_versions = dict(
        exporter_versions or {"warehouse_exporter": "warehouse_export_v1.0.0"}
    )
    manifest = WarehouseExportManifestV1(
        export_id=export_id,
        run_id=run_id,
        run_created_at=run_created_at,
        created_at=created_at,
        export_idempotency_key=compute_export_idempotency_key(
            run_id=run_id,
            source_registry_head_record_id=prefix.head_record_id,
            ordered_input_artifact_record_ids=[item.record_id for item in artifacts],
            taxonomy_versions=effective_taxonomy_versions,
            exporter_versions=effective_exporter_versions,
        ),
        storage_root_ref=package_ref.storage_root_ref,
        manifest_relative_path=manifest_relative_path,
        rows_relative_path=rows_relative_path,
        rows_hash=rows_hash,
        logical_hash=rows_hash,
        row_count=len(ordered_rows),
        row_counts=row_counts,
        source_registry_prefix=prefix,
        input_artifacts=artifacts,
        schema_versions=dict(
            schema_versions
            or {
                "database_schema_version": DATABASE_SCHEMA_VERSION,
                "warehouse_export_schema_version": WAREHOUSE_EXPORT_SCHEMA_VERSION,
                "warehouse_projection_version": WAREHOUSE_PROJECTION_VERSION,
            }
        ),
        taxonomy_versions=effective_taxonomy_versions,
        exporter_versions=effective_exporter_versions,
    )
    manifest_bytes = manifest.canonical_bytes()
    return WarehouseExportResult(
        manifest=manifest,
        manifest_bytes=manifest_bytes,
        rows_bytes=rows_bytes,
        rows=ordered_rows,
    )


def resolve_external_storage_ref(
    storage_roots: Mapping[str, Path | str],
    ref: ExternalStorageRef | Mapping[str, Any],
    *,
    require_root: bool = True,
) -> Path:
    """Resolve a logical root safely, rejecting traversal, symlinks and mount escape."""

    parsed = ref if isinstance(ref, ExternalStorageRef) else ExternalStorageRef.model_validate(ref)
    if parsed.storage_root_ref != DEFAULT_STORAGE_ROOT_REF:
        raise WarehouseContractError(
            "unknown storage_root_ref for Claim warehouse; expected "
            "ubuntu_v02_claim_warehouse"
        )
    if parsed.storage_root_ref not in storage_roots:
        raise WarehouseContractError(
            f"unknown storage_root_ref: {parsed.storage_root_ref}"
        )
    root = Path(storage_roots[parsed.storage_root_ref])
    if not root.is_absolute():
        raise WarehouseContractError("external storage root mapping must be absolute")
    current_root = Path(root.anchor)
    for part in root.parts[1:]:
        current_root = current_root / part
        if os.path.lexists(current_root) and stat.S_ISLNK(os.lstat(current_root).st_mode):
            raise WarehouseContractError(
                "external storage root and its ancestors cannot be symlinks"
            )
    if require_root and not root.is_dir():
        raise WarehouseContractError("external storage root does not exist")
    resolved_root = root.resolve(strict=require_root)
    candidate = root.joinpath(*PurePosixPath(parsed.relative_path).parts)
    current = root
    root_device = os.stat(root).st_dev if root.exists() else None
    for part in PurePosixPath(parsed.relative_path).parts:
        current = current / part
        if not os.path.lexists(current):
            continue
        info = os.lstat(current)
        if stat.S_ISLNK(info.st_mode):
            raise WarehouseContractError("symlink components are forbidden")
        if root_device is not None and os.stat(current).st_dev != root_device:
            raise WarehouseContractError("mount escape below storage root is forbidden")
    resolved_candidate = candidate.resolve(strict=False)
    try:
        resolved_candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise WarehouseContractError("external path escapes logical storage root") from exc
    return candidate


def write_export_package(
    result: WarehouseExportResult,
    *,
    storage_roots: Mapping[str, Path | str],
    task_id: str | None = None,
) -> ExternalStorageRef:
    """Publish one export package create-new; equal replay is idempotent.

    The registered path is always ``exports/<export_id>``.  A never-registered
    package is assembled below ``staging/<task_id>`` and atomically renamed to
    that final path; there is no pending-to-committed directory transition.
    """

    package_ref = ExternalStorageRef(
        storage_root_ref=result.manifest.storage_root_ref,
        relative_path=str(PurePosixPath(result.manifest.manifest_relative_path).parent),
    )
    package_path = resolve_external_storage_ref(storage_roots, package_ref)
    package_path.parent.mkdir(parents=True, exist_ok=True)
    if package_path.exists():
        _verify_existing_package(package_path, result)
        return package_ref
    stage_task_id = task_id or result.manifest.run_id
    if re.fullmatch(r"[a-z][a-z0-9_]{2,95}", stage_task_id) is None:
        raise WarehouseContractError("task_id must be one safe storage segment")
    staging_root_ref = ExternalStorageRef(
        storage_root_ref=result.manifest.storage_root_ref,
        relative_path=f"staging/{stage_task_id}",
    )
    staging_root = resolve_external_storage_ref(storage_roots, staging_root_ref)
    if os.name == "nt":
        # CPython 3.13 gives mode=0o700 special ACL semantics on Windows.  That
        # can make a freshly-created staging directory unreadable even to the
        # process that created it, so Windows must use its inherited ACL.
        staging_root.mkdir(parents=True, exist_ok=True)
    else:
        staging_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    staging = staging_root / f"{result.manifest.export_id}.{uuid.uuid4().hex}"
    if os.name == "nt":
        staging.mkdir()
    else:
        staging.mkdir(mode=0o700)
    try:
        _write_new_bytes(staging / "manifest.json", result.manifest_bytes)
        _write_new_bytes(staging / "rows.jsonl", result.rows_bytes)
        _verify_existing_package(staging, result)
        try:
            staging.rename(package_path)
        except FileExistsError:
            _verify_existing_package(package_path, result)
        _fsync_directory(package_path.parent)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
        try:
            staging_root.rmdir()
        except OSError:
            pass
    return package_ref


def read_export_package(
    *,
    storage_roots: Mapping[str, Path | str],
    manifest_ref: ExternalStorageRef | Mapping[str, Any],
    expected_manifest_hash: str | None = None,
) -> WarehouseExportResult:
    """Read and fully validate canonical export bytes from external storage."""

    parsed_ref, manifest, manifest_bytes = read_export_manifest(
        storage_roots=storage_roots,
        manifest_ref=manifest_ref,
        expected_manifest_hash=expected_manifest_hash,
    )
    rows_path = resolve_external_storage_ref(
        storage_roots,
        ExternalStorageRef(
            storage_root_ref=manifest.storage_root_ref,
            relative_path=manifest.rows_relative_path,
        ),
    )
    rows_bytes = rows_path.read_bytes()
    if sha256_bytes(rows_bytes) != manifest.rows_hash:
        raise WarehouseConflictError("rows hash differs from manifest")
    rows = _parse_canonical_rows(rows_bytes)
    observed_counts = dict(sorted(Counter(row.table_code for row in rows).items()))
    if len(rows) != manifest.row_count or observed_counts != manifest.row_counts:
        raise WarehouseContractError("rows count differs from manifest")
    if sha256_bytes(rows_bytes) != manifest.logical_hash:
        raise WarehouseContractError("logical row hash differs from manifest")
    identities = [
        (
            row.logical_layer,
            row.table_code,
            row.canonical_primary_key,
            row.revision_no,
        )
        for row in rows
    ]
    if identities != sorted(identities) or len(identities) != len(set(identities)):
        raise WarehouseContractError("rows are not uniquely canonical-sorted")
    return WarehouseExportResult(
        manifest=manifest,
        manifest_bytes=manifest_bytes,
        rows_bytes=rows_bytes,
        rows=rows,
    )


def read_export_manifest(
    *,
    storage_roots: Mapping[str, Path | str],
    manifest_ref: ExternalStorageRef | Mapping[str, Any],
    expected_manifest_hash: str | None = None,
) -> tuple[ExternalStorageRef, WarehouseExportManifestV1, bytes]:
    """Validate manifest identity before any business-row sidecar is opened."""

    parsed_ref = (
        manifest_ref
        if isinstance(manifest_ref, ExternalStorageRef)
        else ExternalStorageRef.model_validate(manifest_ref)
    )
    manifest_path = resolve_external_storage_ref(storage_roots, parsed_ref)
    manifest_bytes = manifest_path.read_bytes()
    manifest_hash = sha256_bytes(manifest_bytes)
    if expected_manifest_hash is not None and manifest_hash != expected_manifest_hash:
        raise WarehouseConflictError("manifest hash differs from expected Artifact binding")
    try:
        manifest = WarehouseExportManifestV1.model_validate_json(manifest_bytes)
    except Exception as exc:
        raise WarehouseContractError(f"invalid export manifest: {exc}") from exc
    if manifest.canonical_bytes() != manifest_bytes:
        raise WarehouseContractError("manifest is not canonical JSON")
    if manifest.storage_root_ref != parsed_ref.storage_root_ref:
        raise WarehouseContractError("manifest storage root differs from Registry binding")
    if manifest.manifest_relative_path != parsed_ref.relative_path:
        raise WarehouseContractError("manifest path differs from Registry binding")
    return parsed_ref, manifest, manifest_bytes


def _parse_canonical_rows(data: bytes) -> tuple[WarehouseRow, ...]:
    if not data or not data.endswith(b"\n"):
        raise WarehouseContractError("rows.jsonl must be non-empty and LF-terminated")
    rows: list[WarehouseRow] = []
    for line_number, line in enumerate(data.splitlines(keepends=True), start=1):
        if line == b"\n":
            raise WarehouseContractError(f"blank JSONL line: {line_number}")
        payload = line[:-1]
        try:
            raw = json.loads(payload)
            row = WarehouseRow.model_validate(raw)
        except Exception as exc:
            raise WarehouseContractError(
                f"invalid warehouse row at line {line_number}: {exc}"
            ) from exc
        if row.canonical_bytes() != payload:
            raise WarehouseContractError(f"non-canonical JSONL line: {line_number}")
        rows.append(row)
    return tuple(rows)


def _verify_existing_package(path: Path, result: WarehouseExportResult) -> None:
    if not path.is_dir() or path.is_symlink():
        raise WarehouseConflictError("export package target is not a safe directory")
    expected = {
        "manifest.json": result.manifest_bytes,
        "rows.jsonl": result.rows_bytes,
    }
    observed_names = sorted(item.name for item in path.iterdir())
    if observed_names != sorted(expected):
        raise WarehouseConflictError("export package file set differs from expected")
    for name, data in expected.items():
        file_path = path / name
        if file_path.is_symlink() or file_path.read_bytes() != data:
            raise WarehouseConflictError(f"immutable export conflict: {name}")


def _write_new_bytes(path: Path, data: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


__all__ = [
    "ClaimTextChunk",
    "DEFAULT_STORAGE_ROOT_REF",
    "INLINE_TEXT_MAX_BYTES",
    "WarehouseExportResult",
    "canonicalize_export",
    "chunk_utf8_text",
    "read_export_package",
    "read_export_manifest",
    "resolve_external_storage_ref",
    "text_metrics",
    "write_export_package",
]
