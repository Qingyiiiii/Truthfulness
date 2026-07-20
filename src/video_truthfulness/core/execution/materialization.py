"""Read-only validation for external input cache materialization receipts."""

from __future__ import annotations

import json
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import Field, ValidationError, field_validator, model_validator

from video_truthfulness.core.artifacts.registry import AppendOnlyRegistry
from video_truthfulness.core.execution.events import validate_relative_path
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_bytes,
    sha256_file,
)
from video_truthfulness.core.execution.models import (
    ARTIFACT_ID,
    RECORD_ID,
    RUN_ID,
    SHA256,
    UTC_TIMESTAMP,
    ExecutionContractError,
    StrictFrozenModel,
)


MAX_RECEIPT_BYTES = 64 * 1024


class MaterializationValidationError(ExecutionContractError):
    """Raised when a cache receipt or its bound files violate the contract."""


class FileStatSnapshot(StrictFrozenModel):
    size_bytes: int = Field(ge=1)
    mtime_utc: str = Field(pattern=UTC_TIMESTAMP)
    creation_time_utc: str = Field(pattern=UTC_TIMESTAMP)


class MaterializedFileStat(StrictFrozenModel):
    size_bytes: int = Field(ge=1)
    unix_mode: str = Field(pattern=r"^[0-7]{4}$")
    uid: int = Field(ge=0)
    gid: int = Field(ge=0)
    regular_file: Literal[True]
    symlink: Literal[False]


class MaterializationRegistryBinding(StrictFrozenModel):
    registry_scope: Literal["run"]
    relative_path: str = Field(min_length=1, max_length=512)
    file_hash_algorithm: Literal["sha256"]
    file_hash: str = Field(pattern=SHA256)
    record_count: int = Field(ge=1)
    head_record_id: str = Field(pattern=RECORD_ID)
    head_record_hash: str = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return validate_relative_path(value).as_posix()


class MaterializationRegistryPrefixBinding(StrictFrozenModel):
    """Immutable Registry prefix that remains valid after append-only growth."""

    registry_scope: Literal["run"]
    relative_path: str = Field(min_length=1, max_length=512)
    prefix_hash_algorithm: Literal["sha256"]
    prefix_hash: str = Field(pattern=SHA256)
    prefix_size_bytes: int = Field(ge=1)
    prefix_record_count: int = Field(ge=1)
    prefix_head_record_id: str = Field(pattern=RECORD_ID)
    prefix_head_record_hash: str = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return validate_relative_path(value).as_posix()


class PreviousMaterializationReceiptBinding(StrictFrozenModel):
    """Exact immutable predecessor receipt bound by the v1.1 successor."""

    relative_path: str = Field(min_length=1, max_length=512)
    receipt_version: Literal["input_materialization_v1.0.0"]
    file_hash_algorithm: Literal["sha256"]
    file_hash: str = Field(pattern=SHA256)
    semantic_hash_algorithm: Literal["sha256"]
    semantic_hash: str = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return validate_relative_path(value).as_posix()


class MaterializationValidationArtifactBinding(StrictFrozenModel):
    """The original revision-1 media.validation record for the source media."""

    artifact_id: str = Field(pattern=ARTIFACT_ID)
    record_id: str = Field(pattern=RECORD_ID)
    record_revision: Literal[1]
    record_hash: str = Field(pattern=SHA256)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    validation_status: Literal["passed"]
    lifecycle_state: Literal["validated", "frozen"]


class MaterializationSourceBinding(StrictFrozenModel):
    run_id: str = Field(pattern=RUN_ID)
    artifact_id: str = Field(pattern=ARTIFACT_ID)
    record_id: str = Field(pattern=RECORD_ID)
    record_hash: str = Field(pattern=SHA256)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    size_bytes: int = Field(ge=1)
    validation_status: Literal["passed"]
    lifecycle_state: Literal["validated", "frozen"]
    registry: MaterializationRegistryBinding


class MaterializationSourceBindingV1_1(MaterializationSourceBinding):
    registry: MaterializationRegistryPrefixBinding
    record_revision: Literal[1]
    validation_artifact: MaterializationValidationArtifactBinding


class MaterializedInput(StrictFrozenModel):
    authority_level: Literal["cache"]
    storage_root_ref: str = Field(pattern=r"^[a-z][a-z0-9_]{2,79}$")
    relative_path: str = Field(min_length=1, max_length=512)
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    size_bytes: int = Field(ge=1)
    target_stat: MaterializedFileStat

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return validate_relative_path(value).as_posix()


class CopyEvidence(StrictFrozenModel):
    operation: Literal["copy"]
    copy_tool: str = Field(min_length=1, max_length=120)
    no_clobber: Literal[True]
    destination_existed_before: Literal[False]
    source_stat_before: FileStatSnapshot
    source_stat_after: FileStatSnapshot
    completed_at: str = Field(pattern=UTC_TIMESTAMP)


class InputMaterializationReceipt(StrictFrozenModel):
    input_materialization_version: Literal["input_materialization_v1.0.0"]
    source_binding: MaterializationSourceBinding
    materialized_input: MaterializedInput
    copy_evidence: CopyEvidence
    receipt_hash: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def validate_identity_projection(self) -> InputMaterializationReceipt:
        source = self.source_binding
        cached = self.materialized_input
        if (cached.content_hash, cached.size_bytes) != (
            source.content_hash,
            source.size_bytes,
        ):
            raise ValueError(
                "cache content identity must equal the source Artifact identity"
            )
        if cached.target_stat.size_bytes != source.size_bytes:
            raise ValueError("target stat size does not match the source Artifact")
        before = self.copy_evidence.source_stat_before
        after = self.copy_evidence.source_stat_after
        if before != after:
            raise ValueError("source stat changed during copy")
        if before.size_bytes != source.size_bytes:
            raise ValueError("source stat size does not match the source Artifact")
        return self


class InputMaterializationReceiptV1_1(InputMaterializationReceipt):
    """Prefix-bound successor that permits only append-only Registry growth."""

    input_materialization_version: Literal["input_materialization_v1.1.0"]
    previous_receipt: PreviousMaterializationReceiptBinding
    source_binding: MaterializationSourceBindingV1_1


@dataclass(frozen=True)
class MaterializedContentProof:
    """In-process proof for revalidation without a second content read."""

    relative_path: str
    content_hash: str
    size_bytes: int
    st_dev: int
    st_ino: int
    st_mode: int
    st_uid: int
    st_gid: int
    st_mtime_ns: int
    st_ctime_ns: int


@dataclass(frozen=True)
class MaterializationValidationResult:
    receipt: InputMaterializationReceipt | InputMaterializationReceiptV1_1
    content_proof: MaterializedContentProof

    def summary(self) -> dict[str, Any]:
        source = self.receipt.source_binding
        cached = self.receipt.materialized_input
        registry = source.registry
        result = {
            "status": "VALID",
            "input_materialization_version": self.receipt.input_materialization_version,
            "authority_level": cached.authority_level,
            "storage_root_ref": cached.storage_root_ref,
            "materialized_relative_path": cached.relative_path,
            "run_id": source.run_id,
            "artifact_id": source.artifact_id,
            "record_id": source.record_id,
            "record_hash": source.record_hash,
            "content_hash": source.content_hash,
            "size_bytes": source.size_bytes,
            "source_validation_status": source.validation_status,
            "source_lifecycle_state": source.lifecycle_state,
            "target_unix_mode": cached.target_stat.unix_mode,
            "target_uid": cached.target_stat.uid,
            "target_gid": cached.target_stat.gid,
            "registry_relative_path": registry.relative_path,
            "receipt_hash": self.receipt.receipt_hash,
            "read_count": 3,
            "write_count": 0,
        }
        if isinstance(registry, MaterializationRegistryPrefixBinding):
            result.update(
                {
                    "registry_binding_mode": "immutable_prefix",
                    "registry_prefix_hash": registry.prefix_hash,
                    "registry_prefix_size_bytes": registry.prefix_size_bytes,
                    "registry_prefix_record_count": registry.prefix_record_count,
                    "registry_prefix_head_record_id": registry.prefix_head_record_id,
                    "registry_prefix_head_record_hash": registry.prefix_head_record_hash,
                }
            )
        else:
            result.update(
                {
                    "registry_binding_mode": "complete_file",
                    "registry_file_hash": registry.file_hash,
                    "registry_record_count": registry.record_count,
                    "registry_head_record_id": registry.head_record_id,
                    "registry_head_record_hash": registry.head_record_hash,
                }
            )
        return result


def _receipt_model(version: object) -> type[InputMaterializationReceipt]:
    model = {
        "input_materialization_v1.0.0": InputMaterializationReceipt,
        "input_materialization_v1.1.0": InputMaterializationReceiptV1_1,
    }.get(version)
    if model is None:
        raise MaterializationValidationError(
            "Unsupported or missing input_materialization_version"
        )
    return model


def seal_materialization_receipt(
    value: Mapping[str, Any],
) -> InputMaterializationReceipt | InputMaterializationReceiptV1_1:
    """Validate and self-hash a receipt draft without publishing any file."""

    payload = dict(value)
    payload["receipt_hash"] = "0" * 64
    model_type = _receipt_model(payload.get("input_materialization_version"))
    try:
        provisional = model_type.model_validate(payload)
    except ValidationError as exc:
        raise MaterializationValidationError(
            f"Invalid input materialization receipt: {exc}"
        ) from exc
    normalized = provisional.model_dump(mode="json")
    normalized["receipt_hash"] = embedded_hash(normalized, "receipt_hash")
    return parse_materialization_receipt(normalized)


def parse_materialization_receipt(
    value: Mapping[str, Any]
    | InputMaterializationReceipt
    | InputMaterializationReceiptV1_1,
) -> InputMaterializationReceipt | InputMaterializationReceiptV1_1:
    """Parse one strict receipt and verify its embedded semantic hash."""

    raw_value = (
        value.model_dump(mode="json")
        if isinstance(value, InputMaterializationReceipt)
        else dict(value)
    )
    model_type = _receipt_model(raw_value.get("input_materialization_version"))
    try:
        model = model_type.model_validate(raw_value)
    except ValidationError as exc:
        raise MaterializationValidationError(
            f"Invalid input materialization receipt: {exc}"
        ) from exc
    raw = model.model_dump(mode="json")
    expected = embedded_hash(raw, "receipt_hash")
    if model.receipt_hash != expected:
        raise MaterializationValidationError(
            f"receipt_hash mismatch: expected {expected}, observed {model.receipt_hash}"
        )
    return model


def read_materialization_receipt(
    path: Path,
) -> InputMaterializationReceipt | InputMaterializationReceiptV1_1:
    """Read one bounded canonical JSON receipt without following a symlink."""

    receipt_path = Path(path)
    if receipt_path.is_symlink():
        raise MaterializationValidationError("Receipt path must not be a symlink")
    try:
        data = receipt_path.read_bytes()
    except OSError as exc:
        raise MaterializationValidationError(f"Cannot read receipt: {exc}") from exc
    if not data or len(data) > MAX_RECEIPT_BYTES:
        raise MaterializationValidationError(
            f"Receipt size must be between 1 and {MAX_RECEIPT_BYTES} bytes"
        )
    try:
        raw = json.loads(data)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MaterializationValidationError(
            f"Receipt is not valid UTF-8 JSON: {exc}"
        ) from exc
    if not isinstance(raw, dict):
        raise MaterializationValidationError("Receipt JSON root must be an object")
    if data != canonical_json_bytes(raw) + b"\n":
        raise MaterializationValidationError(
            "Receipt must use canonical JSON with exactly one final LF"
        )
    return parse_materialization_receipt(raw)


def validate_input_materialization(
    receipt_path: Path,
    *,
    repository_root: Path,
    storage_root: Path,
    content_proof: MaterializedContentProof | None = None,
) -> MaterializationValidationResult:
    """Validate one cache replica against its sealed Registry source identity."""

    receipt = read_materialization_receipt(receipt_path)
    return validate_materialization_receipt(
        receipt,
        repository_root=repository_root,
        storage_root=storage_root,
        content_proof=content_proof,
    )


def validate_materialization_receipt(
    receipt: InputMaterializationReceipt | InputMaterializationReceiptV1_1,
    *,
    repository_root: Path,
    storage_root: Path,
    content_proof: MaterializedContentProof | None = None,
) -> MaterializationValidationResult:
    """Validate a parsed receipt, optionally reusing one exact content proof.

    A caller may validate a sealed in-memory receipt before create-new publication,
    then validate the published receipt with the returned proof.  The second pass
    rechecks the target's full file identity and metadata but does not open its
    content again.
    """

    receipt = parse_materialization_receipt(receipt)
    source = receipt.source_binding
    registry_binding = source.registry
    repository = _validated_root(repository_root, "repository root")
    storage = _validated_root(storage_root, "storage root")
    registry_path = _safe_file(
        repository,
        registry_binding.relative_path,
        "Registry",
    )
    target_path = _safe_file(
        storage,
        receipt.materialized_input.relative_path,
        "materialized input",
    )

    registry_bytes_before = _stable_read_bytes(registry_path, "Registry")
    registry_hash_before = sha256_bytes(registry_bytes_before)
    if isinstance(registry_binding, MaterializationRegistryBinding):
        if registry_hash_before != registry_binding.file_hash:
            raise MaterializationValidationError(
                "Registry file hash does not match the receipt binding"
            )
    registry = AppendOnlyRegistry(
        registry_path,
        scope="run",
        expected_run_id=source.run_id,
    )
    try:
        registry.validate_full_history()
        entries = registry.read_entries()
    except (OSError, UnicodeError, ValueError) as exc:
        raise MaterializationValidationError(
            f"Registry validation failed: {exc}"
        ) from exc
    registry_bytes_after = _stable_read_bytes(registry_path, "Registry")
    if registry_bytes_after != registry_bytes_before:
        raise MaterializationValidationError("Registry changed during validation")
    if isinstance(registry_binding, MaterializationRegistryPrefixBinding):
        prefix_count = registry_binding.prefix_record_count
        if len(entries) < prefix_count:
            raise MaterializationValidationError(
                "Registry immutable prefix was truncated"
            )
        prefix_entries = entries[:prefix_count]
        if len(registry_bytes_before) < registry_binding.prefix_size_bytes:
            raise MaterializationValidationError(
                "Registry immutable prefix was truncated"
            )
        prefix_bytes = registry_bytes_before[: registry_binding.prefix_size_bytes]
        if sha256_bytes(prefix_bytes) != registry_binding.prefix_hash:
            raise MaterializationValidationError(
                "Registry immutable prefix hash mismatch"
            )
        _validate_registry_prefix_bytes(
            prefix_bytes,
            prefix_entries,
            expected_record_count=prefix_count,
        )
        head = prefix_entries[-1].wire_record
        if (head.record_id, head.record_hash) != (
            registry_binding.prefix_head_record_id,
            registry_binding.prefix_head_record_hash,
        ):
            raise MaterializationValidationError(
                "Registry immutable prefix head identity mismatch"
            )
    else:
        if len(entries) != registry_binding.record_count:
            raise MaterializationValidationError("Registry record_count mismatch")
        head = entries[-1].wire_record
        if (head.record_id, head.record_hash) != (
            registry_binding.head_record_id,
            registry_binding.head_record_hash,
        ):
            raise MaterializationValidationError("Registry head identity mismatch")

    matches = [
        entry for entry in entries if entry.wire_record.record_id == source.record_id
    ]
    if len(matches) != 1:
        raise MaterializationValidationError(
            "Receipt source record_id must resolve exactly once in the Registry"
        )
    wire = matches[0].wire_record
    view = matches[0].canonical_view
    if wire.record_hash != source.record_hash:
        raise MaterializationValidationError("Source record_hash mismatch")
    if (view.artifact_id, view.run_id) != (source.artifact_id, source.run_id):
        raise MaterializationValidationError("Source Artifact or run identity mismatch")
    if (view.content_hash, view.size_bytes) != (
        source.content_hash,
        source.size_bytes,
    ):
        raise MaterializationValidationError(
            "Source Artifact content identity mismatch"
        )
    if (view.validation_status, view.lifecycle_state) != (
        source.validation_status,
        source.lifecycle_state,
    ):
        raise MaterializationValidationError(
            "Source Artifact validation or lifecycle state mismatch"
        )
    if isinstance(receipt, InputMaterializationReceiptV1_1):
        _validate_previous_receipt(receipt, repository)
        if view.artifact_type != "media.video" or view.record_revision != 1:
            raise MaterializationValidationError(
                "Prefix-bound source must be the unique revision-1 media.video Artifact"
            )
        latest_source_records = [
            entry.canonical_view
            for entry in entries
            if entry.canonical_view.artifact_id == source.artifact_id
        ]
        if (
            len(latest_source_records) != 1
            or latest_source_records[0].record_id != source.record_id
        ):
            raise MaterializationValidationError(
                "Source Artifact gained a new revision after the immutable prefix"
            )
        same_type_sources = [
            entry.canonical_view
            for entry in entries
            if entry.canonical_view.run_id == source.run_id
            and entry.canonical_view.artifact_type == "media.video"
            and entry.canonical_view.artifact_id != source.artifact_id
        ]
        if same_type_sources:
            raise MaterializationValidationError(
                "Registry contains a second source Artifact of the bound type for this run"
            )

        validation = receipt.source_binding.validation_artifact
        validation_matches = [
            entry
            for entry in entries
            if entry.wire_record.record_id == validation.record_id
        ]
        if len(validation_matches) != 1:
            raise MaterializationValidationError(
                "Bound media.validation record_id must resolve exactly once"
            )
        validation_wire = validation_matches[0].wire_record
        validation_view = validation_matches[0].canonical_view
        if (
            validation_wire.record_hash,
            validation_view.artifact_id,
            validation_view.record_revision,
            validation_view.content_hash,
            validation_view.validation_status,
            validation_view.lifecycle_state,
        ) != (
            validation.record_hash,
            validation.artifact_id,
            validation.record_revision,
            validation.content_hash,
            validation.validation_status,
            validation.lifecycle_state,
        ):
            raise MaterializationValidationError(
                "Bound media.validation record identity or lifecycle mismatch"
            )
        if (
            validation_view.artifact_type != "media.validation"
            or validation_view.run_id != source.run_id
            or validation_view.source_id != view.source_id
            or validation_view.source_platform != view.source_platform
            or validation_view.upstream_artifact_ids != [source.artifact_id]
            or view.validation_artifact_ids != [validation.artifact_id]
        ):
            raise MaterializationValidationError(
                "media.video to media.validation association changed"
            )
        latest_validation_records = [
            entry.canonical_view
            for entry in entries
            if entry.canonical_view.artifact_id == validation.artifact_id
        ]
        if (
            len(latest_validation_records) != 1
            or latest_validation_records[0].record_id != validation.record_id
        ):
            raise MaterializationValidationError(
                "Bound media.validation Artifact gained a new revision"
            )

    target_hash, verified_proof = _verified_content_hash(
        target_path,
        receipt.materialized_input.relative_path,
        "materialized input",
        content_proof,
    )
    target_stat = target_path.stat()
    target_size = target_stat.st_size
    if (target_hash, target_size) != (source.content_hash, source.size_bytes):
        raise MaterializationValidationError(
            "Materialized input content does not match the source Artifact"
        )
    declared_target_stat = receipt.materialized_input.target_stat
    observed_target_stat = (
        format(stat.S_IMODE(target_stat.st_mode), "04o"),
        target_stat.st_uid,
        target_stat.st_gid,
        stat.S_ISREG(target_stat.st_mode),
        target_path.is_symlink(),
    )
    if observed_target_stat != (
        declared_target_stat.unix_mode,
        declared_target_stat.uid,
        declared_target_stat.gid,
        declared_target_stat.regular_file,
        declared_target_stat.symlink,
    ):
        raise MaterializationValidationError("Materialized input target_stat mismatch")
    return MaterializationValidationResult(
        receipt=receipt,
        content_proof=verified_proof,
    )


def _validate_previous_receipt(
    receipt: InputMaterializationReceiptV1_1,
    repository: Path,
) -> None:
    binding = receipt.previous_receipt
    previous_path = _safe_file(repository, binding.relative_path, "previous receipt")
    previous_bytes = _stable_read_bytes(previous_path, "previous receipt")
    if sha256_bytes(previous_bytes) != binding.file_hash:
        raise MaterializationValidationError("Previous receipt file hash mismatch")
    previous = read_materialization_receipt(previous_path)
    if not isinstance(previous, InputMaterializationReceiptV1_1) and (
        previous.input_materialization_version == binding.receipt_version
    ):
        pass
    else:
        raise MaterializationValidationError("Previous receipt version mismatch")
    if previous.receipt_hash != binding.semantic_hash:
        raise MaterializationValidationError("Previous receipt semantic hash mismatch")

    source = receipt.source_binding
    previous_source = previous.source_binding
    if (
        previous_source.run_id,
        previous_source.artifact_id,
        previous_source.record_id,
        previous_source.record_hash,
        previous_source.content_hash,
        previous_source.size_bytes,
        previous_source.validation_status,
        previous_source.lifecycle_state,
    ) != (
        source.run_id,
        source.artifact_id,
        source.record_id,
        source.record_hash,
        source.content_hash,
        source.size_bytes,
        source.validation_status,
        source.lifecycle_state,
    ):
        raise MaterializationValidationError(
            "Previous receipt source identity differs from the successor"
        )
    if previous.materialized_input.model_dump(
        mode="json"
    ) != receipt.materialized_input.model_dump(
        mode="json"
    ) or previous.copy_evidence.model_dump(
        mode="json"
    ) != receipt.copy_evidence.model_dump(mode="json"):
        raise MaterializationValidationError(
            "Previous receipt cache or copy evidence differs from the successor"
        )
    previous_registry = previous.source_binding.registry
    current_registry = source.registry
    if not isinstance(previous_registry, MaterializationRegistryBinding):
        raise MaterializationValidationError(
            "Previous receipt must use the immutable v1.0 Registry binding"
        )
    if (
        previous_registry.relative_path,
        previous_registry.file_hash,
        previous_registry.record_count,
        previous_registry.head_record_id,
        previous_registry.head_record_hash,
    ) != (
        current_registry.relative_path,
        current_registry.prefix_hash,
        current_registry.prefix_record_count,
        current_registry.prefix_head_record_id,
        current_registry.prefix_head_record_hash,
    ):
        raise MaterializationValidationError(
            "Previous receipt Registry binding differs from the frozen prefix"
        )


def _stat_identity(observed: Any) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mode,
        observed.st_uid,
        observed.st_gid,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _proof_identity(proof: MaterializedContentProof) -> tuple[int, ...]:
    return (
        proof.st_dev,
        proof.st_ino,
        proof.size_bytes,
        proof.st_mode,
        proof.st_uid,
        proof.st_gid,
        proof.st_mtime_ns,
        proof.st_ctime_ns,
    )


def _verified_content_hash(
    path: Path,
    relative_path: str,
    label: str,
    proof: MaterializedContentProof | None,
) -> tuple[str, MaterializedContentProof]:
    before = path.stat()
    if proof is None:
        digest = sha256_file(path)
        after = path.stat()
        if _stat_identity(before) != _stat_identity(after):
            raise MaterializationValidationError(f"{label} changed while it was hashed")
        return digest, MaterializedContentProof(
            relative_path=relative_path,
            content_hash=digest,
            size_bytes=after.st_size,
            st_dev=after.st_dev,
            st_ino=after.st_ino,
            st_mode=after.st_mode,
            st_uid=after.st_uid,
            st_gid=after.st_gid,
            st_mtime_ns=after.st_mtime_ns,
            st_ctime_ns=after.st_ctime_ns,
        )
    if proof.relative_path != relative_path or _stat_identity(
        before
    ) != _proof_identity(proof):
        raise MaterializationValidationError(
            f"{label} identity changed after the verified content read"
        )
    return proof.content_hash, proof


def _validated_root(path: Path, label: str) -> Path:
    candidate = Path(path)
    if _is_link(candidate):
        raise MaterializationValidationError(
            f"{label} must not be a symlink or junction"
        )
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise MaterializationValidationError(f"Cannot resolve {label}: {exc}") from exc
    if not resolved.is_dir():
        raise MaterializationValidationError(f"{label} must be an existing directory")
    return resolved


def _safe_file(root: Path, relative_path: str, label: str) -> Path:
    relative = validate_relative_path(relative_path)
    candidate = root.joinpath(*relative.parts)
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        if _is_link(cursor):
            raise MaterializationValidationError(
                f"{label} path must not contain a symlink or junction: {relative.as_posix()}"
            )
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise MaterializationValidationError(
            f"{label} must resolve to an existing file below its declared root"
        ) from exc
    if not resolved.is_file():
        raise MaterializationValidationError(f"{label} is not a regular file")
    return resolved


def _is_link(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _stable_read_bytes(path: Path, label: str) -> bytes:
    before = path.stat()
    try:
        data = path.read_bytes()
    except OSError as exc:
        raise MaterializationValidationError(f"Cannot read {label}: {exc}") from exc
    after = path.stat()
    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    if before_identity != after_identity or len(data) != before.st_size:
        raise MaterializationValidationError(f"{label} changed while it was read")
    return data


def _validate_registry_prefix_bytes(
    prefix_bytes: bytes,
    prefix_entries: list[Any],
    *,
    expected_record_count: int,
) -> None:
    """Bind the receipt to the exact canonical LF-terminated physical prefix."""

    lines = prefix_bytes.splitlines(keepends=True)
    if (
        len(lines) != expected_record_count
        or len(prefix_entries) != expected_record_count
    ):
        raise MaterializationValidationError(
            "Registry immutable prefix does not contain the declared canonical LF line count"
        )
    for line, entry in zip(lines, prefix_entries, strict=True):
        expected = (
            canonical_json_bytes(entry.wire_record.model_dump(mode="json")) + b"\n"
        )
        if line != expected:
            raise MaterializationValidationError(
                "Registry immutable prefix is not exact canonical JSONL with one LF per record"
            )
