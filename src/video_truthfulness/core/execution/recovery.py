"""Read-only validation for one explicit Stage 4 recovery package."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from video_truthfulness.core.artifacts.dag import load_dag
from video_truthfulness.core.artifacts.models import ArtifactRecordV1_1
from video_truthfulness.core.execution.checkpoints import (
    RegistryHead,
    parse_checkpoint,
)
from video_truthfulness.core.execution.events import (
    validate_event_stream,
    validate_manifest,
    validate_relative_path,
    validate_session_started_file_binding,
)
from video_truthfulness.core.execution.handoff import (
    HandoffPublication,
    HandoffRegistration,
    HandoffSources,
    NextStageAction,
    ReturnToStageAction,
    parse_handoff,
    render_handoff_markdown,
    validate_handoff,
    validate_handoff_created_event,
    validate_handoff_registration,
    validate_handoff_registry_record,
)
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    sha256_bytes,
)
from video_truthfulness.core.execution.models import ExecutionContractError
from video_truthfulness.core.execution.state import (
    build_current_state,
    current_state_bytes,
    snapshot_registry,
    validate_current_state,
)


RECOVERY_BUNDLE_RELATIVE = PurePosixPath("examples/execution_contract/synthetic_run")
SYNTHETIC_CHECKPOINT_ID = "checkpoint_01j00000000000000000000000"
SYNTHETIC_SOURCE_RECORD_ID = "record_01j00000000000000000000002"
SYNTHETIC_HANDOFF_RECORD_ID = "record_01j00000000000000000000003"
EXPECTED_RECOVERY_PATHS = tuple(
    sorted(
        (
            (RECOVERY_BUNDLE_RELATIVE / "artifact_registry.jsonl").as_posix(),
            (RECOVERY_BUNDLE_RELATIVE / "artifacts" / "input.json").as_posix(),
            (RECOVERY_BUNDLE_RELATIVE / "artifacts" / "output.json").as_posix(),
            (
                RECOVERY_BUNDLE_RELATIVE
                / "checkpoints"
                / f"{SYNTHETIC_CHECKPOINT_ID}.json"
            ).as_posix(),
            (RECOVERY_BUNDLE_RELATIVE / "events.jsonl").as_posix(),
            (RECOVERY_BUNDLE_RELATIVE / "handoff.json").as_posix(),
            (RECOVERY_BUNDLE_RELATIVE / "session_manifest.json").as_posix(),
            (RECOVERY_BUNDLE_RELATIVE / "working_tree_manifest.json").as_posix(),
            (
                RECOVERY_BUNDLE_RELATIVE / "youtube_truthfulness_dag_v1_1.yaml"
            ).as_posix(),
        )
    )
)
EXPECTED_EVENT_COUNT = 9
EXPECTED_SOURCE_EVENT_SEQUENCE = 8
EXPECTED_RECEIPT_EVENT_SEQUENCE = 9
EXPECTED_REGISTRY_PREFIX_COUNT = 2
EXPECTED_REGISTRY_FULL_COUNT = 3
MAX_RECOVERY_FILES = 1_024
MAX_RECOVERY_FILE_BYTES = 8 * 1_024 * 1_024
MAX_RECOVERY_TOTAL_BYTES = 16 * 1_024 * 1_024


class RecoveryValidationError(ExecutionContractError):
    """Raised when an isolated recovery package is incomplete or inconsistent."""


@dataclass(frozen=True)
class RecoveryResult:
    """Deterministic facts rebuilt without writing either projection to disk."""

    task_id: str
    session_id: str
    attempt_no: int
    run_id: str | None
    stage_id: str
    status: str
    checkpoint_id: str
    next_action_type: str
    next_stage: str
    source_event_id: str
    source_event_hash: str
    receipt_event_id: str
    receipt_event_hash: str
    registry_prefix_record_count: int
    registry_prefix_hash: str
    registry_full_record_count: int
    registry_full_hash: str
    state_hash: str
    state_bytes_sha256: str
    markdown_sha256: str
    required_paths: tuple[str, ...]
    actual_paths: tuple[str, ...]
    file_sizes: tuple[tuple[str, int], ...]
    content_hashes: tuple[tuple[str, str], ...]
    total_size_bytes: int

    def summary(self) -> dict[str, object]:
        """Return one deterministic, JSON-serializable CLI result."""

        return {
            "task_id": self.task_id,
            "session_id": self.session_id,
            "attempt_no": self.attempt_no,
            "run_id": self.run_id,
            "stage_id": self.stage_id,
            "status": self.status,
            "checkpoint_id": self.checkpoint_id,
            "next_action_type": self.next_action_type,
            "next_stage": self.next_stage,
            "source_event_id": self.source_event_id,
            "source_event_hash": self.source_event_hash,
            "receipt_event_id": self.receipt_event_id,
            "receipt_event_hash": self.receipt_event_hash,
            "registry_prefix_record_count": self.registry_prefix_record_count,
            "registry_prefix_hash": self.registry_prefix_hash,
            "registry_full_record_count": self.registry_full_record_count,
            "registry_full_hash": self.registry_full_hash,
            "state_hash": self.state_hash,
            "state_bytes_sha256": self.state_bytes_sha256,
            "markdown_sha256": self.markdown_sha256,
            "required_paths": list(self.required_paths),
            "actual_paths": list(self.actual_paths),
            "write_count": 0,
            "file_sizes": dict(self.file_sizes),
            "content_hashes": dict(self.content_hashes),
            "file_count": len(self.required_paths),
            "total_size_bytes": self.total_size_bytes,
            "audit_scope": "contract-declared file reads; not OS-level monitoring",
        }


@dataclass(frozen=True)
class HandoffRecoveryResult:
    """Read-only result for one exact, HANDOFF-declared recovery package."""

    task_id: str
    session_id: str
    attempt_no: int
    run_id: str | None
    stage_id: str
    status: str
    checkpoint_id: str
    handoff_version: str
    workflow_version: str
    dag_version: str
    next_action_type: str
    next_stage: str
    execution_authorized: bool | None
    source_event_id: str
    source_event_hash: str
    receipt_event_id: str
    receipt_event_hash: str
    required_paths: tuple[str, ...]
    actual_paths: tuple[str, ...]
    file_sizes: tuple[tuple[str, int], ...]
    content_hashes: tuple[tuple[str, str], ...]
    total_size_bytes: int

    def summary(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "session_id": self.session_id,
            "attempt_no": self.attempt_no,
            "run_id": self.run_id,
            "stage_id": self.stage_id,
            "status": self.status,
            "checkpoint_id": self.checkpoint_id,
            "handoff_version": self.handoff_version,
            "workflow_version": self.workflow_version,
            "dag_version": self.dag_version,
            "next_action_type": self.next_action_type,
            "next_stage": self.next_stage,
            "execution_authorized": self.execution_authorized,
            "source_event_id": self.source_event_id,
            "source_event_hash": self.source_event_hash,
            "receipt_event_id": self.receipt_event_id,
            "receipt_event_hash": self.receipt_event_hash,
            "required_paths": list(self.required_paths),
            "actual_paths": list(self.actual_paths),
            "read_count": len(self.actual_paths),
            "write_count": 0,
            "file_sizes": dict(self.file_sizes),
            "content_hashes": dict(self.content_hashes),
            "file_count": len(self.required_paths),
            "total_size_bytes": self.total_size_bytes,
            "audit_scope": "contract-declared file reads; not OS-level monitoring",
        }


class _RecoveryReader:
    """Allow-list reader that never discovers paths by scanning a directory."""

    def __init__(self, root: Path, allowed: tuple[str, ...]) -> None:
        self.root = root
        self.allowed = allowed
        self._paths: dict[str, Path] = {}
        self._sizes: dict[str, int] = {}
        self._cache: dict[str, bytes] = {}
        self._actual: set[str] = set()
        self._total_size_bytes = 0

    @property
    def total_size_bytes(self) -> int:
        return self._total_size_bytes

    @property
    def actual_paths(self) -> tuple[str, ...]:
        return tuple(sorted(self._actual))

    @property
    def file_sizes(self) -> tuple[tuple[str, int], ...]:
        return tuple(sorted(self._sizes.items()))

    @property
    def content_hashes(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted((path, sha256_bytes(data)) for path, data in self._cache.items())
        )

    def path(self, relative: str) -> Path:
        if relative not in self.allowed:
            raise RecoveryValidationError(
                f"Recovery source is not declared by required_read_paths: {relative}"
            )
        if relative not in self._paths:
            path = _safe_required_file(self.root, relative)
            size = path.stat().st_size
            if size > MAX_RECOVERY_FILE_BYTES:
                raise RecoveryValidationError(
                    f"Recovery file exceeds {MAX_RECOVERY_FILE_BYTES} bytes: {relative}"
                )
            total = self._total_size_bytes + size
            if total > MAX_RECOVERY_TOTAL_BYTES:
                raise RecoveryValidationError(
                    f"Recovery package exceeds {MAX_RECOVERY_TOTAL_BYTES} bytes"
                )
            self._paths[relative] = path
            self._sizes[relative] = size
            self._total_size_bytes = total
        return self._paths[relative]

    def seed(self, relative: str, data: bytes) -> None:
        path = self.path(relative)
        if len(data) != self._sizes[relative]:
            raise RecoveryValidationError(
                f"Recovery file size changed while opening: {relative}"
            )
        self._cache[relative] = data
        self._actual.add(relative)
        if path.stat().st_size != len(data):
            raise RecoveryValidationError(
                f"Recovery file changed while opening: {relative}"
            )

    def read_bytes(self, relative: str) -> bytes:
        path = self.path(relative)
        if relative not in self._cache:
            try:
                data = path.read_bytes()
            except OSError as exc:
                raise RecoveryValidationError(
                    f"Cannot read required recovery file: {relative}"
                ) from exc
            if len(data) != self._sizes[relative]:
                raise RecoveryValidationError(
                    f"Recovery file size changed while reading: {relative}"
                )
            self._cache[relative] = data
        self._actual.add(relative)
        return self._cache[relative]

    def verify_unchanged(self) -> None:
        for relative in self.allowed:
            if relative not in self._cache:
                raise RecoveryValidationError(
                    f"Recovery path was declared but not read after exact-set validation: {relative}"
                )
            current = _safe_required_file(self.root, relative)
            if current != self._paths[relative]:
                raise RecoveryValidationError(
                    f"Recovery input path identity changed during validation: {relative}"
                )
            try:
                observed = current.read_bytes()
            except OSError as exc:
                raise RecoveryValidationError(
                    f"Cannot re-read required recovery file: {relative}"
                ) from exc
            if observed != self._cache[relative]:
                raise RecoveryValidationError(
                    f"Recovery input changed during validation: {relative}"
                )


def validate_recovery_bundle(bundle: Path) -> RecoveryResult:
    """Validate and rebuild the public nine-file bundle without writing or executing it."""

    try:
        return _validate_recovery_bundle(Path(bundle))
    except RecoveryValidationError:
        raise
    except Exception as exc:  # noqa: BLE001 - normalize the public validation boundary
        raise RecoveryValidationError(f"Recovery validation failed: {exc}") from exc


def validate_handoff_recovery(
    handoff_path: Path,
    *,
    repository_root: Path,
) -> HandoffRecoveryResult:
    """Validate one generic recovery package from an exact HANDOFF path.

    The function never scans a directory and never writes.  Every file opened
    must be present in the HANDOFF action's exact ``required_read_paths``.
    """

    try:
        return _validate_handoff_recovery(
            Path(handoff_path),
            repository_root=Path(repository_root),
        )
    except RecoveryValidationError:
        raise
    except Exception as exc:  # noqa: BLE001 - normalize the public boundary
        raise RecoveryValidationError(
            f"HANDOFF recovery validation failed: {exc}"
        ) from exc


def _validate_handoff_recovery(
    handoff_path: Path,
    *,
    repository_root: Path,
) -> HandoffRecoveryResult:
    root = repository_root.resolve(strict=True)
    if not root.is_dir() or root.is_symlink():
        raise RecoveryValidationError("repository_root must be a real directory")
    if handoff_path.is_absolute():
        raise RecoveryValidationError("--handoff must be repository-relative")
    handoff_relative = validate_relative_path(handoff_path.as_posix()).as_posix()
    physical_handoff = _safe_required_file(root, handoff_relative)
    handoff_bytes = physical_handoff.read_bytes()
    handoff_raw = _canonical_json_object(handoff_bytes, handoff_relative)
    handoff = parse_handoff(handoff_raw)
    if handoff_bytes != canonical_json_bytes(handoff.model_dump(mode="json")) + b"\n":
        raise RecoveryValidationError("HANDOFF is not canonical JSON with one final LF")
    action = handoff.next_action
    if not isinstance(action, (NextStageAction, ReturnToStageAction)):
        raise RecoveryValidationError(
            f"HANDOFF action {action.action_type} is not a recovery entry"
        )
    required = tuple(action.required_read_paths)
    if not required or len(required) > MAX_RECOVERY_FILES:
        raise RecoveryValidationError(
            "Recovery required_read_paths is empty or over budget"
        )
    if required != tuple(sorted(set(required))):
        raise RecoveryValidationError(
            "Recovery required_read_paths must be unique and deterministically sorted"
        )
    if handoff_relative not in required:
        raise RecoveryValidationError(
            "Recovery set must include its exact machine HANDOFF"
        )
    _reject_nonminimal_recovery_inputs(handoff, required)
    (
        manifest_relative,
        events_relative,
        checkpoint_relative,
        task_root,
    ) = _deterministic_control_paths(handoff_relative, handoff)
    missing_control = {
        manifest_relative,
        events_relative,
        checkpoint_relative,
    } - set(required)
    if missing_control:
        raise RecoveryValidationError(
            f"Recovery set omits deterministic control paths: {sorted(missing_control)}"
        )

    reader = _RecoveryReader(root, required)
    reader.seed(handoff_relative, handoff_bytes)
    manifest_raw = _canonical_json_object(
        reader.read_bytes(manifest_relative), manifest_relative
    )
    manifest = validate_manifest(manifest_raw)
    if reader.read_bytes(manifest_relative) != (
        canonical_json_bytes(manifest.model_dump(mode="json")) + b"\n"
    ):
        raise RecoveryValidationError("Session manifest is not canonical JSON")
    event_rows = _canonical_jsonl(reader.read_bytes(events_relative), events_relative)
    event_summary = validate_event_stream(event_rows, manifest, require_terminal=True)
    if not event_summary.frozen:
        raise RecoveryValidationError(
            "Recovery Event stream is not frozen by handoff.created"
        )
    validate_session_started_file_binding(root, manifest, event_rows[0])
    if len(event_rows) != handoff.source_event_head.sequence_no + 1:
        raise RecoveryValidationError(
            "Frozen Event stream must end immediately after the HANDOFF receipt"
        )
    source_event = event_rows[handoff.source_event_head.sequence_no - 1]
    receipt_event = event_rows[-1]
    if (
        source_event.get("event_id") != handoff.source_event_head.event_id
        or source_event.get("event_hash") != handoff.source_event_head.event_hash
        or source_event.get("event_type") != "checkpoint.created"
        or receipt_event.get("event_type") != "handoff.created"
    ):
        raise RecoveryValidationError(
            "HANDOFF source/receipt Event boundary is inconsistent"
        )

    checkpoint_raw = _canonical_json_object(
        reader.read_bytes(checkpoint_relative), checkpoint_relative
    )
    checkpoint = parse_checkpoint(checkpoint_raw)
    if reader.read_bytes(checkpoint_relative) != (
        canonical_json_bytes(checkpoint.model_dump(mode="json")) + b"\n"
    ):
        raise RecoveryValidationError("Checkpoint is not canonical JSON")
    if checkpoint.checkpoint_id != handoff.checkpoint_id:
        raise RecoveryValidationError("Checkpoint identity does not match HANDOFF")
    _validate_preload_checkpoint_boundary(
        handoff=handoff,
        manifest=manifest,
        event_rows=event_rows,
        source_event=source_event,
        checkpoint=checkpoint,
        checkpoint_relative=checkpoint_relative,
        checkpoint_bytes=reader.read_bytes(checkpoint_relative),
    )
    dag_relative = checkpoint.dag_ref.relative_path
    dag_bytes = reader.read_bytes(dag_relative)
    if sha256_bytes(dag_bytes) != checkpoint.dag_ref.content_hash:
        raise RecoveryValidationError("Checkpoint DAG snapshot file hash mismatch")
    dag = load_dag(reader.path(dag_relative))
    if dag.dag_version != handoff.dag_version:
        raise RecoveryValidationError("DAG version does not match HANDOFF")

    prefix_snapshots = []
    full_snapshots: dict[str, Any] = {}
    for source_head in handoff.source_registry_heads:
        registry_relative = source_head.relative_path
        _validate_registry_path_family(
            handoff=handoff,
            handoff_relative=handoff_relative,
            task_root=task_root,
            source_head=source_head,
        )
        reader.read_bytes(registry_relative)
        registry_path = reader.path(registry_relative)
        expected_run_id = (
            handoff.run_id if source_head.registry_scope == "run" else None
        )
        prefix = snapshot_registry(
            registry_path,
            scope=source_head.registry_scope,
            expected_run_id=expected_run_id,
            head_record_id=source_head.head_record_id,
            repository_root=root,
            relative_path=registry_relative,
        )
        full = snapshot_registry(
            registry_path,
            scope=source_head.registry_scope,
            expected_run_id=expected_run_id,
            repository_root=root,
            relative_path=registry_relative,
        )
        if RegistryHead.model_validate(prefix.head()) != source_head:
            raise RecoveryValidationError(
                "Registry prefix differs from HANDOFF source head"
            )
        prefix_snapshots.append(prefix)
        full_snapshots[registry_relative] = full

    terminal_state = build_current_state(
        manifest,
        event_rows[: checkpoint.event_head.sequence_no],
        tuple(prefix_snapshots),
        reader.path(dag_relative),
    )
    sources = HandoffSources(
        repository_root=root,
        manifest=manifest,
        events=event_rows,
        terminal_state=terminal_state,
        registry_snapshots=tuple(prefix_snapshots),
        dag_path=reader.path(dag_relative),
        checkpoint=checkpoint,
    )
    validate_handoff(handoff, sources)
    expected_paths = _generic_expected_recovery_paths(
        handoff=handoff,
        manifest=manifest,
        checkpoint=checkpoint,
        handoff_relative=handoff_relative,
    )
    if set(required) != expected_paths:
        raise RecoveryValidationError(
            "required_read_paths is not the exact recovery package: "
            f"missing={sorted(expected_paths - set(required))}, "
            f"extra={sorted(set(required) - expected_paths)}"
        )
    recovery_artifacts = _recovery_artifact_references(handoff)

    publication = HandoffPublication(
        handoff=handoff,
        path=reader.path(handoff_relative),
        relative_path=handoff_relative,
        file_hash=sha256_bytes(handoff_bytes),
        size_bytes=len(handoff_bytes),
    )
    target_scope = "run" if handoff.handoff_scope == "run" else "cross_run"
    target_heads = [
        head
        for head in handoff.source_registry_heads
        if head.registry_scope == target_scope
    ]
    registrations = []
    for before_head in target_heads:
        full = full_snapshots[before_head.relative_path]
        matching = [
            entry.wire_record
            for entry in full.entries
            if entry.wire_record.artifact_id == handoff.handoff_artifact_id
        ]
        if len(matching) == 1:
            registrations.append((before_head, full, matching[0]))
    if len(registrations) != 1:
        raise RecoveryValidationError(
            "HANDOFF Artifact must resolve exactly once in its target Registry"
        )
    before_head, full_snapshot, record = registrations[0]
    if not isinstance(record, ArtifactRecordV1_1):
        raise RecoveryValidationError("HANDOFF Registry record must use v1.1")
    after_head = RegistryHead.model_validate(full_snapshot.head())
    registration = HandoffRegistration(
        publication=publication,
        record=record,
        registry_path=full_snapshot.path,
        registry_relative_path=before_head.relative_path,
        before_head=before_head,
        after_head=after_head,
    )
    validate_handoff_registry_record(publication, record, sources)
    validate_handoff_registration(registration, sources)
    validated_receipt = validate_handoff_created_event(
        registration, receipt_event, sources
    )

    _validate_bootstrap_files(reader, manifest)
    _validate_working_tree_manifest(
        reader,
        handoff,
        dag_relative,
        readable_artifact_paths={item.relative_path for item in recovery_artifacts},
    )
    _validate_handoff_artifact_bytes(reader, recovery_artifacts)
    if manifest.environment_ref.dependency_manifest_path is not None:
        dependency_path = manifest.environment_ref.dependency_manifest_path
        if sha256_bytes(reader.read_bytes(dependency_path)) != (
            manifest.environment_ref.dependency_manifest_hash
        ):
            raise RecoveryValidationError("Dependency manifest hash mismatch")
    reader.verify_unchanged()
    if reader.actual_paths != required:
        raise RecoveryValidationError(
            "Recovery actual file set differs from required_read_paths"
        )
    next_stage = (
        action.next_stage
        if isinstance(action, NextStageAction)
        else action.target_stage
    )
    execution_authorized = getattr(action, "execution_authorized", None)
    return HandoffRecoveryResult(
        task_id=handoff.task_id,
        session_id=handoff.session_id,
        attempt_no=handoff.attempt_no,
        run_id=handoff.run_id,
        stage_id=handoff.stage_id,
        status=handoff.status,
        checkpoint_id=handoff.checkpoint_id,
        handoff_version=handoff.handoff_version,
        workflow_version=handoff.workflow_version,
        dag_version=handoff.dag_version,
        next_action_type=action.action_type,
        next_stage=next_stage,
        execution_authorized=execution_authorized,
        source_event_id=handoff.source_event_head.event_id,
        source_event_hash=handoff.source_event_head.event_hash,
        receipt_event_id=validated_receipt.event_id,
        receipt_event_hash=validated_receipt.event_hash,
        required_paths=required,
        actual_paths=reader.actual_paths,
        file_sizes=reader.file_sizes,
        content_hashes=reader.content_hashes,
        total_size_bytes=reader.total_size_bytes,
    )


def _deterministic_control_paths(
    handoff_relative: str,
    handoff: Any,
) -> tuple[str, str, str, PurePosixPath]:
    session_root = PurePosixPath(handoff_relative).parent
    if handoff.handoff_version == "handoff_v2.1.0":
        if (
            session_root.name != handoff.session_id
            or session_root.parent.name != "sessions"
        ):
            raise RecoveryValidationError(
                "HANDOFF v2.1 is outside its deterministic Session directory"
            )
        task_root = session_root.parent.parent
        if (
            task_root.name != handoff.task_id
            or task_root.parent.name != "tasks"
            or task_root.parent.parent.name != "control"
        ):
            raise RecoveryValidationError(
                "HANDOFF v2.1 is outside its deterministic task control directory"
            )
        checkpoint_relative = (
            task_root / "checkpoints" / f"{handoff.checkpoint_id}.json"
        ).as_posix()
    else:
        task_root = session_root
        checkpoint_relative = (
            session_root / "checkpoints" / f"{handoff.checkpoint_id}.json"
        ).as_posix()
    return (
        (session_root / "session_manifest.json").as_posix(),
        (session_root / "events.jsonl").as_posix(),
        checkpoint_relative,
        task_root,
    )


def _validate_preload_checkpoint_boundary(
    *,
    handoff: Any,
    manifest: Any,
    event_rows: Sequence[Mapping[str, Any]],
    source_event: Mapping[str, Any],
    checkpoint: Any,
    checkpoint_relative: str,
    checkpoint_bytes: bytes,
) -> None:
    for field in (
        "project_version",
        "storage_version",
        "release_id",
        "task_id",
        "session_id",
        "attempt_no",
        "run_id",
        "stage_id",
        "workflow_version",
        "prompt_version",
        "agent_profile_version",
        "agent_runtime_version",
    ):
        if getattr(checkpoint, field) != getattr(manifest, field):
            raise RecoveryValidationError(
                f"Checkpoint/manifest identity mismatch before DAG read: {field}"
            )
        if getattr(handoff, field) != getattr(manifest, field):
            raise RecoveryValidationError(
                f"HANDOFF/manifest identity mismatch before DAG read: {field}"
            )
    source_head = {
        key: source_event.get(key)
        for key in ("event_id", "sequence_no", "event_hash", "occurred_at")
    }
    if handoff.source_event_head.model_dump(mode="json") != source_head:
        raise RecoveryValidationError(
            "HANDOFF source head is not the checkpoint.created Event"
        )
    event_head = checkpoint.event_head.model_dump(mode="json")
    if source_head["sequence_no"] != event_head["sequence_no"] + 1:
        raise RecoveryValidationError("Checkpoint boundary Events are not contiguous")
    prior_event = event_rows[event_head["sequence_no"] - 1]
    if event_head != {
        key: prior_event.get(key)
        for key in ("event_id", "sequence_no", "event_hash", "occurred_at")
    }:
        raise RecoveryValidationError(
            "Checkpoint event_head does not bind the prior terminal Event"
        )
    payload = source_event.get("payload")
    path_refs = source_event.get("path_refs")
    checkpoint_file_hash = sha256_bytes(checkpoint_bytes)
    if (
        not isinstance(payload, Mapping)
        or payload.get("checkpoint_id") != checkpoint.checkpoint_id
        or payload.get("checkpoint_hash") != checkpoint.checkpoint_hash
        or payload.get("checkpoint_path") != checkpoint_relative
        or not isinstance(path_refs, list)
        or len(path_refs) != 1
        or path_refs[0].get("relative_path") != checkpoint_relative
        or path_refs[0].get("content_hash_algorithm") != "sha256"
        or path_refs[0].get("content_hash") != checkpoint_file_hash
    ):
        raise RecoveryValidationError(
            "checkpoint.created does not bind the deterministic checkpoint bytes"
        )
    if checkpoint.dag_ref.dag_version != handoff.dag_version:
        raise RecoveryValidationError("Checkpoint DAG version does not match HANDOFF")
    if checkpoint.registry_heads != handoff.source_registry_heads:
        raise RecoveryValidationError("Checkpoint Registry heads do not match HANDOFF")


def _validate_registry_path_family(
    *,
    handoff: Any,
    handoff_relative: str,
    task_root: PurePosixPath,
    source_head: Any,
) -> None:
    if handoff.handoff_version != "handoff_v2.1.0":
        return
    observed = PurePosixPath(source_head.relative_path)
    if source_head.registry_scope == "run":
        run_root = task_root.parent.parent.parent
        if (
            len(run_root.parts) != 3
            or run_root.parts[:2] != ("runs", "V02")
            or not PurePosixPath(handoff_relative).is_relative_to(run_root)
        ):
            raise RecoveryValidationError(
                "Run HANDOFF is outside runs/V02/<physical-run-directory>"
            )
        expected = run_root / "artifact_registry.jsonl"
    else:
        expected = PurePosixPath("registry/V02/artifacts.jsonl")
    if observed != expected:
        raise RecoveryValidationError(
            "Registry source path is outside its canonical scope path family: "
            f"expected={expected.as_posix()}, observed={observed.as_posix()}"
        )


def _reject_nonminimal_recovery_inputs(handoff: Any, paths: Sequence[str]) -> None:
    forbidden_extensions = {
        ".mp4",
        ".mkv",
        ".webm",
        ".mov",
        ".wav",
        ".mp3",
        ".flac",
        ".m4a",
    }
    for relative in paths:
        path = validate_relative_path(relative)
        lowered = path.as_posix().lower()
        lower_parts = {part.lower() for part in path.parts}
        if path.suffix.lower() in forbidden_extensions:
            raise RecoveryValidationError(
                f"Recovery package must not contain media: {relative}"
            )
        if path.name.lower() in {"current_state.json", "handoff.md"}:
            raise RecoveryValidationError(
                f"Rebuildable projection cannot be a recovery input: {relative}"
            )
        if lower_parts.intersection({"transcript", "transcripts", "chat", "chats"}):
            raise RecoveryValidationError(
                f"Recovery package contains non-minimal transcript/chat material: {relative}"
            )
        if "/logs/" in f"/{lowered}/" and path.name != "events.jsonl":
            raise RecoveryValidationError(
                f"Recovery package contains an undeclared full log: {relative}"
            )


def _recovery_artifact_references(handoff: Any) -> tuple[Any, ...]:
    """Return payloads authorized for byte reads at this recovery boundary.

    HANDOFF v2.1 keeps every business Artifact as Registry-verifiable metadata,
    but authorizes payload reads only for the exact IDs named by next_action.
    The v2.0 public proof retains its fixed all-payload nine-file behavior.
    """

    if handoff.handoff_version != "handoff_v2.1.0":
        return (
            *handoff.input_artifacts,
            *handoff.output_artifacts,
            *handoff.invalidated_artifacts,
            *handoff.actual_read_set,
            *handoff.actual_write_set,
        )
    action = handoff.next_action
    if not isinstance(action, (NextStageAction, ReturnToStageAction)):
        return ()
    candidates = (*handoff.input_artifacts, *handoff.output_artifacts)
    selected: list[Any] = []
    for artifact_id in sorted(action.required_input_artifact_ids):
        matches = [item for item in candidates if item.artifact_id == artifact_id]
        if len(matches) != 1:
            raise RecoveryValidationError(
                f"Recovery required Artifact must resolve exactly once in HANDOFF metadata: {artifact_id}"
            )
        selected.append(matches[0])
    return tuple(selected)


def _generic_expected_recovery_paths(
    *,
    handoff: Any,
    manifest: Any,
    checkpoint: Any,
    handoff_relative: str,
) -> set[str]:
    manifest_relative, events_relative, checkpoint_relative, _ = (
        _deterministic_control_paths(handoff_relative, handoff)
    )
    paths = {
        manifest_relative,
        events_relative,
        handoff_relative,
        checkpoint_relative,
        checkpoint.dag_ref.relative_path,
        *(head.relative_path for head in handoff.source_registry_heads),
    }
    for reference in manifest.bootstrap_refs:
        relative = getattr(reference, "relative_path", None)
        if relative is not None:
            paths.add(validate_relative_path(relative).as_posix())
    dependency_path = manifest.environment_ref.dependency_manifest_path
    if dependency_path is not None:
        paths.add(validate_relative_path(dependency_path).as_posix())
    paths.update(
        reference.relative_path for reference in _recovery_artifact_references(handoff)
    )
    return paths


def _validate_recovery_bundle(bundle: Path) -> RecoveryResult:
    root, _ = _recovery_root(bundle)
    bundle_relative = RECOVERY_BUNDLE_RELATIVE.as_posix()
    handoff_relative = (RECOVERY_BUNDLE_RELATIVE / "handoff.json").as_posix()
    handoff_path = _safe_required_file(root, handoff_relative)
    try:
        handoff_bytes = handoff_path.read_bytes()
    except OSError as exc:
        raise RecoveryValidationError(
            "Cannot read required recovery file: handoff.json"
        ) from exc
    handoff_raw = _canonical_json_object(handoff_bytes, "handoff.json")
    handoff = parse_handoff(handoff_raw)
    if handoff_bytes != canonical_json_bytes(handoff.model_dump(mode="json")) + b"\n":
        raise RecoveryValidationError(
            "handoff.json is not canonical JSON with one final LF"
        )

    action = handoff.next_action
    if not isinstance(action, (NextStageAction, ReturnToStageAction)):
        raise RecoveryValidationError(
            f"Unsupported recovery action without required_read_paths: {action.action_type}"
        )
    required = tuple(action.required_read_paths)
    if not required:
        raise RecoveryValidationError(
            "Recovery action has an empty required_read_paths"
        )
    if len(required) > MAX_RECOVERY_FILES:
        raise RecoveryValidationError(
            f"Recovery package exceeds {MAX_RECOVERY_FILES} files"
        )
    if len(required) != len(set(required)):
        raise RecoveryValidationError(
            "Recovery required_read_paths contains duplicates"
        )
    if required != tuple(sorted(required)):
        raise RecoveryValidationError(
            "Recovery required_read_paths must be deterministically sorted"
        )
    if len(required) != 9:
        raise RecoveryValidationError(
            f"Stage 4 synthetic recovery requires exactly 9 files; observed {len(required)}"
        )
    if required != EXPECTED_RECOVERY_PATHS:
        missing = sorted(set(EXPECTED_RECOVERY_PATHS) - set(required))
        extra = sorted(set(required) - set(EXPECTED_RECOVERY_PATHS))
        raise RecoveryValidationError(
            "Recovery required_read_paths must exactly match the frozen nine-file package: "
            f"missing={missing}, extra={extra}"
        )
    if handoff_relative not in required:
        raise RecoveryValidationError(
            "Recovery package does not include its machine handoff.json"
        )
    prefix = f"{bundle_relative}/"
    for relative in required:
        validated = validate_relative_path(relative).as_posix()
        if validated != relative or not relative.startswith(prefix):
            raise RecoveryValidationError(
                f"Recovery path is outside the fixed synthetic bundle: {relative}"
            )
        if relative.endswith("/current_state.json") or relative.endswith("/HANDOFF.md"):
            raise RecoveryValidationError(
                "Rebuildable projections cannot be recovery inputs"
            )
        if relative in {action.workflow_reference, action.prompt_reference}:
            raise RecoveryValidationError(
                "Workflow and Prompt files are not recovery inputs"
            )

    reader = _RecoveryReader(root, required)
    reader.seed(handoff_relative, handoff_bytes)
    for relative in required:
        reader.read_bytes(relative)

    manifest_relative = (RECOVERY_BUNDLE_RELATIVE / "session_manifest.json").as_posix()
    events_relative = (RECOVERY_BUNDLE_RELATIVE / "events.jsonl").as_posix()
    registry_relative = (
        RECOVERY_BUNDLE_RELATIVE / "artifact_registry.jsonl"
    ).as_posix()
    checkpoint_relative = (
        RECOVERY_BUNDLE_RELATIVE / "checkpoints" / f"{handoff.checkpoint_id}.json"
    ).as_posix()

    manifest_raw = _canonical_json_object(
        reader.read_bytes(manifest_relative), "session_manifest.json"
    )
    manifest = validate_manifest(manifest_raw)
    expected_manifest_bytes = (
        canonical_json_bytes(manifest.model_dump(mode="json")) + b"\n"
    )
    if reader.read_bytes(manifest_relative) != expected_manifest_bytes:
        raise RecoveryValidationError(
            "session_manifest.json is not canonical JSON with one final LF"
        )

    event_rows = _canonical_jsonl(reader.read_bytes(events_relative), "events.jsonl")
    event_summary = validate_event_stream(event_rows, manifest, require_terminal=True)
    if not event_summary.frozen:
        raise RecoveryValidationError(
            "Recovery event stream is not frozen by handoff.created"
        )
    if (
        handoff.source_event_head.sequence_no != EXPECTED_SOURCE_EVENT_SEQUENCE
        or len(event_rows) != EXPECTED_EVENT_COUNT
    ):
        raise RecoveryValidationError(
            "Stage 4 recovery requires the frozen event8 source plus event9 receipt shape"
        )
    source_event = event_rows[handoff.source_event_head.sequence_no - 1]
    receipt_event = event_rows[-1]
    if (
        source_event["event_id"] != handoff.source_event_head.event_id
        or source_event["event_hash"] != handoff.source_event_head.event_hash
        or source_event.get("event_type") != "checkpoint.created"
        or receipt_event.get("event_type") != "handoff.created"
        or receipt_event.get("sequence_no") != EXPECTED_RECEIPT_EVENT_SEQUENCE
    ):
        raise RecoveryValidationError(
            "HANDOFF source/receipt event boundary is inconsistent"
        )
    started_payload = event_rows[0].get("payload")
    started_path_refs = event_rows[0].get("path_refs")
    if (
        not isinstance(started_payload, Mapping)
        or started_payload.get("manifest_path") != manifest_relative
        or not isinstance(started_path_refs, list)
        or len(started_path_refs) != 1
        or not isinstance(started_path_refs[0], Mapping)
        or started_path_refs[0].get("relative_path") != manifest_relative
    ):
        raise RecoveryValidationError(
            "session.started must bind the fixed recovery session_manifest.json"
        )
    validate_session_started_file_binding(root, manifest, event_rows[0])

    checkpoint_raw = _canonical_json_object(
        reader.read_bytes(checkpoint_relative),
        f"{handoff.checkpoint_id}.json",
    )
    checkpoint = parse_checkpoint(checkpoint_raw)
    if reader.read_bytes(checkpoint_relative) != (
        canonical_json_bytes(checkpoint.model_dump(mode="json")) + b"\n"
    ):
        raise RecoveryValidationError(
            "Checkpoint is not canonical JSON with one final LF"
        )
    if checkpoint.checkpoint_id != handoff.checkpoint_id:
        raise RecoveryValidationError("Checkpoint identity does not match HANDOFF")

    dag_relative = checkpoint.dag_ref.relative_path
    dag_path = reader.path(dag_relative)
    dag = load_dag(dag_path)
    if dag.dag_version != handoff.dag_version:
        raise RecoveryValidationError("DAG version does not match HANDOFF")

    if len(handoff.source_registry_heads) != 1:
        raise RecoveryValidationError(
            "Stage 4 synthetic recovery requires one Registry source head"
        )
    source_head = handoff.source_registry_heads[0]
    if source_head.relative_path != registry_relative:
        raise RecoveryValidationError(
            "HANDOFF Registry source path is not the fixed recovery Registry"
        )
    expected_run_id = handoff.run_id if source_head.registry_scope == "run" else None
    registry_path = reader.path(registry_relative)
    registry_rows = _canonical_jsonl(
        reader.read_bytes(registry_relative), "artifact_registry.jsonl"
    )
    prefix_snapshot = snapshot_registry(
        registry_path,
        scope=source_head.registry_scope,
        expected_run_id=expected_run_id,
        head_record_id=source_head.head_record_id,
        repository_root=root,
        relative_path=registry_relative,
    )
    full_snapshot = snapshot_registry(
        registry_path,
        scope=source_head.registry_scope,
        expected_run_id=expected_run_id,
        repository_root=root,
        relative_path=registry_relative,
    )
    before_head = RegistryHead.model_validate(prefix_snapshot.head())
    after_head = RegistryHead.model_validate(full_snapshot.head())
    if before_head != source_head:
        raise RecoveryValidationError(
            "Registry prefix differs from HANDOFF source head"
        )
    if (
        len(prefix_snapshot.entries) != EXPECTED_REGISTRY_PREFIX_COUNT
        or len(full_snapshot.entries) != EXPECTED_REGISTRY_FULL_COUNT
        or len(full_snapshot.entries) != len(prefix_snapshot.entries) + 1
        or prefix_snapshot.entries[-1].wire_record.record_id
        != SYNTHETIC_SOURCE_RECORD_ID
        or full_snapshot.entries[-1].wire_record.record_id
        != SYNTHETIC_HANDOFF_RECORD_ID
    ):
        raise RecoveryValidationError(
            "Stage 4 recovery requires the frozen Registry record2 prefix plus record3 receipt shape"
        )
    if len(registry_rows) != len(full_snapshot.entries):
        raise RecoveryValidationError(
            "Registry byte rows do not match the validated full snapshot"
        )

    terminal_state = build_current_state(
        manifest,
        event_rows[: checkpoint.event_head.sequence_no],
        (prefix_snapshot,),
        dag_path,
    )
    sources = HandoffSources(
        repository_root=root,
        manifest=manifest,
        events=event_rows,
        terminal_state=terminal_state,
        registry_snapshots=(prefix_snapshot,),
        dag_path=dag_path,
        checkpoint=checkpoint,
    )
    validate_handoff(handoff, sources)

    publication = HandoffPublication(
        handoff=handoff,
        path=reader.path(handoff_relative),
        relative_path=handoff_relative,
        file_hash=sha256_bytes(handoff_bytes),
        size_bytes=len(handoff_bytes),
    )
    record = full_snapshot.entries[-1].wire_record
    if not isinstance(record, ArtifactRecordV1_1):
        raise RecoveryValidationError("HANDOFF receipt must be a Registry v1.1 record")
    validate_handoff_registry_record(publication, record, sources)
    registration = HandoffRegistration(
        publication=publication,
        record=record,
        registry_path=registry_path,
        registry_relative_path=registry_relative,
        before_head=before_head,
        after_head=after_head,
    )
    validate_handoff_registration(registration, sources)
    validated_receipt = validate_handoff_created_event(
        registration, receipt_event, sources
    )

    legacy_artifacts = _recovery_artifact_references(handoff)
    _validate_working_tree_manifest(
        reader,
        handoff,
        checkpoint.dag_ref.relative_path,
        readable_artifact_paths={item.relative_path for item in legacy_artifacts},
    )
    _validate_handoff_artifact_bytes(reader, legacy_artifacts)

    final_state = build_current_state(
        manifest,
        event_rows,
        (full_snapshot,),
        dag_path,
    )
    validate_current_state(final_state)
    for field, expected in (
        ("task_id", handoff.task_id),
        ("session_id", handoff.session_id),
        ("run_id", handoff.run_id),
        ("stage_id", handoff.stage_id),
        ("status", handoff.status),
        ("workflow_version", handoff.workflow_version),
        ("dag_version", handoff.dag_version),
    ):
        if final_state.get(field) != expected:
            raise RecoveryValidationError(
                f"Rebuilt state identity mismatch for {field}"
            )

    state_bytes = current_state_bytes(final_state)
    markdown_bytes = render_handoff_markdown(handoff)
    reader.verify_unchanged()
    if reader.actual_paths != required:
        raise RecoveryValidationError(
            "Recovery actual file set does not equal required_read_paths: "
            f"required={list(required)}, actual={list(reader.actual_paths)}"
        )

    next_stage = (
        action.next_stage
        if isinstance(action, NextStageAction)
        else action.target_stage
    )
    return RecoveryResult(
        task_id=handoff.task_id,
        session_id=handoff.session_id,
        attempt_no=handoff.attempt_no,
        run_id=handoff.run_id,
        stage_id=handoff.stage_id,
        status=handoff.status,
        checkpoint_id=handoff.checkpoint_id,
        next_action_type=action.action_type,
        next_stage=next_stage,
        source_event_id=handoff.source_event_head.event_id,
        source_event_hash=handoff.source_event_head.event_hash,
        receipt_event_id=validated_receipt.event_id,
        receipt_event_hash=validated_receipt.event_hash,
        registry_prefix_record_count=len(prefix_snapshot.entries),
        registry_prefix_hash=prefix_snapshot.content_hash,
        registry_full_record_count=len(full_snapshot.entries),
        registry_full_hash=full_snapshot.content_hash,
        state_hash=str(final_state["state_hash"]),
        state_bytes_sha256=sha256_bytes(state_bytes),
        markdown_sha256=sha256_bytes(markdown_bytes),
        required_paths=required,
        actual_paths=reader.actual_paths,
        file_sizes=reader.file_sizes,
        content_hashes=reader.content_hashes,
        total_size_bytes=reader.total_size_bytes,
    )


def _recovery_root(bundle: Path) -> tuple[Path, Path]:
    candidate = bundle if bundle.is_absolute() else Path.cwd() / bundle
    for component in (candidate, candidate.parent, candidate.parent.parent):
        if component.is_symlink():
            raise RecoveryValidationError(
                f"Recovery bundle uses a symlink: {component.name}"
            )
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise RecoveryValidationError("Recovery bundle directory is missing") from exc
    if not resolved.is_dir():
        raise RecoveryValidationError("Recovery bundle path is not a directory")
    if (
        resolved.name != RECOVERY_BUNDLE_RELATIVE.name
        or resolved.parent.name != RECOVERY_BUNDLE_RELATIVE.parent.name
        or resolved.parent.parent.name != RECOVERY_BUNDLE_RELATIVE.parent.parent.name
    ):
        raise RecoveryValidationError(
            f"Recovery bundle must end with {RECOVERY_BUNDLE_RELATIVE.as_posix()}"
        )
    root = resolved.parents[2]
    if resolved.relative_to(root).as_posix() != RECOVERY_BUNDLE_RELATIVE.as_posix():
        raise RecoveryValidationError(
            "Recovery bundle relative prefix is not canonical"
        )
    return root, resolved


def _safe_required_file(root: Path, relative: str) -> Path:
    path = validate_relative_path(relative)
    current = root
    for part in path.parts:
        current = current / part
        if current.is_symlink():
            raise RecoveryValidationError(f"Recovery path uses a symlink: {relative}")
    try:
        resolved = current.resolve(strict=True)
    except OSError as exc:
        raise RecoveryValidationError(
            f"Required recovery file is missing: {relative}"
        ) from exc
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise RecoveryValidationError(
            f"Recovery path is outside the root or not a file: {relative}"
        )
    return resolved


def _canonical_json_object(data: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise RecoveryValidationError(f"Invalid UTF-8 JSON in {label}") from exc
    if not isinstance(value, dict):
        raise RecoveryValidationError(f"{label} must contain one JSON object")
    return value


def _canonical_jsonl(data: bytes, label: str) -> list[dict[str, Any]]:
    if not data or not data.endswith(b"\n"):
        raise RecoveryValidationError(f"{label} must end with one LF")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(data.splitlines(keepends=True), start=1):
        if not line.endswith(b"\n") or line == b"\n":
            raise RecoveryValidationError(
                f"Invalid JSONL line at {label}:{line_number}"
            )
        raw_line = line[:-1]
        value = _canonical_json_object(raw_line, f"{label}:{line_number}")
        if canonical_json_bytes(value) != raw_line:
            raise RecoveryValidationError(
                f"Non-canonical JSONL line at {label}:{line_number}"
            )
        rows.append(value)
    return rows


def _validate_bootstrap_files(reader: _RecoveryReader, manifest: Any) -> None:
    for reference in manifest.bootstrap_refs:
        relative = getattr(reference, "relative_path", None)
        if relative is None:
            continue
        data = reader.read_bytes(relative)
        if sha256_bytes(data) != reference.content_hash:
            raise RecoveryValidationError(f"Bootstrap file hash mismatch: {relative}")


def _validate_working_tree_manifest(
    reader: _RecoveryReader,
    handoff: Any,
    dag_relative: str,
    *,
    readable_artifact_paths: set[str],
) -> None:
    relative = handoff.code_ref.working_tree_manifest_path
    if relative is None:
        raise RecoveryValidationError(
            "Dirty recovery code_ref lacks a working-tree manifest path"
        )
    data = reader.read_bytes(relative)
    if sha256_bytes(data) != handoff.code_ref.working_tree_manifest_hash:
        raise RecoveryValidationError("Working-tree manifest file hash mismatch")
    raw = _canonical_json_object(data, "working_tree_manifest.json")
    if set(raw) != {"working_tree_manifest_version", "paths"}:
        raise RecoveryValidationError("Working-tree manifest has unexpected fields")
    if raw["working_tree_manifest_version"] != "working_tree_manifest_v1.0.0":
        raise RecoveryValidationError("Unsupported working-tree manifest version")
    entries = raw["paths"]
    if (
        not isinstance(entries, list)
        or not entries
        or len(entries) > MAX_RECOVERY_FILES
    ):
        raise RecoveryValidationError(
            "Working-tree manifest paths are empty or over budget"
        )
    expected_paths = {
        dag_relative,
        *(item.relative_path for item in handoff.input_artifacts),
        *(item.relative_path for item in handoff.output_artifacts),
    }
    artifact_hashes = {
        item.relative_path: item.content_hash
        for item in (*handoff.input_artifacts, *handoff.output_artifacts)
    }
    observed_paths: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Mapping) or set(entry) != {
            "path",
            "size_bytes",
            "sha256",
        }:
            raise RecoveryValidationError("Invalid working-tree manifest path entry")
        entry_relative = validate_relative_path(str(entry["path"])).as_posix()
        if entry_relative in observed_paths:
            raise RecoveryValidationError("Duplicate working-tree manifest path")
        observed_paths.add(entry_relative)
        size_bytes = entry["size_bytes"]
        if (
            isinstance(size_bytes, bool)
            or not isinstance(size_bytes, int)
            or size_bytes < 0
        ):
            raise RecoveryValidationError("Invalid working-tree manifest size_bytes")
        if entry_relative == dag_relative or entry_relative in readable_artifact_paths:
            content = reader.read_bytes(entry_relative)
            if size_bytes != len(content) or entry["sha256"] != sha256_bytes(content):
                raise RecoveryValidationError(
                    f"Working-tree manifest content binding mismatch: {entry_relative}"
                )
        elif entry["sha256"] != artifact_hashes.get(entry_relative):
            raise RecoveryValidationError(
                f"Working-tree manifest metadata binding mismatch: {entry_relative}"
            )
    if observed_paths != expected_paths:
        raise RecoveryValidationError(
            "Working-tree manifest does not bind exactly the DAG and input/output Artifacts"
        )


def _validate_handoff_artifact_bytes(
    reader: _RecoveryReader,
    references: Sequence[Any],
) -> None:
    for reference in references:
        data = reader.read_bytes(reference.relative_path)
        if reference.content_hash_algorithm != "sha256":
            raise RecoveryValidationError(
                "Recovery Artifact uses an unsupported hash algorithm"
            )
        if sha256_bytes(data) != reference.content_hash:
            raise RecoveryValidationError(
                f"Recovery Artifact content hash mismatch: {reference.relative_path}"
            )
