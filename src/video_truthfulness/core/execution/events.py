"""Validation and append-only publication for execution event streams."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Sequence

from video_truthfulness.core.artifacts.models import new_typed_ulid
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_file,
    verify_embedded_hash,
)
from video_truthfulness.core.execution.io import (
    ContractIOError,
    _publish_temp,
    read_json,
    read_jsonl,
)
from video_truthfulness.core.execution.models import (
    EventChainError,
    ExecutionEvent,
    ExecutionHashError,
    ExecutionSchemaError,
    ScopeViolationError,
    SensitiveMaterialError,
    SessionFrozenError,
    SessionManifest,
    parse_execution_event,
    parse_session_manifest,
)


TERMINAL_EVENT_TYPES = {
    "task.completed": "COMPLETED",
    "task.failed": "FAILED",
    "task.waiting_for_human": "WAITING_FOR_HUMAN",
    "task.blocked_by_input": "BLOCKED_BY_INPUT",
    "task.skipped_by_gate": "SKIPPED_BY_GATE",
}
POST_TERMINAL_EVENT_TYPES = {"checkpoint.created", "handoff.created"}
READ_EVENT_TYPES = {"artifact.read"}
WRITE_EVENT_TYPES = {"artifact.written", "checkpoint.created", "handoff.created"}
VALIDATION_EVENT_TYPES = {"artifact.validated", "artifact.invalidated"}
SUPPORTED_EVENT_SCHEMA_VERSIONS = {
    "execution_event_v1.0.0",
    "execution_event_v1.0.1",
}

_DRIVE_PATH = re.compile(r"^[A-Za-z]:")
_PRIVATE_POSIX_PATH = re.compile(r"/(?:home|Users|private|root)/", re.IGNORECASE)
_PRIVATE_WINDOWS_PATH = re.compile(r"[A-Za-z]:[\\/]")
_SENSITIVE_VALUE_MARKERS = (
    re.compile(r"\bbearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE),
    re.compile(r"\bauthorization\s*:", re.IGNORECASE),
    re.compile(r"\bapi[ _-]?key\s*[:=]", re.IGNORECASE),
    re.compile(r"\bpassword\s*[:=]", re.IGNORECASE),
    re.compile(r"\bcookie\s*[:=]", re.IGNORECASE),
    re.compile(r"\btoken\s*[:=]", re.IGNORECASE),
)
_SENSITIVE_KEYS = {
    "api_key",
    "authorization",
    "authorization_header",
    "bearer",
    "browser_profile",
    "cookie",
    "cookies",
    "password",
    "secret",
    "token",
}
_BROAD_RECURSIVE_ROOTS = {".", "data", "runs", "runtime"}
_CREDENTIAL_ROOTS = {
    "browser-profile",
    "browser-profiles",
    "browser_profile",
    "browser_profiles",
    "cookie",
    "cookie-catch",
    "cookies",
    "credential",
    "credentials",
    "secret",
    "secrets",
    "token",
    "tokens",
}


@dataclass(frozen=True)
class EventLogSummary:
    event_count: int
    head_event_id: str | None
    head_event_hash: str | None
    head_occurred_at: str | None
    terminal_event_type: str | None
    terminal_state: str | None
    checkpoint_id: str | None
    frozen: bool


def _raw(value: Mapping[str, Any] | SessionManifest | ExecutionEvent) -> dict[str, Any]:
    if isinstance(value, (SessionManifest, ExecutionEvent)):
        return value.model_dump(mode="json")
    return dict(value)


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _scope_raw(entry: Any) -> Mapping[str, Any]:
    if hasattr(entry, "model_dump"):
        return entry.model_dump(mode="json")
    return entry


def reject_sensitive_material(value: Any, *, location: str = "event") -> None:
    """Recursively reject credential material, private absolute paths, and oversized text."""

    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = _normalize_key(str(key))
            if normalized in _SENSITIVE_KEYS and normalized != "credential_ref":
                raise SensitiveMaterialError(
                    f"Sensitive key rejected at {location}.{key}"
                )
            reject_sensitive_material(child, location=f"{location}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            reject_sensitive_material(child, location=f"{location}[{index}]")
        return
    if not isinstance(value, str):
        return
    if len(value) > 4096:
        raise SensitiveMaterialError(f"Oversized text rejected at {location}")
    if _PRIVATE_WINDOWS_PATH.search(value) or _PRIVATE_POSIX_PATH.search(value):
        raise SensitiveMaterialError(f"Private absolute path rejected at {location}")
    for marker in _SENSITIVE_VALUE_MARKERS:
        if marker.search(value):
            raise SensitiveMaterialError(
                f"Sensitive value marker rejected at {location}"
            )


def validate_relative_path(value: str) -> PurePosixPath:
    if not value or value in {".", ".."}:
        raise ScopeViolationError(f"Invalid relative path: {value!r}")
    if "\\" in value or value.startswith(("/", "~")) or _DRIVE_PATH.match(value):
        raise ScopeViolationError(f"Absolute or non-POSIX path rejected: {value}")
    if any(marker in value for marker in ("*", "?", "$", "%")):
        raise ScopeViolationError(f"Wildcard or unresolved variable rejected: {value}")
    path = PurePosixPath(value)
    if any(part in {"", ".", "..", "latest"} for part in path.parts):
        raise ScopeViolationError(f"Escaping or implicit-latest path rejected: {value}")
    if path.parts[0].lower() in _CREDENTIAL_ROOTS:
        raise ScopeViolationError(f"Credential material path rejected: {value}")
    return path


def _validate_scope_entry(entry: Mapping[str, Any]) -> None:
    entry = _scope_raw(entry)
    scope_type = entry.get("scope_type")
    if scope_type == "artifact":
        if not isinstance(entry.get("artifact_id"), str):
            raise ScopeViolationError("Artifact scope requires an exact artifact_id")
        return
    if scope_type not in {"path", "path_prefix"}:
        raise ScopeViolationError(f"Unsupported scope_type: {scope_type}")
    path = validate_relative_path(str(entry.get("relative_path", "")))
    if scope_type == "path_prefix":
        recursive = entry.get("recursive")
        if not isinstance(recursive, bool):
            raise ScopeViolationError("path_prefix scope requires recursive=true/false")
        if recursive and path.as_posix() in _BROAD_RECURSIVE_ROOTS:
            raise ScopeViolationError(
                f"Broad recursive scope rejected: {path.as_posix()}"
            )
        if recursive and path.parts[0] in {"runs", "data"} and len(path.parts) < 3:
            raise ScopeViolationError(
                f"Version-wide recursive scope rejected: {path.as_posix()}"
            )
        if recursive and path.parts[0] == "runtime":
            valid_runtime_task = (
                len(path.parts) >= 5
                and path.parts[1:4] == ("V02", "execution", "tasks")
                and re.fullmatch(r"task_[0-9a-hjkmnp-tv-z]{26}", path.parts[4])
                is not None
            )
            if not valid_runtime_task:
                raise ScopeViolationError(
                    f"Unbounded runtime scope rejected: {path.as_posix()}"
                )


def validate_manifest(manifest: Mapping[str, Any] | SessionManifest) -> SessionManifest:
    raw = _raw(manifest)
    reject_sensitive_material(raw, location="manifest")
    try:
        verify_embedded_hash(raw, "manifest_hash")
    except ValueError as exc:
        raise ExecutionHashError(str(exc)) from exc
    model = parse_session_manifest(raw)
    _declared_event_schema_version(model)
    for entry in (*model.declared_read_set, *model.declared_write_set):
        _validate_scope_entry(entry)
    for reference in model.bootstrap_refs:
        raw_reference = reference.model_dump(mode="json")
        if "relative_path" in raw_reference:
            validate_relative_path(raw_reference["relative_path"])
    if model.code_ref.working_tree_manifest_path is not None:
        validate_relative_path(model.code_ref.working_tree_manifest_path)
    if model.environment_ref.dependency_manifest_path is not None:
        validate_relative_path(model.environment_ref.dependency_manifest_path)
    return model


def _declared_event_schema_version(manifest: SessionManifest) -> str:
    declared = [
        version
        for version in manifest.schema_versions
        if version.startswith("execution_event_v")
    ]
    if len(declared) != 1 or declared[0] not in SUPPORTED_EVENT_SCHEMA_VERSIONS:
        raise ExecutionSchemaError(
            "Session manifest must declare exactly one supported execution Event schema version"
        )
    return declared[0]


def validate_session_started_file_binding(
    repository_root: Path,
    manifest: Mapping[str, Any] | SessionManifest,
    event: Mapping[str, Any] | ExecutionEvent,
) -> ExecutionEvent:
    """Bind session.started to both manifest semantics and canonical bytes on disk."""

    root = repository_root.resolve()
    if not root.is_dir():
        raise EventChainError(f"Repository root is not a directory: {root}")
    manifest_raw = _raw(manifest)
    manifest_model = validate_manifest(manifest_raw)
    semantic_hash = embedded_hash(manifest_raw, "manifest_hash")
    event_raw = _raw(event)
    validate_event_stream([event_raw], manifest_model)
    event_model = parse_execution_event(event_raw)
    if event_model.event_type != "session.started":
        raise EventChainError(
            "Session file binding requires event_type=session.started"
        )
    if event_model.artifact_refs or len(event_model.path_refs) != 1:
        raise EventChainError(
            "session.started requires zero Artifact refs and exactly one manifest path ref"
        )
    payload = event_model.payload
    if payload.get("manifest_hash") != semantic_hash:
        raise ExecutionHashError(
            "session.started payload manifest_hash does not match the manifest semantic hash"
        )
    relative = validate_relative_path(str(payload.get("manifest_path", ""))).as_posix()
    path_ref = event_model.path_refs[0]
    if path_ref.relative_path != relative:
        raise EventChainError("session.started payload/path reference mismatch")
    if PurePosixPath(relative).name != "session_manifest.json":
        raise EventChainError(
            "session.started must bind a file named session_manifest.json"
        )
    manifest_path = (root / relative).resolve()
    if not manifest_path.is_relative_to(root) or not manifest_path.is_file():
        raise EventChainError(
            f"session.started manifest file is missing or outside repository root: {relative}"
        )
    try:
        file_raw = read_json(manifest_path)
        file_bytes = manifest_path.read_bytes()
    except (ContractIOError, OSError) as exc:
        raise EventChainError(
            f"Cannot read session manifest file: {relative}: {exc}"
        ) from exc
    if file_raw != manifest_raw:
        raise EventChainError(
            "session.started manifest file object differs from the bound manifest"
        )
    expected_bytes = canonical_json_bytes(manifest_raw) + b"\n"
    if file_bytes != expected_bytes:
        raise EventChainError(
            "session.started manifest file is not canonical JSON plus one LF"
        )
    physical_hash = sha256_file(manifest_path)
    if path_ref.content_hash != physical_hash:
        raise ExecutionHashError(
            "session.started path_ref content_hash does not match the physical manifest file"
        )
    return event_model


def _path_matches_scope(target: PurePosixPath, entry: Mapping[str, Any]) -> bool:
    entry = _scope_raw(entry)
    scope_type = entry.get("scope_type")
    if scope_type == "path":
        return target == validate_relative_path(str(entry["relative_path"]))
    if scope_type != "path_prefix":
        return False
    prefix = validate_relative_path(str(entry["relative_path"]))
    if target == prefix:
        return True
    if entry.get("recursive") is True:
        return target.is_relative_to(prefix)
    return target.parent == prefix


def _artifact_matches_scope(
    artifact_id: str, entries: Iterable[Mapping[str, Any]]
) -> bool:
    return any(
        _scope_raw(entry).get("scope_type") == "artifact"
        and _scope_raw(entry).get("artifact_id") == artifact_id
        for entry in entries
    )


def _validate_event_scope(event: ExecutionEvent, manifest: SessionManifest) -> None:
    for reference in event.path_refs:
        validate_relative_path(reference.relative_path)
    for reference in event.artifact_refs:
        validate_relative_path(reference.relative_path)
    if event.event_type in READ_EVENT_TYPES:
        entries = manifest.declared_read_set
    elif event.event_type in WRITE_EVENT_TYPES:
        entries = manifest.declared_write_set
    elif event.event_type in VALIDATION_EVENT_TYPES:
        entries = [*manifest.declared_read_set, *manifest.declared_write_set]
    else:
        return
    for reference in event.path_refs:
        target = validate_relative_path(reference.relative_path)
        if not any(_path_matches_scope(target, entry) for entry in entries):
            raise ScopeViolationError(
                f"{event.event_type} path outside declared scope: {reference.relative_path}"
            )
    for reference in event.artifact_refs:
        target = validate_relative_path(reference.relative_path)
        if not _artifact_matches_scope(reference.artifact_id, entries) and not any(
            _path_matches_scope(target, entry) for entry in entries
        ):
            raise ScopeViolationError(
                f"{event.event_type} Artifact outside declared scope: {reference.artifact_id} at {reference.relative_path}"
            )


def _validate_identity(event: ExecutionEvent, manifest: SessionManifest) -> None:
    expected = {
        "task_id": manifest.task_id,
        "session_id": manifest.session_id,
        "attempt_no": manifest.attempt_no,
        "run_id": manifest.run_id,
        "stage_id": manifest.stage_id,
        "dag_node_id": manifest.dag_node_id,
    }
    for field, value in expected.items():
        if getattr(event, field) != value:
            raise EventChainError(
                f"Event identity mismatch for {field}: expected {value!r}, observed {getattr(event, field)!r}"
            )


def _validate_cross_references(
    event: ExecutionEvent, manifest: SessionManifest
) -> None:
    payload = event.payload
    if event.event_type == "session.started":
        if payload.get("manifest_hash") != manifest.manifest_hash:
            raise EventChainError(
                "session.started manifest_hash does not bind the Session manifest"
            )
        if len(event.path_refs) != 1 or event.path_refs[0].relative_path != payload.get(
            "manifest_path"
        ):
            raise EventChainError(
                "session.started must bind exactly its declared manifest path"
            )
    elif event.event_type == "task.created":
        if payload.get("task_scope") != manifest.task_scope:
            raise EventChainError(
                "task.created task_scope does not match the Session manifest"
            )
        if payload.get("parent_checkpoint_id") != manifest.parent_checkpoint_id:
            raise EventChainError(
                "task.created parent checkpoint does not match the Session manifest"
            )
    elif event.event_type == "task.retried":
        if manifest.attempt_no < 2 or manifest.parent_checkpoint_id is None:
            raise EventChainError(
                "task.retried is valid only inside a new retry Session bound to a parent checkpoint"
            )
        if payload.get("new_session_id") != manifest.session_id:
            raise EventChainError(
                "task.retried new_session_id must identify the current retry Session"
            )
        if payload.get("new_attempt_no") != manifest.attempt_no:
            raise EventChainError(
                "task.retried new_attempt_no must identify the current retry attempt"
            )
        if payload.get("parent_checkpoint_id") != manifest.parent_checkpoint_id:
            raise EventChainError(
                "task.retried parent checkpoint must match the retry Session manifest"
            )
    elif event.event_type == "checkpoint.created":
        if event.checkpoint_id != payload.get("checkpoint_id"):
            raise EventChainError(
                "checkpoint.created envelope/payload checkpoint_id mismatch"
            )
        if not event.path_refs or event.path_refs[0].relative_path != payload.get(
            "checkpoint_path"
        ):
            raise EventChainError("checkpoint.created path reference mismatch")
    elif event.event_type == "handoff.created":
        if not event.artifact_refs or not event.path_refs:
            raise EventChainError(
                "handoff.created requires Artifact and path references"
            )
        artifact = event.artifact_refs[0]
        path = event.path_refs[0]
        expected = {
            "handoff_artifact_id": artifact.artifact_id,
            "record_id": artifact.record_id,
            "handoff_path": artifact.relative_path,
        }
        for field, value in expected.items():
            if payload.get(field) != value:
                raise EventChainError(
                    f"handoff.created {field} cross-reference mismatch"
                )
        if (
            path.relative_path != payload.get("handoff_path")
            or path.content_hash != artifact.content_hash
        ):
            raise EventChainError("handoff.created path/content reference mismatch")


def validate_event_stream(
    events: Sequence[Mapping[str, Any] | ExecutionEvent],
    manifest: Mapping[str, Any] | SessionManifest,
    *,
    require_terminal: bool = False,
) -> EventLogSummary:
    manifest_model = validate_manifest(manifest)
    declared_event_schema_version = _declared_event_schema_version(manifest_model)
    models: list[ExecutionEvent] = []
    seen_ids: set[str] = set()
    terminal: ExecutionEvent | None = None
    checkpoint: ExecutionEvent | None = None
    handoff: ExecutionEvent | None = None
    previous: ExecutionEvent | None = None
    observed_artifact_ids: set[str] = set()

    for expected_sequence, candidate in enumerate(events, start=1):
        raw = _raw(candidate)
        reject_sensitive_material(raw)
        try:
            model = parse_execution_event(raw)
        except ExecutionSchemaError:
            raise
        if model.event_schema_version != declared_event_schema_version:
            raise EventChainError(
                "Event schema version does not match the Session manifest: "
                f"expected {declared_event_schema_version}, observed {model.event_schema_version}"
            )
        expected_hash = embedded_hash(raw, "event_hash")
        if model.event_hash != expected_hash:
            raise ExecutionHashError(
                f"event_hash mismatch at sequence {expected_sequence}: expected {expected_hash}, observed {model.event_hash}"
            )
        if model.sequence_no != expected_sequence:
            raise EventChainError(
                f"Non-contiguous sequence_no: expected {expected_sequence}, observed {model.sequence_no}"
            )
        if model.event_id in seen_ids:
            raise EventChainError(f"Duplicate event_id: {model.event_id}")
        seen_ids.add(model.event_id)
        _validate_identity(model, manifest_model)
        _validate_cross_references(model, manifest_model)
        _validate_event_scope(model, manifest_model)
        if model.event_type in {"artifact.validated", "artifact.invalidated"}:
            unknown = {
                reference.artifact_id for reference in model.artifact_refs
            } - observed_artifact_ids
            if unknown:
                raise EventChainError(
                    f"{model.event_type} references Artifacts not previously read or written: {sorted(unknown)}"
                )

        if previous is None:
            if model.event_type != "session.started":
                raise EventChainError("The first event must be session.started")
            if (
                model.previous_event_id is not None
                or model.previous_event_hash is not None
            ):
                raise EventChainError(
                    "The first event must have null previous-event references"
                )
        elif (
            model.previous_event_id != previous.event_id
            or model.previous_event_hash != previous.event_hash
        ):
            raise EventChainError(
                f"Broken previous-event link at sequence {expected_sequence}"
            )

        if terminal is not None and model.event_type not in POST_TERMINAL_EVENT_TYPES:
            raise EventChainError(
                f"Illegal event after terminal state: {model.event_type}"
            )
        if model.event_type in TERMINAL_EVENT_TYPES:
            if terminal is not None:
                raise EventChainError("A Session may contain only one terminal event")
            terminal = model
        elif model.event_type == "checkpoint.created":
            if terminal is None:
                raise EventChainError(
                    "checkpoint.created requires a preceding terminal event"
                )
            if checkpoint is not None or handoff is not None:
                raise EventChainError(
                    "checkpoint.created is duplicated or out of order"
                )
            checkpoint = model
        elif model.event_type == "handoff.created":
            if terminal is None or checkpoint is None or handoff is not None:
                raise EventChainError(
                    "handoff.created requires one prior terminal and checkpoint.created event"
                )
            if model.checkpoint_id != checkpoint.checkpoint_id:
                raise EventChainError(
                    "handoff.created checkpoint_id does not match checkpoint.created"
                )
            handoff = model

        if model.event_type in {"artifact.read", "artifact.written"}:
            observed_artifact_ids.update(
                reference.artifact_id for reference in model.artifact_refs
            )

        models.append(model)
        previous = model

    if require_terminal and terminal is None:
        raise EventChainError("A terminal event is required")
    if handoff is not None and models[-1] is not handoff:
        raise SessionFrozenError("handoff.created must be the final event")
    return EventLogSummary(
        event_count=len(models),
        head_event_id=previous.event_id if previous else None,
        head_event_hash=previous.event_hash if previous else None,
        head_occurred_at=previous.occurred_at if previous else None,
        terminal_event_type=terminal.event_type if terminal else None,
        terminal_state=TERMINAL_EVENT_TYPES[terminal.event_type] if terminal else None,
        checkpoint_id=checkpoint.checkpoint_id if checkpoint else None,
        frozen=handoff is not None,
    )


class EventLog:
    """One append-only event file bound to one immutable Session manifest."""

    def __init__(
        self, path: Path, manifest: Mapping[str, Any] | SessionManifest
    ) -> None:
        self.path = path
        self.manifest = validate_manifest(manifest)

    def read(self) -> list[ExecutionEvent]:
        try:
            rows = read_jsonl(self.path)
        except ContractIOError as exc:
            raise EventChainError(str(exc)) from exc
        validate_event_stream(rows, self.manifest)
        return [parse_execution_event(row) for row in rows]

    def validate(self, *, require_terminal: bool = False) -> EventLogSummary:
        try:
            rows = read_jsonl(self.path)
        except ContractIOError as exc:
            raise EventChainError(str(exc)) from exc
        return validate_event_stream(
            rows, self.manifest, require_terminal=require_terminal
        )

    def append(
        self,
        draft: Mapping[str, Any],
        *,
        event_id: str | None = None,
        occurred_at: str | None = None,
    ) -> ExecutionEvent:
        try:
            existing = read_jsonl(self.path)
        except ContractIOError as exc:
            raise EventChainError(str(exc)) from exc
        summary = validate_event_stream(existing, self.manifest)
        if summary.frozen:
            raise SessionFrozenError("The Session is frozen after handoff.created")

        previous = existing[-1] if existing else None
        raw = dict(draft)
        derived = {
            "event_id": event_id or new_typed_ulid("event"),
            "sequence_no": len(existing) + 1,
            "occurred_at": occurred_at or _utc_now_text(),
            "task_id": self.manifest.task_id,
            "session_id": self.manifest.session_id,
            "attempt_no": self.manifest.attempt_no,
            "run_id": self.manifest.run_id,
            "stage_id": self.manifest.stage_id,
            "dag_node_id": self.manifest.dag_node_id,
            "previous_event_id": previous["event_id"] if previous else None,
            "previous_event_hash": previous["event_hash"] if previous else None,
        }
        for field, value in derived.items():
            if field in raw and raw[field] != value:
                raise EventChainError(
                    f"Caller-supplied {field} conflicts with the derived append value"
                )
            raw[field] = value
        raw.setdefault(
            "event_schema_version", _declared_event_schema_version(self.manifest)
        )
        raw.setdefault("checkpoint_id", None)
        raw.setdefault("artifact_refs", [])
        raw.setdefault("path_refs", [])
        raw["event_hash"] = "0" * 64
        raw["event_hash"] = embedded_hash(raw, "event_hash")

        model = parse_execution_event(raw)
        validate_event_stream([*existing, raw], self.manifest)
        line = canonical_json_bytes(raw) + b"\n"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        existing_bytes = self.path.read_bytes() if self.path.exists() else b""
        _publish_temp(
            self.path,
            existing_bytes + line,
            immutable=not self.path.exists(),
        )
        with self.path.open("rb") as handle:
            os.fsync(handle.fileno())
        try:
            self.validate()
        except Exception as exc:
            raise EventChainError(
                "Event bytes were appended but write-back validation failed; history was not truncated"
            ) from exc
        return model


def _utc_now_text() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    )


def event_file_summary(
    path: Path, manifest: Mapping[str, Any] | SessionManifest
) -> dict[str, Any]:
    """Return a short JSON-compatible validation summary for CLI/reporting."""

    summary = EventLog(path, manifest).validate()
    return json.loads(json.dumps(summary.__dict__, sort_keys=True))
