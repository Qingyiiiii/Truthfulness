"""Deterministic current-state projection from authoritative execution sources."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from pathlib import PurePosixPath
from typing import Annotated, Any, Iterable, Literal, Mapping, Sequence

from pydantic import Field, ValidationError, field_validator, model_validator

from video_truthfulness.core.artifacts.hashing import record_hash
from video_truthfulness.core.artifacts.dag import load_dag, validate_dag
from video_truthfulness.core.artifacts.models import (
    ArtifactRecordView,
    DAGDefinition,
    parse_artifact_record,
    to_artifact_record_view,
)
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    RegistryEntry,
    RegistryValidationError,
)
from video_truthfulness.core.execution.events import (
    TERMINAL_EVENT_TYPES,
    validate_event_stream,
    validate_manifest,
)
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_bytes,
)
from video_truthfulness.core.execution.io import write_json
from video_truthfulness.core.execution.models import (
    ExecutionContractError,
    ExecutionEvent,
    ExecutionHashError,
    SessionManifest,
    StrictFrozenModel,
    ARTIFACT_ID,
    ARTIFACT_TYPE,
    DAG_NODE_ID,
    EVENT_ID,
    RECORD_ID,
    RUN_ID,
    SESSION_ID,
    SHA256,
    STAGE_ID,
    TASK_ID,
    UTC_TIMESTAMP,
    parse_execution_event,
)


class StateProjectionError(ExecutionContractError):
    """Raised when current state cannot be derived without inventing facts."""


_RELATIVE_PATH = (
    r"^(?![A-Za-z]:)(?!/)(?!~)(?!.*(?:^|/)\.\.(?:/|$))"
    r"(?!.*(?:^|/)latest(?:/|$))(?!.*[\\*?$])[^\r\n]+$"
)
RelativePath = Annotated[str, Field(min_length=1, max_length=512)]


def _validate_schema_relative_path(value: str) -> str:
    if re.fullmatch(_RELATIVE_PATH, value) is None:
        raise ValueError(f"unsafe relative path: {value!r}")
    return value


def _validate_relative_path(value: str) -> str:
    if not isinstance(value, str):
        raise StateProjectionError(f"Unsafe or non-canonical relative path: {value!r}")
    try:
        _validate_schema_relative_path(value)
    except ValueError as exc:
        raise StateProjectionError(str(exc)) from exc
    if PurePosixPath(value).as_posix() != value:
        raise StateProjectionError(
            f"Relative path is not canonical POSIX form: {value!r}"
        )
    return value


def _registry_relative_path(
    path: Path,
    *,
    repository_root: Path | None,
    claimed_relative_path: str | None,
) -> str:
    if repository_root is not None:
        try:
            actual = (
                path.resolve(strict=True)
                .relative_to(repository_root.resolve(strict=True))
                .as_posix()
            )
        except (OSError, ValueError) as exc:
            raise StateProjectionError(
                f"Registry is outside repository_root or missing: {path}"
            ) from exc
    else:
        if path.is_absolute():
            raise StateProjectionError(
                "Absolute Registry paths require repository_root for relative projection"
            )
        actual = PurePosixPath(path.as_posix()).as_posix()
    _validate_relative_path(actual)
    if claimed_relative_path is not None:
        _validate_relative_path(claimed_relative_path)
        if claimed_relative_path != actual:
            raise StateProjectionError(
                f"Registry relative_path does not bind the actual file: {claimed_relative_path!r} != {actual!r}"
            )
    return actual


def _read_registry_prefix(
    path: Path,
    registry: AppendOnlyRegistry,
    head_record_id: str | None,
) -> tuple[tuple[RegistryEntry, ...], bytes]:
    """Read and validate exactly one immutable Registry prefix.

    A requested historical head deliberately stops the read at that JSONL line,
    so a later corrupt or partially-written tail cannot invalidate the frozen
    prefix used by an older checkpoint.
    """

    entries: list[RegistryEntry] = []
    prefix_lines: list[bytes] = []
    found = head_record_id is None
    try:
        with path.open("rb") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.endswith(b"\n"):
                    raise StateProjectionError(
                        f"Registry bytes do not form an LF-terminated JSONL prefix: {path}:{line_number}"
                    )
                if not line.strip():
                    raise StateProjectionError(
                        f"Blank Registry line at {path}:{line_number}"
                    )
                try:
                    raw = json.loads(line.decode("utf-8"))
                    wire_record = parse_artifact_record(raw)
                    entry = RegistryEntry(
                        wire_record=wire_record,
                        canonical_view=to_artifact_record_view(wire_record),
                    )
                except Exception as exc:  # noqa: BLE001 - normalize boundary diagnostics
                    raise StateProjectionError(
                        f"Invalid Registry line at {path}:{line_number}: {exc}"
                    ) from exc
                entries.append(entry)
                prefix_lines.append(line)
                if (
                    head_record_id is not None
                    and wire_record.record_id == head_record_id
                ):
                    found = True
                    break
    except OSError as exc:
        raise StateProjectionError(
            f"Cannot read Registry snapshot: {path}: {exc}"
        ) from exc
    if not found:
        raise StateProjectionError(f"Unknown Registry head_record_id: {head_record_id}")
    try:
        registry._validate_history(entries)  # noqa: SLF001 - validate an intentionally bounded prefix
    except RegistryValidationError as exc:
        raise StateProjectionError(f"Invalid Registry prefix at {path}: {exc}") from exc
    return tuple(entries), b"".join(prefix_lines)


@dataclass(frozen=True)
class RegistrySnapshot:
    path: Path
    repository_root: Path | None
    registry_scope: str
    expected_run_id: str | None
    relative_path: str
    content_hash: str
    entries: tuple[RegistryEntry, ...]
    prefix_bytes: bytes

    @property
    def records(self) -> tuple[ArtifactRecordView, ...]:
        return tuple(entry.canonical_view for entry in self.entries)

    @property
    def head_record_id(self) -> str | None:
        return self.entries[-1].wire_record.record_id if self.entries else None

    @property
    def head_record_hash(self) -> str | None:
        return self.entries[-1].wire_record.record_hash if self.entries else None

    def head(self) -> dict[str, Any]:
        return {
            "registry_scope": self.registry_scope,
            "relative_path": self.relative_path,
            "content_hash_algorithm": "sha256",
            "content_hash": self.content_hash,
            "record_count": len(self.entries),
            "artifact_count": len(
                {entry.canonical_view.artifact_id for entry in self.entries}
            ),
            "head_record_id": self.head_record_id,
            "head_record_hash": self.head_record_hash,
        }


def snapshot_registry(
    path: Path,
    *,
    scope: str,
    expected_run_id: str | None = None,
    head_record_id: str | None = None,
    repository_root: Path | None = None,
    relative_path: str | None = None,
) -> RegistrySnapshot:
    """Validate a Registry and freeze either its full history or one exact prefix."""

    registry = AppendOnlyRegistry(path, scope=scope, expected_run_id=expected_run_id)
    relative_path = _registry_relative_path(
        path,
        repository_root=repository_root,
        claimed_relative_path=relative_path,
    )
    entries, prefix = _read_registry_prefix(path, registry, head_record_id)
    return RegistrySnapshot(
        path=path,
        repository_root=repository_root.resolve(strict=True)
        if repository_root is not None
        else None,
        registry_scope=scope,
        expected_run_id=expected_run_id,
        relative_path=relative_path,
        content_hash=sha256_bytes(prefix),
        entries=entries,
        prefix_bytes=prefix,
    )


def _validate_registry_snapshots(
    snapshots: Sequence[RegistrySnapshot],
    manifest: SessionManifest,
) -> None:
    identities: set[tuple[str, str]] = set()
    for snapshot in snapshots:
        if snapshot.registry_scope not in {"run", "cross_run"}:
            raise StateProjectionError(
                f"Unsupported Registry scope: {snapshot.registry_scope!r}"
            )
        _validate_relative_path(snapshot.relative_path)
        _registry_relative_path(
            snapshot.path,
            repository_root=snapshot.repository_root,
            claimed_relative_path=snapshot.relative_path,
        )
        identity = (snapshot.registry_scope, snapshot.relative_path)
        if identity in identities:
            raise StateProjectionError(
                f"Duplicate Registry snapshot identity is ambiguous: {snapshot.registry_scope}:{snapshot.relative_path}"
            )
        identities.add(identity)
        if re.fullmatch(SHA256, snapshot.content_hash) is None:
            raise StateProjectionError(
                f"Invalid Registry snapshot content hash: {snapshot.relative_path}"
            )
        if sha256_bytes(snapshot.prefix_bytes) != snapshot.content_hash:
            raise StateProjectionError(
                f"Registry snapshot bytes/hash mismatch: {snapshot.relative_path}"
            )
        if snapshot.registry_scope == "run" and snapshot.expected_run_id is None:
            raise StateProjectionError("Run Registry snapshot requires expected_run_id")
        if (
            manifest.task_scope == "run"
            and snapshot.registry_scope == "run"
            and snapshot.expected_run_id != manifest.run_id
        ):
            raise StateProjectionError(
                "Run Registry snapshot expected_run_id does not match the Session manifest"
            )
        disk_registry = AppendOnlyRegistry(
            snapshot.path,
            scope=snapshot.registry_scope,
            expected_run_id=snapshot.expected_run_id,
        )
        if snapshot.entries:
            disk_entries, disk_prefix = _read_registry_prefix(
                snapshot.path,
                disk_registry,
                snapshot.head_record_id,
            )
        else:
            try:
                with snapshot.path.open("rb"):
                    pass
            except OSError as exc:
                raise StateProjectionError(
                    f"Cannot re-open empty Registry snapshot: {snapshot.path}: {exc}"
                ) from exc
            disk_entries, disk_prefix = (), b""
        if disk_prefix != snapshot.prefix_bytes:
            raise StateProjectionError(
                f"Registry snapshot no longer matches its disk prefix: {snapshot.relative_path}"
            )
        if tuple(
            entry.wire_record.model_dump(mode="json") for entry in disk_entries
        ) != tuple(
            entry.wire_record.model_dump(mode="json") for entry in snapshot.entries
        ):
            raise StateProjectionError(
                f"Registry snapshot entries no longer match disk: {snapshot.relative_path}"
            )
        raw_lines = snapshot.prefix_bytes.splitlines(keepends=True)
        if len(raw_lines) != len(snapshot.entries) or any(
            not line.endswith(b"\n") for line in raw_lines
        ):
            raise StateProjectionError(
                f"Registry snapshot bytes/entry count mismatch: {snapshot.relative_path}"
            )
        for line_number, (line, entry) in enumerate(
            zip(raw_lines, snapshot.entries), start=1
        ):
            try:
                wire = parse_artifact_record(json.loads(line.decode("utf-8")))
            except Exception as exc:  # noqa: BLE001 - normalize snapshot diagnostics
                raise StateProjectionError(
                    f"Invalid bounded Registry snapshot line {snapshot.relative_path}:{line_number}: {exc}"
                ) from exc
            if wire.model_dump(mode="json") != entry.wire_record.model_dump(
                mode="json"
            ):
                raise StateProjectionError(
                    f"Registry snapshot wire entry mismatch: {entry.wire_record.record_id}"
                )
            canonical = to_artifact_record_view(wire)
            if canonical.model_dump(mode="json") != entry.canonical_view.model_dump(
                mode="json"
            ):
                raise StateProjectionError(
                    f"Registry snapshot canonical entry mismatch: {wire.record_id}"
                )
            if record_hash(wire.model_dump(mode="json")) != wire.record_hash:
                raise StateProjectionError(
                    f"Registry snapshot record_hash mismatch: {wire.record_id}"
                )
            if canonical.storage_scope != snapshot.registry_scope:
                raise StateProjectionError(
                    f"Registry snapshot scope mismatch for record {canonical.record_id}: "
                    f"{snapshot.registry_scope} != {canonical.storage_scope}"
                )
            if (
                manifest.task_scope == "run"
                and snapshot.registry_scope == "run"
                and canonical.run_id != manifest.run_id
            ):
                raise StateProjectionError(
                    f"Run-scoped state cannot consume another run Registry record: {canonical.record_id}"
                )


def _load_dag(value: DAGDefinition | Mapping[str, Any] | Path) -> DAGDefinition:
    if isinstance(value, Path):
        return load_dag(value)
    if isinstance(value, DAGDefinition):
        dag = value
    else:
        dag = DAGDefinition.model_validate(value)
    validate_dag(dag)
    return dag


def _workflow_for_stage(dag: DAGDefinition, stage_id: str) -> str:
    if dag.dag_version == "youtube_truthfulness_dag_v1.2.0":
        resolver = getattr(dag, "workflow_version_for_stage", None)
        if resolver is None:
            raise StateProjectionError("DAG v1.2 lacks stage-scoped Workflow versions")
        return str(resolver(stage_id))
    return dag.workflow_version


def _record_index(
    snapshots: Sequence[RegistrySnapshot],
) -> tuple[dict[str, ArtifactRecordView], dict[str, ArtifactRecordView]]:
    latest: dict[str, ArtifactRecordView] = {}
    by_record_id: dict[str, ArtifactRecordView] = {}
    for snapshot in snapshots:
        for record in snapshot.records:
            previous = by_record_id.get(record.record_id)
            if previous is not None and previous.model_dump(
                mode="json"
            ) != record.model_dump(mode="json"):
                raise StateProjectionError(
                    f"Conflicting Registry record_id across snapshots: {record.record_id}"
                )
            by_record_id[record.record_id] = record
            current = latest.get(record.artifact_id)
            if current is None or record.record_revision > current.record_revision:
                latest[record.artifact_id] = record
            elif (
                record.record_revision == current.record_revision
                and record.record_id != current.record_id
            ):
                raise StateProjectionError(
                    f"Conflicting latest revision for Artifact: {record.artifact_id}"
                )
    return latest, by_record_id


def _artifact_ref(record: ArtifactRecordView) -> dict[str, Any]:
    return {
        "artifact_id": record.artifact_id,
        "artifact_type": record.artifact_type,
        "record_id": record.record_id,
        "relative_path": record.relative_path,
        "content_hash_algorithm": record.content_hash_algorithm,
        "content_hash": record.content_hash,
        "input_fingerprint": record.input_fingerprint,
        "validation_status": record.validation_status,
        "lifecycle_state": record.lifecycle_state,
    }


def _assert_event_artifact_ref(
    reference: Any,
    latest: Mapping[str, ArtifactRecordView],
    by_record_id: Mapping[str, ArtifactRecordView],
) -> ArtifactRecordView:
    if reference.record_id is not None:
        record = by_record_id.get(reference.record_id)
    else:
        record = latest.get(reference.artifact_id)
    if record is None:
        raise StateProjectionError(
            f"Event references an Artifact absent from the bounded Registry snapshots: {reference.artifact_id}"
        )
    expected = (
        record.artifact_id,
        record.artifact_type,
        record.relative_path,
        record.content_hash_algorithm,
        record.content_hash,
    )
    observed = (
        reference.artifact_id,
        reference.artifact_type,
        reference.relative_path,
        reference.content_hash_algorithm,
        reference.content_hash,
    )
    if observed != expected:
        raise StateProjectionError(
            f"Event/Registry Artifact identity mismatch: {reference.artifact_id}"
        )
    if reference.record_id is not None:
        expected_metadata = (
            record.input_fingerprint,
            record.validation_status,
            record.lifecycle_state,
        )
        observed_metadata = (
            reference.input_fingerprint,
            reference.validation_status,
            reference.lifecycle_state,
        )
        if observed_metadata != expected_metadata:
            raise StateProjectionError(
                f"Event/Registry Artifact metadata mismatch: {reference.artifact_id}"
            )
    return record


def _observed_access(event: ExecutionEvent, reference: Any) -> dict[str, Any]:
    return {
        "artifact_id": reference.artifact_id,
        "record_id": reference.record_id,
        "relative_path": reference.relative_path,
        "content_hash_algorithm": reference.content_hash_algorithm,
        "content_hash": reference.content_hash,
        "event_id": event.event_id,
    }


def _observed_path_access(event: ExecutionEvent, reference: Any) -> dict[str, Any]:
    return {
        "artifact_id": None,
        "record_id": None,
        "relative_path": reference.relative_path,
        "content_hash_algorithm": reference.content_hash_algorithm,
        "content_hash": reference.content_hash,
        "event_id": event.event_id,
    }


def _event_observed_accesses(event: ExecutionEvent) -> list[dict[str, Any]]:
    accesses = [_observed_access(event, reference) for reference in event.artifact_refs]
    artifact_bindings = {
        (
            reference.relative_path,
            reference.content_hash_algorithm,
            reference.content_hash,
        )
        for reference in event.artifact_refs
    }
    accesses.extend(
        _observed_path_access(event, reference)
        for reference in event.path_refs
        if (
            reference.relative_path,
            reference.content_hash_algorithm,
            reference.content_hash,
        )
        not in artifact_bindings
    )
    return accesses


def _validation_summary(events: Sequence[ExecutionEvent]) -> dict[str, Any]:
    validators: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    counts = {"passed": 0, "failed": 0, "partial": 0}
    for event in events:
        if event.event_type != "artifact.validated":
            continue
        payload = event.payload
        counts[payload["result"]] += 1
        key = (
            payload["validator_id"],
            payload["validator_version"],
            payload["validation_artifact_id"] or "",
            payload["result"],
        )
        validators[key] = {
            "validator_id": payload["validator_id"],
            "validator_version": payload["validator_version"],
            "result": payload["result"],
            "validation_artifact_id": payload["validation_artifact_id"],
        }
    ordered = [validators[key] for key in sorted(validators)]
    if counts["failed"]:
        overall = "failed"
    elif counts["partial"]:
        overall = "partial"
    elif counts["passed"]:
        overall = "passed"
    else:
        overall = "not_run"
    return {
        "overall_status": overall,
        "passed_count": counts["passed"],
        "failed_count": counts["failed"],
        "partial_count": counts["partial"],
        "validators": ordered,
    }


def _pending_human(events: Sequence[ExecutionEvent]) -> list[dict[str, Any]]:
    pending: dict[str, dict[str, Any]] = {}
    for event in events:
        payload = event.payload
        if event.event_type == "human.approval_requested":
            pending[payload["decision_artifact_id"]] = {
                "decision_artifact_id": payload["decision_artifact_id"],
                "gate_node_id": payload["gate_node_id"],
                "requested_event_id": event.event_id,
                "status": "pending",
                "summary": payload["question_summary"],
            }
        elif (
            event.event_type == "human.approval_received"
            and payload["decision"] != "deferred"
        ):
            pending.pop(payload["decision_artifact_id"], None)
    return sorted(
        pending.values(),
        key=lambda item: (
            item["gate_node_id"],
            item["decision_artifact_id"],
            item["requested_event_id"],
        ),
    )


def _candidate_nodes(
    dag: DAGDefinition,
    records: Iterable[ArtifactRecordView],
    manifest: SessionManifest,
) -> list[dict[str, Any]]:
    latest: dict[str, ArtifactRecordView] = {}
    for record in records:
        current = latest.get(record.artifact_id)
        if current is None or record.record_revision > current.record_revision:
            latest[record.artifact_id] = record
    active_lifecycle = {"validated", "frozen"}
    if manifest.task_scope == "run":
        usable = [
            record
            for record in latest.values()
            if record.storage_scope == "run"
            and record.run_id == manifest.run_id
            and record.validation_status == "passed"
            and record.lifecycle_state in active_lifecycle
        ]
    else:
        usable = [
            record
            for record in latest.values()
            if record.storage_scope == "cross_run"
            and record.run_id is None
            and record.validation_status == "passed"
            and record.lifecycle_state in active_lifecycle
        ]
    node_by_id = {node.node_id: node for node in dag.nodes}
    usable = [
        record
        for record in usable
        if record.dag_node_id in node_by_id
        and record.artifact_type in node_by_id[record.dag_node_id].declared_outputs
    ]
    by_type: dict[str, list[ArtifactRecordView]] = {}
    completed_nodes: set[str] = set()
    for record in usable:
        by_type.setdefault(record.artifact_type, []).append(record)
        completed_nodes.add(record.dag_node_id)
    predecessors: dict[str, set[str]] = {node.node_id: set() for node in dag.nodes}
    for node in dag.nodes:
        successors = set(node.next_nodes) | set(node.fallback_nodes)
        if node.failure_terminal is not None:
            successors.add(node.failure_terminal)
        for successor in successors:
            predecessors[successor].add(node.node_id)
    candidates: list[dict[str, Any]] = []
    visible_stages = {manifest.stage_id}
    if manifest.session_manifest_version == "session_manifest_v1.1.0":
        stage_number = int(manifest.stage_id[1:])
        if stage_number < 9:
            visible_stages.add(f"S{stage_number + 1:02d}")
    for node in dag.nodes:
        if node.stage_id not in visible_stages:
            continue
        outputs = [
            record
            for artifact_type in node.declared_outputs
            for record in by_type.get(artifact_type, [])
            if record.dag_node_id == node.node_id
        ]
        if outputs:
            continue
        missing = [
            artifact_type
            for artifact_type in node.required_inputs
            if not by_type.get(artifact_type)
        ]
        if missing or node.node_type == "terminal":
            continue
        node_predecessors = predecessors[node.node_id]
        if node_predecessors and not node_predecessors.intersection(completed_nodes):
            continue
        if not node_predecessors and node.node_id not in dag.entry_nodes:
            continue
        status = "manual_gate" if node.manual_gate else "ready"
        reason = (
            "required inputs exist; explicit human decision is required"
            if node.manual_gate
            else "required inputs exist; node has not been executed"
        )
        candidates.append(
            {
                "node_id": node.node_id,
                "stage_id": node.stage_id,
                "status": status,
                "reason": reason,
            }
        )
    return sorted(candidates, key=lambda item: (item["stage_id"], item["node_id"]))


def _completed_actions(events: Sequence[ExecutionEvent]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for event in events:
        payload = event.payload
        if event.event_type in TERMINAL_EVENT_TYPES:
            summary = (
                payload.get("result_summary")
                or payload.get("summary")
                or payload.get("reason")
                or "terminal task state recorded"
            )
            actions.append(
                {
                    "action_key": event.event_type.replace(".", "_"),
                    "summary": summary,
                    "source_event_id": event.event_id,
                }
            )
        elif event.event_type == "checkpoint.created":
            actions.append(
                {
                    "action_key": "checkpoint_created",
                    "summary": f"created immutable checkpoint {payload['checkpoint_id']}",
                    "source_event_id": event.event_id,
                }
            )
        elif event.event_type == "handoff.created":
            actions.append(
                {
                    "action_key": "handoff_finalized",
                    "summary": f"created and registered HANDOFF Artifact {payload['handoff_artifact_id']}",
                    "source_event_id": event.event_id,
                }
            )
    return sorted(
        actions, key=lambda item: (item["source_event_id"], item["action_key"])
    )


def _remaining_actions(
    candidates: Sequence[dict[str, Any]], pending: Sequence[dict[str, Any]]
) -> list[dict[str, Any]]:
    actions = [
        {"action_key": f"await_{item['gate_node_id']}", "summary": item["summary"]}
        for item in pending
    ]
    actions.extend(
        {
            "action_key": f"execute_{item['node_id']}",
            "summary": item["reason"],
        }
        for item in candidates
    )
    return sorted(actions, key=lambda item: item["action_key"])


def _project_artifact_refs(
    exact_records: Mapping[str, ArtifactRecordView],
    unresolved_artifact_ids: Iterable[str],
    latest: Mapping[str, ArtifactRecordView],
) -> list[dict[str, Any]]:
    records = dict(exact_records)
    exact_artifact_ids = {record.artifact_id for record in records.values()}
    for artifact_id in unresolved_artifact_ids:
        if artifact_id in exact_artifact_ids:
            continue
        record = latest.get(artifact_id)
        if record is None:
            raise StateProjectionError(
                f"Projected Artifact has no Registry record: {artifact_id}"
            )
        records[record.record_id] = record
    return [
        _artifact_ref(record)
        for record in sorted(
            records.values(),
            key=lambda item: (item.artifact_id, item.record_revision, item.record_id),
        )
    ]


def build_current_state(
    manifest: Mapping[str, Any] | SessionManifest,
    events: Sequence[Mapping[str, Any] | ExecutionEvent],
    registry_snapshots: Sequence[RegistrySnapshot],
    dag: DAGDefinition | Mapping[str, Any] | Path,
) -> dict[str, Any]:
    """Purely project current state from validated sources; never read Markdown or wall-clock state."""

    if not events:
        raise StateProjectionError(
            "Current state requires at least one execution event"
        )
    if not registry_snapshots:
        raise StateProjectionError(
            "Current state requires at least one bounded Registry snapshot"
        )
    manifest_model = validate_manifest(manifest)
    summary = validate_event_stream(events, manifest_model)
    event_models = [
        event
        if isinstance(event, ExecutionEvent)
        else parse_execution_event(dict(event))
        for event in events
    ]
    dag_model = _load_dag(dag)
    if dag_model.dag_version != manifest_model.dag_version:
        raise StateProjectionError("DAG version does not match the Session manifest")
    stage_workflow = _workflow_for_stage(dag_model, manifest_model.stage_id)
    if stage_workflow != manifest_model.workflow_version:
        raise StateProjectionError(
            "DAG stage Workflow does not match the Session manifest"
        )
    _validate_registry_snapshots(registry_snapshots, manifest_model)
    latest, by_record_id = _record_index(registry_snapshots)
    resolved_event_records: dict[str, tuple[ArtifactRecordView, ...]] = {}
    for event in event_models:
        resolved_event_records[event.event_id] = tuple(
            _assert_event_artifact_ref(reference, latest, by_record_id)
            for reference in event.artifact_refs
        )

    read_set: list[dict[str, Any]] = []
    write_set: list[dict[str, Any]] = []
    input_ids: set[str] = set()
    output_ids: set[str] = set()
    invalidated_ids: set[str] = set()
    input_records: dict[str, ArtifactRecordView] = {}
    output_records: dict[str, ArtifactRecordView] = {}
    invalidated_records: dict[str, ArtifactRecordView] = {}
    for event in event_models:
        event_records = resolved_event_records[event.event_id]
        if event.event_type == "task.started":
            input_ids.update(event.payload["required_input_artifact_ids"])
        elif event.event_type == "artifact.read":
            input_records.update((record.record_id, record) for record in event_records)
            read_set.extend(_event_observed_accesses(event))
        elif event.event_type == "artifact.written":
            output_records.update(
                (record.record_id, record) for record in event_records
            )
            write_set.extend(_event_observed_accesses(event))
        elif event.event_type == "artifact.invalidated":
            invalidated_records.update(
                (record.record_id, record) for record in event_records
            )

    for artifact_id in input_ids | output_ids | invalidated_ids:
        if artifact_id not in latest:
            raise StateProjectionError(
                f"Projected Artifact has no Registry record: {artifact_id}"
            )
    read_set.sort(
        key=lambda item: (
            item["relative_path"],
            item["event_id"],
            item["artifact_id"] or "",
            item["record_id"] or "",
        )
    )
    write_set.sort(
        key=lambda item: (
            item["relative_path"],
            item["event_id"],
            item["artifact_id"] or "",
            item["record_id"] or "",
        )
    )
    pending = _pending_human(event_models)
    candidates = _candidate_nodes(dag_model, latest.values(), manifest_model)
    terminal_state = summary.terminal_state or "IN_PROGRESS"
    head = event_models[-1]
    state: dict[str, Any] = {
        "current_state_version": (
            "current_state_v1.1.0"
            if manifest_model.session_manifest_version == "session_manifest_v1.1.0"
            else "current_state_v1.0.0"
        ),
        "task_id": manifest_model.task_id,
        "session_id": manifest_model.session_id,
        "attempt_no": manifest_model.attempt_no,
        "run_id": manifest_model.run_id,
        "stage_id": manifest_model.stage_id,
        "status": terminal_state,
        "as_of_event_id": head.event_id,
        "as_of_occurred_at": head.occurred_at,
        "event_count": len(event_models),
        "event_head_hash": head.event_hash,
        "registry_heads": sorted(
            (snapshot.head() for snapshot in registry_snapshots),
            key=lambda item: (item["registry_scope"], item["relative_path"]),
        ),
        "dag_version": dag_model.dag_version,
        "workflow_version": stage_workflow,
        "actual_read_set": read_set,
        "actual_write_set": write_set,
        "input_artifacts": _project_artifact_refs(input_records, input_ids, latest),
        "output_artifacts": _project_artifact_refs(output_records, output_ids, latest),
        "invalidated_artifacts": _project_artifact_refs(
            invalidated_records, invalidated_ids, latest
        ),
        "completed_actions": _completed_actions(event_models),
        "remaining_actions": _remaining_actions(candidates, pending),
        "pending_human_decisions": pending,
        "validation_summary": _validation_summary(event_models),
        "candidate_next_nodes": candidates,
        "state_hash": "0" * 64,
    }
    state["state_hash"] = embedded_hash(state, "state_hash")
    validate_current_state(state)
    return state


class _StateArtifactRef(StrictFrozenModel):
    artifact_id: str = Field(pattern=ARTIFACT_ID)
    artifact_type: str = Field(pattern=ARTIFACT_TYPE)
    record_id: str = Field(pattern=RECORD_ID)
    relative_path: RelativePath
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    input_fingerprint: str | None = Field(pattern=SHA256)
    validation_status: Literal["not_validated", "passed", "failed", "partial"]
    lifecycle_state: Literal[
        "created",
        "validated",
        "frozen",
        "stale",
        "superseded",
        "invalid",
        "archived",
        "purged",
    ]

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        return _validate_schema_relative_path(value)


class _ObservedAccess(StrictFrozenModel):
    artifact_id: str | None = Field(pattern=ARTIFACT_ID)
    record_id: str | None = Field(pattern=RECORD_ID)
    relative_path: RelativePath
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    event_id: str = Field(pattern=EVENT_ID)

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        return _validate_schema_relative_path(value)


class _RegistryHead(StrictFrozenModel):
    registry_scope: Literal["run", "cross_run"]
    relative_path: RelativePath
    content_hash_algorithm: Literal["sha256"]
    content_hash: str = Field(pattern=SHA256)
    record_count: int = Field(ge=0)
    artifact_count: int = Field(ge=0)
    head_record_id: str | None = Field(pattern=RECORD_ID)
    head_record_hash: str | None = Field(pattern=SHA256)

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        return _validate_schema_relative_path(value)

    @model_validator(mode="after")
    def validate_head_boundary(self) -> _RegistryHead:
        if self.record_count == 0:
            if self.head_record_id is not None or self.head_record_hash is not None:
                raise ValueError("empty Registry head must use null record id/hash")
        elif self.head_record_id is None or self.head_record_hash is None:
            raise ValueError("non-empty Registry head requires record id/hash")
        if self.artifact_count > self.record_count:
            raise ValueError("Registry artifact_count cannot exceed record_count")
        return self


class _CompletedAction(StrictFrozenModel):
    action_key: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    summary: str = Field(min_length=1, max_length=500)
    source_event_id: str = Field(pattern=EVENT_ID)


class _RemainingAction(StrictFrozenModel):
    action_key: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=120)
    summary: str = Field(min_length=1, max_length=500)


class _PendingHumanDecision(StrictFrozenModel):
    decision_artifact_id: str = Field(pattern=ARTIFACT_ID)
    gate_node_id: str = Field(pattern=DAG_NODE_ID)
    requested_event_id: str = Field(pattern=EVENT_ID)
    status: Literal["pending"]
    summary: str = Field(min_length=1, max_length=500)


class _ValidatorResult(StrictFrozenModel):
    validator_id: str = Field(pattern=r"^[a-z][a-z0-9_.-]*$", max_length=160)
    validator_version: str = Field(min_length=1, max_length=120)
    result: Literal["passed", "failed", "partial"]
    validation_artifact_id: str | None = Field(pattern=ARTIFACT_ID)


def _assert_unique(values: Sequence[StrictFrozenModel], field_name: str) -> None:
    encoded = [canonical_json_bytes(value.model_dump(mode="json")) for value in values]
    if len(encoded) != len(set(encoded)):
        raise ValueError(f"{field_name} must contain unique items")


class _ValidationSummary(StrictFrozenModel):
    overall_status: Literal["passed", "failed", "partial", "not_run"]
    passed_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    partial_count: int = Field(ge=0)
    validators: list[_ValidatorResult]

    @model_validator(mode="after")
    def validate_summary(self) -> _ValidationSummary:
        _assert_unique(self.validators, "validation_summary.validators")
        if self.failed_count:
            expected = "failed"
        elif self.partial_count:
            expected = "partial"
        elif self.passed_count:
            expected = "passed"
        else:
            expected = "not_run"
        if self.overall_status != expected:
            raise ValueError(f"validation overall_status must be {expected}")
        return self


class _CandidateNode(StrictFrozenModel):
    node_id: str = Field(pattern=DAG_NODE_ID)
    stage_id: str = Field(pattern=STAGE_ID)
    status: Literal["ready", "manual_gate", "blocked", "failed", "stale", "invalid"]
    reason: str = Field(min_length=1, max_length=500)


class _CurrentState(StrictFrozenModel):
    current_state_version: Literal[
        "current_state_v1.0.0",
        "current_state_v1.1.0",
    ]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str | None = Field(pattern=RUN_ID)
    stage_id: str = Field(pattern=STAGE_ID)
    status: Literal[
        "IN_PROGRESS",
        "COMPLETED",
        "FAILED",
        "WAITING_FOR_HUMAN",
        "BLOCKED_BY_INPUT",
        "SKIPPED_BY_GATE",
    ]
    as_of_event_id: str = Field(pattern=EVENT_ID)
    as_of_occurred_at: str = Field(pattern=UTC_TIMESTAMP)
    event_count: int = Field(ge=1)
    event_head_hash: str = Field(pattern=SHA256)
    registry_heads: list[_RegistryHead] = Field(min_length=1)
    dag_version: Literal[
        "youtube_truthfulness_dag_v1.1.0",
        "youtube_truthfulness_dag_v1.2.0",
    ]
    workflow_version: Literal[
        "youtube_truthfulness_workflow_v1.1.0",
        "youtube_truthfulness_workflow_v1.3.0",
    ]
    actual_read_set: list[_ObservedAccess]
    actual_write_set: list[_ObservedAccess]
    input_artifacts: list[_StateArtifactRef]
    output_artifacts: list[_StateArtifactRef]
    invalidated_artifacts: list[_StateArtifactRef]
    completed_actions: list[_CompletedAction]
    remaining_actions: list[_RemainingAction]
    pending_human_decisions: list[_PendingHumanDecision]
    validation_summary: _ValidationSummary
    candidate_next_nodes: list[_CandidateNode]
    state_hash: str = Field(pattern=SHA256)

    @field_validator("as_of_occurred_at")
    @classmethod
    def validate_occurred_at(cls, value: str) -> str:
        try:
            parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        except ValueError as exc:
            raise ValueError("as_of_occurred_at must be a real UTC date-time") from exc
        if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
            raise ValueError("as_of_occurred_at must be UTC")
        return value

    @model_validator(mode="after")
    def validate_unique_arrays(self) -> _CurrentState:
        if self.current_state_version == "current_state_v1.0.0":
            if (
                self.dag_version != "youtube_truthfulness_dag_v1.1.0"
                or self.workflow_version != "youtube_truthfulness_workflow_v1.1.0"
            ):
                raise ValueError("current_state_v1.0.0 requires Workflow/DAG v1.1")
        else:
            if self.dag_version != "youtube_truthfulness_dag_v1.2.0":
                raise ValueError("current_state_v1.1.0 requires DAG v1.2")
            expected_workflow = (
                "youtube_truthfulness_workflow_v1.3.0"
                if self.stage_id == "S02"
                else "youtube_truthfulness_workflow_v1.1.0"
            )
            if self.workflow_version != expected_workflow:
                raise ValueError(
                    "current_state_v1.1.0 source Workflow does not match stage"
                )
        for field_name in (
            "registry_heads",
            "actual_read_set",
            "actual_write_set",
            "input_artifacts",
            "output_artifacts",
            "invalidated_artifacts",
            "completed_actions",
            "remaining_actions",
            "pending_human_decisions",
            "candidate_next_nodes",
        ):
            _assert_unique(getattr(self, field_name), field_name)
        return self


class _CurrentStateV1_1(_CurrentState):
    current_state_version: Literal["current_state_v1.1.0"]


def validate_current_state(state: Mapping[str, Any]) -> None:
    raw = dict(state)
    model_type: type[_CurrentState] = (
        _CurrentStateV1_1
        if raw.get("current_state_version") == "current_state_v1.1.0"
        else _CurrentState
    )
    try:
        model_type.model_validate(raw)
    except ValidationError as exc:
        raise StateProjectionError(f"Invalid CurrentState contract: {exc}") from exc
    try:
        expected = embedded_hash(raw, "state_hash")
    except ValueError as exc:
        raise ExecutionHashError(str(exc)) from exc
    if raw["state_hash"] != expected:
        raise ExecutionHashError(
            f"state_hash mismatch: expected {expected}, observed {raw['state_hash']}"
        )


def validate_state_projection(
    state: Mapping[str, Any],
    manifest: Mapping[str, Any] | SessionManifest,
    events: Sequence[Mapping[str, Any] | ExecutionEvent],
    registry_snapshots: Sequence[RegistrySnapshot],
    dag: DAGDefinition | Mapping[str, Any] | Path,
) -> None:
    validate_current_state(state)
    rebuilt = build_current_state(manifest, events, registry_snapshots, dag)
    if canonical_json_bytes(dict(state)) != canonical_json_bytes(rebuilt):
        raise StateProjectionError(
            "Current state does not match its authoritative source projection"
        )


def current_state_bytes(state: Mapping[str, Any]) -> bytes:
    validate_current_state(state)
    return canonical_json_bytes(dict(state)) + b"\n"


def write_current_state(path: Path, state: Mapping[str, Any]) -> str:
    validate_current_state(state)
    return write_json(path, state, immutable=False)
