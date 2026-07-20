"""Append-only Stage 5 non-model observation collector.

Model identity, provider timing, and token facts belong exclusively to the
per-Session model telemetry ledger.  This collector only stores exact event and
file bindings to that ledger and its deterministic summary.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from video_truthfulness.core.artifacts.models import new_typed_ulid
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
)
from video_truthfulness.versions.v02.business_models import (
    FileBinding,
    ObservationMetric,
    Stage5Observation,
    parse_stage5_observation,
)


class Stage5CollectorError(ValueError):
    """Base error for observation writer, chain, and closure violations."""


class Stage5CollectorWriterError(Stage5CollectorError):
    pass


class Stage5CollectorChainError(Stage5CollectorError):
    pass


class Stage5CollectorClosedError(Stage5CollectorError):
    pass


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _read_locked_rows(handle: Any) -> list[dict[str, Any]]:
    handle.flush()
    handle.seek(0)
    data = handle.read()
    if not data:
        return []
    try:
        text = data.decode("utf-8")
    except UnicodeError as exc:
        raise Stage5CollectorChainError("observation ledger is not UTF-8") from exc
    if not text.endswith("\n"):
        raise Stage5CollectorChainError("observation ledger must end with LF")
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line:
            raise Stage5CollectorChainError(
                f"blank observation ledger line {line_number}"
            )
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise Stage5CollectorChainError(
                f"invalid observation ledger JSON at line {line_number}"
            ) from exc
        if not isinstance(value, dict):
            raise Stage5CollectorChainError(
                f"observation ledger line {line_number} is not an object"
            )
        rows.append(value)
    return rows


def validate_observation_ledger(
    rows: Iterable[Mapping[str, Any]],
) -> list[Stage5Observation]:
    models: list[Stage5Observation] = []
    previous_hash: str | None = None
    identity: tuple[str, str, int, str, str] | None = None
    closed = False
    for sequence_no, row in enumerate(rows, start=1):
        model = parse_stage5_observation(row)
        if model.sequence_no != sequence_no:
            raise Stage5CollectorChainError("observation sequence is not contiguous")
        if model.previous_record_hash != previous_hash:
            raise Stage5CollectorChainError("observation previous_record_hash mismatch")
        current_identity = (
            model.task_id,
            model.session_id,
            model.attempt_no,
            model.run_id,
            model.stage_id,
        )
        if identity is None:
            identity = current_identity
        elif current_identity != identity:
            raise Stage5CollectorChainError(
                "observation ledger changed Session identity"
            )
        if closed:
            raise Stage5CollectorClosedError(
                "observation ledger contains data after closure"
            )
        closed = model.observation_type == "observation.closed"
        previous_hash = model.record_hash
        models.append(model)
    return models


class Stage5Collector:
    """Own one declared observation ledger until its explicit close record."""

    def __init__(
        self,
        ledger_path: Path,
        *,
        task_id: str,
        session_id: str,
        attempt_no: int,
        run_id: str,
        stage_id: Literal["S01", "S02"],
        create_new: bool = True,
    ) -> None:
        if not ledger_path.parent.is_dir():
            raise Stage5CollectorWriterError(
                "declared observation parent directory does not exist"
            )
        self.ledger_path = ledger_path
        self.identity = {
            "task_id": task_id,
            "session_id": session_id,
            "attempt_no": attempt_no,
            "run_id": run_id,
            "stage_id": stage_id,
        }
        self._handle = ledger_path.open("a+b")
        self._locked = False
        self._closed = False
        try:
            self._acquire_lock()
            rows = _read_locked_rows(self._handle)
            models = validate_observation_ledger(rows)
            if create_new and rows:
                raise Stage5CollectorWriterError(
                    "create-new observation ledger already contains rows"
                )
            if models and models[-1].observation_type == "observation.closed":
                raise Stage5CollectorClosedError("observation ledger is already closed")
            if models:
                observed = {
                    "task_id": models[0].task_id,
                    "session_id": models[0].session_id,
                    "attempt_no": models[0].attempt_no,
                    "run_id": models[0].run_id,
                    "stage_id": models[0].stage_id,
                }
                if observed != self.identity:
                    raise Stage5CollectorWriterError(
                        "collector identity does not own existing ledger"
                    )
        except Exception:
            self._release_lock()
            self._handle.close()
            raise

    def _acquire_lock(self) -> None:
        self._handle.seek(0)
        if os.name == "nt":
            import msvcrt

            try:
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise Stage5CollectorWriterError(
                    "observation ledger is locked by another writer"
                ) from exc
        else:
            import fcntl

            try:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise Stage5CollectorWriterError(
                    "observation ledger is locked by another writer"
                ) from exc
        self._locked = True

    def _release_lock(self) -> None:
        if not self._locked:
            return
        try:
            self._handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._locked = False

    def __enter__(self) -> "Stage5Collector":
        return self

    def __exit__(self, *_: Any) -> None:
        self.release()

    def release(self) -> None:
        if self._handle.closed:
            return
        try:
            self._release_lock()
        finally:
            self._handle.close()

    def _append(self, values: Mapping[str, Any]) -> Stage5Observation:
        if self._handle.closed or not self._locked:
            raise Stage5CollectorWriterError(
                "collector no longer owns the observation ledger"
            )
        if self._closed:
            raise Stage5CollectorClosedError("cannot append after observation.closed")
        rows = _read_locked_rows(self._handle)
        models = validate_observation_ledger(rows)
        if models and models[-1].observation_type == "observation.closed":
            self._closed = True
            raise Stage5CollectorClosedError("cannot append after observation.closed")
        raw = {
            **dict(values),
            **self.identity,
            "observation_version": "stage5_observation_v1.0.0",
            "observation_id": new_typed_ulid("event"),
            "sequence_no": len(rows) + 1,
            "recorded_at": _now(),
            "scope": "contract_observed",
            "previous_record_hash": models[-1].record_hash if models else None,
            "record_hash": "0" * 64,
        }
        raw["record_hash"] = embedded_hash(raw, "record_hash")
        model = parse_stage5_observation(raw)
        validate_observation_ledger([*rows, raw])
        self._handle.seek(0, os.SEEK_END)
        self._handle.write(canonical_json_bytes(raw) + b"\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        validate_observation_ledger(_read_locked_rows(self._handle))
        if model.observation_type == "observation.closed":
            self._closed = True
        return model

    def record(
        self,
        *,
        node_id: str,
        actor_role: str,
        tool_name: str,
        started_at: str,
        finished_at: str,
        active_elapsed_ms: ObservationMetric,
        tool_profile: FileBinding | None = None,
        exit_code: int | None = None,
        accelerator_peak_memory_bytes: ObservationMetric | None = None,
        retry_parent_session_id: str | None = None,
        rework_observation_ids: Iterable[str] = (),
        supersedes_observation_ids: Iterable[str] = (),
        input_files: Iterable[FileBinding] = (),
        output_files: Iterable[FileBinding] = (),
        model_event_ids: Iterable[str] = (),
        external_cost: ObservationMetric | None = None,
        failure_class: str | None = None,
        invalid_read_count: int = 0,
    ) -> Stage5Observation:
        return self._append(
            {
                "observation_type": "observation.recorded",
                "node_id": node_id,
                "actor_role": actor_role,
                "tool_name": tool_name,
                "tool_profile": tool_profile.model_dump(mode="json")
                if tool_profile
                else None,
                "exit_code": exit_code,
                "accelerator_peak_memory_bytes": (
                    accelerator_peak_memory_bytes.model_dump(mode="json")
                    if accelerator_peak_memory_bytes
                    else None
                ),
                "retry_parent_session_id": retry_parent_session_id,
                "rework_observation_ids": list(rework_observation_ids),
                "supersedes_observation_ids": list(supersedes_observation_ids),
                "started_at": started_at,
                "finished_at": finished_at,
                "active_elapsed_ms": active_elapsed_ms.model_dump(mode="json"),
                "input_files": [item.model_dump(mode="json") for item in input_files],
                "output_files": [item.model_dump(mode="json") for item in output_files],
                "model_event_ids": list(model_event_ids),
                "model_summary": None,
                "external_cost": external_cost.model_dump(mode="json")
                if external_cost
                else None,
                "failure_class": failure_class,
                "invalid_read_count": invalid_read_count,
            }
        )

    def close(
        self,
        *,
        model_summary: FileBinding,
        model_event_ids: Iterable[str] = (),
        invalid_read_count: int = 0,
    ) -> Stage5Observation:
        model = self._append(
            {
                "observation_type": "observation.closed",
                "node_id": None,
                "actor_role": None,
                "tool_name": None,
                "tool_profile": None,
                "exit_code": None,
                "accelerator_peak_memory_bytes": None,
                "retry_parent_session_id": None,
                "rework_observation_ids": [],
                "supersedes_observation_ids": [],
                "started_at": None,
                "finished_at": None,
                "active_elapsed_ms": None,
                "input_files": [],
                "output_files": [],
                "model_event_ids": list(model_event_ids),
                "model_summary": model_summary.model_dump(mode="json"),
                "external_cost": None,
                "failure_class": None,
                "invalid_read_count": invalid_read_count,
            }
        )
        self.release()
        return model


__all__ = [
    "Stage5Collector",
    "Stage5CollectorChainError",
    "Stage5CollectorClosedError",
    "Stage5CollectorError",
    "Stage5CollectorWriterError",
    "validate_observation_ledger",
]
