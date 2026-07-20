"""Single-writer, per-Session model telemetry ledger for Stage 5.

This module never calls a model.  A caller must durably append ``started``
before an interceptable invocation, and append exactly one ``finished`` after
the invocation returns or raises.  Codex-host and browser work use explicitly
retrospective receipt modes and never masquerade as project hooks.
"""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from video_truthfulness.core.artifacts.models import new_typed_ulid
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_file,
)
from video_truthfulness.core.execution.io import ContractIOError, read_jsonl, write_json


SHA256 = r"^[0-9a-f]{64}$"
EVENT_ID = r"^event_[0-9a-hjkmnp-tv-z]{26}$"
TASK_ID = r"^task_[0-9a-hjkmnp-tv-z]{26}$"
SESSION_ID = r"^session_[0-9a-hjkmnp-tv-z]{26}$"
RUN_ID = r"^run_[0-9a-hjkmnp-tv-z]{26}$"
UTC_TIMESTAMP = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?Z$"

TokenStatus = Literal["measured", "derived", "unavailable", "not_applicable"]
InstrumentationMode = Literal["project_hook", "host_receipt", "external_ui"]
RecordingMode = Literal[
    "synchronous", "retrospective_host_receipt", "retrospective_manual_receipt"
]


class ModelTelemetryError(ValueError):
    """Base error for ledger, usage, privacy, and publication violations."""


class ModelTelemetrySchemaError(ModelTelemetryError):
    pass


class ModelTelemetryChainError(ModelTelemetryError):
    pass


class ModelTelemetryClosedError(ModelTelemetryError):
    pass


class ModelTelemetryWriterError(ModelTelemetryError):
    pass


class ModelTelemetryPrivacyError(ModelTelemetryError):
    pass


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ValueError("timestamp must be a real UTC instant") from exc
    if not value.endswith("Z") or parsed.tzinfo != timezone.utc:
        raise ValueError("timestamp must use a UTC Z suffix")
    return parsed


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class RequestedModel(StrictModel):
    name: str = Field(min_length=1, max_length=160)
    revision: str | None = Field(default=None, max_length=160)
    reasoning: str | None = Field(default=None, max_length=80)


class ObservedModel(StrictModel):
    value: str | None = Field(default=None, max_length=160)
    status: Literal["reported", "unavailable"]
    source: Literal[
        "provider_response",
        "runtime_metadata",
        "ui_label",
        "local_manifest",
        "not_exposed",
    ]
    match_status: Literal["match", "mismatch", "unverifiable"]

    @model_validator(mode="after")
    def _status_value(self) -> "ObservedModel":
        if self.status == "unavailable":
            if (
                self.value is not None
                or self.source != "not_exposed"
                or self.match_status != "unverifiable"
            ):
                raise ValueError(
                    "unavailable observed model requires null/not_exposed/unverifiable"
                )
        elif self.value is None or self.source == "not_exposed":
            raise ValueError(
                "reported observed model requires a value and reporting source"
            )
        return self


class UsageMetric(StrictModel):
    value: int | None = Field(default=None, ge=0)
    status: TokenStatus
    source: Literal[
        "provider_usage",
        "runtime_usage",
        "ui_usage",
        "sum_of_measured_components",
        "not_exposed",
        "not_applicable",
    ]

    @model_validator(mode="after")
    def _null_semantics(self) -> "UsageMetric":
        if self.status in {"unavailable", "not_applicable"}:
            expected = (
                "not_exposed" if self.status == "unavailable" else "not_applicable"
            )
            if self.value is not None or self.source != expected:
                raise ValueError(
                    f"{self.status} usage requires value=null and source={expected}"
                )
        elif self.value is None:
            raise ValueError("measured/derived usage requires a non-negative integer")
        if self.status == "derived" and self.source != "sum_of_measured_components":
            raise ValueError(
                "derived usage is only legal as an exact measured-component sum"
            )
        if self.status == "measured" and self.source not in {
            "provider_usage",
            "runtime_usage",
            "ui_usage",
        }:
            raise ValueError("measured usage requires an authoritative usage source")
        return self


class TokenUsage(StrictModel):
    input_tokens: UsageMetric
    output_tokens: UsageMetric
    total_tokens: UsageMetric
    cached_tokens: UsageMetric
    reasoning_tokens: UsageMetric

    @model_validator(mode="after")
    def _derived_total(self) -> "TokenUsage":
        if self.total_tokens.status == "derived":
            parts = (self.input_tokens, self.output_tokens)
            if any(item.status != "measured" or item.value is None for item in parts):
                raise ValueError(
                    "derived total requires measured input and output Token counts"
                )
            if (
                self.total_tokens.value
                != self.input_tokens.value + self.output_tokens.value
            ):
                raise ValueError("derived total must equal measured input + output")
        return self


class ElapsedMetric(StrictModel):
    value_ms: int | None = Field(default=None, ge=0)
    status: Literal["measured", "derived", "unavailable", "not_applicable"]
    source: Literal[
        "monotonic_clock",
        "provider_timing",
        "receipt_interval",
        "not_exposed",
        "not_applicable",
    ]

    @model_validator(mode="after")
    def _semantics(self) -> "ElapsedMetric":
        if self.status in {"unavailable", "not_applicable"}:
            expected = (
                "not_exposed" if self.status == "unavailable" else "not_applicable"
            )
            if self.value_ms is not None or self.source != expected:
                raise ValueError(
                    "unavailable/not_applicable elapsed values must be null"
                )
        elif self.value_ms is None:
            raise ValueError("measured/derived elapsed value is required")
        if self.status == "derived" and self.source != "receipt_interval":
            raise ValueError("derived elapsed is restricted to a receipt interval")
        return self


class CallTiming(StrictModel):
    wall_elapsed: ElapsedMetric
    active_elapsed: ElapsedMetric
    provider_elapsed: ElapsedMetric


class ContractRefs(StrictModel):
    workflow_version: str = Field(min_length=1, max_length=120)
    workflow_hash: str = Field(pattern=SHA256)
    dag_version: str = Field(min_length=1, max_length=120)
    dag_hash: str = Field(pattern=SHA256)
    prompt_version: str | None = Field(default=None, max_length=120)
    prompt_hash: str | None = Field(default=None, pattern=SHA256)
    agent_profile_version: str | None = Field(default=None, max_length=120)
    agent_profile_hash: str | None = Field(default=None, pattern=SHA256)

    @model_validator(mode="after")
    def _paired(self) -> "ContractRefs":
        if (self.prompt_version is None) != (self.prompt_hash is None):
            raise ValueError("prompt version/hash must be paired")
        if (self.agent_profile_version is None) != (self.agent_profile_hash is None):
            raise ValueError("Agent Profile version/hash must be paired")
        return self


class LedgerClosure(StrictModel):
    started_count: int = Field(ge=0)
    finished_count: int = Field(ge=0)
    unmatched_count: Literal[0]


class ModelCallEvent(StrictModel):
    model_call_event_version: Literal["model_call_event_v1.0.0"]
    event_type: Literal[
        "model_call.started", "model_call.finished", "model_ledger.closed"
    ]
    event_id: str = Field(pattern=EVENT_ID)
    sequence_no: int = Field(ge=1)
    recorded_at: str = Field(pattern=UTC_TIMESTAMP)
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str = Field(pattern=RUN_ID)
    stage_id: Literal["S01", "S02"]
    node_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]*$")
    actor_role: str | None = Field(
        default=None, pattern=r"^[a-z][a-z0-9_]*$", max_length=80
    )
    provider: str | None = Field(
        default=None, pattern=r"^[a-z][a-z0-9_.-]*$", max_length=80
    )
    call_kind: Literal["llm", "asr", "ocr", "external_research"] | None
    invocation_surface: (
        Literal["project_api", "codex_runtime", "browser_ui", "local_runtime"] | None
    )
    instrumentation_mode: InstrumentationMode | None
    recording_mode: RecordingMode | None
    writer_id: str = Field(pattern=r"^[a-z][a-z0-9_.-]*$", max_length=120)
    started_event_id: str | None = Field(default=None, pattern=EVENT_ID)
    started_at: str | None = Field(default=None, pattern=UTC_TIMESTAMP)
    finished_at: str | None = Field(default=None, pattern=UTC_TIMESTAMP)
    outcome: Literal["succeeded", "failed", "cancelled"] | None
    error_class: str | None = Field(
        default=None, pattern=r"^[A-Za-z][A-Za-z0-9_.-]*$", max_length=160
    )
    requested_model: RequestedModel | None
    observed_model: ObservedModel | None
    timing: CallTiming | None
    token_usage: TokenUsage | None
    contract_refs: ContractRefs | None
    closure: LedgerClosure | None
    previous_record_hash: str | None = Field(default=None, pattern=SHA256)
    record_hash: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def _event_semantics(self) -> "ModelCallEvent":
        _timestamp(self.recorded_at)
        if self.started_at is not None:
            _timestamp(self.started_at)
        if self.finished_at is not None:
            _timestamp(self.finished_at)
        call_fields = (
            self.node_id,
            self.actor_role,
            self.provider,
            self.call_kind,
            self.invocation_surface,
            self.instrumentation_mode,
            self.recording_mode,
            self.started_event_id,
            self.started_at,
            self.requested_model,
            self.contract_refs,
        )
        if self.event_type == "model_call.started":
            if any(value is None for value in call_fields):
                raise ValueError("started event requires all call identity fields")
            if self.started_event_id != self.event_id:
                raise ValueError(
                    "started event is its own canonical invocation identity"
                )
            if any(
                value is not None
                for value in (
                    self.finished_at,
                    self.outcome,
                    self.error_class,
                    self.observed_model,
                    self.timing,
                    self.token_usage,
                    self.closure,
                )
            ):
                raise ValueError(
                    "started event cannot contain finish or closure fields"
                )
        elif self.event_type == "model_call.finished":
            if any(value is None for value in call_fields):
                raise ValueError("finished event requires all call identity fields")
            if any(
                value is None
                for value in (
                    self.finished_at,
                    self.outcome,
                    self.observed_model,
                    self.timing,
                    self.token_usage,
                )
            ):
                raise ValueError(
                    "finished event requires outcome, model, timing, and usage"
                )
            if self.closure is not None:
                raise ValueError("finished event cannot contain closure")
            if self.outcome == "failed" and self.error_class is None:
                raise ValueError("failed finish requires error_class")
            if self.outcome != "failed" and self.error_class is not None:
                raise ValueError("only failed finish may contain error_class")
            if _timestamp(self.finished_at) < _timestamp(self.started_at):
                raise ValueError("finished_at cannot predate started_at")
        else:
            if any(value is not None for value in call_fields[:-2]):
                raise ValueError("closed event cannot claim a model call")
            if any(
                value is not None
                for value in (
                    self.started_event_id,
                    self.started_at,
                    self.finished_at,
                    self.outcome,
                    self.error_class,
                    self.requested_model,
                    self.observed_model,
                    self.timing,
                    self.token_usage,
                    self.contract_refs,
                )
            ):
                raise ValueError("closed event contains call-only fields")
            if self.closure is None:
                raise ValueError("closed event requires closure counts")
        if self.record_hash != embedded_hash(
            self.model_dump(mode="json"), "record_hash"
        ):
            raise ValueError("model telemetry record_hash mismatch")
        _reject_private_payload(self.model_dump(mode="json"))
        return self


class TokenCoverage(StrictModel):
    measured_or_derived_sum: int = Field(ge=0)
    measured_or_derived_count: int = Field(ge=0)
    unavailable_count: int = Field(ge=0)
    not_applicable_count: int = Field(ge=0)


class ModelUsageGroup(StrictModel):
    provider: str
    invocation_surface: str
    actor_role: str
    call_kind: str
    requested_model: str
    observed_model: str | None
    observed_status: Literal["reported", "unavailable"]
    call_count: int = Field(ge=1)
    failed_count: int = Field(ge=0)
    mismatch_count: int = Field(ge=0)
    wall_elapsed_ms: int = Field(ge=0)
    wall_elapsed_covered_count: int = Field(ge=0)
    token_coverage: dict[
        Literal[
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "cached_tokens",
            "reasoning_tokens",
        ],
        TokenCoverage,
    ]


class ModelUsageSummary(StrictModel):
    model_usage_summary_version: Literal["model_usage_summary_v1.0.0"]
    task_id: str = Field(pattern=TASK_ID)
    session_id: str = Field(pattern=SESSION_ID)
    attempt_no: int = Field(ge=1)
    run_id: str = Field(pattern=RUN_ID)
    stage_id: Literal["S01", "S02"]
    ledger_file_hash: str = Field(pattern=SHA256)
    ledger_closed_event_id: str = Field(pattern=EVENT_ID)
    closed_at: str = Field(pattern=UTC_TIMESTAMP)
    complete: Literal[True]
    started_count: int = Field(ge=0)
    finished_count: int = Field(ge=0)
    unmatched_count: Literal[0]
    groups: list[ModelUsageGroup]
    summary_hash: str = Field(pattern=SHA256)

    @model_validator(mode="after")
    def _hash(self) -> "ModelUsageSummary":
        _timestamp(self.closed_at)
        if self.started_count != self.finished_count:
            raise ValueError(
                "complete summary requires equal started and finished counts"
            )
        if self.summary_hash != embedded_hash(
            self.model_dump(mode="json"), "summary_hash"
        ):
            raise ValueError("model usage summary_hash mismatch")
        return self


_FORBIDDEN_KEYS = {
    "prompt",
    "prompt_text",
    "response",
    "response_text",
    "url",
    "uri",
    "headers",
    "request_headers",
    "api_key",
    "authorization",
    "cookie",
    "password",
    "secret",
    "absolute_path",
}
_ABS_WINDOWS = re.compile(r"^[A-Za-z]:[\\/]")
_ABS_PRIVATE_POSIX = re.compile(r"^/(?:home|root|Users|private)/", re.IGNORECASE)


def _reject_private_payload(value: Any, *, location: str = "telemetry") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", str(key).lower()).strip("_")
            if normalized in _FORBIDDEN_KEYS:
                raise ModelTelemetryPrivacyError(
                    f"private telemetry key rejected at {location}.{key}"
                )
            _reject_private_payload(child, location=f"{location}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_private_payload(child, location=f"{location}[{index}]")
        return
    if not isinstance(value, str):
        return
    if len(value) > 1024:
        raise ModelTelemetryPrivacyError(
            f"oversized telemetry text rejected at {location}"
        )
    if value.lower().startswith(("http://", "https://")):
        raise ModelTelemetryPrivacyError(f"URL rejected at {location}")
    if _ABS_WINDOWS.match(value) or _ABS_PRIVATE_POSIX.match(value):
        raise ModelTelemetryPrivacyError(
            f"private absolute path rejected at {location}"
        )


def parse_model_call_event(raw: Mapping[str, Any]) -> ModelCallEvent:
    try:
        return ModelCallEvent.model_validate(dict(raw))
    except ValidationError as exc:
        first = exc.errors(include_url=False)[0]
        location = "/".join(str(part) for part in first["loc"]) or "<root>"
        raise ModelTelemetrySchemaError(
            f"Invalid model call event at {location}: {first['msg']}"
        ) from exc


def parse_model_usage_summary(raw: Mapping[str, Any]) -> ModelUsageSummary:
    try:
        return ModelUsageSummary.model_validate(dict(raw))
    except ValidationError as exc:
        first = exc.errors(include_url=False)[0]
        location = "/".join(str(part) for part in first["loc"]) or "<root>"
        raise ModelTelemetrySchemaError(
            f"Invalid model usage summary at {location}: {first['msg']}"
        ) from exc


def unavailable_usage() -> TokenUsage:
    metric = UsageMetric(value=None, status="unavailable", source="not_exposed")
    return TokenUsage(
        input_tokens=metric,
        output_tokens=metric,
        total_tokens=metric,
        cached_tokens=metric,
        reasoning_tokens=metric,
    )


def not_applicable_usage() -> TokenUsage:
    metric = UsageMetric(value=None, status="not_applicable", source="not_applicable")
    return TokenUsage(
        input_tokens=metric,
        output_tokens=metric,
        total_tokens=metric,
        cached_tokens=metric,
        reasoning_tokens=metric,
    )


def unavailable_observed_model() -> ObservedModel:
    return ObservedModel(
        value=None,
        status="unavailable",
        source="not_exposed",
        match_status="unverifiable",
    )


def unavailable_timing(
    *, wall_elapsed_ms: int | None = None, derived_wall: bool = False
) -> CallTiming:
    if wall_elapsed_ms is None:
        wall = ElapsedMetric(value_ms=None, status="unavailable", source="not_exposed")
    else:
        wall = ElapsedMetric(
            value_ms=wall_elapsed_ms,
            status="derived" if derived_wall else "measured",
            source="receipt_interval" if derived_wall else "monotonic_clock",
        )
    unavailable = ElapsedMetric(
        value_ms=None, status="unavailable", source="not_exposed"
    )
    return CallTiming(
        wall_elapsed=wall, active_elapsed=unavailable, provider_elapsed=unavailable
    )


def validate_model_ledger(rows: Iterable[Mapping[str, Any]]) -> list[ModelCallEvent]:
    models: list[ModelCallEvent] = []
    started: dict[str, ModelCallEvent] = {}
    finished: set[str] = set()
    writer_id: str | None = None
    previous_hash: str | None = None
    closed = False
    for index, row in enumerate(rows, start=1):
        model = parse_model_call_event(row)
        if model.sequence_no != index:
            raise ModelTelemetryChainError("model ledger sequence is not contiguous")
        if model.previous_record_hash != previous_hash:
            raise ModelTelemetryChainError("model ledger previous_record_hash mismatch")
        if writer_id is None:
            writer_id = model.writer_id
        elif model.writer_id != writer_id:
            raise ModelTelemetryWriterError(
                "model ledger contains more than one writer"
            )
        if closed:
            raise ModelTelemetryClosedError(
                "model ledger contains data after model_ledger.closed"
            )
        if model.event_type == "model_call.started":
            if model.event_id in started:
                raise ModelTelemetryChainError("duplicate model_call.started identity")
            started[model.event_id] = model
        elif model.event_type == "model_call.finished":
            started_model = started.get(str(model.started_event_id))
            if started_model is None:
                raise ModelTelemetryChainError("orphan model_call.finished")
            if model.started_event_id in finished:
                raise ModelTelemetryChainError("duplicate model_call.finished")
            for field in (
                "task_id",
                "session_id",
                "attempt_no",
                "run_id",
                "stage_id",
                "node_id",
                "actor_role",
                "provider",
                "call_kind",
                "invocation_surface",
                "instrumentation_mode",
                "recording_mode",
                "started_at",
                "requested_model",
                "contract_refs",
            ):
                if getattr(model, field) != getattr(started_model, field):
                    raise ModelTelemetryChainError(
                        f"finished event changed started field {field}"
                    )
            finished.add(str(model.started_event_id))
        else:
            unmatched = set(started) - finished
            if unmatched:
                raise ModelTelemetryChainError(
                    "cannot close model ledger with unmatched started calls"
                )
            if (
                model.closure is None
                or model.closure.started_count != len(started)
                or model.closure.finished_count != len(finished)
            ):
                raise ModelTelemetryChainError(
                    "model_ledger.closed counts do not match ledger"
                )
            closed = True
        previous_hash = model.record_hash
        models.append(model)
    return models


def _group_summary(finished: list[ModelCallEvent]) -> list[dict[str, Any]]:
    fields = (
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_tokens",
        "reasoning_tokens",
    )
    groups: dict[tuple[str, ...], dict[str, Any]] = {}
    for event in finished:
        assert (
            event.requested_model
            and event.observed_model
            and event.token_usage
            and event.timing
        )
        key = (
            str(event.provider),
            str(event.invocation_surface),
            str(event.actor_role),
            str(event.call_kind),
            event.requested_model.name,
            event.observed_model.value or "",
            event.observed_model.status,
        )
        group = groups.setdefault(
            key,
            {
                "provider": key[0],
                "invocation_surface": key[1],
                "actor_role": key[2],
                "call_kind": key[3],
                "requested_model": key[4],
                "observed_model": event.observed_model.value,
                "observed_status": key[6],
                "call_count": 0,
                "failed_count": 0,
                "mismatch_count": 0,
                "wall_elapsed_ms": 0,
                "wall_elapsed_covered_count": 0,
                "token_coverage": {
                    field: {
                        "measured_or_derived_sum": 0,
                        "measured_or_derived_count": 0,
                        "unavailable_count": 0,
                        "not_applicable_count": 0,
                    }
                    for field in fields
                },
            },
        )
        group["call_count"] += 1
        group["failed_count"] += int(event.outcome == "failed")
        group["mismatch_count"] += int(event.observed_model.match_status == "mismatch")
        wall = event.timing.wall_elapsed
        if wall.status in {"measured", "derived"} and wall.value_ms is not None:
            group["wall_elapsed_ms"] += wall.value_ms
            group["wall_elapsed_covered_count"] += 1
        for field in fields:
            metric = getattr(event.token_usage, field)
            coverage = group["token_coverage"][field]
            if metric.status in {"measured", "derived"}:
                coverage["measured_or_derived_sum"] += int(metric.value or 0)
                coverage["measured_or_derived_count"] += 1
            elif metric.status == "unavailable":
                coverage["unavailable_count"] += 1
            else:
                coverage["not_applicable_count"] += 1
    return [groups[key] for key in sorted(groups)]


def rebuild_model_usage_summary(ledger_path: Path) -> ModelUsageSummary:
    try:
        rows = read_jsonl(ledger_path)
    except ContractIOError as exc:
        raise ModelTelemetryChainError(str(exc)) from exc
    models = validate_model_ledger(rows)
    if not models or models[-1].event_type != "model_ledger.closed":
        raise ModelTelemetryClosedError(
            "model ledger must be closed before summary rebuild"
        )
    closed = models[-1]
    assert closed.closure is not None
    finished = [item for item in models if item.event_type == "model_call.finished"]
    raw = {
        "model_usage_summary_version": "model_usage_summary_v1.0.0",
        "task_id": closed.task_id,
        "session_id": closed.session_id,
        "attempt_no": closed.attempt_no,
        "run_id": closed.run_id,
        "stage_id": closed.stage_id,
        "ledger_file_hash": sha256_file(ledger_path),
        "ledger_closed_event_id": closed.event_id,
        "closed_at": closed.recorded_at,
        "complete": True,
        "started_count": closed.closure.started_count,
        "finished_count": closed.closure.finished_count,
        "unmatched_count": 0,
        "groups": _group_summary(finished),
        "summary_hash": "0" * 64,
    }
    raw["summary_hash"] = embedded_hash(raw, "summary_hash")
    return parse_model_usage_summary(raw)


class ModelTelemetryHook:
    """Append and close one per-Session model ledger as one declared writer."""

    def __init__(
        self,
        ledger_path: Path,
        summary_path: Path,
        *,
        task_id: str,
        session_id: str,
        attempt_no: int,
        run_id: str,
        stage_id: Literal["S01", "S02"],
        writer_id: str = "codex_coordinator",
    ) -> None:
        self.ledger_path = ledger_path
        self.summary_path = summary_path
        self.identity = {
            "task_id": task_id,
            "session_id": session_id,
            "attempt_no": attempt_no,
            "run_id": run_id,
            "stage_id": stage_id,
        }
        self.writer_id = writer_id

    @contextmanager
    def _lock(self) -> Iterable[Any]:
        """Take an advisory lock on the declared ledger itself.

        No hidden lock file or undeclared path is created.  The Session owner
        must create the declared parent directory before constructing the Hook.
        """

        if not self.ledger_path.parent.is_dir():
            raise ModelTelemetryWriterError(
                "declared model ledger parent directory does not exist"
            )
        handle = self.ledger_path.open("a+b")
        try:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    raise ModelTelemetryWriterError(
                        "model ledger is locked by another writer"
                    ) from exc
            else:
                import fcntl

                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    raise ModelTelemetryWriterError(
                        "model ledger is locked by another writer"
                    ) from exc
            yield handle
        finally:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _rows(self) -> list[dict[str, Any]]:
        try:
            return read_jsonl(self.ledger_path)
        except ContractIOError as exc:
            raise ModelTelemetryChainError(str(exc)) from exc

    @staticmethod
    def _locked_rows(handle: Any) -> list[dict[str, Any]]:
        handle.flush()
        handle.seek(0)
        data = handle.read()
        if not data:
            return []
        try:
            text = data.decode("utf-8")
        except UnicodeError as exc:
            raise ModelTelemetryChainError("model ledger is not UTF-8") from exc
        if not text.endswith("\n"):
            raise ModelTelemetryChainError("model ledger must end with LF")
        rows: list[dict[str, Any]] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line:
                raise ModelTelemetryChainError(f"blank model ledger line {line_number}")
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ModelTelemetryChainError(
                    f"invalid model ledger JSON at line {line_number}"
                ) from exc
            if not isinstance(raw, dict):
                raise ModelTelemetryChainError(
                    f"model ledger line {line_number} is not an object"
                )
            rows.append(raw)
        return rows

    def _append(
        self,
        values: Mapping[str, Any],
        *,
        event_id: str | None = None,
        recorded_at: str | None = None,
    ) -> ModelCallEvent:
        with self._lock() as locked_handle:
            rows = self._locked_rows(locked_handle)
            models = validate_model_ledger(rows)
            if models and models[-1].event_type == "model_ledger.closed":
                raise ModelTelemetryClosedError(
                    "cannot append after model_ledger.closed"
                )
            if models and models[0].writer_id != self.writer_id:
                raise ModelTelemetryWriterError(
                    "writer_id does not own the existing model ledger"
                )
            raw = dict(values)
            raw.update(self.identity)
            raw.update(
                {
                    "model_call_event_version": "model_call_event_v1.0.0",
                    "event_id": event_id or new_typed_ulid("event"),
                    "sequence_no": len(rows) + 1,
                    "recorded_at": recorded_at or _now(),
                    "writer_id": self.writer_id,
                    "previous_record_hash": models[-1].record_hash if models else None,
                    "record_hash": "0" * 64,
                }
            )
            raw["record_hash"] = embedded_hash(raw, "record_hash")
            model = parse_model_call_event(raw)
            validate_model_ledger([*rows, raw])
            line = canonical_json_bytes(raw) + b"\n"
            locked_handle.seek(0, os.SEEK_END)
            locked_handle.write(line)
            locked_handle.flush()
            os.fsync(locked_handle.fileno())
            validate_model_ledger(self._locked_rows(locked_handle))
            return model

    def start_call(
        self,
        *,
        node_id: str,
        actor_role: str,
        provider: str,
        call_kind: Literal["llm", "asr", "ocr", "external_research"],
        invocation_surface: Literal[
            "project_api", "codex_runtime", "browser_ui", "local_runtime"
        ],
        instrumentation_mode: InstrumentationMode,
        recording_mode: RecordingMode,
        requested_model: RequestedModel,
        contract_refs: ContractRefs,
        started_at: str | None = None,
        event_id: str | None = None,
    ) -> ModelCallEvent:
        started = started_at or _now()
        identity = event_id or new_typed_ulid("event")
        return self._append(
            {
                "event_type": "model_call.started",
                "node_id": node_id,
                "actor_role": actor_role,
                "provider": provider,
                "call_kind": call_kind,
                "invocation_surface": invocation_surface,
                "instrumentation_mode": instrumentation_mode,
                "recording_mode": recording_mode,
                "started_event_id": identity,
                "started_at": started,
                "finished_at": None,
                "outcome": None,
                "error_class": None,
                "requested_model": requested_model.model_dump(mode="json"),
                "observed_model": None,
                "timing": None,
                "token_usage": None,
                "contract_refs": contract_refs.model_dump(mode="json"),
                "closure": None,
            },
            event_id=identity,
        )

    def finish_call(
        self,
        started_event_id: str,
        *,
        outcome: Literal["succeeded", "failed", "cancelled"],
        observed_model: ObservedModel,
        timing: CallTiming,
        token_usage: TokenUsage,
        finished_at: str | None = None,
        error_class: str | None = None,
    ) -> ModelCallEvent:
        rows = self._rows()
        models = validate_model_ledger(rows)
        matches = [
            item
            for item in models
            if item.event_type == "model_call.started"
            and item.event_id == started_event_id
        ]
        if len(matches) != 1:
            raise ModelTelemetryChainError(
                "finished call must bind exactly one started event"
            )
        if any(
            item.event_type == "model_call.finished"
            and item.started_event_id == started_event_id
            for item in models
        ):
            raise ModelTelemetryChainError("started event already has a finished event")
        started = matches[0]
        return self._append(
            {
                "event_type": "model_call.finished",
                "node_id": started.node_id,
                "actor_role": started.actor_role,
                "provider": started.provider,
                "call_kind": started.call_kind,
                "invocation_surface": started.invocation_surface,
                "instrumentation_mode": started.instrumentation_mode,
                "recording_mode": started.recording_mode,
                "started_event_id": started.event_id,
                "started_at": started.started_at,
                "finished_at": finished_at or _now(),
                "outcome": outcome,
                "error_class": error_class,
                "requested_model": started.requested_model.model_dump(mode="json")
                if started.requested_model
                else None,
                "observed_model": observed_model.model_dump(mode="json"),
                "timing": timing.model_dump(mode="json"),
                "token_usage": token_usage.model_dump(mode="json"),
                "contract_refs": started.contract_refs.model_dump(mode="json")
                if started.contract_refs
                else None,
                "closure": None,
            }
        )

    def record_external_ui(
        self,
        *,
        node_id: str,
        provider: str,
        requested_model: RequestedModel,
        observed_model: ObservedModel,
        contract_refs: ContractRefs,
        prompt_handoff_at: str,
        result_ready_at: str,
        token_usage: TokenUsage | None = None,
    ) -> tuple[ModelCallEvent, ModelCallEvent]:
        start_time = _timestamp(prompt_handoff_at)
        finish_time = _timestamp(result_ready_at)
        if finish_time < start_time:
            raise ModelTelemetrySchemaError(
                "result-ready time cannot predate prompt HANDOFF"
            )
        elapsed = int((finish_time - start_time).total_seconds() * 1000)
        started = self.start_call(
            node_id=node_id,
            actor_role="gemini_web_user",
            provider=provider,
            call_kind="external_research",
            invocation_surface="browser_ui",
            instrumentation_mode="external_ui",
            recording_mode="retrospective_manual_receipt",
            requested_model=requested_model,
            contract_refs=contract_refs,
            started_at=prompt_handoff_at,
        )
        finished = self.finish_call(
            started.event_id,
            outcome="succeeded",
            observed_model=observed_model,
            timing=unavailable_timing(wall_elapsed_ms=elapsed, derived_wall=True),
            token_usage=token_usage or unavailable_usage(),
            finished_at=result_ready_at,
        )
        return started, finished

    def close(self, *, closed_at: str | None = None) -> ModelUsageSummary:
        rows = self._rows()
        models = validate_model_ledger(rows)
        if models and models[-1].event_type == "model_ledger.closed":
            raise ModelTelemetryClosedError("model ledger is already closed")
        started_count = sum(item.event_type == "model_call.started" for item in models)
        finished_count = sum(
            item.event_type == "model_call.finished" for item in models
        )
        if started_count != finished_count:
            raise ModelTelemetryChainError(
                "cannot close model ledger with unfinished calls"
            )
        self._append(
            {
                "event_type": "model_ledger.closed",
                "node_id": None,
                "actor_role": None,
                "provider": None,
                "call_kind": None,
                "invocation_surface": None,
                "instrumentation_mode": None,
                "recording_mode": None,
                "started_event_id": None,
                "started_at": None,
                "finished_at": None,
                "outcome": None,
                "error_class": None,
                "requested_model": None,
                "observed_model": None,
                "timing": None,
                "token_usage": None,
                "contract_refs": None,
                "closure": {
                    "started_count": started_count,
                    "finished_count": finished_count,
                    "unmatched_count": 0,
                },
            },
            recorded_at=closed_at,
        )
        summary = rebuild_model_usage_summary(self.ledger_path)
        try:
            write_json(
                self.summary_path, summary.model_dump(mode="json"), immutable=True
            )
        except ContractIOError as exc:
            raise ModelTelemetryClosedError(str(exc)) from exc
        return summary
