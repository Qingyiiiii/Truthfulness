from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from video_truthfulness.core.execution.hashing import canonical_json_bytes
from video_truthfulness.core.execution.model_telemetry import (
    CallTiming,
    ContractRefs,
    ElapsedMetric,
    ModelTelemetryChainError,
    ModelTelemetryClosedError,
    ModelTelemetryHook,
    ModelTelemetrySchemaError,
    ModelTelemetryWriterError,
    ObservedModel,
    RequestedModel,
    TokenUsage,
    UsageMetric,
    not_applicable_usage,
    parse_model_call_event,
    parse_model_usage_summary,
    rebuild_model_usage_summary,
    unavailable_observed_model,
    unavailable_timing,
    unavailable_usage,
    validate_model_ledger,
)


ROOT = Path(__file__).resolve().parents[3]
H = "1" * 64
TASK = "task_01j00000000000000000000000"
SESSION = "session_01j00000000000000000000000"
RUN = "run_01j00000000000000000000000"


def _hook(
    tmp_path: Path, *, writer_id: str = "codex_coordinator"
) -> ModelTelemetryHook:
    return ModelTelemetryHook(
        tmp_path / "model_calls.jsonl",
        tmp_path / "model_usage_summary.json",
        task_id=TASK,
        session_id=SESSION,
        attempt_no=1,
        run_id=RUN,
        stage_id="S01",
        writer_id=writer_id,
    )


def _refs() -> ContractRefs:
    return ContractRefs(
        workflow_version="youtube_truthfulness_workflow_v1.1.0",
        workflow_hash=H,
        dag_version="youtube_truthfulness_dag_v1.2.0",
        dag_hash=H,
        prompt_version="claim_extract_prompt_v1.0.0",
        prompt_hash=H,
        agent_profile_version="stage5_v02_agent_v1.0.0",
        agent_profile_hash=H,
    )


def _measured_usage() -> TokenUsage:
    measured_input = UsageMetric(value=12, status="measured", source="provider_usage")
    measured_output = UsageMetric(value=5, status="measured", source="provider_usage")
    derived_total = UsageMetric(
        value=17, status="derived", source="sum_of_measured_components"
    )
    measured_zero = UsageMetric(value=0, status="measured", source="provider_usage")
    return TokenUsage(
        input_tokens=measured_input,
        output_tokens=measured_output,
        total_tokens=derived_total,
        cached_tokens=measured_zero,
        reasoning_tokens=measured_zero,
    )


def _timing() -> CallTiming:
    unavailable = ElapsedMetric(
        value_ms=None, status="unavailable", source="not_exposed"
    )
    return CallTiming(
        wall_elapsed=ElapsedMetric(
            value_ms=1000, status="measured", source="monotonic_clock"
        ),
        active_elapsed=ElapsedMetric(
            value_ms=900, status="measured", source="monotonic_clock"
        ),
        provider_elapsed=unavailable,
    )


def _one_call(hook: ModelTelemetryHook) -> tuple[str, object]:
    started = hook.start_call(
        node_id="claim_extract",
        actor_role="codex_coordinator",
        provider="openai",
        call_kind="llm",
        invocation_surface="project_api",
        instrumentation_mode="project_hook",
        recording_mode="synchronous",
        requested_model=RequestedModel(
            name="gpt-5.6-sol", revision=None, reasoning="xhigh"
        ),
        contract_refs=_refs(),
        started_at="2026-07-19T00:00:00Z",
    )
    finished = hook.finish_call(
        started.event_id,
        outcome="succeeded",
        observed_model=ObservedModel(
            value="gpt-5.6-sol-2026-07-01",
            status="reported",
            source="provider_response",
            match_status="match",
        ),
        timing=_timing(),
        token_usage=_measured_usage(),
        finished_at="2026-07-19T00:00:01Z",
    )
    return started.event_id, finished


def test_hash_chain_close_and_deterministic_summary_are_schema_valid(
    tmp_path: Path,
) -> None:
    hook = _hook(tmp_path)
    started_id, _ = _one_call(hook)
    summary = hook.close(closed_at="2026-07-19T00:00:02Z")

    rows = [
        json.loads(line)
        for line in hook.ledger_path.read_text(encoding="utf-8").splitlines()
    ]
    models = validate_model_ledger(rows)
    assert [item.event_type for item in models] == [
        "model_call.started",
        "model_call.finished",
        "model_ledger.closed",
    ]
    assert models[0].event_id == started_id == models[1].started_event_id
    assert summary.complete is True
    assert (
        summary.groups[0].token_coverage["total_tokens"].measured_or_derived_sum == 17
    )
    assert not (tmp_path / ".model_calls.jsonl.lock").exists()

    event_schema = json.loads(
        (ROOT / "schemas/execution/model_call_event_v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    summary_schema = json.loads(
        (ROOT / "schemas/execution/model_usage_summary_v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    for row in rows:
        Draft202012Validator(event_schema, format_checker=FormatChecker()).validate(row)
        assert parse_model_call_event(row).model_dump(mode="json") == row
    raw_summary = json.loads(hook.summary_path.read_text(encoding="utf-8"))
    Draft202012Validator(summary_schema, format_checker=FormatChecker()).validate(
        raw_summary
    )
    assert parse_model_usage_summary(raw_summary) == summary

    rebuilt = rebuild_model_usage_summary(hook.ledger_path)
    assert canonical_json_bytes(
        rebuilt.model_dump(mode="json")
    ) == canonical_json_bytes(summary.model_dump(mode="json"))
    with pytest.raises(ModelTelemetryClosedError):
        hook.close()


def test_unfinished_duplicate_or_second_writer_fail_closed(tmp_path: Path) -> None:
    hook = _hook(tmp_path)
    started = hook.start_call(
        node_id="claim_extract",
        actor_role="codex_coordinator",
        provider="openai",
        call_kind="llm",
        invocation_surface="project_api",
        instrumentation_mode="project_hook",
        recording_mode="synchronous",
        requested_model=RequestedModel(
            name="gpt-5.6-sol", revision=None, reasoning="xhigh"
        ),
        contract_refs=_refs(),
        started_at="2026-07-19T00:00:00Z",
    )
    with pytest.raises(ModelTelemetryChainError):
        hook.close()
    with pytest.raises(ModelTelemetryWriterError):
        _hook(tmp_path, writer_id="helper_writer").start_call(
            node_id="claim_extract",
            actor_role="transcript_claim_helper",
            provider="openai",
            call_kind="llm",
            invocation_surface="codex_runtime",
            instrumentation_mode="host_receipt",
            recording_mode="retrospective_host_receipt",
            requested_model=RequestedModel(
                name="gpt-5.6-terra", revision=None, reasoning="high"
            ),
            contract_refs=_refs(),
        )
    hook.finish_call(
        started.event_id,
        outcome="failed",
        error_class="SyntheticModelError",
        observed_model=unavailable_observed_model(),
        timing=unavailable_timing(),
        token_usage=unavailable_usage(),
        finished_at="2026-07-19T00:00:01Z",
    )
    with pytest.raises(ModelTelemetryChainError):
        hook.finish_call(
            started.event_id,
            outcome="failed",
            error_class="SyntheticModelError",
            observed_model=unavailable_observed_model(),
            timing=unavailable_timing(),
            token_usage=unavailable_usage(),
        )


def test_usage_missingness_is_null_and_asr_is_not_applicable() -> None:
    for usage in (unavailable_usage(), not_applicable_usage()):
        for name in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "cached_tokens",
            "reasoning_tokens",
        ):
            assert getattr(usage, name).value is None
    with pytest.raises(ValueError):
        UsageMetric(value=0, status="unavailable", source="not_exposed")
    with pytest.raises(ValueError):
        UsageMetric(value=3, status="derived", source="runtime_usage")


def test_external_ui_is_retrospective_manual_wall_not_active_time(
    tmp_path: Path,
) -> None:
    hook = ModelTelemetryHook(
        tmp_path / "model_calls.jsonl",
        tmp_path / "model_usage_summary.json",
        task_id=TASK,
        session_id=SESSION,
        attempt_no=1,
        run_id=RUN,
        stage_id="S02",
    )
    started, finished = hook.record_external_ui(
        node_id="external_depth_action",
        provider="google",
        requested_model=RequestedModel(
            name="gemini-web", revision=None, reasoning=None
        ),
        observed_model=unavailable_observed_model(),
        contract_refs=ContractRefs(
            workflow_version="youtube_truthfulness_workflow_v1.3.0",
            workflow_hash=H,
            dag_version="youtube_truthfulness_dag_v1.2.0",
            dag_hash=H,
            prompt_version="s02_source_depth_prompt_v1.2.0",
            prompt_hash=H,
            agent_profile_version="source_depth_agent_v1.2.0",
            agent_profile_hash=H,
        ),
        prompt_handoff_at="2026-07-19T00:00:00Z",
        result_ready_at="2026-07-19T00:01:00Z",
    )
    assert started.instrumentation_mode == "external_ui"
    assert started.recording_mode == "retrospective_manual_receipt"
    assert finished.timing and finished.timing.wall_elapsed.status == "derived"
    assert finished.timing.wall_elapsed.value_ms == 60_000
    assert finished.timing.active_elapsed.value_ms is None
    assert finished.token_usage and finished.token_usage.total_tokens.value is None


def test_requested_model_never_becomes_observed_without_receipt(tmp_path: Path) -> None:
    hook = _hook(tmp_path)
    started = hook.start_call(
        node_id="claim_extract",
        actor_role="codex_coordinator",
        provider="openai",
        call_kind="llm",
        invocation_surface="codex_runtime",
        instrumentation_mode="host_receipt",
        recording_mode="retrospective_host_receipt",
        requested_model=RequestedModel(
            name="gpt-5.6-sol", revision=None, reasoning="xhigh"
        ),
        contract_refs=_refs(),
        started_at="2026-07-19T00:00:00Z",
    )
    finished = hook.finish_call(
        started.event_id,
        outcome="succeeded",
        observed_model=unavailable_observed_model(),
        timing=unavailable_timing(),
        token_usage=unavailable_usage(),
        finished_at="2026-07-19T00:00:01Z",
    )
    assert finished.requested_model and finished.requested_model.name == "gpt-5.6-sol"
    assert finished.observed_model and finished.observed_model.value is None


def test_private_absolute_path_is_rejected_from_telemetry(tmp_path: Path) -> None:
    hook = _hook(tmp_path)
    with pytest.raises((ModelTelemetrySchemaError, ValueError)):
        hook.start_call(
            node_id="claim_extract",
            actor_role="codex_coordinator",
            provider="openai",
            call_kind="llm",
            invocation_surface="project_api",
            instrumentation_mode="project_hook",
            recording_mode="synchronous",
            requested_model=RequestedModel(
                name="C:/Users/private/model", revision=None, reasoning="xhigh"
            ),
            contract_refs=_refs(),
        )
