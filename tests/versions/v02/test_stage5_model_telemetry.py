from __future__ import annotations

import json
from pathlib import Path

from video_truthfulness.core.execution.hashing import sha256_file
from video_truthfulness.core.execution.model_telemetry import (
    ContractRefs,
    ModelTelemetryHook,
    RequestedModel,
    not_applicable_usage,
    rebuild_model_usage_summary,
    unavailable_observed_model,
    unavailable_timing,
)
from video_truthfulness.versions.v02.business_models import (
    FileBinding,
    ObservationMetric,
)
from video_truthfulness.versions.v02.stage5_collector import (
    Stage5Collector,
    validate_observation_ledger,
)


U = "01j00000000000000000000000"
TASK = f"task_{U}"
SESSION = f"session_{U}"
RUN = f"run_{U}"
H = "1" * 64


def _binding(path: Path, root: Path) -> FileBinding:
    return FileBinding(
        relative_path=path.relative_to(root).as_posix(),
        content_hash_algorithm="sha256",
        content_hash=sha256_file(path),
        size_bytes=path.stat().st_size,
    )


def test_stage5_collector_references_frozen_model_facts_without_copying_them(
    tmp_path: Path,
) -> None:
    session = tmp_path / "runs" / "V02" / "sessions" / SESSION
    session.mkdir(parents=True)
    ledger = session / "model_calls.jsonl"
    summary_path = session / "model_usage_summary.json"
    hook = ModelTelemetryHook(
        ledger,
        summary_path,
        task_id=TASK,
        session_id=SESSION,
        attempt_no=1,
        run_id=RUN,
        stage_id="S01",
    )
    started = hook.start_call(
        node_id="audio_asr",
        actor_role="asr_ocr_agent",
        provider="local",
        call_kind="asr",
        invocation_surface="local_runtime",
        instrumentation_mode="project_hook",
        recording_mode="synchronous",
        requested_model=RequestedModel(
            name="faster-whisper-large-v3", revision="fixture", reasoning=None
        ),
        contract_refs=ContractRefs(
            workflow_version="youtube_truthfulness_workflow_v1.1.0",
            workflow_hash=H,
            dag_version="youtube_truthfulness_dag_v1.2.0",
            dag_hash=H,
            prompt_version=None,
            prompt_hash=None,
            agent_profile_version="stage5_v02_agent_v1.0.0",
            agent_profile_hash=H,
        ),
        started_at="2026-07-19T00:00:00Z",
    )
    finished = hook.finish_call(
        started.event_id,
        outcome="succeeded",
        observed_model=unavailable_observed_model(),
        timing=unavailable_timing(),
        token_usage=not_applicable_usage(),
        finished_at="2026-07-19T00:00:01Z",
    )
    summary = hook.close(closed_at="2026-07-19T00:00:02Z")
    model_rows = [
        json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()
    ]

    observations = session / "observations.jsonl"
    collector = Stage5Collector(
        observations,
        task_id=TASK,
        session_id=SESSION,
        attempt_no=1,
        run_id=RUN,
        stage_id="S01",
    )
    collector.record(
        node_id="audio_asr",
        actor_role="asr_ocr_agent",
        tool_name="faster_whisper",
        started_at="2026-07-19T00:00:00Z",
        finished_at="2026-07-19T00:00:01Z",
        active_elapsed_ms=ObservationMetric(
            value=None, status="unavailable", source="collector"
        ),
        model_event_ids=[started.event_id, finished.event_id],
        external_cost=ObservationMetric(
            value=None, status="not_applicable", source="collector"
        ),
    )
    collector.close(
        model_summary=_binding(summary_path, tmp_path),
        model_event_ids=[model_rows[-1]["event_id"]],
    )
    observation_rows = [
        json.loads(line)
        for line in observations.read_text(encoding="utf-8").splitlines()
    ]
    validate_observation_ledger(observation_rows)
    serialized = observations.read_text(encoding="utf-8")
    assert "faster-whisper-large-v3" not in serialized
    assert "token_usage" not in serialized
    assert observation_rows[-1]["model_summary"]["content_hash"] == sha256_file(
        summary_path
    )
    rebuilt = rebuild_model_usage_summary(ledger)
    assert rebuilt.model_dump(mode="json") == summary.model_dump(mode="json")
    for field in (
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_tokens",
        "reasoning_tokens",
    ):
        assert model_rows[1]["token_usage"][field]["value"] is None
