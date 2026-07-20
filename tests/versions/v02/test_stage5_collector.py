from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from video_truthfulness.core.execution.hashing import embedded_hash
from video_truthfulness.versions.v02.business_models import (
    FileBinding,
    ObservationMetric,
)
from video_truthfulness.versions.v02.stage5_collector import (
    Stage5Collector,
    Stage5CollectorClosedError,
    Stage5CollectorWriterError,
    validate_observation_ledger,
)


ROOT = Path(__file__).resolve().parents[3]
U = "01j00000000000000000000000"
TASK = f"task_{U}"
SESSION = f"session_{U}"
RUN = f"run_{U}"
EVENT = f"event_{U}"
H = "1" * 64


def _binding(relative: str) -> FileBinding:
    return FileBinding(
        relative_path=relative,
        content_hash_algorithm="sha256",
        content_hash=H,
        size_bytes=12,
    )


def _collector(tmp_path: Path) -> Stage5Collector:
    return Stage5Collector(
        tmp_path / "observations.jsonl",
        task_id=TASK,
        session_id=SESSION,
        attempt_no=1,
        run_id=RUN,
        stage_id="S01",
    )


def test_collector_append_hash_chain_close_and_schema_parity(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    recorded = collector.record(
        node_id="claim_extract",
        actor_role="codex_coordinator",
        tool_name="synthetic_claim_tool",
        started_at="2026-07-19T00:00:00Z",
        finished_at="2026-07-19T00:00:01Z",
        active_elapsed_ms=ObservationMetric(
            value=900, status="measured", source="collector"
        ),
        tool_profile=_binding("configs/tools/synthetic_profile.json"),
        exit_code=0,
        accelerator_peak_memory_bytes=ObservationMetric(
            value=1024, status="measured", source="tool"
        ),
        retry_parent_session_id=SESSION,
        rework_observation_ids=[f"event_{U[:-1]}2"],
        supersedes_observation_ids=[f"event_{U[:-1]}3"],
        input_files=[_binding("runs/V02/input.json")],
        output_files=[_binding("runs/V02/output.json")],
        model_event_ids=[EVENT],
        external_cost=ObservationMetric(
            value=None, status="unavailable", source="collector"
        ),
    )
    closed = collector.close(
        model_summary=_binding("runs/V02/model_usage_summary.json"),
        model_event_ids=[f"event_{U[:-1]}1"],
    )
    rows = [
        json.loads(line)
        for line in collector.ledger_path.read_text(encoding="utf-8").splitlines()
    ]
    models = validate_observation_ledger(rows)
    assert models == [recorded, closed]
    assert models[1].previous_record_hash == models[0].record_hash
    assert models[0].tool_profile and models[0].tool_profile.relative_path.endswith(
        "synthetic_profile.json"
    )
    assert models[0].accelerator_peak_memory_bytes
    assert models[0].accelerator_peak_memory_bytes.value == 1024
    assert not (tmp_path / ".observations.jsonl.lock").exists()
    schema = json.loads(
        (ROOT / "schemas/execution/stage5_observation_v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    for row in rows:
        Draft202012Validator(schema).validate(row)
    serialized = collector.ledger_path.read_text(encoding="utf-8")
    assert "requested_model" not in serialized
    assert "observed_model" not in serialized
    assert "token_usage" not in serialized
    with pytest.raises(Stage5CollectorWriterError):
        collector.record(
            node_id="claim_extract",
            actor_role="codex_coordinator",
            tool_name="synthetic_claim_tool",
            started_at="2026-07-19T00:00:02Z",
            finished_at="2026-07-19T00:00:03Z",
            active_elapsed_ms=ObservationMetric(
                value=1, status="measured", source="collector"
            ),
        )


def test_collector_second_writer_and_create_new_reopen_fail_closed(
    tmp_path: Path,
) -> None:
    first = _collector(tmp_path)
    try:
        with pytest.raises(
            Stage5CollectorWriterError, match="locked by another writer"
        ):
            _collector(tmp_path)
    finally:
        first.release()
    with pytest.raises(Stage5CollectorWriterError, match="already contains rows"):
        # Seed a valid but open stream, then prove create-new cannot resume it.
        seeded = _collector(tmp_path)
        seeded.record(
            node_id="claim_extract",
            actor_role="codex_coordinator",
            tool_name="synthetic_claim_tool",
            started_at="2026-07-19T00:00:00Z",
            finished_at="2026-07-19T00:00:01Z",
            active_elapsed_ms=ObservationMetric(
                value=1, status="measured", source="collector"
            ),
        )
        seeded.release()
        _collector(tmp_path)


def test_observation_validator_rejects_tampering_and_after_close(
    tmp_path: Path,
) -> None:
    collector = _collector(tmp_path)
    collector.close(model_summary=_binding("runs/V02/model_usage_summary.json"))
    rows = [
        json.loads(line)
        for line in collector.ledger_path.read_text(encoding="utf-8").splitlines()
    ]
    tampered = json.loads(json.dumps(rows))
    tampered[0]["invalid_read_count"] = 1
    with pytest.raises(ValueError, match="record_hash mismatch"):
        validate_observation_ledger(tampered)
    after_close = json.loads(json.dumps(rows[0]))
    after_close["observation_id"] = f"event_{U[:-1]}2"
    after_close["sequence_no"] = 2
    after_close["previous_record_hash"] = rows[0]["record_hash"]
    after_close["record_hash"] = embedded_hash(after_close, "record_hash")
    with pytest.raises(Stage5CollectorClosedError):
        validate_observation_ledger([*rows, after_close])
