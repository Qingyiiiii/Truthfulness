from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "execution_contract" / "synthetic_run"
TERMINAL_EVENTS = {
    "task.completed",
    "task.failed",
    "task.waiting_for_human",
    "task.blocked_by_input",
    "task.skipped_by_gate",
}
POST_TERMINAL_EVENTS = {"checkpoint.created", "handoff.created"}


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines and all(line.strip() for line in lines), (
        f"blank or empty JSONL fixture: {path}"
    )
    return [json.loads(line) for line in lines]


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _embedded_hash(value: dict[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field)
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _event_head(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": event["event_id"],
        "sequence_no": event["sequence_no"],
        "event_hash": event["event_hash"],
        "occurred_at": event["occurred_at"],
    }


def test_manifest_embedded_hash_binds_canonical_content() -> None:
    manifest = _load(EXAMPLE / "session_manifest.json")
    assert manifest["manifest_hash"] == _embedded_hash(manifest, "manifest_hash")
    assert manifest["code_ref"]["working_tree_dirty"] is True
    assert manifest["code_ref"]["working_tree_manifest_path"]
    assert manifest["code_ref"]["working_tree_manifest_hash"]


def test_event_stream_has_valid_canonical_hash_chain_and_one_terminal_event() -> None:
    schema = _load(ROOT / "schemas" / "execution" / "execution_event_v1.schema.json")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    events = _jsonl(EXAMPLE / "events.jsonl")

    seen_ids: set[str] = set()
    terminal_positions: list[int] = []
    for index, event in enumerate(events, start=1):
        validator.validate(event)
        assert event["sequence_no"] == index
        assert event["event_id"] not in seen_ids
        seen_ids.add(event["event_id"])
        assert event["event_hash"] == _embedded_hash(event, "event_hash")
        if index == 1:
            assert event["previous_event_id"] is None
            assert event["previous_event_hash"] is None
            assert event["event_type"] == "session.started"
        else:
            previous = events[index - 2]
            assert event["previous_event_id"] == previous["event_id"]
            assert event["previous_event_hash"] == previous["event_hash"]
        if event["event_type"] in TERMINAL_EVENTS:
            terminal_positions.append(index - 1)

    assert len(terminal_positions) == 1
    terminal_index = terminal_positions[0]
    assert all(
        event["event_type"] in POST_TERMINAL_EVENTS
        for event in events[terminal_index + 1 :]
    )


def test_registry_records_have_valid_wire_hashes_and_v11_shape() -> None:
    schema = _load(
        ROOT / "schemas" / "artifact_registry" / "artifact_record_v1_1.schema.json"
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    records = _jsonl(EXAMPLE / "artifact_registry.jsonl")
    seen_record_ids: set[str] = set()
    for record in records:
        validator.validate(record)
        assert record["registry_schema_version"] == "artifact_record_v1.1.0"
        assert record["record_id"] not in seen_record_ids
        seen_record_ids.add(record["record_id"])
        assert record["record_hash"] == _embedded_hash(record, "record_hash")


def test_checkpoint_state_and_handoff_self_hashes_are_canonical() -> None:
    checkpoint_paths = sorted((EXAMPLE / "checkpoints").glob("checkpoint_*.json"))
    assert len(checkpoint_paths) == 1
    checkpoint = _load(checkpoint_paths[0])
    state = _load(EXAMPLE / "current_state.json")
    handoff = _load(EXAMPLE / "handoff.json")

    assert checkpoint_paths[0].stem == checkpoint["checkpoint_id"]
    assert checkpoint["checkpoint_hash"] == _embedded_hash(
        checkpoint, "checkpoint_hash"
    )
    assert state["state_hash"] == _embedded_hash(state, "state_hash")
    assert handoff["handoff_hash"] == _embedded_hash(handoff, "handoff_hash")


def test_cross_object_heads_follow_the_two_phase_creation_order() -> None:
    events = _jsonl(EXAMPLE / "events.jsonl")
    checkpoint = _load(next((EXAMPLE / "checkpoints").glob("checkpoint_*.json")))
    state = _load(EXAMPLE / "current_state.json")
    handoff = _load(EXAMPLE / "handoff.json")
    records = _jsonl(EXAMPLE / "artifact_registry.jsonl")

    terminal = next(event for event in events if event["event_type"] in TERMINAL_EVENTS)
    checkpoint_created = next(
        event for event in events if event["event_type"] == "checkpoint.created"
    )
    handoff_created = next(
        event for event in events if event["event_type"] == "handoff.created"
    )

    assert checkpoint["event_head"] == _event_head(terminal)
    assert checkpoint_created["payload"]["checkpoint_id"] == checkpoint["checkpoint_id"]
    assert (
        checkpoint_created["payload"]["checkpoint_hash"]
        == checkpoint["checkpoint_hash"]
    )
    assert handoff["checkpoint_id"] == checkpoint["checkpoint_id"]
    assert handoff["source_event_head"] == _event_head(checkpoint_created)
    assert state["as_of_event_id"] == handoff_created["event_id"]
    assert state["event_head_hash"] == handoff_created["event_hash"]
    assert state["event_count"] == len(events)

    assert (
        handoff_created["payload"]["handoff_artifact_id"]
        == handoff["handoff_artifact_id"]
    )
    assert handoff_created["payload"]["handoff_hash"] == handoff["handoff_hash"]
    handoff_record = next(
        record
        for record in records
        if record["artifact_id"] == handoff["handoff_artifact_id"]
    )
    assert handoff_created["payload"]["record_id"] == handoff_record["record_id"]
    assert handoff_created["payload"]["record_hash"] == handoff_record["record_hash"]
    assert handoff["handoff_artifact_id"] not in {
        artifact["artifact_id"] for artifact in handoff["output_artifacts"]
    }
