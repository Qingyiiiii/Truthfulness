from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path
from typing import Any

import pytest

from video_truthfulness.core.artifacts.dag import load_dag
from video_truthfulness.core.execution.checkpoints import (
    CheckpointImmutableError,
    CheckpointPublication,
    CheckpointSources,
    CheckpointValidationError,
    build_checkpoint,
    checkpoint_created_draft,
    create_checkpoint,
    parse_checkpoint,
    read_checkpoint,
    validate_checkpoint,
    validate_checkpoint_chain,
    validate_checkpoint_created_event,
)
from video_truthfulness.core.execution import checkpoints as checkpoint_runtime
from video_truthfulness.core.execution.events import EventLog
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_file,
)
from video_truthfulness.core.execution.models import ExecutionContractError
from video_truthfulness.core.execution.state import (
    build_current_state,
    snapshot_registry,
)


ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "execution_contract" / "synthetic_run"
CHECKPOINT_ID = "checkpoint_01j00000000000000000000099"
RUN_ID = "run_01j00000000000000000000000"
RECORD_2 = "record_01j00000000000000000000002"


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _sources(
    *,
    root: Path = ROOT,
    example: Path = EXAMPLE,
    events: list[dict[str, Any]] | None = None,
) -> CheckpointSources:
    manifest = _json(example / "session_manifest.json")
    event_rows = events if events is not None else _jsonl(example / "events.jsonl")[:7]
    registry = snapshot_registry(
        example / "artifact_registry.jsonl",
        scope="run",
        expected_run_id=RUN_ID,
        head_record_id=RECORD_2,
        repository_root=root,
    )
    dag_path = example / "youtube_truthfulness_dag_v1_1.yaml"
    terminal_state = build_current_state(
        manifest, event_rows, [registry], load_dag(dag_path)
    )
    return CheckpointSources(
        repository_root=root,
        manifest=manifest,
        events=event_rows,
        terminal_state=terminal_state,
        registry_snapshots=(registry,),
        dag_path=dag_path,
        dag_relative_path=dag_path.relative_to(root).as_posix(),
    )


def _checkpoint(sources: CheckpointSources | None = None) -> Any:
    return build_checkpoint(
        sources or _sources(),
        checkpoint_kind="stage_boundary",
        created_at="2026-01-01T00:00:08Z",
        checkpoint_id=CHECKPOINT_ID,
    )


def _successor_checkpoint_raw() -> dict[str, Any]:
    checkpoint_path = next((EXAMPLE / "checkpoints").glob("checkpoint_*.json"))
    raw = _json(checkpoint_path)
    raw["checkpoint_schema_version"] = "execution_checkpoint_v1.1.0"
    raw["dag_ref"]["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    raw["schema_versions"] = [
        "execution_checkpoint_v1.1.0"
        if value == "execution_checkpoint_v1.0.0"
        else value
        for value in raw["schema_versions"]
    ]
    raw["checkpoint_hash"] = embedded_hash(raw, "checkpoint_hash")
    return raw


def test_checkpoint_successor_parses_without_weakening_v1() -> None:
    successor = parse_checkpoint(_successor_checkpoint_raw())
    assert successor.checkpoint_schema_version == "execution_checkpoint_v1.1.0"
    assert successor.dag_ref.dag_version == "youtube_truthfulness_dag_v1.2.0"

    legacy = _json(next((EXAMPLE / "checkpoints").glob("checkpoint_*.json")))
    assert (
        parse_checkpoint(legacy).checkpoint_schema_version
        == "execution_checkpoint_v1.0.0"
    )


def test_checkpoint_successor_rejects_wrong_stage_workflow() -> None:
    raw = _successor_checkpoint_raw()
    raw["stage_id"] = "S02"
    raw["checkpoint_hash"] = embedded_hash(raw, "checkpoint_hash")
    with pytest.raises(CheckpointValidationError, match="does not match stage"):
        parse_checkpoint(raw)


def test_build_checkpoint_emits_successor_for_dag_v12(tmp_path: Path) -> None:
    root = tmp_path / "repository"
    example = root / "examples/execution_contract/synthetic_run"
    shutil.copytree(EXAMPLE, example)
    dag_path = root / "configs/workflows/youtube_truthfulness_dag_v1_2.yaml"
    dag_path.parent.mkdir(parents=True)
    shutil.copy2(
        ROOT / "configs/workflows/youtube_truthfulness_dag_v1_2.yaml",
        dag_path,
    )
    manifest_path = example / "session_manifest.json"
    manifest = _json(manifest_path)
    manifest["session_manifest_version"] = "session_manifest_v1.1.0"
    manifest["dag_node_id"] = None
    manifest["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    manifest["schema_versions"] = [
        {
            "session_manifest_v1.0.0": "session_manifest_v1.1.0",
            "current_state_v1.0.0": "current_state_v1.1.0",
            "execution_checkpoint_v1.0.0": "execution_checkpoint_v1.1.0",
            "handoff_v2.0.0": "handoff_v2.1.0",
        }.get(value, value)
        for value in manifest["schema_versions"]
    ]
    dag_relative = dag_path.relative_to(root).as_posix()
    dag_ref = next(
        item for item in manifest["bootstrap_refs"] if item["ref_type"] == "dag_config"
    )
    dag_ref["relative_path"] = dag_relative
    dag_ref["content_hash"] = sha256_file(dag_path)
    manifest["manifest_hash"] = embedded_hash(manifest, "manifest_hash")
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")

    events = _jsonl(example / "events.jsonl")[:7]
    for event in events:
        event["dag_node_id"] = None
    events[0]["payload"]["manifest_hash"] = manifest["manifest_hash"]
    events[0]["path_refs"][0]["content_hash"] = sha256_file(manifest_path)
    events = _rehash_events(events)
    registry = snapshot_registry(
        example / "artifact_registry.jsonl",
        scope="run",
        expected_run_id=RUN_ID,
        head_record_id=RECORD_2,
        repository_root=root,
    )
    terminal_state = build_current_state(
        manifest,
        events,
        [registry],
        load_dag(dag_path),
    )
    sources = CheckpointSources(
        repository_root=root,
        manifest=manifest,
        events=events,
        terminal_state=terminal_state,
        registry_snapshots=(registry,),
        dag_path=dag_path,
        dag_relative_path=dag_relative,
    )

    checkpoint = build_checkpoint(
        sources,
        checkpoint_kind="stage_boundary",
        created_at="2026-01-01T00:00:08Z",
        checkpoint_id=CHECKPOINT_ID,
    )

    assert checkpoint.checkpoint_schema_version == "execution_checkpoint_v1.1.0"
    assert checkpoint.dag_ref.dag_version == "youtube_truthfulness_dag_v1.2.0"
    assert checkpoint.dag_ref.workflow_version == manifest["workflow_version"]


def _rehash_checkpoint(raw: dict[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(raw)
    changed["checkpoint_hash"] = "0" * 64
    changed["checkpoint_hash"] = embedded_hash(changed, "checkpoint_hash")
    return changed


def _rehash_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    changed = copy.deepcopy(events)
    for index, event in enumerate(changed):
        event["sequence_no"] = index + 1
        event["previous_event_id"] = changed[index - 1]["event_id"] if index else None
        event["previous_event_hash"] = (
            changed[index - 1]["event_hash"] if index else None
        )
        event["event_hash"] = "0" * 64
        event["event_hash"] = embedded_hash(event, "event_hash")
    return changed


def _isolated_sources(tmp_path: Path) -> tuple[Path, Path, CheckpointSources]:
    root = tmp_path / "repository"
    example = root / "examples" / "execution_contract" / "synthetic_run"
    shutil.copytree(EXAMPLE, example)
    return root, example, _sources(root=root, example=example)


def _derived_sources(
    root: Path,
    example: Path,
    *,
    parent_checkpoint_id: str,
    session_index: int,
    event_id_start: int,
    occurred_second_start: int,
    attempt_no: int,
) -> CheckpointSources:
    manifest = _json(example / "session_manifest.json")
    manifest["session_id"] = f"{manifest['session_id'][:-1]}{session_index}"
    manifest["attempt_no"] = attempt_no
    manifest["parent_checkpoint_id"] = parent_checkpoint_id
    manifest["bootstrap_refs"] = []
    manifest["created_at"] = f"2026-01-01T00:00:{occurred_second_start - 1:02d}Z"
    manifest["manifest_hash"] = "0" * 64
    manifest["manifest_hash"] = embedded_hash(manifest, "manifest_hash")

    manifest_relative_path = (
        (example / "sessions" / manifest["session_id"] / "session_manifest.json")
        .relative_to(root)
        .as_posix()
    )
    manifest_path = root / manifest_relative_path
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")

    events = _jsonl(example / "events.jsonl")[:7]
    for index, event in enumerate(events):
        event["event_id"] = f"{event['event_id'][:-2]}{event_id_start + index:02d}"
        event["session_id"] = manifest["session_id"]
        event["attempt_no"] = attempt_no
        event["occurred_at"] = f"2026-01-01T00:00:{occurred_second_start + index:02d}Z"
    events[0]["payload"]["manifest_hash"] = manifest["manifest_hash"]
    events[0]["payload"]["manifest_path"] = manifest_relative_path
    events[0]["path_refs"][0]["relative_path"] = manifest_relative_path
    events[0]["path_refs"][0]["content_hash"] = sha256_file(manifest_path)
    events[1]["payload"]["parent_checkpoint_id"] = parent_checkpoint_id
    events = _rehash_events(events)

    registry = snapshot_registry(
        example / "artifact_registry.jsonl",
        scope="run",
        expected_run_id=RUN_ID,
        head_record_id=RECORD_2,
        repository_root=root,
    )
    dag_path = example / "youtube_truthfulness_dag_v1_1.yaml"
    terminal_state = build_current_state(
        manifest, events, [registry], load_dag(dag_path)
    )
    return CheckpointSources(
        repository_root=root,
        manifest=manifest,
        events=events,
        terminal_state=terminal_state,
        registry_snapshots=(registry,),
        dag_path=dag_path,
        dag_relative_path=dag_path.relative_to(root).as_posix(),
    )


def _write_event_prefix(path: Path, events: Any) -> None:
    path.write_bytes(
        b"".join(canonical_json_bytes(dict(event)) + b"\n" for event in events)
    )


def test_build_binds_terminal_prefix_and_supplements_root_registry_bootstrap() -> None:
    sources = _sources()
    checkpoint = _checkpoint(sources)

    assert checkpoint.event_head.sequence_no == 7
    assert checkpoint.event_head.event_id == sources.terminal_state["as_of_event_id"]
    assert checkpoint.state_hash == sources.terminal_state["state_hash"]
    assert (
        checkpoint.state_hash
        == "fe43eff8708ccdf4795679bc402e2d2a55aca1c28dbb0353fc34a93a2e62ddf1"
    )
    assert [item.ref_type for item in checkpoint.bootstrap_refs].count("registry") == 1
    assert (
        len(checkpoint.bootstrap_refs)
        == len(_json(EXAMPLE / "session_manifest.json")["bootstrap_refs"]) + 1
    )
    validate_checkpoint(checkpoint, sources)


@pytest.mark.parametrize(
    "mutation",
    [
        "nested_extra",
        "bad_timestamp",
        "bad_id",
        "wrong_nested_type",
        "duplicate_schema_version",
    ],
)
def test_parse_checkpoint_matches_strict_nested_schema_boundaries(
    mutation: str,
) -> None:
    raw = _checkpoint().model_dump(mode="json")
    if mutation == "nested_extra":
        raw["event_head"]["unexpected"] = True
    elif mutation == "bad_timestamp":
        raw["created_at"] = "2026-02-30T00:00:00Z"
    elif mutation == "bad_id":
        raw["checkpoint_id"] = "checkpoint_not_a_canonical_ulid"
    elif mutation == "wrong_nested_type":
        raw["registry_heads"][0]["record_count"] = "2"
    else:
        raw["schema_versions"].append(raw["schema_versions"][0])
    raw = _rehash_checkpoint(raw)

    with pytest.raises(CheckpointValidationError):
        parse_checkpoint(raw)


@pytest.mark.parametrize(
    "mutation",
    ["state_hash", "event_head", "registry_head", "output_artifact", "dag_ref"],
)
def test_rehashed_checkpoint_cannot_forge_authoritative_source_cross_references(
    mutation: str,
) -> None:
    sources = _sources()
    raw = _checkpoint(sources).model_dump(mode="json")
    if mutation == "state_hash":
        raw["state_hash"] = "0" * 64
    elif mutation == "event_head":
        raw["event_head"]["sequence_no"] = 6
    elif mutation == "registry_head":
        raw["registry_heads"][0]["content_hash"] = "0" * 64
    elif mutation == "output_artifact":
        raw["output_artifacts"][0]["content_hash"] = "0" * 64
    else:
        raw["dag_ref"]["content_hash"] = "0" * 64
    raw = _rehash_checkpoint(raw)

    with pytest.raises(
        ExecutionContractError, match="mismatch|event_count|terminal event"
    ):
        validate_checkpoint(raw, sources)


def test_checkpoint_validation_ignores_shape_invalid_events_after_its_fixed_head() -> (
    None
):
    sources = _sources()
    checkpoint = _checkpoint(sources)
    with_invalid_tail = CheckpointSources(
        repository_root=sources.repository_root,
        manifest=sources.manifest,
        events=(*sources.events, {"not": "an execution event"}),
        terminal_state=sources.terminal_state,
        registry_snapshots=sources.registry_snapshots,
        dag_path=sources.dag_path,
        dag_relative_path=sources.dag_relative_path,
    )

    validate_checkpoint(checkpoint, with_invalid_tail)


def test_validation_counts_count_events_while_validator_summaries_are_distinct() -> (
    None
):
    events = _jsonl(EXAMPLE / "events.jsonl")[:7]
    duplicate_validation = copy.deepcopy(events[5])
    duplicate_validation["event_id"] = "event_01j00000000000000000000010"
    duplicate_validation["occurred_at"] = "2026-01-01T00:00:06.500Z"
    changed = _rehash_events([*events[:6], duplicate_validation, events[6]])
    sources = _sources(events=changed)

    assert sources.terminal_state["validation_summary"]["passed_count"] == 2
    assert len(sources.terminal_state["validation_summary"]["validators"]) == 1
    checkpoint = _checkpoint(sources)
    assert checkpoint.validation_summary.passed_count == 2
    assert len(checkpoint.validation_summary.validators) == 1
    validate_checkpoint(checkpoint, sources)


@pytest.mark.parametrize("array_name", ["registry_heads", "bootstrap_refs"])
def test_parse_rejects_keyed_duplicate_source_bindings(array_name: str) -> None:
    raw = _checkpoint().model_dump(mode="json")
    if array_name == "registry_heads":
        duplicate = copy.deepcopy(raw["registry_heads"][0])
        duplicate["content_hash"] = "0" * 64
        raw["registry_heads"].append(duplicate)
    else:
        duplicate = next(
            copy.deepcopy(item)
            for item in raw["bootstrap_refs"]
            if item["ref_type"] == "dag_config"
        )
        duplicate["content_hash"] = "0" * 64
        raw["bootstrap_refs"].append(duplicate)
    raw = _rehash_checkpoint(raw)

    with pytest.raises(CheckpointValidationError, match="unique|Duplicate|conflict"):
        parse_checkpoint(raw)


def test_immutable_publication_rejects_overwrite_and_preserves_original_bytes(
    tmp_path: Path,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    checkpoints_dir = example / "checkpoints"
    publication = create_checkpoint(
        checkpoints_dir,
        sources,
        checkpoint_kind="stage_boundary",
        created_at="2026-01-01T00:00:08Z",
        checkpoint_id=CHECKPOINT_ID,
    )
    original = publication.path.read_bytes()

    assert publication.file_hash == sha256_file(publication.path)
    assert publication.file_hash != publication.checkpoint.checkpoint_hash
    assert read_checkpoint(publication.path) == publication.checkpoint
    with pytest.raises(CheckpointImmutableError, match="already exists"):
        create_checkpoint(
            checkpoints_dir,
            sources,
            checkpoint_kind="stage_boundary",
            created_at="2026-01-01T00:00:08Z",
            checkpoint_id=CHECKPOINT_ID,
        )
    assert publication.path.read_bytes() == original


def test_read_checkpoint_rejects_wrong_filename_even_when_bytes_are_valid(
    tmp_path: Path,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_checkpoint(
        example / "checkpoints",
        sources,
        checkpoint_kind="stage_boundary",
        created_at="2026-01-01T00:00:08Z",
        checkpoint_id=CHECKPOINT_ID,
    )
    wrong = publication.path.with_name("checkpoint_01j00000000000000000000098.json")
    shutil.copyfile(publication.path, wrong)

    with pytest.raises(CheckpointValidationError, match="filename"):
        read_checkpoint(wrong)


def test_writeback_failure_keeps_published_evidence_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    checkpoint_id = "checkpoint_01j00000000000000000000097"
    target = example / "checkpoints" / f"{checkpoint_id}.json"

    def fail_readback(_path: Path) -> Any:
        raise CheckpointValidationError("injected read-back failure")

    monkeypatch.setattr(checkpoint_runtime, "read_checkpoint", fail_readback)
    with pytest.raises(CheckpointImmutableError, match="bytes were preserved"):
        create_checkpoint(
            example / "checkpoints",
            sources,
            checkpoint_kind="stage_boundary",
            created_at="2026-01-01T00:00:08Z",
            checkpoint_id=checkpoint_id,
        )
    assert target.is_file()
    assert target.read_bytes().endswith(b"\n")


def _checkpoint_receipt(
    tmp_path: Path,
) -> tuple[Any, Any, CheckpointSources]:
    _, example, sources = _isolated_sources(tmp_path)
    publication = create_checkpoint(
        example / "checkpoints",
        sources,
        checkpoint_kind="stage_boundary",
        created_at="2026-01-01T00:00:08Z",
        checkpoint_id=CHECKPOINT_ID,
    )
    event_path = example / "checkpoint_receipt_events.jsonl"
    _write_event_prefix(event_path, sources.events)
    event_log = EventLog(event_path, sources.manifest)
    draft = checkpoint_created_draft(publication, actor=sources.events[-1]["actor"])
    event = event_log.append(
        draft,
        event_id="event_01j00000000000000000000098",
        occurred_at="2026-01-01T00:00:08Z",
    )
    return publication, event, sources


def test_checkpoint_created_receipt_separates_embedded_and_file_hash_domains(
    tmp_path: Path,
) -> None:
    publication, event, sources = _checkpoint_receipt(tmp_path)

    validated = validate_checkpoint_created_event(publication, event, sources)
    assert (
        validated.payload["checkpoint_hash"] == publication.checkpoint.checkpoint_hash
    )
    assert validated.path_refs[0].content_hash == publication.file_hash
    assert publication.checkpoint.checkpoint_hash != publication.file_hash


@pytest.mark.parametrize(
    "mutation",
    ["embedded_hash_domain", "file_hash_domain", "time", "order", "previous_head"],
)
def test_checkpoint_created_receipt_rejects_wrong_hash_domain_or_order(
    tmp_path: Path, mutation: str
) -> None:
    publication, event, sources = _checkpoint_receipt(tmp_path)
    raw = event.model_dump(mode="json")
    if mutation == "embedded_hash_domain":
        raw["payload"]["checkpoint_hash"] = publication.file_hash
    elif mutation == "file_hash_domain":
        raw["path_refs"][0]["content_hash"] = publication.checkpoint.checkpoint_hash
    elif mutation == "time":
        raw["occurred_at"] = "2026-01-01T00:00:07.500Z"
    elif mutation == "order":
        raw["sequence_no"] = 9
    else:
        raw["previous_event_hash"] = "0" * 64
    raw["event_hash"] = "0" * 64
    raw["event_hash"] = embedded_hash(raw, "event_hash")

    with pytest.raises(
        CheckpointValidationError,
        match="payload|path ref|precede|follow|terminal|sequence|event stream|link",
    ):
        validate_checkpoint_created_event(publication, raw, sources)


def test_checkpoint_chain_validates_child_to_root_and_preserves_parent_bytes(
    tmp_path: Path,
) -> None:
    root, example, root_sources = _isolated_sources(tmp_path)
    root_id = "checkpoint_01j00000000000000000000090"
    child_id = "checkpoint_01j00000000000000000000091"
    root_publication = create_checkpoint(
        example / "checkpoints",
        root_sources,
        checkpoint_kind="stage_boundary",
        created_at="2026-01-01T00:00:08Z",
        checkpoint_id=root_id,
    )
    root_bytes = root_publication.path.read_bytes()
    child_sources = _derived_sources(
        root,
        example,
        parent_checkpoint_id=root_id,
        session_index=1,
        event_id_start=20,
        occurred_second_start=10,
        attempt_no=2,
    )
    child_publication = create_checkpoint(
        example / "checkpoints",
        child_sources,
        checkpoint_kind="retry_boundary",
        created_at="2026-01-01T00:00:18Z",
        checkpoint_id=child_id,
    )
    sources = {root_id: root_sources, child_id: child_sources}
    paths = {root_id: root_publication.path, child_id: child_publication.path}

    chain = validate_checkpoint_chain(
        child_publication.path,
        sources_for=sources.__getitem__,
        path_for=paths.__getitem__,
    )
    assert [item.checkpoint_id for item in chain] == [child_id, root_id]
    assert root_publication.path.read_bytes() == root_bytes


@pytest.mark.parametrize("failure", ["missing", "wrong_path"])
def test_checkpoint_chain_rejects_missing_or_wrong_parent_path(
    tmp_path: Path, failure: str
) -> None:
    root, example, root_sources = _isolated_sources(tmp_path)
    root_id = "checkpoint_01j00000000000000000000090"
    child_id = "checkpoint_01j00000000000000000000091"
    root_publication = create_checkpoint(
        example / "checkpoints",
        root_sources,
        checkpoint_kind="stage_boundary",
        created_at="2026-01-01T00:00:08Z",
        checkpoint_id=root_id,
    )
    child_sources = _derived_sources(
        root,
        example,
        parent_checkpoint_id=root_id,
        session_index=1,
        event_id_start=20,
        occurred_second_start=10,
        attempt_no=2,
    )
    child_publication = create_checkpoint(
        example / "checkpoints",
        child_sources,
        checkpoint_kind="retry_boundary",
        created_at="2026-01-01T00:00:18Z",
        checkpoint_id=child_id,
    )
    sources = {root_id: root_sources, child_id: child_sources}
    if failure == "missing":
        parent_path = root / "missing" / "checkpoints" / f"{root_id}.json"
    else:
        parent_path = root_publication.path.with_name("wrong.json")

    with pytest.raises(CheckpointValidationError, match="Cannot|Parent path"):
        validate_checkpoint_chain(
            child_publication.path,
            sources_for=sources.__getitem__,
            path_for=lambda _checkpoint_id: parent_path,
        )


def test_checkpoint_chain_rejects_self_parent_before_source_resolution(
    tmp_path: Path,
) -> None:
    root, example, sources = _isolated_sources(tmp_path)
    checkpoint_id = "checkpoint_01j00000000000000000000092"
    raw = _checkpoint(sources).model_dump(mode="json")
    raw["checkpoint_id"] = checkpoint_id
    raw["parent_checkpoint_id"] = checkpoint_id
    raw = _rehash_checkpoint(raw)
    path = example / "checkpoints" / f"{checkpoint_id}.json"
    path.write_bytes(canonical_json_bytes(raw) + b"\n")

    with pytest.raises(CheckpointValidationError, match="own parent"):
        validate_checkpoint_chain(
            path,
            sources_for=lambda _checkpoint_id: sources,
            path_for=lambda _checkpoint_id: path,
        )
    assert path.resolve().is_relative_to(root.resolve())


def test_checkpoint_chain_rejects_two_node_cycle(tmp_path: Path) -> None:
    root, example, _ = _isolated_sources(tmp_path)
    first_id = "checkpoint_01j00000000000000000000093"
    second_id = "checkpoint_01j00000000000000000000094"
    first_sources = _derived_sources(
        root,
        example,
        parent_checkpoint_id=second_id,
        session_index=1,
        event_id_start=20,
        occurred_second_start=20,
        attempt_no=2,
    )
    second_sources = _derived_sources(
        root,
        example,
        parent_checkpoint_id=first_id,
        session_index=2,
        event_id_start=30,
        occurred_second_start=10,
        attempt_no=3,
    )
    first = create_checkpoint(
        example / "checkpoints",
        first_sources,
        checkpoint_kind="retry_boundary",
        created_at="2026-01-01T00:00:30Z",
        checkpoint_id=first_id,
    )
    second = create_checkpoint(
        example / "checkpoints",
        second_sources,
        checkpoint_kind="retry_boundary",
        created_at="2026-01-01T00:00:19Z",
        checkpoint_id=second_id,
    )
    sources = {first_id: first_sources, second_id: second_sources}
    paths = {first_id: first.path, second_id: second.path}

    with pytest.raises(CheckpointValidationError, match="cycle"):
        validate_checkpoint_chain(
            first.path,
            sources_for=sources.__getitem__,
            path_for=paths.__getitem__,
        )


def test_static_checkpoint_negative_fixtures_are_rejected() -> None:
    invalid = EXAMPLE / "invalid"
    with pytest.raises(CheckpointValidationError, match="filename|stored"):
        read_checkpoint(invalid / "checkpoint_wrong_filename.json")
    with pytest.raises(ExecutionContractError, match="checkpoint_hash mismatch"):
        parse_checkpoint(
            json.loads(
                (invalid / "checkpoint_hash_mismatch.json").read_text(encoding="utf-8")
            )
        )


def test_static_checkpoint_and_receipt_match_authoritative_sources() -> None:
    sources = _sources()
    path = next((EXAMPLE / "checkpoints").glob("checkpoint_*.json"))
    checkpoint = read_checkpoint(path)
    validate_checkpoint(checkpoint, sources, path=path)
    rebuilt = build_checkpoint(
        sources,
        checkpoint_kind=checkpoint.checkpoint_kind,
        created_at=checkpoint.created_at,
        checkpoint_id=checkpoint.checkpoint_id,
    )
    assert rebuilt.model_dump(mode="json") == checkpoint.model_dump(mode="json")
    publication = CheckpointPublication(
        checkpoint=checkpoint,
        path=path,
        relative_path=path.relative_to(ROOT).as_posix(),
        file_hash=sha256_file(path),
    )
    validate_checkpoint_created_event(
        publication,
        _jsonl(EXAMPLE / "events.jsonl")[7],
        sources,
    )


@pytest.mark.parametrize("mutation", ["tamper", "missing"])
def test_checkpoint_recomputes_payload_hashes_before_build(
    tmp_path: Path,
    mutation: str,
) -> None:
    _, example, sources = _isolated_sources(tmp_path)
    payload = example / "artifacts" / "input.json"
    if mutation == "tamper":
        payload.write_text('{"synthetic":"tampered"}\n', encoding="utf-8")
    else:
        payload.unlink()

    with pytest.raises(
        CheckpointValidationError, match="payload.*(?:hash mismatch|missing)"
    ):
        build_checkpoint(
            sources,
            checkpoint_kind="stage_boundary",
            created_at="2026-01-01T00:00:08Z",
            checkpoint_id=CHECKPOINT_ID,
        )


def _sources_with_registry_prefix_write(
    root: Path,
    example: Path,
) -> CheckpointSources:
    sources = _sources(root=root, example=example)
    snapshot = sources.registry_snapshots[0]
    events = copy.deepcopy(list(sources.events))
    registry_write = copy.deepcopy(events[4])
    registry_write.update(
        {
            "event_id": "event_01j00000000000000000000010",
            "event_type": "artifact.written",
            "occurred_at": "2026-01-01T00:00:06.500Z",
            "artifact_refs": [],
            "path_refs": [
                {
                    "relative_path": snapshot.relative_path,
                    "content_hash_algorithm": "sha256",
                    "content_hash": snapshot.content_hash,
                    "purpose": "bind the exact append-only Registry prefix",
                }
            ],
            "payload": {
                "write_method": "append_only",
                "size_bytes": len(snapshot.prefix_bytes),
            },
        }
    )
    events = _rehash_events([*events[:6], registry_write, events[6]])
    terminal_state = build_current_state(
        sources.manifest,
        events,
        list(sources.registry_snapshots),
        load_dag(sources.dag_path),
    )
    return CheckpointSources(
        repository_root=root,
        manifest=sources.manifest,
        events=events,
        terminal_state=terminal_state,
        registry_snapshots=sources.registry_snapshots,
        dag_path=sources.dag_path,
        dag_relative_path=sources.dag_relative_path,
    )


def test_checkpoint_registry_payload_accepts_valid_history_after_frozen_prefix(
    tmp_path: Path,
) -> None:
    root, example, _ = _isolated_sources(tmp_path)
    sources = _sources_with_registry_prefix_write(root, example)
    checkpoint = _checkpoint(sources)

    snapshot = sources.registry_snapshots[0]
    current = (root / snapshot.relative_path).read_bytes()
    assert current.startswith(snapshot.prefix_bytes)
    assert len(current) > len(snapshot.prefix_bytes)
    validate_checkpoint(checkpoint, sources)


@pytest.mark.parametrize("mutation", ["prefix_tamper", "non_prefix_replacement"])
def test_checkpoint_registry_payload_rejects_changed_historical_prefix(
    tmp_path: Path,
    mutation: str,
) -> None:
    root, example, _ = _isolated_sources(tmp_path)
    sources = _sources_with_registry_prefix_write(root, example)
    checkpoint = _checkpoint(sources)
    snapshot = sources.registry_snapshots[0]
    registry_path = root / snapshot.relative_path
    if mutation == "prefix_tamper":
        registry_path.write_bytes(b" " + registry_path.read_bytes())
    else:
        registry_path.write_bytes(snapshot.prefix_bytes.splitlines(keepends=True)[0])

    with pytest.raises(
        ExecutionContractError,
        match="historical prefix mismatch|no longer matches its disk prefix|head_record_id",
    ):
        validate_checkpoint(checkpoint, sources)


def test_checkpoint_publication_preflights_declared_write_scope(tmp_path: Path) -> None:
    root, example, sources = _isolated_sources(tmp_path)
    manifest = copy.deepcopy(dict(sources.manifest))
    manifest["declared_write_set"] = [
        {
            "scope_type": "path",
            "relative_path": "examples/execution_contract/synthetic_run/artifacts/output.json",
            "purpose": "allow the business output but not a checkpoint path",
        }
    ]
    manifest["manifest_hash"] = "0" * 64
    manifest["manifest_hash"] = embedded_hash(manifest, "manifest_hash")
    events = copy.deepcopy(list(sources.events))
    events[0]["payload"]["manifest_hash"] = manifest["manifest_hash"]
    manifest_path = root / events[0]["payload"]["manifest_path"]
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")
    events[0]["path_refs"][0]["content_hash"] = sha256_file(manifest_path)
    events = _rehash_events(events)
    terminal_state = build_current_state(
        manifest,
        events,
        list(sources.registry_snapshots),
        load_dag(sources.dag_path),
    )
    restricted = CheckpointSources(
        repository_root=root,
        manifest=manifest,
        events=events,
        terminal_state=terminal_state,
        registry_snapshots=sources.registry_snapshots,
        dag_path=sources.dag_path,
        dag_relative_path=sources.dag_relative_path,
    )
    target = example / "checkpoints" / f"{CHECKPOINT_ID}.json"

    with pytest.raises(CheckpointValidationError, match="declared write scope"):
        create_checkpoint(
            example / "checkpoints",
            restricted,
            checkpoint_kind="stage_boundary",
            created_at="2026-01-01T00:00:08Z",
            checkpoint_id=CHECKPOINT_ID,
        )
    assert not target.exists()


def test_checkpoint_receipt_rebinds_publication_to_repository_root(
    tmp_path: Path,
) -> None:
    publication, event, sources = _checkpoint_receipt(tmp_path)
    forged = CheckpointPublication(
        checkpoint=publication.checkpoint,
        path=publication.path,
        relative_path="examples/execution_contract/synthetic_run/handoff.json",
        file_hash=publication.file_hash,
    )

    with pytest.raises(CheckpointValidationError, match="repository-relative path"):
        validate_checkpoint_created_event(forged, event, sources)
