"""Isolated, read-only recovery validation for the public Stage 4 bundle."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any

import pytest

from video_truthfulness.core.execution import recovery as recovery_runtime
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
)
from video_truthfulness.core.execution.recovery import (
    HandoffRecoveryResult,
    RecoveryResult,
    RecoveryValidationError,
    validate_handoff_recovery,
    validate_recovery_bundle,
)


ROOT = Path(__file__).resolve().parents[3]
BUNDLE_PREFIX = PurePosixPath("examples/execution_contract/synthetic_run")
PUBLIC_BUNDLE = ROOT.joinpath(*BUNDLE_PREFIX.parts)
CHECKPOINT_ID = "checkpoint_01j00000000000000000000000"
TASK_ID = "task_01j00000000000000000000000"
SESSION_ID = "session_01j00000000000000000000000"

REQUIRED_READ_PATHS = (
    f"{BUNDLE_PREFIX}/artifact_registry.jsonl",
    f"{BUNDLE_PREFIX}/artifacts/input.json",
    f"{BUNDLE_PREFIX}/artifacts/output.json",
    f"{BUNDLE_PREFIX}/checkpoints/{CHECKPOINT_ID}.json",
    f"{BUNDLE_PREFIX}/events.jsonl",
    f"{BUNDLE_PREFIX}/handoff.json",
    f"{BUNDLE_PREFIX}/session_manifest.json",
    f"{BUNDLE_PREFIX}/working_tree_manifest.json",
    f"{BUNDLE_PREFIX}/youtube_truthfulness_dag_v1_1.yaml",
)


def _repository_path(root: Path, relative_path: str) -> Path:
    return root.joinpath(*PurePosixPath(relative_path).parts)


def _isolated_bundle(tmp_path: Path, *, trap: bool = False) -> tuple[Path, Path]:
    isolated_root = tmp_path / "isolated_repository"
    for relative_path in REQUIRED_READ_PATHS:
        source = _repository_path(ROOT, relative_path)
        destination = _repository_path(isolated_root, relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    bundle = isolated_root.joinpath(*BUNDLE_PREFIX.parts)
    if trap:
        (bundle / "unlisted_sensitive_trap.json").write_text(
            '{"api_key":"must-not-be-read"}\n',
            encoding="utf-8",
        )
    return isolated_root, bundle


def _file_manifest(root: Path) -> dict[str, tuple[int, int, str]]:
    return {
        path.relative_to(root).as_posix(): (
            path.stat().st_size,
            path.stat().st_mtime_ns,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(item for item in root.rglob("*") if item.is_file())
    }


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def _write_jsonl(path: Path, values: list[dict[str, Any]]) -> None:
    path.write_bytes(b"".join(canonical_json_bytes(value) + b"\n" for value in values))


def _rehash(value: dict[str, Any], field: str) -> None:
    value[field] = "0" * 64
    value[field] = embedded_hash(value, field)


def _propagate_handoff_change(
    bundle: Path,
    mutation: Callable[[dict[str, Any]], None],
) -> None:
    """Keep the registration receipt coherent after a HANDOFF semantic mutation."""

    handoff_path = bundle / "handoff.json"
    handoff = _json(handoff_path)
    mutation(handoff)
    _rehash(handoff, "handoff_hash")
    _write_json(handoff_path, handoff)
    handoff_bytes = handoff_path.read_bytes()
    file_hash = hashlib.sha256(handoff_bytes).hexdigest()

    registry_path = bundle / "artifact_registry.jsonl"
    records = _jsonl(registry_path)
    record = records[2]
    record["content_hash"] = file_hash
    record["semantic_hash"] = handoff["handoff_hash"]
    record["size_bytes"] = len(handoff_bytes)
    _rehash(record, "record_hash")
    _write_jsonl(registry_path, records)

    events_path = bundle / "events.jsonl"
    events = _jsonl(events_path)
    receipt = events[8]
    receipt["artifact_refs"][0]["content_hash"] = file_hash
    receipt["path_refs"][0]["content_hash"] = file_hash
    receipt["payload"]["handoff_hash"] = handoff["handoff_hash"]
    receipt["payload"]["record_hash"] = record["record_hash"]
    _rehash(receipt, "event_hash")
    _write_jsonl(events_path, events)


def _assert_invalid(bundle: Path) -> None:
    with pytest.raises(RecoveryValidationError):
        validate_recovery_bundle(bundle)


def test_public_bundle_recovers_expected_identity_and_projections() -> None:
    result = validate_recovery_bundle(PUBLIC_BUNDLE)

    assert isinstance(result, RecoveryResult)
    summary = result.summary()
    assert summary["task_id"] == TASK_ID
    assert summary["session_id"] == SESSION_ID
    assert summary["attempt_no"] == 1
    assert summary["run_id"] == "run_01j00000000000000000000000"
    assert summary["stage_id"] == "S01"
    assert summary["status"] == "COMPLETED"
    assert summary["checkpoint_id"] == CHECKPOINT_ID
    assert summary["next_action_type"] == "return_to_stage"
    assert summary["next_stage"] == "S01"
    events = _jsonl(PUBLIC_BUNDLE / "events.jsonl")
    assert summary["source_event_id"] == "event_01j00000000000000000000008"
    assert summary["source_event_hash"] == events[7]["event_hash"]
    assert summary["receipt_event_id"] == "event_01j00000000000000000000009"
    assert summary["receipt_event_hash"] == events[8]["event_hash"]
    assert summary["registry_prefix_record_count"] == 2
    assert summary["registry_full_record_count"] == 3
    registry_lines = (
        (PUBLIC_BUNDLE / "artifact_registry.jsonl").read_bytes().splitlines()
    )
    registry_prefix = b"\n".join(registry_lines[:2]) + b"\n"
    assert (
        summary["registry_prefix_hash"] == hashlib.sha256(registry_prefix).hexdigest()
    )
    assert (
        summary["registry_full_hash"]
        == hashlib.sha256(
            (PUBLIC_BUNDLE / "artifact_registry.jsonl").read_bytes()
        ).hexdigest()
    )
    assert (
        summary["state_hash"]
        == _json(PUBLIC_BUNDLE / "current_state.json")["state_hash"]
    )
    assert (
        summary["state_bytes_sha256"]
        == hashlib.sha256(
            (PUBLIC_BUNDLE / "current_state.json").read_bytes()
        ).hexdigest()
    )
    assert (
        summary["markdown_sha256"]
        == hashlib.sha256((PUBLIC_BUNDLE / "HANDOFF.md").read_bytes()).hexdigest()
    )


def test_generic_handoff_recovery_is_exact_and_read_only() -> None:
    before = _file_manifest(PUBLIC_BUNDLE)
    result = validate_handoff_recovery(
        BUNDLE_PREFIX / "handoff.json",
        repository_root=ROOT,
    )
    after = _file_manifest(PUBLIC_BUNDLE)

    assert isinstance(result, HandoffRecoveryResult)
    assert result.required_paths == REQUIRED_READ_PATHS
    assert result.actual_paths == REQUIRED_READ_PATHS
    assert result.summary()["read_count"] == len(REQUIRED_READ_PATHS)
    assert result.summary()["write_count"] == 0
    assert result.next_action_type == "return_to_stage"
    assert after == before


def test_generic_handoff_recovery_requires_repository_relative_handoff() -> None:
    with pytest.raises(RecoveryValidationError, match="repository-relative"):
        validate_handoff_recovery(
            PUBLIC_BUNDLE / "handoff.json",
            repository_root=ROOT,
        )


@pytest.mark.parametrize(
    ("mode", "source_relative", "sentinel_relative"),
    [
        (
            "extra",
            f"{BUNDLE_PREFIX}/session_manifest.json",
            f"{BUNDLE_PREFIX}/sentinel/session_manifest.json",
        ),
        (
            "replacement",
            f"{BUNDLE_PREFIX}/session_manifest.json",
            f"{BUNDLE_PREFIX}/sentinel/session_manifest.json",
        ),
        (
            "extra",
            f"{BUNDLE_PREFIX}/events.jsonl",
            f"{BUNDLE_PREFIX}/sentinel/events.jsonl",
        ),
        (
            "replacement",
            f"{BUNDLE_PREFIX}/events.jsonl",
            f"{BUNDLE_PREFIX}/sentinel/events.jsonl",
        ),
        (
            "extra",
            f"{BUNDLE_PREFIX}/checkpoints/{CHECKPOINT_ID}.json",
            f"{BUNDLE_PREFIX}/sentinel/checkpoints/{CHECKPOINT_ID}.json",
        ),
        (
            "replacement",
            f"{BUNDLE_PREFIX}/checkpoints/{CHECKPOINT_ID}.json",
            f"{BUNDLE_PREFIX}/sentinel/checkpoints/{CHECKPOINT_ID}.json",
        ),
    ],
)
def test_generic_recovery_never_opens_same_suffix_sentinel_before_exact_set_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    source_relative: str,
    sentinel_relative: str,
) -> None:
    isolated_root, bundle = _isolated_bundle(tmp_path)
    sentinel = _repository_path(isolated_root, sentinel_relative)
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_bytes(_repository_path(isolated_root, source_relative).read_bytes())

    def mutate(handoff: dict[str, Any]) -> None:
        paths = handoff["next_action"]["required_read_paths"]
        if mode == "replacement":
            paths.remove(source_relative)
        paths.append(sentinel_relative)
        paths.sort()

    _propagate_handoff_change(bundle, mutate)
    sentinel_path = sentinel.resolve()
    original_read_bytes = Path.read_bytes
    original_open = Path.open
    sentinel_opened = False

    def guarded_read_bytes(path: Path) -> bytes:
        nonlocal sentinel_opened
        if path.resolve() == sentinel_path:
            sentinel_opened = True
        return original_read_bytes(path)

    def guarded_open(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal sentinel_opened
        if path.resolve() == sentinel_path:
            sentinel_opened = True
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    monkeypatch.setattr(Path, "open", guarded_open)
    with pytest.raises(RecoveryValidationError):
        validate_handoff_recovery(
            BUNDLE_PREFIX / "handoff.json",
            repository_root=isolated_root,
        )
    assert sentinel_opened is False


def test_v21_media_and_transcript_metadata_do_not_force_payload_reads() -> None:
    raw = _json(PUBLIC_BUNDLE / "handoff.json")
    raw["handoff_version"] = "handoff_v2.1.0"
    raw["dag_version"] = "youtube_truthfulness_dag_v1.2.0"
    raw["schema_versions"] = [
        "handoff_v2.1.0" if value == "handoff_v2.0.0" else value
        for value in raw["schema_versions"]
    ]
    raw["input_artifacts"][0]["artifact_type"] = "media.video"
    raw["output_artifacts"][0]["artifact_type"] = "transcript.raw"
    raw["next_action"]["required_input_artifact_ids"] = []
    _rehash(raw, "handoff_hash")
    handoff = recovery_runtime.parse_handoff(raw)
    handoff_relative = (
        PurePosixPath("runs/V02/synthetic/control/tasks")
        / handoff.task_id
        / "sessions"
        / handoff.session_id
        / "handoff.json"
    ).as_posix()
    expected = recovery_runtime._generic_expected_recovery_paths(
        handoff=handoff,
        manifest=recovery_runtime.validate_manifest(
            _json(PUBLIC_BUNDLE / "session_manifest.json")
        ),
        checkpoint=recovery_runtime.parse_checkpoint(
            _json(PUBLIC_BUNDLE / "checkpoints" / f"{CHECKPOINT_ID}.json")
        ),
        handoff_relative=handoff_relative,
    )

    assert raw["input_artifacts"][0]["relative_path"] not in expected
    assert raw["output_artifacts"][0]["relative_path"] not in expected
    recovery_runtime._reject_nonminimal_recovery_inputs(
        handoff, tuple(sorted(expected))
    )


def test_v21_registry_paths_are_restricted_to_their_scope_families() -> None:
    task_id = "task_01j00000000000000000000000"
    session_id = "session_01j00000000000000000000000"
    task_root = PurePosixPath("runs/V02/synthetic/control/tasks") / task_id
    handoff_relative = (task_root / "sessions" / session_id / "handoff.json").as_posix()
    handoff = SimpleNamespace(handoff_version="handoff_v2.1.0")
    recovery_runtime._validate_registry_path_family(
        handoff=handoff,
        handoff_relative=handoff_relative,
        task_root=task_root,
        source_head=SimpleNamespace(
            registry_scope="run",
            relative_path="runs/V02/synthetic/artifact_registry.jsonl",
        ),
    )

    for scope, relative_path in (
        ("run", "runs/V02/other/artifact_registry.jsonl"),
        ("cross_run", "registry/V02/other.jsonl"),
    ):
        with pytest.raises(
            RecoveryValidationError, match="canonical scope path family"
        ):
            recovery_runtime._validate_registry_path_family(
                handoff=handoff,
                handoff_relative=handoff_relative,
                task_root=task_root,
                source_head=SimpleNamespace(
                    registry_scope=scope,
                    relative_path=relative_path,
                ),
            )


def test_generic_handoff_recovery_cli_emits_canonical_summary() -> None:
    expected = (
        canonical_json_bytes(
            validate_handoff_recovery(
                BUNDLE_PREFIX / "handoff.json",
                repository_root=ROOT,
            ).summary()
        )
        + b"\n"
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "video_truthfulness.core.execution",
            "recovery",
            "validate-handoff",
            "--handoff",
            (BUNDLE_PREFIX / "handoff.json").as_posix(),
            "--repository-root",
            str(ROOT),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
    assert completed.stdout == expected
    assert completed.stderr == b""


def test_isolated_copy_reads_exact_package_and_writes_nothing(tmp_path: Path) -> None:
    isolated_root, bundle = _isolated_bundle(tmp_path, trap=True)
    before = _file_manifest(isolated_root)

    result = validate_recovery_bundle(bundle)

    after = _file_manifest(isolated_root)
    summary = result.summary()
    assert tuple(summary["required_paths"]) == REQUIRED_READ_PATHS
    assert tuple(summary["actual_paths"]) == REQUIRED_READ_PATHS
    assert summary["write_count"] == 0
    assert (
        f"{BUNDLE_PREFIX}/unlisted_sensitive_trap.json" not in summary["actual_paths"]
    )
    assert before == after
    assert not (bundle / "current_state.json").exists()
    assert not (bundle / "HANDOFF.md").exists()


def test_cli_recovers_from_an_independent_working_directory(tmp_path: Path) -> None:
    isolated_root, bundle = _isolated_bundle(tmp_path)
    outside = tmp_path / "independent_cwd"
    outside.mkdir()
    expected = canonical_json_bytes(validate_recovery_bundle(bundle).summary()) + b"\n"
    before = _file_manifest(isolated_root)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "video_truthfulness.core.execution",
            "recovery",
            "validate",
            "--bundle",
            str(bundle),
        ],
        cwd=outside,
        env=environment,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode("utf-8", errors="replace")
    assert completed.stdout == expected
    assert completed.stderr == b""
    assert _file_manifest(isolated_root) == before


def test_cli_failures_are_exit_two_with_one_lf_and_no_stdout(tmp_path: Path) -> None:
    outside = tmp_path / "independent_cwd"
    outside.mkdir()
    missing_bundle = tmp_path.joinpath(*BUNDLE_PREFIX.parts)
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    invocations = (
        ["recovery", "validate"],
        ["recovery", "validate", "--bundle", str(missing_bundle)],
    )
    for arguments in invocations:
        completed = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "video_truthfulness.core.execution",
                *arguments,
            ],
            cwd=outside,
            env=environment,
            capture_output=True,
            check=False,
        )

        assert completed.returncode == 2
        assert completed.stdout == b""
        assert completed.stderr.startswith(b"error: ")
        assert completed.stderr.endswith(b"\n")
        assert completed.stderr.count(b"\n") == 1
        assert b"\r" not in completed.stderr


@pytest.mark.parametrize(
    "relative_path", REQUIRED_READ_PATHS, ids=lambda value: value.rsplit("/", 1)[-1]
)
def test_each_required_file_is_mandatory(tmp_path: Path, relative_path: str) -> None:
    isolated_root, bundle = _isolated_bundle(tmp_path)
    _repository_path(isolated_root, relative_path).unlink()

    _assert_invalid(bundle)


@pytest.mark.parametrize(
    "relative_path", REQUIRED_READ_PATHS, ids=lambda value: value.rsplit("/", 1)[-1]
)
def test_each_authoritative_file_class_rejects_byte_tampering(
    tmp_path: Path,
    relative_path: str,
) -> None:
    isolated_root, bundle = _isolated_bundle(tmp_path)
    path = _repository_path(isolated_root, relative_path)
    path.write_bytes(path.read_bytes() + b"\nTAMPERED\n")

    _assert_invalid(bundle)


def test_broken_event_chain_is_rejected_even_with_a_valid_event_hash(
    tmp_path: Path,
) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    path = bundle / "events.jsonl"
    events = _jsonl(path)
    events[8]["previous_event_hash"] = "0" * 64
    _rehash(events[8], "event_hash")
    _write_jsonl(path, events)

    _assert_invalid(bundle)


def test_missing_handoff_created_event_is_rejected(tmp_path: Path) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    path = bundle / "events.jsonl"
    _write_jsonl(path, _jsonl(path)[:8])

    _assert_invalid(bundle)


def test_cross_object_version_mismatch_is_rejected_after_self_rehash(
    tmp_path: Path,
) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    path = bundle / "session_manifest.json"
    manifest = _json(path)
    manifest["workflow_version"] = "youtube_truthfulness_workflow_v1.0.0"
    _rehash(manifest, "manifest_hash")
    _write_json(path, manifest)

    _assert_invalid(bundle)


@pytest.mark.parametrize("domain", ["semantic", "file", "registry"])
def test_handoff_receipt_rejects_three_cross_object_hash_domain_mismatches(
    tmp_path: Path,
    domain: str,
) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    path = bundle / "events.jsonl"
    events = _jsonl(path)
    receipt = events[8]
    if domain == "semantic":
        receipt["payload"]["handoff_hash"] = "0" * 64
    elif domain == "file":
        receipt["artifact_refs"][0]["content_hash"] = "0" * 64
        receipt["path_refs"][0]["content_hash"] = "0" * 64
    else:
        receipt["payload"]["record_hash"] = "0" * 64
    _rehash(receipt, "event_hash")
    _write_jsonl(path, events)

    _assert_invalid(bundle)


@pytest.mark.parametrize("mutation", ["missing", "invalid_authority"])
def test_handoff_registry_record_three_is_required_and_validated(
    tmp_path: Path,
    mutation: str,
) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    registry_path = bundle / "artifact_registry.jsonl"
    records = _jsonl(registry_path)
    if mutation == "missing":
        _write_jsonl(registry_path, records[:2])
    else:
        records[2]["authority_level"] = "human_validated"
        _rehash(records[2], "record_hash")
        _write_jsonl(registry_path, records)
        events_path = bundle / "events.jsonl"
        events = _jsonl(events_path)
        events[8]["payload"]["record_hash"] = records[2]["record_hash"]
        _rehash(events[8], "event_hash")
        _write_jsonl(events_path, events)

    _assert_invalid(bundle)


def _missing_path(paths: list[str]) -> None:
    paths.pop()


def _extra_path(paths: list[str]) -> None:
    paths.append(f"{BUNDLE_PREFIX}/unexpected.json")


def _duplicate_path(paths: list[str]) -> None:
    paths.append(paths[0])


def _absolute_path(paths: list[str]) -> None:
    paths[-1] = "D:/private/session_manifest.json"


def _escape_path(paths: list[str]) -> None:
    paths[-1] = "../private/session_manifest.json"


def _latest_path(paths: list[str]) -> None:
    paths[-1] = "examples/latest/session_manifest.json"


@pytest.mark.parametrize(
    "mutation",
    [
        _missing_path,
        _extra_path,
        _duplicate_path,
        _absolute_path,
        _escape_path,
        _latest_path,
    ],
    ids=lambda mutation: mutation.__name__.removeprefix("_"),
)
def test_required_read_path_contract_fails_closed(
    tmp_path: Path,
    mutation: Callable[[list[str]], None],
) -> None:
    _, bundle = _isolated_bundle(tmp_path)

    def mutate(handoff: dict[str, Any]) -> None:
        mutation(handoff["next_action"]["required_read_paths"])

    _propagate_handoff_change(bundle, mutate)
    _assert_invalid(bundle)


def test_nine_path_substitution_is_rejected_before_the_trap_is_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    isolated_root, bundle = _isolated_bundle(tmp_path)
    trap = bundle / "artifacts" / "injected.json"
    trap.write_text('{"api_key":"must-not-be-read"}\n', encoding="utf-8")
    trap_relative = f"{BUNDLE_PREFIX}/artifacts/injected.json"

    def mutate(handoff: dict[str, Any]) -> None:
        paths = handoff["next_action"]["required_read_paths"]
        paths[paths.index(f"{BUNDLE_PREFIX}/artifacts/input.json")] = trap_relative

    _propagate_handoff_change(bundle, mutate)
    original_read_bytes = Path.read_bytes
    trap_path = _repository_path(isolated_root, trap_relative).resolve()
    trap_read = False

    def guarded_read_bytes(path: Path) -> bytes:
        nonlocal trap_read
        if path.resolve() == trap_path:
            trap_read = True
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    with pytest.raises(
        RecoveryValidationError, match="exactly match the frozen nine-file package"
    ):
        validate_recovery_bundle(bundle)
    assert trap_read is False


def test_required_file_symlink_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    isolated_root, bundle = _isolated_bundle(tmp_path)
    relative_path = f"{BUNDLE_PREFIX}/artifacts/input.json"
    path = _repository_path(isolated_root, relative_path)
    outside = tmp_path / "outside_input.json"
    shutil.copy2(path, outside)
    path.unlink()
    try:
        path.symlink_to(outside)
    except OSError:
        original_is_symlink = Path.is_symlink

        def simulated_is_symlink(candidate: Path) -> bool:
            return candidate == path or original_is_symlink(candidate)

        monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)

    with pytest.raises(RecoveryValidationError, match="symlink"):
        validate_recovery_bundle(bundle)


def test_persistent_input_change_is_detected_by_the_final_reread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    input_path = bundle / "artifacts" / "input.json"
    original_render = recovery_runtime.render_handoff_markdown

    def mutate_after_validation(handoff: Any) -> bytes:
        rendered = original_render(handoff)
        input_path.write_bytes(input_path.read_bytes() + b" ")
        return rendered

    monkeypatch.setattr(
        recovery_runtime, "render_handoff_markdown", mutate_after_validation
    )
    with pytest.raises(RecoveryValidationError, match="changed during validation"):
        validate_recovery_bundle(bundle)


def test_persistent_same_byte_symlink_is_rejected_by_the_final_reread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    input_path = (bundle / "artifacts" / "input.json").resolve()
    original_render = recovery_runtime.render_handoff_markdown
    original_is_symlink = Path.is_symlink
    replacement_active = False

    def simulated_is_symlink(path: Path) -> bool:
        return (
            replacement_active
            and path.resolve() == input_path
            or original_is_symlink(path)
        )

    def activate_replacement(handoff: Any) -> bytes:
        nonlocal replacement_active
        rendered = original_render(handoff)
        replacement_active = True
        return rendered

    monkeypatch.setattr(Path, "is_symlink", simulated_is_symlink)
    monkeypatch.setattr(
        recovery_runtime, "render_handoff_markdown", activate_replacement
    )
    with pytest.raises(RecoveryValidationError, match="uses a symlink"):
        validate_recovery_bundle(bundle)


def test_source_head_must_be_the_frozen_event_eight(tmp_path: Path) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    event_seven = _jsonl(bundle / "events.jsonl")[6]

    def mutate(handoff: dict[str, Any]) -> None:
        handoff["source_event_head"] = {
            key: event_seven[key]
            for key in ("event_id", "sequence_no", "event_hash", "occurred_at")
        }
        handoff["metrics"]["event_count"] = event_seven["sequence_no"]

    _propagate_handoff_change(bundle, mutate)
    with pytest.raises(
        RecoveryValidationError, match="event8 source plus event9 receipt"
    ):
        validate_recovery_bundle(bundle)


def test_handoff_receipt_must_keep_the_frozen_record_three_id(tmp_path: Path) -> None:
    _, bundle = _isolated_bundle(tmp_path)
    replacement_id = "record_01j00000000000000000000004"
    registry_path = bundle / "artifact_registry.jsonl"
    records = _jsonl(registry_path)
    records[2]["record_id"] = replacement_id
    _rehash(records[2], "record_hash")
    _write_jsonl(registry_path, records)

    events_path = bundle / "events.jsonl"
    events = _jsonl(events_path)
    events[8]["artifact_refs"][0]["record_id"] = replacement_id
    events[8]["payload"]["record_id"] = replacement_id
    events[8]["payload"]["record_hash"] = records[2]["record_hash"]
    _rehash(events[8], "event_hash")
    _write_jsonl(events_path, events)

    with pytest.raises(
        RecoveryValidationError, match="record2 prefix plus record3 receipt"
    ):
        validate_recovery_bundle(bundle)


def test_sensitive_value_is_rejected_after_receipt_rehash(tmp_path: Path) -> None:
    _, bundle = _isolated_bundle(tmp_path)

    def mutate(handoff: dict[str, Any]) -> None:
        handoff["next_action"]["reason"] = (
            "api_key=synthetic-must-never-enter-a-handoff"
        )

    _propagate_handoff_change(bundle, mutate)
    _assert_invalid(bundle)


@pytest.mark.parametrize("action_type", ["wait_for_human", "terminate"])
def test_recovery_fails_closed_for_actions_without_an_explicit_read_package(
    tmp_path: Path,
    action_type: str,
) -> None:
    _, bundle = _isolated_bundle(tmp_path)

    def mutate(handoff: dict[str, Any]) -> None:
        if action_type == "terminate":
            handoff["next_action"] = {
                "action_type": "terminate",
                "termination_kind": "project_complete",
                "reason": "synthetic closeout",
            }
            return
        decision_id = "artifact_01j00000000000000000000004"
        handoff["status"] = "WAITING_FOR_HUMAN"
        handoff["human_decisions_required"] = [
            {
                "decision_artifact_id": decision_id,
                "gate_node_id": "authorized_cookie_fallback",
                "reason": "synthetic approval is pending",
            }
        ]
        handoff["next_action"] = {
            "action_type": "wait_for_human",
            "decision_artifact_ids": [decision_id],
            "reason": "wait for the synthetic approval",
        }

    _propagate_handoff_change(bundle, mutate)
    _assert_invalid(bundle)
