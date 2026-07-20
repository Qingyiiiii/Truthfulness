"""Machine-verifiable external input cache materialization receipts."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import video_truthfulness.core.execution.materialization as materialization_module
from video_truthfulness.core.artifacts.registry import (
    AppendOnlyRegistry,
    create_artifact_record,
    create_metadata_revision,
)
from video_truthfulness.core.execution.hashing import (
    canonical_json_bytes,
    embedded_hash,
    sha256_bytes,
    sha256_file,
)
from video_truthfulness.core.execution.materialization import (
    MaterializationValidationError,
    seal_materialization_receipt,
    validate_input_materialization,
)


ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = ROOT / "examples" / "execution_contract" / "synthetic_run"
SCHEMA = ROOT / "schemas" / "execution" / "input_materialization_v1.schema.json"
SCHEMA_V1_1 = ROOT / "schemas" / "execution" / "input_materialization_v1_1.schema.json"


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _write_receipt(path: Path, raw: dict[str, Any]) -> None:
    path.write_bytes(canonical_json_bytes(raw) + b"\n")


def _rehash(raw: dict[str, Any]) -> None:
    raw["receipt_hash"] = embedded_hash(raw, "receipt_hash")


def _materialization_fixture(
    tmp_path: Path,
) -> tuple[Path, Path, Path, Path, dict[str, Any]]:
    repository = tmp_path / "repository"
    storage = tmp_path / "ubuntu_cache"
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    target_path = storage / "stage5_inputs" / "source_input.json"
    receipt_path = repository / "runtime" / "input_materialization.json"
    registry_path.parent.mkdir(parents=True)
    target_path.parent.mkdir(parents=True)
    receipt_path.parent.mkdir(parents=True)
    registry_path.write_bytes((EXAMPLE / "artifact_registry.jsonl").read_bytes())
    target_path.write_bytes((EXAMPLE / "artifacts" / "input.json").read_bytes())

    records = _jsonl(registry_path)
    source = records[0]
    head = records[-1]
    source_stat = {
        "size_bytes": source["size_bytes"],
        "mtime_utc": "2026-07-16T13:30:51.7374532Z",
        "creation_time_utc": "2026-07-16T13:30:54.9196909Z",
    }
    draft = {
        "input_materialization_version": "input_materialization_v1.0.0",
        "source_binding": {
            "run_id": source["run_id"],
            "artifact_id": source["artifact_id"],
            "record_id": source["record_id"],
            "record_hash": source["record_hash"],
            "content_hash_algorithm": "sha256",
            "content_hash": source["content_hash"],
            "size_bytes": source["size_bytes"],
            "validation_status": "passed",
            "lifecycle_state": "validated",
            "registry": {
                "registry_scope": "run",
                "relative_path": "sealed/artifact_registry.jsonl",
                "file_hash_algorithm": "sha256",
                "file_hash": sha256_file(registry_path),
                "record_count": len(records),
                "head_record_id": head["record_id"],
                "head_record_hash": head["record_hash"],
            },
        },
        "materialized_input": {
            "authority_level": "cache",
            "storage_root_ref": "ubuntu_stage5_input_cache",
            "relative_path": "stage5_inputs/source_input.json",
            "content_hash_algorithm": "sha256",
            "content_hash": source["content_hash"],
            "size_bytes": source["size_bytes"],
            "target_stat": {
                "size_bytes": target_path.stat().st_size,
                "unix_mode": format(stat.S_IMODE(target_path.stat().st_mode), "04o"),
                "uid": target_path.stat().st_uid,
                "gid": target_path.stat().st_gid,
                "regular_file": True,
                "symlink": False,
            },
        },
        "copy_evidence": {
            "operation": "copy",
            "copy_tool": "synthetic_test_copy_v1",
            "no_clobber": True,
            "destination_existed_before": False,
            "source_stat_before": source_stat,
            "source_stat_after": dict(source_stat),
            "completed_at": "2026-07-18T12:00:00Z",
        },
    }
    receipt = seal_materialization_receipt(draft).model_dump(mode="json")
    _write_receipt(receipt_path, receipt)
    return repository, storage, receipt_path, target_path, receipt


def _materialization_v11_fixture(
    tmp_path: Path,
    *,
    appended_type: str = "synthetic.audit_note",
) -> tuple[Path, Path, Path, Path, dict[str, Any]]:
    repository, storage, previous_path, target_path, receipt = _materialization_fixture(
        tmp_path
    )
    receipt_path = previous_path.with_name("input_materialization_v1_1.json")
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    records = _jsonl(registry_path)
    records[0]["artifact_type"] = "media.video"
    records[0]["validation_artifact_ids"] = [records[1]["artifact_id"]]
    records[1]["artifact_type"] = "media.validation"
    records[1]["upstream_artifact_ids"] = [records[0]["artifact_id"]]
    records[1]["validation_artifact_ids"] = []
    for index, record in enumerate(records):
        record["source_platform"] = "youtube"
        record["source_id"] = "youtube_dQw4w9WgXcQ"
        record["record_hash"] = "0" * 64
        records[index] = create_artifact_record(**record).model_dump(mode="json")
    source_record = records[0]
    registry_path.write_bytes(
        b"".join(canonical_json_bytes(row) + b"\n" for row in records)
    )
    head = records[-1]
    receipt["source_binding"].update(
        {
            "record_hash": source_record["record_hash"],
            "registry": {
                "registry_scope": "run",
                "relative_path": "sealed/artifact_registry.jsonl",
                "file_hash_algorithm": "sha256",
                "file_hash": sha256_file(registry_path),
                "record_count": len(records),
                "head_record_id": head["record_id"],
                "head_record_hash": head["record_hash"],
            },
        }
    )
    receipt = seal_materialization_receipt(receipt).model_dump(mode="json")
    _write_receipt(previous_path, receipt)

    prefix_bytes = registry_path.read_bytes()
    receipt["input_materialization_version"] = "input_materialization_v1.1.0"
    receipt["previous_receipt"] = {
        "relative_path": "runtime/input_materialization.json",
        "receipt_version": "input_materialization_v1.0.0",
        "file_hash_algorithm": "sha256",
        "file_hash": sha256_file(previous_path),
        "semantic_hash_algorithm": "sha256",
        "semantic_hash": receipt["receipt_hash"],
    }
    receipt["source_binding"]["record_revision"] = 1
    validation_record = records[1]
    receipt["source_binding"]["validation_artifact"] = {
        "artifact_id": validation_record["artifact_id"],
        "record_id": validation_record["record_id"],
        "record_revision": 1,
        "record_hash": validation_record["record_hash"],
        "content_hash_algorithm": "sha256",
        "content_hash": validation_record["content_hash"],
        "validation_status": "passed",
        "lifecycle_state": "validated",
    }
    receipt["source_binding"]["registry"] = {
        "registry_scope": "run",
        "relative_path": "sealed/artifact_registry.jsonl",
        "prefix_hash_algorithm": "sha256",
        "prefix_hash": sha256_bytes(prefix_bytes),
        "prefix_size_bytes": len(prefix_bytes),
        "prefix_record_count": len(records),
        "prefix_head_record_id": head["record_id"],
        "prefix_head_record_hash": head["record_hash"],
    }
    receipt = seal_materialization_receipt(receipt).model_dump(mode="json")
    _write_receipt(receipt_path, receipt)

    note_path = repository / "sealed" / "append-note.json"
    note_path.write_bytes(b'{"status":"append-only growth"}\n')
    template = dict(records[-1])
    template.update(
        {
            "artifact_id": "artifact_01j00000000000000000000004",
            "artifact_type": appended_type,
            "record_id": "record_01j00000000000000000000004",
            "record_revision": 1,
            "previous_record_id": None,
            "previous_record_hash": None,
            "record_hash": "0" * 64,
            "relative_path": "sealed/append-note.json",
            "content_hash": sha256_file(note_path),
            "size_bytes": note_path.stat().st_size,
            "semantic_hash_algorithm": None,
            "semantic_hash": None,
            "created_at": "2026-01-01T00:00:10Z",
            "recorded_at": "2026-01-01T00:00:11Z",
            "validated_at": "2026-01-01T00:00:11Z",
            "frozen_at": None,
            "lifecycle_state": "validated",
            "schema_versions": ["artifact_record_v1.1.0"],
            "upstream_artifact_ids": [],
            "input_fingerprint": None,
            "prompt_id": None,
        }
    )
    appended = create_artifact_record(**template)
    AppendOnlyRegistry(
        registry_path,
        scope="run",
        expected_run_id=receipt["source_binding"]["run_id"],
    ).append(appended)
    return repository, storage, receipt_path, target_path, receipt


def test_schema_and_runtime_accept_one_cache_receipt_without_absolute_paths(
    tmp_path: Path,
) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_fixture(tmp_path)
    schema = _json(SCHEMA)
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(receipt)

    result = validate_input_materialization(
        receipt_path,
        repository_root=repository,
        storage_root=storage,
    )

    summary = result.summary()
    assert summary["status"] == "VALID"
    assert summary["authority_level"] == "cache"
    assert summary["read_count"] == 3
    assert summary["write_count"] == 0
    assert summary["content_hash"] == receipt["source_binding"]["content_hash"]
    assert b"/home/" not in receipt_path.read_bytes()
    assert b"D:\\" not in receipt_path.read_bytes()


def test_v11_receipt_accepts_append_only_registry_growth(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_v11_fixture(
        tmp_path
    )
    schema = _json(SCHEMA_V1_1)
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(receipt)

    result = validate_input_materialization(
        receipt_path,
        repository_root=repository,
        storage_root=storage,
    )

    assert result.summary()["registry_binding_mode"] == "immutable_prefix"
    assert result.summary()["registry_prefix_record_count"] == 3


def test_v11_receipt_binds_exact_v10_predecessor(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_v11_fixture(
        tmp_path
    )
    previous_path = repository / receipt["previous_receipt"]["relative_path"]
    previous_path.write_bytes(previous_path.read_bytes() + b"\n")

    with pytest.raises(
        MaterializationValidationError, match="Previous receipt file hash"
    ):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_v11_receipt_binds_original_media_validation_record(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_v11_fixture(
        tmp_path
    )
    receipt["source_binding"]["validation_artifact"]["record_hash"] = "f" * 64
    _rehash(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(MaterializationValidationError, match="media.validation"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_v11_published_receipt_revalidation_reuses_one_content_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository, storage, receipt_path, target_path, _ = _materialization_v11_fixture(
        tmp_path
    )
    first = validate_input_materialization(
        receipt_path,
        repository_root=repository,
        storage_root=storage,
    )

    def reject_second_hash(path: Path) -> str:
        if Path(path) == target_path:
            raise AssertionError("materialized content was opened twice")
        return sha256_file(Path(path))

    monkeypatch.setattr(materialization_module, "sha256_file", reject_second_hash)
    second = validate_input_materialization(
        receipt_path,
        repository_root=repository,
        storage_root=storage,
        content_proof=first.content_proof,
    )

    assert second.content_proof == first.content_proof


def test_v11_content_proof_rejects_target_identity_change(tmp_path: Path) -> None:
    repository, storage, receipt_path, target_path, _ = _materialization_v11_fixture(
        tmp_path
    )
    first = validate_input_materialization(
        receipt_path,
        repository_root=repository,
        storage_root=storage,
    )
    original = target_path.read_bytes()
    target_path.write_bytes(b"X" + original[1:])

    with pytest.raises(MaterializationValidationError, match="identity changed"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
            content_proof=first.content_proof,
        )


def test_v11_receipt_rejects_prefix_mutation_after_append(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, _ = _materialization_v11_fixture(tmp_path)
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    rows = registry_path.read_bytes().splitlines(keepends=True)
    rows[0] = rows[0].replace(
        b"Synthetic execution input", b"Synthetic execution Input"
    )
    registry_path.write_bytes(b"".join(rows))

    with pytest.raises(MaterializationValidationError):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_v11_receipt_hashes_the_physical_prefix_not_parsed_records(
    tmp_path: Path,
) -> None:
    repository, storage, receipt_path, _, _ = _materialization_v11_fixture(tmp_path)
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    rows = registry_path.read_bytes().splitlines(keepends=True)
    first = json.loads(rows[0])
    reordered = dict(reversed(list(first.items())))
    rows[0] = (
        json.dumps(
            reordered,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    registry_path.write_bytes(b"".join(rows))

    with pytest.raises(MaterializationValidationError, match="prefix hash"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_v11_receipt_cannot_reseal_a_noncanonical_physical_prefix(
    tmp_path: Path,
) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_v11_fixture(
        tmp_path
    )
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    rows = registry_path.read_bytes().splitlines(keepends=True)
    first = json.loads(rows[0])
    rows[0] = json.dumps(first, ensure_ascii=False).encode("utf-8") + b"\n"
    registry_path.write_bytes(b"".join(rows))
    prefix_count = receipt["source_binding"]["registry"]["prefix_record_count"]
    prefix_bytes = b"".join(rows[:prefix_count])
    binding = receipt["source_binding"]["registry"]
    binding["prefix_size_bytes"] = len(prefix_bytes)
    binding["prefix_hash"] = sha256_bytes(prefix_bytes)
    _rehash(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(MaterializationValidationError, match="exact canonical JSONL"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_v11_receipt_cannot_end_the_prefix_before_its_declared_lf(
    tmp_path: Path,
) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_v11_fixture(
        tmp_path
    )
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    binding = receipt["source_binding"]["registry"]
    truncated_size = binding["prefix_size_bytes"] - 1
    truncated_prefix = registry_path.read_bytes()[:truncated_size]
    binding["prefix_size_bytes"] = truncated_size
    binding["prefix_hash"] = sha256_bytes(truncated_prefix)
    _rehash(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(MaterializationValidationError, match="exact canonical JSONL"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_v11_receipt_rejects_second_run_media_video(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, _ = _materialization_v11_fixture(
        tmp_path,
        appended_type="media.video",
    )

    with pytest.raises(MaterializationValidationError, match="second source Artifact"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_v11_receipt_rejects_source_revision_after_prefix(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_v11_fixture(
        tmp_path
    )
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    registry = AppendOnlyRegistry(
        registry_path,
        scope="run",
        expected_run_id=receipt["source_binding"]["run_id"],
    )
    source = registry.read_entries()[0].wire_record
    registry.append(
        create_metadata_revision(
            source,
            metadata_revision_reason="synthetic drift after sealed prefix",
            logical_name="Mutated source metadata",
        )
    )

    with pytest.raises(MaterializationValidationError, match="new revision"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


@pytest.mark.parametrize(
    ("mutation", "expected_fragment"),
    [
        (
            lambda raw: raw["materialized_input"].update(
                {"authority_level": "machine_derived"}
            ),
            "cache",
        ),
        (
            lambda raw: raw["materialized_input"].update(
                {"relative_path": "/home/example/source.mp4"}
            ),
            "relative_path",
        ),
        (
            lambda raw: raw["copy_evidence"].update({"no_clobber": False}),
            "True",
        ),
        (
            lambda raw: raw["copy_evidence"].update(
                {"destination_existed_before": True}
            ),
            "False",
        ),
        (
            lambda raw: raw["source_binding"].update({"validation_status": "failed"}),
            "passed",
        ),
    ],
)
def test_schema_and_runtime_reject_non_cache_or_unsafe_copy_claims(
    tmp_path: Path,
    mutation: Callable[[dict[str, Any]], None],
    expected_fragment: str,
) -> None:
    _, _, _, _, receipt = _materialization_fixture(tmp_path)
    mutation(receipt)
    _rehash(receipt)
    schema = _json(SCHEMA)

    assert list(Draft202012Validator(schema).iter_errors(receipt))
    with pytest.raises(MaterializationValidationError, match=expected_fragment):
        seal_materialization_receipt(receipt)


def test_runtime_rejects_source_stat_change_even_after_receipt_rehash(
    tmp_path: Path,
) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_fixture(tmp_path)
    receipt["copy_evidence"]["source_stat_after"]["mtime_utc"] = (
        "2026-07-16T13:30:51.7374533Z"
    )
    _rehash(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(MaterializationValidationError, match="source stat changed"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_runtime_rejects_registry_drift_after_receipt_seal(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, _ = _materialization_fixture(tmp_path)
    registry_path = repository / "sealed" / "artifact_registry.jsonl"
    registry_path.write_bytes(registry_path.read_bytes() + b"\n")

    with pytest.raises(MaterializationValidationError, match="Registry file hash"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_runtime_rejects_registry_record_identity_substitution(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_fixture(tmp_path)
    records = _jsonl(repository / "sealed" / "artifact_registry.jsonl")
    receipt["source_binding"]["artifact_id"] = records[1]["artifact_id"]
    _rehash(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(
        MaterializationValidationError, match="Artifact or run identity"
    ):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_runtime_rejects_registry_lifecycle_substitution(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_fixture(tmp_path)
    receipt["source_binding"]["lifecycle_state"] = "frozen"
    _rehash(receipt)
    _write_receipt(receipt_path, receipt)

    with pytest.raises(MaterializationValidationError, match="lifecycle state"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_runtime_rejects_materialized_content_drift(tmp_path: Path) -> None:
    repository, storage, receipt_path, target_path, _ = _materialization_fixture(
        tmp_path
    )
    target_path.write_bytes(b"changed after copy\n")

    with pytest.raises(MaterializationValidationError, match="does not match"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_runtime_rejects_materialized_symlink(tmp_path: Path) -> None:
    repository, storage, receipt_path, target_path, _ = _materialization_fixture(
        tmp_path
    )
    backing = target_path.with_name("backing.json")
    backing.write_bytes(target_path.read_bytes())
    target_path.unlink()
    try:
        target_path.symlink_to(backing.name)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    with pytest.raises(MaterializationValidationError, match="symlink or junction"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_runtime_rejects_noncanonical_receipt_bytes(tmp_path: Path) -> None:
    repository, storage, receipt_path, _, receipt = _materialization_fixture(tmp_path)
    receipt_path.write_text(json.dumps(receipt, indent=2), encoding="utf-8")

    with pytest.raises(MaterializationValidationError, match="canonical JSON"):
        validate_input_materialization(
            receipt_path,
            repository_root=repository,
            storage_root=storage,
        )


def test_materialization_cli_is_read_only_and_emits_canonical_summary(
    tmp_path: Path,
) -> None:
    repository, storage, receipt_path, _, _ = _materialization_fixture(tmp_path)
    expected = (
        canonical_json_bytes(
            validate_input_materialization(
                receipt_path,
                repository_root=repository,
                storage_root=storage,
            ).summary()
        )
        + b"\n"
    )
    before = {
        path.relative_to(tmp_path).as_posix(): sha256_file(path)
        for path in tmp_path.rglob("*")
        if path.is_file()
    }
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "video_truthfulness.core.execution",
            "materialization",
            "validate",
            "--receipt",
            str(receipt_path),
            "--repository-root",
            str(repository),
            "--storage-root",
            str(storage),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        check=False,
    )
    after = {
        path.relative_to(tmp_path).as_posix(): sha256_file(path)
        for path in tmp_path.rglob("*")
        if path.is_file()
    }

    assert completed.returncode == 0, completed.stderr.decode(errors="replace")
    assert completed.stdout == expected
    assert completed.stderr == b""
    assert after == before
