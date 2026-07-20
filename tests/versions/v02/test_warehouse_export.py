from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from video_truthfulness.versions.v02.warehouse_export import (
    INLINE_TEXT_MAX_BYTES,
    canonicalize_export,
    chunk_utf8_text,
    read_export_package,
    resolve_external_storage_ref,
    text_metrics,
    write_export_package,
)
from video_truthfulness.versions.v02.warehouse_models import (
    DATABASE_SCHEMA_VERSION,
    LABEL_TAXONOMY_VERSION,
    ExternalStorageRef,
    WarehouseConflictError,
    WarehouseContractError,
    WarehouseRow,
    deterministic_typed_id,
)


ROOT = Path(__file__).resolve().parents[3]
NOW = "2026-07-20T00:00:00Z"
SHA = "0" * 64


def _sample_result(*, raw_text: str = "完整原文🙂"):
    run_id = deterministic_typed_id("run", "export-run")
    source_id = "youtube_abcdefghijk"
    artifact_id = deterministic_typed_id("artifact", "export-artifact")
    record_id = deterministic_typed_id("record", "export-record")
    parent_claim_id = deterministic_typed_id("claim", "export-parent")
    parent_revision_id = deterministic_typed_id(
        "parent_claim_revision", "export-parent-r1"
    )
    metrics = text_metrics(raw_text)
    rows = [
        WarehouseRow.build(
            logical_layer="core_provenance",
            table_code="source_media",
            canonical_primary_key=source_id,
            revision_no=1,
            is_active=True,
            effective_at=NOW,
            run_id=run_id,
            artifact_id=artifact_id,
            artifact_record_id=record_id,
            artifact_content_hash=SHA,
            created_at=NOW,
            writer_role="s01_acquisition_writer",
            schema_versions={"database": DATABASE_SCHEMA_VERSION},
            taxonomy_versions={"label": LABEL_TAXONOMY_VERSION},
            data={
                "source_id": source_id,
                "platform": "youtube",
                "platform_source_key": "abcdefghijk",
                "media_kind": "video",
                "synthetic": True,
            },
        ),
        WarehouseRow.build(
            logical_layer="core_provenance",
            table_code="parent_claim_revision",
            canonical_primary_key=parent_revision_id,
            revision_no=1,
            is_active=True,
            effective_at=NOW,
            run_id=run_id,
            artifact_id=artifact_id,
            artifact_record_id=record_id,
            artifact_content_hash=SHA,
            created_at=NOW,
            writer_role="claim_extractor",
            schema_versions={"database": DATABASE_SCHEMA_VERSION},
            taxonomy_versions={"label": LABEL_TAXONOMY_VERSION},
            data={
                "source_id": source_id,
                "revision": {
                    "parent_claim_id": parent_claim_id,
                    "parent_revision_id": parent_revision_id,
                    "revision_no": 1,
                    "supersedes_revision_id": None,
                    "display_no": 1,
                    "text": {
                        **metrics,
                        "inline_text": raw_text,
                        "chunks": [],
                    },
                    "normalized_text": None,
                    "preview": None,
                    "source_spans": [{"start_ms": 0, "end_ms": 1000}],
                    "taxonomy_version": LABEL_TAXONOMY_VERSION,
                    "writer_role": "claim_extractor",
                },
            },
        ),
    ]
    export_id = deterministic_typed_id("export", "sample-export")
    return canonicalize_export(
        rows,
        export_id=export_id,
        run_id=run_id,
        source_registry_prefix={
            "record_count": 2,
            "prefix_hash": SHA,
            "head_record_id": record_id,
            "head_record_hash": SHA,
        },
        input_artifacts=[
            {
                "artifact_id": artifact_id,
                "record_id": record_id,
                "artifact_type": "claim.collection",
                "content_hash": SHA,
            }
        ],
        run_created_at=NOW,
        created_at=NOW,
        storage_ref={
            "storage_root_ref": "ubuntu_v02_claim_warehouse",
            "relative_path": f"exports/{export_id}",
        },
    )


def test_export_is_byte_deterministic_and_sorted() -> None:
    first = _sample_result()
    second = canonicalize_export(
        reversed(first.rows),
        export_id=first.manifest.export_id,
        run_id=first.manifest.run_id,
        source_registry_prefix=first.manifest.source_registry_prefix,
        input_artifacts=reversed(first.manifest.input_artifacts),
        run_created_at=first.manifest.run_created_at,
        created_at=first.manifest.created_at,
        storage_ref={
            "storage_root_ref": first.manifest.storage_root_ref,
            "relative_path": str(Path(first.manifest.manifest_relative_path).parent).replace("\\", "/"),
        },
        schema_versions=first.manifest.schema_versions,
        taxonomy_versions=first.manifest.taxonomy_versions,
        exporter_versions=first.manifest.exporter_versions,
    )
    assert first.manifest_bytes == second.manifest_bytes
    assert first.rows_bytes == second.rows_bytes
    identities = [
        (row.logical_layer, row.table_code, row.canonical_primary_key)
        for row in first.rows
    ]
    assert identities == sorted(identities)


def test_export_publish_read_and_equal_replay(tmp_path: Path) -> None:
    root = tmp_path / "warehouse"
    root.mkdir()
    roots = {"ubuntu_v02_claim_warehouse": root}
    result = _sample_result()
    package_ref = write_export_package(result, storage_roots=roots)
    assert write_export_package(result, storage_roots=roots) == package_ref
    observed = read_export_package(
        storage_roots=roots,
        manifest_ref=ExternalStorageRef(
            storage_root_ref=result.manifest.storage_root_ref,
            relative_path=result.manifest.manifest_relative_path,
        ),
        expected_manifest_hash=result.manifest_hash,
    )
    assert observed.manifest_bytes == result.manifest_bytes
    assert observed.rows_bytes == result.rows_bytes
    assert observed.rows[0].data == result.rows[0].data


def test_export_no_clobber_detects_changed_rows(tmp_path: Path) -> None:
    root = tmp_path / "warehouse"
    root.mkdir()
    roots = {"ubuntu_v02_claim_warehouse": root}
    result = _sample_result()
    package_ref = write_export_package(result, storage_roots=roots)
    rows_path = root / package_ref.relative_path / "rows.jsonl"
    rows_path.write_bytes(rows_path.read_bytes() + b"{}\n")
    with pytest.raises(WarehouseConflictError, match="rows.jsonl"):
        write_export_package(result, storage_roots=roots)


def test_long_claim_inline_and_chunk_boundaries_are_exact() -> None:
    inline = "🙂" * 65_536
    chunked = "🙂" * 65_537
    assert len(inline) == 65_536
    assert len(inline.encode("utf-8")) == INLINE_TEXT_MAX_BYTES
    assert chunk_utf8_text(inline) == ()
    chunks = chunk_utf8_text(chunked)
    assert len(chunked.encode("utf-8")) == 262_148
    assert [item.chunk_index for item in chunks] == list(range(len(chunks)))
    assert b"".join(item.text.encode("utf-8") for item in chunks) == chunked.encode("utf-8")
    assert chunks[0].byte_start == 0
    assert chunks[-1].byte_end == 262_148


def test_65536_mixed_character_claim_is_preserved_without_normalization() -> None:
    pattern = '中A🙂e\u0301"\n'
    raw = (pattern * ((65_536 // len(pattern)) + 1))[:65_536]
    result = _sample_result(raw_text=raw)
    row = next(item for item in result.rows if item.table_code == "parent_claim_revision")
    text = row.data["revision"]["text"]
    assert len(text["inline_text"]) == 65_536
    assert text["inline_text"] == raw
    assert text["text_sha256"] == text_metrics(raw)["text_sha256"]


def test_business_payload_has_only_frozen_canonical_keys() -> None:
    payload = _sample_result().business_payload()
    assert set(payload) == {
        "export_id",
        "run_id",
        "storage_root_ref",
        "manifest_relative_path",
        "manifest_hash",
        "rows_relative_path",
        "rows_hash",
        "logical_hash",
        "row_count",
        "row_counts",
        "schema_versions",
        "taxonomy_versions",
        "exporter_versions",
        "projection_status",
    }
    assert payload["projection_status"] == "pending"
    assert payload["exporter_versions"] == {
        "warehouse_exporter": "warehouse_export_v1.0.0"
    }
    assert "manifest_sha256" not in payload and "rows_sha256" not in payload


def test_manifest_binds_authoritative_run_time_and_export_idempotency_key() -> None:
    result = _sample_result()
    assert result.manifest.run_created_at == NOW
    assert len(result.manifest.export_idempotency_key) == 64
    tampered = result.manifest.model_dump(mode="json")
    tampered["export_idempotency_key"] = "0" * 64
    with pytest.raises(ValueError, match="export_idempotency_key mismatch"):
        type(result.manifest).model_validate(tampered)


def test_external_ref_rejects_unknown_root_and_symlink(tmp_path: Path) -> None:
    root = tmp_path / "warehouse"
    root.mkdir()
    with pytest.raises(WarehouseContractError, match="unknown storage_root_ref"):
        resolve_external_storage_ref(
            {"ubuntu_v02_claim_warehouse": root},
            ExternalStorageRef(storage_root_ref="unknown_root", relative_path="x/y"),
        )
    target = root / "target"
    target.mkdir()
    link = root / "link"
    try:
        os.symlink(target, link, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows test host")
    with pytest.raises(WarehouseContractError, match="symlink"):
        resolve_external_storage_ref(
            {"ubuntu_v02_claim_warehouse": root},
            ExternalStorageRef(
                storage_root_ref="ubuntu_v02_claim_warehouse",
                relative_path="link/file.json",
            ),
        )


def test_external_ref_rejects_symlink_storage_root_before_resolve(
    tmp_path: Path,
) -> None:
    target = tmp_path / "real-warehouse"
    target.mkdir()
    root_link = tmp_path / "warehouse-link"
    try:
        os.symlink(target, root_link, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows test host")
    with pytest.raises(WarehouseContractError, match="cannot be symlinks"):
        resolve_external_storage_ref(
            {"ubuntu_v02_claim_warehouse": root_link},
            ExternalStorageRef(
                storage_root_ref="ubuntu_v02_claim_warehouse",
                relative_path="exports/export_00000000000000000000000000/manifest.json",
            ),
        )


def test_resolver_checks_storage_root_symlink_before_resolve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = (Path.cwd() / "synthetic-root-link").absolute()
    original_lexists = os.path.lexists
    original_lstat = os.lstat

    def fake_lexists(path: os.PathLike[str] | str) -> bool:
        return Path(path) == root or original_lexists(path)

    def fake_lstat(path: os.PathLike[str] | str):
        if Path(path) == root:
            return type("SyntheticStat", (), {"st_mode": stat.S_IFLNK})()
        return original_lstat(path)

    monkeypatch.setattr(os.path, "lexists", fake_lexists)
    monkeypatch.setattr(os, "lstat", fake_lstat)
    with pytest.raises(WarehouseContractError, match="cannot be symlinks"):
        resolve_external_storage_ref(
            {"ubuntu_v02_claim_warehouse": root},
            ExternalStorageRef(
                storage_root_ref="ubuntu_v02_claim_warehouse",
                relative_path="exports/export_00000000000000000000000000/manifest.json",
            ),
        )


def test_resolver_rejects_internal_symlink_component_without_following_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path.cwd().resolve()
    synthetic_link = root / "synthetic-internal-link"
    original_lexists = os.path.lexists
    original_lstat = os.lstat

    def fake_lexists(path: os.PathLike[str] | str) -> bool:
        return Path(path) == synthetic_link or original_lexists(path)

    def fake_lstat(path: os.PathLike[str] | str):
        if Path(path) == synthetic_link:
            return type("SyntheticStat", (), {"st_mode": stat.S_IFLNK})()
        return original_lstat(path)

    monkeypatch.setattr(os.path, "lexists", fake_lexists)
    monkeypatch.setattr(os, "lstat", fake_lstat)
    with pytest.raises(WarehouseContractError, match="symlink components"):
        resolve_external_storage_ref(
            {"ubuntu_v02_claim_warehouse": root},
            ExternalStorageRef(
                storage_root_ref="ubuntu_v02_claim_warehouse",
                relative_path="synthetic-internal-link/manifest.json",
            ),
        )


def test_export_manifest_schema_accepts_canonical_result() -> None:
    schema = json.loads(
        (ROOT / "schemas/warehouse/claim_warehouse_export_manifest_v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(
        _sample_result().manifest.model_dump(mode="json")
    )
