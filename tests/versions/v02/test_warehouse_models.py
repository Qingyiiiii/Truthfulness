from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from video_truthfulness.versions.v02.warehouse_models import (
    DATABASE_SCHEMA_VERSION,
    CONTROL_LEDGER_TABLE_CODES,
    EXACT_SCALE_COUNTS,
    LABEL_TAXONOMY_VERSION,
    ExternalStorageRef,
    ParquetFileDescriptor,
    RegistryPrefix,
    WarehouseContractError,
    WarehouseExportBinding,
    WarehouseLoadBatchV1,
    WarehouseLoadPlanV1,
    WarehouseLoadReceiptV1,
    WarehouseProjectionAttemptV1,
    WarehouseRow,
    TABLE_DATA_MODELS,
    WAREHOUSE_ENTITY_CODES,
    build_exact_scale_bindings,
    deterministic_typed_id,
    split_export_bindings,
    validate_exact_scale_counts,
)


ROOT = Path(__file__).resolve().parents[3]
NOW = "2026-07-20T00:00:00Z"
SHA = "0" * 64


def _binding(seed: str = "one") -> WarehouseExportBinding:
    export_id = deterministic_typed_id("export", seed)
    run_id = deterministic_typed_id("run", seed)
    return WarehouseExportBinding(
        export_id=export_id,
        export_idempotency_key=hashlib.sha256(seed.encode("utf-8")).hexdigest(),
        source_run_id=run_id,
        source_registry_ref=ExternalStorageRef(
            storage_root_ref="repository",
            relative_path=f"runs/V02/{run_id}/artifact_registry.jsonl",
        ),
        logical_layer="core_provenance",
        storage_root_ref="ubuntu_v02_claim_warehouse",
        manifest_relative_path=f"exports/{export_id}/manifest.json",
        manifest_hash="1" * 64,
        rows_hash="2" * 64,
        logical_hash="2" * 64,
        row_count=2,
    )


def _plan() -> WarehouseLoadPlanV1:
    plan_id = deterministic_typed_id("load_plan", "plan")
    return WarehouseLoadPlanV1.build(
        load_plan_id=plan_id,
        created_at=NOW,
        storage_root_ref="ubuntu_v02_claim_warehouse",
        plan_relative_path=f"receipts/{plan_id}.json",
        exports=[_binding()],
    )


def _receipt() -> WarehouseLoadReceiptV1:
    plan = _plan()
    batch = WarehouseLoadBatchV1.build(
        load_batch_id=deterministic_typed_id("load_batch", "batch"),
        load_plan_id=plan.load_plan_id,
        load_plan_hash=plan.plan_hash,
        started_at=NOW,
        completed_at="2026-07-20T00:01:00Z",
        export_count=1,
        row_count=2,
        logical_hash="3" * 64,
    )
    descriptor = ParquetFileDescriptor(
        logical_layer="core_provenance",
        table_code="source_media",
        export_id=plan.exports[0].export_id,
        relative_path=(
            "parquet/logical_layer=core_provenance/table_code=source_media/"
            f"schema_version={DATABASE_SCHEMA_VERSION}/run_date=2026-07-20/"
            f"export_id={plan.exports[0].export_id}/part-00000.parquet"
        ),
        size_bytes=10,
        file_hash="4" * 64,
        row_count=2,
        row_logical_hash="5" * 64,
    )
    receipt_id = deterministic_typed_id("load_receipt", "receipt")
    return WarehouseLoadReceiptV1.build(
        receipt_id=receipt_id,
        receipt_relative_path=f"receipts/{receipt_id}.json",
        load_batch=batch,
        exports=list(plan.exports),
        parquet_manifest=[descriptor],
        row_counts={"source_media": 2},
        watermark={"core_provenance": plan.exports[0].export_id},
        dependency_versions={"pyarrow": "21.0.0", "duckdb": "1.5.1"},
        duckdb_transaction_marker="6" * 64,
    )


def test_row_is_strict_json_and_self_hashed() -> None:
    row = WarehouseRow.build(
        logical_layer="core_provenance",
        table_code="source_media",
        canonical_primary_key="youtube_abcdefghijk",
        revision_no=1,
        is_active=True,
        effective_at=NOW,
        run_id=deterministic_typed_id("run", "run"),
        artifact_id=deterministic_typed_id("artifact", "artifact"),
        artifact_record_id=deterministic_typed_id("record", "record"),
        artifact_content_hash=SHA,
        created_at=NOW,
        writer_role="s01_acquisition_writer",
        schema_versions={"database": DATABASE_SCHEMA_VERSION},
        taxonomy_versions={"label": LABEL_TAXONOMY_VERSION},
        data={
            "source_id": "youtube_abcdefghijk",
            "platform": "youtube",
            "platform_source_key": "abcdefghijk",
            "media_kind": "video",
            "synthetic": True,
        },
    )
    assert row.row_hash != SHA
    assert b"\\ud83d" not in row.canonical_bytes()
    tampered = row.model_dump(mode="json")
    tampered["data"]["platform_source_key"] = "changed"
    with pytest.raises(ValueError, match="row_hash mismatch"):
        WarehouseRow.model_validate(tampered)
    with pytest.raises(ValueError, match="NaN"):
        WarehouseRow.build(**{**tampered, "row_hash": None, "data": {"x": float("nan")}})


@pytest.mark.parametrize(
    "path",
    ["/absolute/file", "../escape", "safe/../../escape", "C:/private/file", r"safe\file"],
)
def test_external_storage_ref_rejects_unsafe_paths(path: str) -> None:
    with pytest.raises(ValueError):
        ExternalStorageRef(
            storage_root_ref="ubuntu_v02_claim_warehouse", relative_path=path
        )


def test_registry_prefix_is_complete_and_uses_prefix_not_current_head() -> None:
    empty_hash = hashlib.sha256(b"").hexdigest()
    assert RegistryPrefix(record_count=0, prefix_hash=empty_hash).head_record_id is None
    with pytest.raises(ValueError):
        RegistryPrefix(record_count=1)
    prefix = RegistryPrefix(
        record_count=10,
        prefix_hash=SHA,
        head_record_id=deterministic_typed_id("record", "head"),
        head_record_hash=SHA,
    )
    assert prefix.record_count == 10


def test_frozen_catalog_has_34_strict_exports_and_8_control_ledgers() -> None:
    assert len(TABLE_DATA_MODELS) == 34
    assert len(CONTROL_LEDGER_TABLE_CODES) == 8
    assert len(WAREHOUSE_ENTITY_CODES) == 42
    assert set(TABLE_DATA_MODELS).isdisjoint(CONTROL_LEDGER_TABLE_CODES)


def test_claim_warehouse_models_reject_unknown_storage_root() -> None:
    raw = _binding().model_dump(mode="json")
    raw["storage_root_ref"] = "unknown_root"
    with pytest.raises(ValueError):
        WarehouseExportBinding.model_validate(raw)
    plan = _plan().model_dump(mode="json")
    plan["storage_root_ref"] = "unknown_root"
    with pytest.raises(ValueError):
        WarehouseLoadPlanV1.model_validate(plan)


def test_load_plan_is_canonical_hashed_and_bounded() -> None:
    first = _binding("z")
    second_raw = _binding("a").model_dump(mode="json")
    second_raw["logical_layer"] = "human_annotation"
    second = WarehouseExportBinding.model_validate(second_raw)
    plan_id = deterministic_typed_id("load_plan", "ordered")
    plan = WarehouseLoadPlanV1.build(
        load_plan_id=plan_id,
        created_at=NOW,
        storage_root_ref="ubuntu_v02_claim_warehouse",
        plan_relative_path=f"receipts/{plan_id}.json",
        exports=[second, first],
    )
    assert [item.logical_layer for item in plan.exports] == [
        "core_provenance",
        "human_annotation",
    ]
    tampered = plan.model_dump(mode="json")
    tampered["ordered_export_set_hash"] = SHA
    with pytest.raises(ValueError, match="ordered_export_set_hash"):
        WarehouseLoadPlanV1.model_validate(tampered)


def test_attempt_and_receipt_embedded_hashes_are_fail_closed() -> None:
    receipt = _receipt()
    attempt = WarehouseProjectionAttemptV1.build(
        attempt_id=deterministic_typed_id("attempt", "attempt"),
        load_plan_id=receipt.load_batch.load_plan_id,
        load_plan_hash=receipt.load_batch.load_plan_hash,
        attempt_no=1,
        status="succeeded",
        last_completed_stage="registry_append",
        started_at=NOW,
        completed_at="2026-07-20T00:02:00Z",
        error_code=None,
        error_message=None,
        receipt_id=receipt.receipt_id,
        receipt_hash=receipt.receipt_hash,
    )
    assert receipt.receipt_hash != SHA
    assert attempt.attempt_hash != SHA
    bad = attempt.model_dump(mode="json")
    bad["status"] = "failed"
    with pytest.raises(ValueError):
        WarehouseProjectionAttemptV1.model_validate(bad)


def test_frozen_501_counts_and_global_batches_are_exact() -> None:
    validate_exact_scale_counts(dict(EXACT_SCALE_COUNTS))
    with pytest.raises(WarehouseContractError, match="501 scale mismatch"):
        validate_exact_scale_counts({**EXACT_SCALE_COUNTS, "load_batches": 11})
    bindings = build_exact_scale_bindings()
    batches = split_export_bindings(bindings)
    assert len(bindings) == 919
    assert [len(batch) for batch in batches] == [100] * 9 + [19]


def test_json_schemas_accept_canonical_models() -> None:
    plan = _plan()
    receipt = _receipt()
    attempt = WarehouseProjectionAttemptV1.build(
        attempt_id=deterministic_typed_id("attempt", "schema"),
        load_plan_id=plan.load_plan_id,
        load_plan_hash=plan.plan_hash,
        attempt_no=1,
        status="succeeded",
        last_completed_stage="registry_append",
        started_at=NOW,
        completed_at="2026-07-20T00:02:00Z",
        error_code=None,
        error_message=None,
        receipt_id=receipt.receipt_id,
        receipt_hash=receipt.receipt_hash,
    )
    cases = {
        "claim_warehouse_load_plan_v1.schema.json": plan.model_dump(mode="json"),
        "claim_warehouse_load_batch_v1.schema.json": receipt.load_batch.model_dump(mode="json"),
        "claim_warehouse_load_receipt_v1.schema.json": receipt.model_dump(mode="json"),
        "claim_warehouse_projection_attempt_v1.schema.json": attempt.model_dump(mode="json"),
    }
    for filename, payload in cases.items():
        schema = json.loads((ROOT / "schemas/warehouse" / filename).read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(payload)
