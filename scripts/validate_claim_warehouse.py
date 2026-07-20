#!/usr/bin/env python3
"""Execute the GDB1 Claim-warehouse acceptance using invented synthetic data only.

The scale path is intentionally opt-in.  It creates 919 immutable canonical
export packages under a caller-provided or temporary external-storage root,
loads them in ten global batches, replays those batches, removes only that
synthetic projection, and rebuilds it from the immutable packages.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from video_truthfulness.core.artifacts.hashing import (  # noqa: E402
    canonical_json_bytes as artifact_canonical_json_bytes,
)
from video_truthfulness.core.artifacts.registry import (  # noqa: E402
    AppendOnlyRegistry,
    create_artifact_record,
)
from video_truthfulness.versions.v02.business_models import (  # noqa: E402
    AtomicClaimRevisionV1_2,
    ClaimEvidenceLinkV1_2,
    ClaimSplitSetRevisionV1_2,
    ClaimTextChunkV1_2,
    ClaimTextStorageV1_2,
    EvidenceRevisionV1_2,
    HumanGoldLabelV1_2,
    MachineClaimAssessmentV1_2,
    ParentClaimRevisionV1_2,
    SplitSetMemberV1_2,
)
from video_truthfulness.versions.v02.warehouse_export import (  # noqa: E402
    WarehouseExportResult,
    canonicalize_export,
    chunk_utf8_text,
    read_export_package,
    write_export_package,
)
from video_truthfulness.versions.v02.warehouse_loader import (  # noqa: E402
    WarehouseLoader,
    build_loader_admission_context,
    build_load_plan,
    build_load_plans,
)
from video_truthfulness.versions.v02.warehouse_models import (  # noqa: E402
    DATABASE_SCHEMA_VERSION,
    EXACT_SCALE_COUNTS,
    LABEL_TAXONOMY_VERSION,
    WAREHOUSE_EXPORT_SCHEMA_VERSION,
    ExternalStorageRef,
    InputArtifactRef,
    RegistryPrefix,
    WarehouseExportBinding,
    WarehouseLoadReceiptV1,
    WarehouseRow,
    deterministic_typed_id,
    sha256_bytes,
    validate_exact_scale_counts,
)
from video_truthfulness.versions.v02.warehouse_projection import (  # noqa: E402
    build_parquet_artifacts,
    dependency_status,
    publish_parquet_artifacts,
    rebuild_duckdb_projection,
)


ROOT_REF = "ubuntu_v02_claim_warehouse"
CORE_AT = "2026-07-20T00:00:00Z"
DEPTH_AT = "2026-07-20T01:00:00Z"
HUMAN_AT = "2026-07-20T02:00:00Z"
LOAD_DONE_AT = "2026-07-20T03:00:00Z"
SCHEMA_VERSIONS = {
    "database_schema_version": DATABASE_SCHEMA_VERSION,
    "warehouse_row_contract_version": "claim_warehouse_table_rows_v1.0.0",
}
TAXONOMY_VERSIONS = {
    "label_taxonomy_version": LABEL_TAXONOMY_VERSION,
}
LOADER_STAGES = (
    "load_plan",
    "export_validate",
    "parquet_staging",
    "parquet_validate",
    "parquet_publish",
    "duckdb_transaction",
    "receipt_publish",
    "registry_append",
)
GOLD_LABELS = (
    "gold_supports",
    "gold_partially_supports",
    "gold_refutes",
    "gold_misleading",
    "gold_missing_context",
    "gold_insufficient_evidence",
    "gold_uncheckable",
)


def _gdb1_coverage_matrix() -> dict[str, dict[str, Any]]:
    """Map every frozen section 14.2 dimension to executable evidence."""

    return {
        "01_schema_and_enum_matrix": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_warehouse_models.py",
                "tests/versions/v02/test_claim_taxonomy.py",
            ],
        },
        "02_parent_child_dependency_and_minimum_child": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_claim_taxonomy.py::"
                "test_resolved_split_and_context_only_dependency_rules_fail_closed",
                "tests/versions/v02/test_stage5_runner.py",
            ],
        },
        "03_needs_human_split_block": {
            "scale_validator": False,
            "tests": [
                "tests/versions/v02/test_claim_taxonomy.py::"
                "test_atomic_over_5000_is_preserved_then_blocks_s02_for_human_split"
            ],
        },
        "04_machine_human_writer_permission": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_claim_taxonomy.py::"
                "test_gold_refutes_is_not_mapped_to_misleading_and_machine_cannot_write_gold",
            ],
        },
        "05_seven_gold_labels": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_claim_taxonomy.py::"
                "test_all_seven_human_gold_values_are_supported"
            ],
        },
        "06_seven_evidence_axes": {
            "scale_validator": True,
            "tests": ["tests/versions/v02/test_claim_taxonomy.py"],
        },
        "11_revision_current_as_of": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_warehouse_scale.py::"
                "test_current_as_of_isolation_and_cross_video_queries_use_duckdb"
            ],
        },
        "12_export_determinism_and_full_rebuild": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_warehouse_export.py",
                "tests/versions/v02/test_warehouse_scale.py::"
                "test_loader_replay_delete_and_rebuild_are_logically_identical",
            ],
        },
        "13_loader_idempotency_conflict_and_second_writer": {
            "scale_validator": True,
            "tests": ["tests/versions/v02/test_warehouse_loader.py"],
        },
        "14_backup_and_restore": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_warehouse_scale.py::"
                "test_loader_replay_delete_and_rebuild_are_logically_identical"
            ],
        },
        "15_no_private_paths_or_real_data": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_warehouse_export.py::"
                "test_external_ref_rejects_unknown_root_and_symlink",
                "tests/versions/v02/test_warehouse_scale.py::"
                "test_metrics_and_real_data_counters_are_explicit",
            ],
        },
        "16_fresh_database_creation": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_warehouse_loader.py::"
                "test_full_load_is_idempotent_and_queryable"
            ],
        },
        "17_old_schema_migration_and_rollback": {
            "scale_validator": False,
            "tests": [
                "tests/versions/v02/test_warehouse_scale.py::"
                "test_side_by_side_old_schema_migration_and_rollback"
            ],
        },
        "18_cross_video_stage_label_queries": {
            "scale_validator": True,
            "tests": [
                "tests/versions/v02/test_warehouse_scale.py::"
                "test_current_as_of_isolation_and_cross_video_queries_use_duckdb"
            ],
        },
        "19_full_repository_regression": {
            "scale_validator": False,
            "tests": ["tests"],
        },
        "20_existing_ocr_and_g1a_read_only_review": {
            "scale_validator": False,
            "tests": [
                "tests/versions/v02/test_s01_publication.py",
                "tests/versions/v02/test_s01_finalizer.py",
            ],
        },
    }


@dataclass(frozen=True, slots=True)
class ExportContext:
    group: str
    source_index: int
    source_id: str
    run_id: str
    export_id: str
    artifact_id: str
    record_id: str
    content_hash: str
    created_at: str


@dataclass(frozen=True, slots=True)
class ScaleCorpus:
    bindings: tuple[WarehouseExportBinding, ...]
    observed_counts: dict[str, int]
    package_validation_count: int
    long_claim_checks: dict[str, int | str | bool]
    auxiliary_row_counts: dict[str, int]


class _PeakRssSampler:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.peak_bytes = 0

    def __enter__(self) -> "_PeakRssSampler":
        import psutil

        process = psutil.Process(os.getpid())

        def sample() -> None:
            while not self._stop.wait(0.02):
                self.peak_bytes = max(self.peak_bytes, process.memory_info().rss)

        self.peak_bytes = process.memory_info().rss
        self._thread = threading.Thread(target=sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)


def _typed(prefix: str, *parts: object) -> str:
    return deterministic_typed_id(prefix, ":".join(str(part) for part in parts))


def _storage_root(path: Path) -> Path:
    """Return an absolute root, using the native long-path form on Windows."""

    resolved = path.resolve()
    if os.name == "nt" and not str(resolved).startswith("\\\\?\\"):
        return Path("\\\\?\\" + str(resolved))
    return resolved


def _source_id(source_index: int) -> str:
    return f"youtube_SYN{source_index:08d}"


def _run_id(source_index: int) -> str:
    return _typed("run", "gdb1", source_index)


def _atomic_count(source_index: int) -> int:
    if source_index == 0:
        return 131
    if source_index <= 379:
        return 10
    return 9


def _parent_index_for_atomic(source_index: int, atomic_index: int) -> int:
    if source_index == 0:
        return 0 if atomic_index < 128 else atomic_index - 127
    return atomic_index % 4


def _gold_label_for(source_index: int, atomic_index: int) -> str:
    return GOLD_LABELS[(source_index * 10 + atomic_index) % len(GOLD_LABELS)]


def _claim_checkability(source_index: int, atomic_index: int) -> str:
    if (
        source_index < EXACT_SCALE_COUNTS["human_annotation_synthetic_exports"]
        and atomic_index < 10
        and _gold_label_for(source_index, atomic_index) == "gold_uncheckable"
    ):
        return "not_checkable"
    return "checkable"


def _context(group: str, source_index: int, created_at: str) -> ExportContext:
    export_seed = f"gdb1-synthetic-scale:{group}:{source_index:04d}"
    source_seed = f"gdb1-synthetic-scale:source:{source_index:04d}"
    return ExportContext(
        group=group,
        source_index=source_index,
        source_id=_source_id(source_index),
        run_id=_run_id(source_index),
        export_id=_typed("export", export_seed),
        artifact_id=_typed("artifact", source_seed),
        record_id=_typed("record", source_seed),
        content_hash=sha256_bytes(_synthetic_input_bytes(source_index)),
        created_at=created_at,
    )


def _synthetic_input_bytes(source_index: int) -> bytes:
    return artifact_canonical_json_bytes(
        {
            "fixture": "gdb1_synthetic_source_input_v1",
            "source_id": _source_id(source_index),
            "synthetic": True,
        }
    ) + b"\n"


def _row(
    context: ExportContext,
    *,
    logical_layer: str,
    table_code: str,
    canonical_primary_key: str,
    revision_no: int = 1,
    is_active: bool = True,
    writer_role: str,
    data: Mapping[str, Any],
) -> WarehouseRow:
    return WarehouseRow.build(
        logical_layer=logical_layer,
        table_code=table_code,
        canonical_primary_key=canonical_primary_key,
        revision_no=revision_no,
        is_active=is_active,
        effective_at=context.created_at,
        run_id=context.run_id,
        artifact_id=context.artifact_id,
        artifact_record_id=context.record_id,
        artifact_content_hash=context.content_hash,
        created_at=context.created_at,
        writer_role=writer_role,
        schema_versions=SCHEMA_VERSIONS,
        taxonomy_versions=TAXONOMY_VERSIONS,
        data=dict(data),
    )


def _long_inline_claim() -> str:
    motif = "合成政策事实主张：时间范围、统计口径与因果边界均需逐项核验。"
    return (motif * (65_536 // len(motif) + 1))[:65_536]


def _text_storage(text: str, owner_revision_id: str) -> ClaimTextStorageV1_2:
    chunks = chunk_utf8_text(text)
    encoded = text.encode("utf-8")
    parsed_chunks = [
        ClaimTextChunkV1_2(
            chunk_id=_typed("claim_text_chunk", owner_revision_id, chunk.chunk_index),
            owner_kind="parent_claim_revision",
            owner_revision_id=owner_revision_id,
            chunk_index=chunk.chunk_index,
            byte_start=chunk.byte_start,
            byte_end_exclusive=chunk.byte_end,
            text=chunk.text,
            chunk_sha256=chunk.text_hash,
        )
        for chunk in chunks
    ]
    return ClaimTextStorageV1_2(
        text_char_count=len(text),
        text_utf8_byte_count=len(encoded),
        text_sha256=sha256_bytes(encoded),
        inline_text=None if parsed_chunks else text,
        chunks=parsed_chunks,
    )


def _atomic_text_storage(text: str) -> ClaimTextStorageV1_2:
    encoded = text.encode("utf-8")
    return ClaimTextStorageV1_2(
        text_char_count=len(text),
        text_utf8_byte_count=len(encoded),
        text_sha256=sha256_bytes(encoded),
        inline_text=text,
        chunks=[],
    )


def _parent_ids(source_index: int, parent_index: int) -> tuple[str, str, str]:
    parent_id = _typed("claim", "parent", source_index, parent_index)
    revision_id = _typed(
        "parent_claim_revision", "parent", source_index, parent_index, 1
    )
    split_id = _typed("claim_split_revision", "split", source_index, parent_index, 1)
    return parent_id, revision_id, split_id


def _atomic_ids(source_index: int, atomic_index: int) -> tuple[str, str]:
    return (
        _typed("claim", "atomic", source_index, atomic_index),
        _typed("atomic_claim_revision", "atomic", source_index, atomic_index, 1),
    )


def _evidence_ids(source_index: int, evidence_index: int) -> tuple[str, str]:
    return (
        _typed("evidence", "evidence", source_index, evidence_index),
        _typed("evidence_revision", "evidence", source_index, evidence_index, 1),
    )


def _evidence_link_id(source_index: int, link_index: int) -> str:
    return _typed("evidence_link", "link", source_index, link_index)


def _base_parent_text(source_index: int, parent_index: int) -> str:
    if source_index == 0 and parent_index == 0:
        return _long_inline_claim()
    if source_index == 1 and parent_index == 0:
        return "🙂" * 65_537
    return f"合成视频 {source_index:03d} 的父主张 {parent_index + 1}，仅用于 GDB1 契约验收。"


def _core_rows(context: ExportContext) -> tuple[list[WarehouseRow], Counter[str]]:
    source_index = context.source_index
    rows: list[WarehouseRow] = []
    counts: Counter[str] = Counter()
    rows.append(
        _row(
            context,
            logical_layer="core_provenance",
            table_code="source_media",
            canonical_primary_key=context.source_id,
            writer_role="s01_acquisition_writer",
            data={
                "source_id": context.source_id,
                "platform": "youtube",
                "platform_source_key": f"SYN{source_index:08d}",
                "media_kind": "video",
                "synthetic": True,
            },
        )
    )
    rows.append(
        _row(
            context,
            logical_layer="core_provenance",
            table_code="run",
            canonical_primary_key=context.run_id,
            writer_role="stage5_coordinator",
            data={
                "run_id": context.run_id,
                "source_id": context.source_id,
                "run_created_at": CORE_AT,
                "execution_scope": "gdb1_synthetic_scale",
                "synthetic": True,
            },
        )
    )
    counts.update(distinct_source_ids=1, temporary_synthetic_contract_runs=1)

    atomic_count = _atomic_count(source_index)
    atomic_by_parent: dict[int, list[int]] = {index: [] for index in range(4)}
    for atomic_index in range(atomic_count):
        atomic_by_parent[_parent_index_for_atomic(source_index, atomic_index)].append(
            atomic_index
        )
    for parent_index in range(4):
        parent_id, parent_revision_id, split_id = _parent_ids(
            source_index, parent_index
        )
        parent_text = _base_parent_text(source_index, parent_index)
        text_storage = _text_storage(parent_text, parent_revision_id)
        rows.append(
            _row(
                context,
                logical_layer="core_provenance",
                table_code="parent_claim",
                canonical_primary_key=parent_id,
                writer_role="claim_extractor",
                data={
                    "parent_claim_id": parent_id,
                    "source_id": context.source_id,
                    "display_no": parent_index + 1,
                },
            )
        )
        parent_model = ParentClaimRevisionV1_2(
            parent_claim_id=parent_id,
            parent_revision_id=parent_revision_id,
            revision_no=1,
            supersedes_revision_id=None,
            display_no=parent_index + 1,
            text=text_storage,
            normalized_text=None,
            preview=parent_text[:120],
            source_spans=[
                {"start_ms": parent_index * 1_000, "end_ms": parent_index * 1_000 + 900}
            ],
            taxonomy_version=LABEL_TAXONOMY_VERSION,
            writer_role="claim_extractor",
        )
        parent_data = {
            "source_id": context.source_id,
            "revision": parent_model.model_dump(mode="json"),
        }
        rows.append(
            _row(
                context,
                logical_layer="core_provenance",
                table_code="parent_claim_revision",
                canonical_primary_key=parent_revision_id,
                writer_role="claim_extractor",
                data=parent_data,
            )
        )
        for chunk in text_storage.chunks:
            chunk_data = {
                "source_id": context.source_id,
                "chunk": chunk.model_dump(mode="json"),
            }
            rows.append(
                _row(
                    context,
                    logical_layer="core_provenance",
                    table_code="parent_claim_text_chunk",
                    canonical_primary_key=chunk.chunk_id,
                    writer_role="claim_extractor",
                    data=chunk_data,
                )
            )

        members: list[SplitSetMemberV1_2] = []
        for ordinal, atomic_index in enumerate(atomic_by_parent[parent_index]):
            atomic_id, atomic_revision_id = _atomic_ids(source_index, atomic_index)
            member_id = _typed("split_member", split_id, ordinal)
            member = SplitSetMemberV1_2(
                member_id=member_id,
                atomic_revision_id=atomic_revision_id,
                ordinal=ordinal,
            )
            members.append(member)
            rows.append(
                _row(
                    context,
                    logical_layer="core_provenance",
                    table_code="atomic_claim",
                    canonical_primary_key=atomic_id,
                    writer_role="claim_splitter",
                    data={
                        "atomic_claim_id": atomic_id,
                        "parent_claim_id": parent_id,
                        "source_id": context.source_id,
                    },
                )
            )
            atomic_text = (
                f"合成视频 {source_index:03d} 的原子主张 {atomic_index + 1}，"
                "用于验证父子拆分、证据关系和阶段标签。"
            )
            atomic_model = AtomicClaimRevisionV1_2(
                atomic_claim_id=atomic_id,
                parent_claim_id=parent_id,
                atomic_revision_id=atomic_revision_id,
                revision_no=1,
                supersedes_revision_id=None,
                split_revision_id=split_id,
                text=_atomic_text_storage(atomic_text),
                checkability=_claim_checkability(source_index, atomic_index),
                quality_warnings=[],
                machine_verdict_eligible=True,
                taxonomy_version=LABEL_TAXONOMY_VERSION,
                writer_role="claim_splitter",
            )
            atomic_data = {
                "source_id": context.source_id,
                "revision": atomic_model.model_dump(mode="json"),
            }
            rows.append(
                _row(
                    context,
                    logical_layer="core_provenance",
                    table_code="atomic_claim_revision",
                    canonical_primary_key=atomic_revision_id,
                    writer_role="claim_splitter",
                    data=atomic_data,
                )
            )
            member_data = {
                "source_id": context.source_id,
                "parent_claim_id": parent_id,
                "split_revision_id": split_id,
                "writer_role": "claim_splitter",
                "member": member.model_dump(mode="json"),
            }
            rows.append(
                _row(
                    context,
                    logical_layer="core_provenance",
                    table_code="split_set_member",
                    canonical_primary_key=member_id,
                    writer_role="claim_splitter",
                    data=member_data,
                )
            )
        split_model = ClaimSplitSetRevisionV1_2(
            split_revision_id=split_id,
            parent_claim_id=parent_id,
            parent_revision_id=parent_revision_id,
            revision_no=1,
            supersedes_split_revision_id=None,
            split_status="resolved_atomic",
            failure_reason=None,
            members=members,
            coverage_reviewed=True,
            taxonomy_version=LABEL_TAXONOMY_VERSION,
            writer_role="claim_splitter",
        )
        split_data = {
            "source_id": context.source_id,
            "split_set": split_model.model_dump(mode="json"),
        }
        rows.append(
            _row(
                context,
                logical_layer="core_provenance",
                table_code="claim_split_set_revision",
                canonical_primary_key=split_id,
                writer_role="claim_splitter",
                data=split_data,
            )
        )

    counts.update(parent_claims=4, atomic_claims=atomic_count)
    retrieval_batch_id = _typed("retrieval_batch", "core", source_index)
    assessment_batch_id = _typed("machine_assessment_batch", "core", source_index)
    rows.extend(
        [
            _row(
                context,
                logical_layer="machine_screening",
                table_code="retrieval_batch",
                canonical_primary_key=retrieval_batch_id,
                writer_role="retrieval_batch_closer",
                data={
                    "retrieval_batch_id": retrieval_batch_id,
                    "source_id": context.source_id,
                    "run_id": context.run_id,
                    "phase": "initial_machine",
                    "batch_closed": True,
                    "writer_role": "retrieval_batch_closer",
                },
            ),
            _row(
                context,
                logical_layer="machine_screening",
                table_code="machine_assessment_batch",
                canonical_primary_key=assessment_batch_id,
                writer_role="machine_assessor",
                data={
                    "assessment_batch_id": assessment_batch_id,
                    "source_id": context.source_id,
                    "phase": "initial_machine",
                    "model_version": "synthetic-machine-v1",
                    "prompt_version": "synthetic-prompt-v1",
                    "config_hash": sha256_bytes(b"gdb1-synthetic-machine-config"),
                },
            ),
        ]
    )

    for evidence_index in range(15):
        evidence_id, evidence_revision_id = _evidence_ids(
            source_index, evidence_index
        )
        rows.append(
            _row(
                context,
                logical_layer="machine_screening",
                table_code="evidence_item",
                canonical_primary_key=evidence_id,
                writer_role="machine_evidence_writer",
            data={
                "evidence_id": evidence_id,
                "source_id": context.source_id,
            },
            )
        )
        evidence_model = EvidenceRevisionV1_2(
            evidence_id=evidence_id,
            evidence_revision_id=evidence_revision_id,
            revision_no=1,
            supersedes_revision_id=None,
            source_kind=(
                "manufacturer" if source_index < 101 else "high_quality_secondary"
            ),
            publisher="GDB1 synthetic evidence publisher",
            published_date="2026-07-19",
            retrieved_at=CORE_AT,
            canonical_url=None,
            stable_locator=f"gdb1:synthetic:{source_index:04d}:{evidence_index:02d}",
            excerpt=(
                f"Invented evidence {evidence_index + 1} for synthetic source "
                f"{source_index + 1}; it is not a real source."
            ),
            taxonomy_version=LABEL_TAXONOMY_VERSION,
            writer_role="machine_evidence_writer",
        )
        evidence_data = {
            "source_id": context.source_id,
            "revision": evidence_model.model_dump(mode="json"),
        }
        rows.append(
            _row(
                context,
                logical_layer="machine_screening",
                table_code="evidence_revision",
                canonical_primary_key=evidence_revision_id,
                writer_role="machine_evidence_writer",
                data=evidence_data,
            )
        )

    for link_index in range(20):
        atomic_index = link_index % atomic_count
        _, atomic_revision_id = _atomic_ids(source_index, atomic_index)
        _, evidence_revision_id = _evidence_ids(source_index, link_index % 15)
        link_id = _evidence_link_id(source_index, link_index)
        link_model = ClaimEvidenceLinkV1_2(
            evidence_link_id=link_id,
            atomic_revision_id=atomic_revision_id,
            evidence_revision_id=evidence_revision_id,
            source_role=(
                "primary_source" if link_index % 2 == 0 else "secondary_source"
            ),
            use_status="evidence",
            evidence_strength=("high" if link_index % 3 == 0 else "medium"),
            evidence_relation=("supports" if link_index % 2 == 0 else "refutes"),
            rejection_reason=None,
            taxonomy_version=LABEL_TAXONOMY_VERSION,
            writer_role="machine_evidence_writer",
        )
        link_data = {
            "source_id": context.source_id,
            "link": link_model.model_dump(mode="json"),
        }
        rows.append(
            _row(
                context,
                logical_layer="machine_screening",
                table_code="claim_evidence_link",
                canonical_primary_key=link_id,
                writer_role="machine_evidence_writer",
                data=link_data,
            )
        )

    for atomic_index in range(atomic_count):
        _, atomic_revision_id = _atomic_ids(source_index, atomic_index)
        checkability = _claim_checkability(source_index, atomic_index)
        candidate_verdict = (
            "unverifiable"
            if checkability == "not_checkable"
            else ("supported" if atomic_index % 2 == 0 else "refuted")
        )
        evidence_link_ids = (
            [_evidence_link_id(source_index, atomic_index)]
            if atomic_index < 20
            else []
        )
        assessment_revision_id = _typed(
            "assessment_revision", "initial", source_index, atomic_index
        )
        assessment_model = MachineClaimAssessmentV1_2(
            assessment_revision_id=assessment_revision_id,
            atomic_revision_id=atomic_revision_id,
            claim_checkability=checkability,
            evidence_link_ids=evidence_link_ids,
            candidate_verdict=candidate_verdict,
            reason="Invented machine assessment for GDB1 synthetic scale validation.",
            uncertainty="low",
            model_version="synthetic-machine-v1",
            prompt_version="synthetic-prompt-v1",
            config_hash=sha256_bytes(b"gdb1-synthetic-machine-config"),
            review_status="machine_pending",
            writer_role="machine_assessor",
        )
        assessment_data = {
            "source_id": context.source_id,
            "phase": "initial_machine",
            "label_namespace": "machine_candidate",
            "assessment_batch_id": assessment_batch_id,
            "revision_no": 1,
            "supersedes_revision_id": None,
            "assessment": assessment_model.model_dump(mode="json"),
        }
        rows.append(
            _row(
                context,
                logical_layer="machine_screening",
                table_code="machine_claim_assessment",
                canonical_primary_key=assessment_revision_id,
                revision_no=1,
                writer_role="machine_assessor",
                data=assessment_data,
            )
        )

    counts.update(
        evidence_items=15,
        claim_evidence_links=20,
        initial_machine_verdicts=atomic_count,
    )
    return rows, counts


def _depth_rows(context: ExportContext) -> tuple[list[WarehouseRow], Counter[str]]:
    source_index = context.source_index
    rows: list[WarehouseRow] = []
    retrieval_batch_id = _typed("retrieval_batch", "source-depth", source_index)
    rows.append(
        _row(
            context,
            logical_layer="source_depth",
            table_code="retrieval_batch",
            canonical_primary_key=retrieval_batch_id,
            writer_role="retrieval_batch_closer",
            data={
                "retrieval_batch_id": retrieval_batch_id,
                "source_id": context.source_id,
                "run_id": context.run_id,
                "phase": "source_depth",
                "batch_closed": True,
                "writer_role": "retrieval_batch_closer",
            },
        )
    )
    for atomic_index in range(10):
        _, atomic_revision_id = _atomic_ids(source_index, atomic_index)
        checkability = _claim_checkability(source_index, atomic_index)
        candidate_verdict = (
            "unverifiable"
            if checkability == "not_checkable"
            else ("refuted" if atomic_index % 2 == 0 else "supported")
        )
        assessment_revision_id = _typed(
            "source_depth_assessment", "source-depth", source_index, atomic_index
        )
        data = {
            "assessment_revision_id": assessment_revision_id,
            "source_id": context.source_id,
            "atomic_revision_id": atomic_revision_id,
            "retrieval_batch_id": retrieval_batch_id,
            "base_assessment_revision_id": _typed(
                "assessment_revision", "initial", source_index, atomic_index
            ),
            "revision_no": 1,
            "supersedes_revision_id": None,
            "label_namespace": "machine_candidate",
            "rebuilt_verdict": candidate_verdict,
            "reason": "Invented source-depth reassessment; no real source was accessed.",
            "uncertainty": "medium",
            "writer_role": "source_depth_assessor",
        }
        rows.append(
            _row(
                context,
                logical_layer="source_depth",
                table_code="source_depth_assessment",
                canonical_primary_key=assessment_revision_id,
                revision_no=1,
                writer_role="source_depth_assessor",
                data=data,
            )
        )
    return rows, Counter(rebuilt_verdicts=10)


def _gold_model(
    source_index: int,
    atomic_index: int,
    *,
    annotation_id: str,
) -> HumanGoldLabelV1_2:
    del annotation_id  # FK is represented by the table-specific row wrapper.
    _, atomic_revision_id = _atomic_ids(source_index, atomic_index)
    label = _gold_label_for(source_index, atomic_index)
    evidence = []
    if label not in {"gold_insufficient_evidence", "gold_uncheckable"}:
        evidence = [_evidence_link_id(source_index, atomic_index % 20)]
    values: dict[str, Any] = {
        "gold_revision_id": _typed("gold_revision", source_index, atomic_index, 1),
        "target_kind": "atomic_claim_revision",
        "target_revision_id": atomic_revision_id,
        "annotation_scope": "atomic_truth",
        "claim_checkability": _claim_checkability(source_index, atomic_index),
        "gold_label": label,
        "reason": "Invented authorized-human decision for synthetic contract validation.",
        "evidence_link_ids": evidence,
        "supported_scope": None,
        "unsupported_scope": None,
        "misleading_mechanism": None,
        "missing_context": None,
        "retrieval_batch_id": None,
        "taxonomy_version": LABEL_TAXONOMY_VERSION,
        "approval_status": "approved",
        "writer_role": "authorized_human",
    }
    if label == "gold_partially_supports":
        values["supported_scope"] = "Synthetic supported scope."
        values["unsupported_scope"] = "Synthetic unsupported scope."
    elif label == "gold_misleading":
        values["misleading_mechanism"] = "Synthetic omission of a qualifying condition."
    elif label == "gold_missing_context":
        values["missing_context"] = "Synthetic missing comparison context."
    elif label == "gold_insufficient_evidence":
        values["retrieval_batch_id"] = _typed(
            "retrieval_batch", "core", source_index
        )
    return HumanGoldLabelV1_2.model_validate(values)


def _human_rows(context: ExportContext) -> tuple[list[WarehouseRow], Counter[str]]:
    source_index = context.source_index
    rows: list[WarehouseRow] = []
    for atomic_index in range(10):
        _, atomic_revision_id = _atomic_ids(source_index, atomic_index)
        task_id = _typed("annotation_task", source_index, atomic_index)
        annotation_id = _typed("annotation", source_index, atomic_index, 1)
        gold = _gold_model(
            source_index,
            atomic_index,
            annotation_id=annotation_id,
        )
        rows.extend(
            [
                _row(
                    context,
                    logical_layer="human_annotation",
                    table_code="annotation_task",
                    canonical_primary_key=task_id,
                    writer_role="annotation_coordinator",
                    data={
                        "annotation_task_id": task_id,
                        "source_id": context.source_id,
                        "atomic_revision_id": atomic_revision_id,
                        "state": "approved",
                        "synthetic": True,
                    },
                ),
                _row(
                    context,
                    logical_layer="human_annotation",
                    table_code="human_annotation",
                    canonical_primary_key=annotation_id,
                    writer_role="authorized_human",
                    data={
                        "annotation_id": annotation_id,
                        "annotation_task_id": task_id,
                        "source_id": context.source_id,
                        "atomic_revision_id": atomic_revision_id,
                        "label_namespace": "human_canonical",
                        "verdict": gold.gold_label,
                        "reason": "Invented annotation for synthetic contract validation.",
                        "evidence_link_ids": list(gold.evidence_link_ids),
                        "revision_no": 1,
                        "supersedes_annotation_id": None,
                        "synthetic": True,
                        "writer_role": "authorized_human",
                    },
                ),
            ]
        )
        gold_data = {
            "source_id": context.source_id,
            "annotation_id": annotation_id,
            "label_namespace": "human_canonical",
            "revision_no": 1,
            "supersedes_gold_revision_id": None,
            "gold": gold.model_dump(mode="json"),
            "synthetic": True,
        }
        rows.append(
            _row(
                context,
                logical_layer="human_annotation",
                table_code="gold_label",
                canonical_primary_key=gold.gold_revision_id,
                writer_role="authorized_human",
                data=gold_data,
            )
        )
    return rows, Counter(
        human_annotation_revisions=10,
        gold_label_revisions=10,
    )


def _source_input_record(context: ExportContext) -> Any:
    relative_path = (
        f"runs/V02/{context.run_id}/gdb1_synthetic_source_input.json"
    )
    return create_artifact_record(
        registry_schema_version="artifact_record_v1.2.0",
        record_id=context.record_id,
        recorded_at=CORE_AT,
        artifact_id=context.artifact_id,
        artifact_type="warehouse.synthetic_input",
        logical_name=f"gdb1_synthetic_source_{context.source_index:04d}",
        container_kind="file",
        project_version="v0.2",
        storage_version="V02",
        source_platform="youtube",
        source_id=context.source_id,
        run_id=context.run_id,
        dag_node_id="gdb1_synthetic_input",
        relative_path=relative_path,
        storage_root_ref="repository",
        storage_scope="run",
        media_type="application/json",
        size_bytes=len(_synthetic_input_bytes(context.source_index)),
        content_hash=context.content_hash,
        producer_type="workflow",
        workflow_id="gdb1_synthetic_scale_acceptance",
        workflow_version="1.0.0",
        schema_versions=["gdb1_synthetic_source_input_v1"],
        authority_level="machine_derived",
        lifecycle_state="frozen",
        validation_status="passed",
        privacy_class="public_synthetic",
        access_scope="public",
        retention_policy="GDB1 synthetic acceptance fixture",
        created_at=CORE_AT,
        validated_at=CORE_AT,
        frozen_at=CORE_AT,
    )


def _source_registry_prefix(context: ExportContext) -> RegistryPrefix:
    record = _source_input_record(context)
    prefix_bytes = (
        artifact_canonical_json_bytes(record.model_dump(mode="json")) + b"\n"
    )
    return RegistryPrefix(
        record_count=1,
        prefix_hash=sha256_bytes(prefix_bytes),
        head_record_id=record.record_id,
        head_record_hash=record.record_hash,
    )


def _source_registry_path(root: Path, run_id: str) -> Path:
    return root / "runs" / "V02" / run_id / "artifact_registry.jsonl"


def _ensure_source_registry(
    root: Path,
    context: ExportContext,
) -> AppendOnlyRegistry:
    marker = (
        root
        / "runs"
        / "V02"
        / context.run_id
        / "gdb1_synthetic_source_input.json"
    )
    expected_marker = _synthetic_input_bytes(context.source_index)
    marker.parent.mkdir(parents=True, exist_ok=True)
    if marker.exists():
        if marker.read_bytes() != expected_marker:
            raise AssertionError("synthetic source input bytes drifted")
    else:
        marker.write_bytes(expected_marker)

    registry = AppendOnlyRegistry(
        _source_registry_path(root, context.run_id),
        scope="run",
        expected_run_id=context.run_id,
    )
    expected = _source_input_record(context)
    entries = registry.read_entries()
    if not entries:
        registry.append(expected)
    elif (
        artifact_canonical_json_bytes(entries[0].wire_record.model_dump(mode="json"))
        != artifact_canonical_json_bytes(expected.model_dump(mode="json"))
    ):
        raise AssertionError("synthetic source Registry prefix drifted")
    return registry


def _append_source_export_record(
    root: Path,
    context: ExportContext,
    result: WarehouseExportResult,
) -> None:
    registry = _ensure_source_registry(root, context)
    artifact_id = _typed("artifact", "warehouse_export", result.manifest.export_id)
    record = create_artifact_record(
        registry_schema_version="artifact_record_v1.2.0",
        record_id=_typed("record", "warehouse_export", result.manifest.export_id),
        recorded_at=context.created_at,
        artifact_id=artifact_id,
        artifact_type="warehouse.export_batch",
        logical_name=f"{result.manifest.export_id}_manifest",
        container_kind="file",
        project_version="v0.2",
        storage_version="V02",
        source_platform="youtube",
        source_id=context.source_id,
        run_id=context.run_id,
        dag_node_id="warehouse_export",
        relative_path=result.manifest.manifest_relative_path,
        storage_root_ref=ROOT_REF,
        storage_scope="run",
        media_type="application/json",
        size_bytes=len(result.manifest_bytes),
        content_hash=result.manifest_hash,
        semantic_hash_algorithm="sha256",
        semantic_hash=result.logical_hash,
        producer_type="projection_builder",
        writer_agent_id="gdb1_synthetic_warehouse_exporter",
        workflow_id="gdb1_synthetic_scale_acceptance",
        workflow_version="1.0.0",
        schema_versions=[WAREHOUSE_EXPORT_SCHEMA_VERSION],
        tool_versions={"warehouse_exporter": "warehouse_export_v1.0.0"},
        upstream_artifact_ids=[context.artifact_id],
        input_fingerprint=result.manifest.export_idempotency_key,
        authority_level="projection",
        lifecycle_state="validated",
        validation_status="passed",
        privacy_class="public_synthetic",
        access_scope="public",
        retention_policy="GDB1 synthetic canonical export manifest",
        created_at=context.created_at,
        validated_at=context.created_at,
    )
    for entry in registry.read_entries():
        if entry.canonical_view.artifact_id != artifact_id:
            continue
        if (
            artifact_canonical_json_bytes(entry.wire_record.model_dump(mode="json"))
            != artifact_canonical_json_bytes(record.model_dump(mode="json"))
        ):
            raise AssertionError("warehouse export Registry record drifted")
        return
    registry.append(record)


def _canonical_result(
    context: ExportContext,
    rows: Sequence[WarehouseRow],
) -> WarehouseExportResult:
    input_ref = InputArtifactRef(
        artifact_id=context.artifact_id,
        record_id=context.record_id,
        artifact_type="warehouse.synthetic_input",
        content_hash=context.content_hash,
    )
    return canonicalize_export(
        rows,
        export_id=context.export_id,
        run_id=context.run_id,
        source_registry_prefix=_source_registry_prefix(context),
        input_artifacts=[input_ref],
        run_created_at=CORE_AT,
        created_at=context.created_at,
        storage_ref=ExternalStorageRef(
            storage_root_ref=ROOT_REF,
            relative_path=f"exports/{context.export_id}",
        ),
    )


def _publish_and_validate(
    context: ExportContext,
    result: WarehouseExportResult,
    *,
    storage_roots: Mapping[str, Path | str],
    source_registry_root: Path,
) -> int:
    first = write_export_package(
        result,
        storage_roots=storage_roots,
        task_id="gdb1scale",
    )
    replay = write_export_package(
        result,
        storage_roots=storage_roots,
        task_id="gdb1scale",
    )
    if first != replay:
        raise AssertionError("canonical export replay changed package identity")
    observed = read_export_package(
        storage_roots=storage_roots,
        manifest_ref=ExternalStorageRef(
            storage_root_ref=ROOT_REF,
            relative_path=result.manifest.manifest_relative_path,
        ),
        expected_manifest_hash=result.manifest_hash,
    )
    if (
        observed.manifest_bytes != result.manifest_bytes
        or observed.rows_bytes != result.rows_bytes
        or observed.logical_hash != result.logical_hash
    ):
        raise AssertionError("canonical package bytes/hash changed after publication")
    _append_source_export_record(source_registry_root, context, result)
    return 2


def build_and_publish_exact_scale(
    storage_root: Path,
) -> ScaleCorpus:
    """Create all 919 real manifest+rows packages from invented entities."""

    storage_root = _storage_root(storage_root)
    storage_root.mkdir(parents=True, exist_ok=True)
    storage_roots = {ROOT_REF: storage_root}
    bindings: list[WarehouseExportBinding] = []
    observed: Counter[str] = Counter()
    auxiliary: Counter[str] = Counter()
    validation_count = 0

    for source_index in range(EXACT_SCALE_COUNTS["s01_machine_export_packages"]):
        context = _context("core", source_index, CORE_AT)
        rows, counts = _core_rows(context)
        result = _canonical_result(context, rows)
        validation_count += _publish_and_validate(
            context,
            result,
            storage_roots=storage_roots,
            source_registry_root=storage_root,
        )
        bindings.append(result.load_binding(logical_layer="core_provenance"))
        observed.update(counts)
        observed["s01_machine_export_packages"] += 1
        auxiliary.update(row.table_code for row in rows)

    for source_index in range(EXACT_SCALE_COUNTS["source_depth_synthetic_exports"]):
        context = _context("depth", source_index, DEPTH_AT)
        rows, counts = _depth_rows(context)
        result = _canonical_result(context, rows)
        validation_count += _publish_and_validate(
            context,
            result,
            storage_roots=storage_roots,
            source_registry_root=storage_root,
        )
        bindings.append(result.load_binding(logical_layer="source_depth"))
        observed.update(counts)
        observed["source_depth_synthetic_exports"] += 1
        auxiliary.update(row.table_code for row in rows)

    for source_index in range(
        EXACT_SCALE_COUNTS["human_annotation_synthetic_exports"]
    ):
        context = _context("human", source_index, HUMAN_AT)
        rows, counts = _human_rows(context)
        result = _canonical_result(context, rows)
        validation_count += _publish_and_validate(
            context,
            result,
            storage_roots=storage_roots,
            source_registry_root=storage_root,
        )
        bindings.append(result.load_binding(logical_layer="human_annotation"))
        observed.update(counts)
        observed["human_annotation_synthetic_exports"] += 1
        auxiliary.update(row.table_code for row in rows)

    observed["total_export_packages"] = len(bindings)
    observed["load_batches"] = len(
        build_load_plans(
            bindings,
            created_at=LOAD_DONE_AT,
            storage_root_ref=ROOT_REF,
        )
    )
    validate_exact_scale_counts(observed)
    if len({(item.logical_layer, item.export_id) for item in bindings}) != len(
        bindings
    ):
        raise AssertionError("(logical_layer, export_id) is not globally unique")

    inline = _long_inline_claim()
    chunked = "🙂" * 65_537
    chunks = chunk_utf8_text(chunked)
    if "".join(chunk.text for chunk in chunks) != chunked:
        raise AssertionError("long Claim chunk reassembly failed")
    long_claim_checks: dict[str, int | str | bool] = {
        "inline_codepoints": len(inline),
        "inline_utf8_bytes": len(inline.encode("utf-8")),
        "inline_child_count": 128,
        "chunked_codepoints": len(chunked),
        "chunked_utf8_bytes": len(chunked.encode("utf-8")),
        "chunk_count": len(chunks),
        "chunked_sha256": sha256_bytes(chunked.encode("utf-8")),
        "chunk_reassembled": True,
    }
    return ScaleCorpus(
        bindings=tuple(bindings),
        observed_counts=dict(observed),
        package_validation_count=validation_count,
        long_claim_checks=long_claim_checks,
        auxiliary_row_counts=dict(sorted(auxiliary.items())),
    )


def _read_bound_exports(
    bindings: Sequence[WarehouseExportBinding],
    *,
    storage_roots: Mapping[str, Path | str],
) -> tuple[WarehouseExportResult, ...]:
    results: list[WarehouseExportResult] = []
    for binding in bindings:
        result = read_export_package(
            storage_roots=storage_roots,
            manifest_ref=ExternalStorageRef(
                storage_root_ref=binding.storage_root_ref,
                relative_path=binding.manifest_relative_path,
            ),
            expected_manifest_hash=binding.manifest_hash,
        )
        if (
            result.rows_hash != binding.rows_hash
            or result.logical_hash != binding.logical_hash
            or result.manifest.row_count != binding.row_count
        ):
            raise AssertionError("export binding differs from canonical package")
        results.append(result)
    return tuple(results)


def load_and_replay_exact_scale(
    storage_root: Path,
    bindings: Sequence[WarehouseExportBinding],
) -> tuple[tuple[WarehouseLoadReceiptV1, ...], dict[str, Any]]:
    storage_root = _storage_root(storage_root)
    storage_roots = {ROOT_REF: storage_root}
    plans = build_load_plans(
        bindings,
        created_at=LOAD_DONE_AT,
        storage_root_ref=ROOT_REF,
    )
    batch_sizes = [len(plan.exports) for plan in plans]
    if batch_sizes != [100] * 9 + [19]:
        raise AssertionError(f"global batch sizes drifted: {batch_sizes}")
    loader = WarehouseLoader(
        storage_roots=storage_roots,
        storage_root_ref=ROOT_REF,
        source_registry_roots={"repository": storage_root},
        cross_run_registry=AppendOnlyRegistry(
            storage_root / "registry" / "claim_warehouse_loader_registry.jsonl",
            scope="cross_run",
        ),
    )
    receipts: list[WarehouseLoadReceiptV1] = []
    first_hashes: list[str] = []
    replay_hashes: list[str] = []
    admissions = {
        plan.load_plan_id: build_loader_admission_context(
            plan,
            source_registry_roots={"repository": storage_root},
        )
        for plan in plans
    }
    for plan in plans:
        first = loader.load(
            plan,
            admission=admissions[plan.load_plan_id],
            attempt_no=1,
            started_at=LOAD_DONE_AT,
            completed_at="2026-07-20T03:01:00Z",
        )
        receipts.append(first.receipt)
        first_hashes.append(first.receipt.receipt_hash)
    for plan in plans:
        replay = loader.load(
            plan,
            admission=admissions[plan.load_plan_id],
            attempt_no=2,
            started_at="2026-07-20T03:02:00Z",
            completed_at="2026-07-20T03:03:00Z",
        )
        replay_hashes.append(replay.receipt.receipt_hash)
    if first_hashes != replay_hashes:
        raise AssertionError("idempotent Loader replay changed receipt hashes")
    return tuple(receipts), {
        "load_plan_count": len(plans),
        "global_batch_sizes": batch_sizes,
        "first_receipt_hashes": first_hashes,
        "replay_receipt_hashes": replay_hashes,
        "idempotent_replay": True,
    }


def _assert_inside(root: Path, target: Path) -> None:
    try:
        target.resolve(strict=False).relative_to(root.resolve(strict=True))
    except ValueError as exc:
        raise AssertionError("synthetic projection deletion escaped its root") from exc


def delete_and_rebuild_projection(
    storage_root: Path,
    bindings: Sequence[WarehouseExportBinding],
    receipts: Sequence[WarehouseLoadReceiptV1],
) -> dict[str, Any]:
    """Delete only synthetic DuckDB/Parquet outputs, then rebuild from packages."""

    storage_root = _storage_root(storage_root)
    database_path = storage_root / "duckdb" / "truthfulness_v02.duckdb"
    parquet_root = storage_root / "parquet"
    _assert_inside(storage_root, database_path)
    _assert_inside(storage_root, parquet_root)
    deleted_database = database_path.is_file()
    deleted_parquet_files = (
        len(list(parquet_root.rglob("*.parquet"))) if parquet_root.is_dir() else 0
    )
    if database_path.exists():
        database_path.unlink()
    if parquet_root.exists():
        shutil.rmtree(parquet_root)

    storage_roots = {ROOT_REF: storage_root}
    package_read_count = 0
    rebuilt_parquet_files = 0
    for offset in range(0, len(bindings), 100):
        exports = _read_bound_exports(
            bindings[offset : offset + 100],
            storage_roots=storage_roots,
        )
        package_read_count += len(exports)
        artifacts = build_parquet_artifacts(exports)
        publish_parquet_artifacts(
            artifacts,
            storage_root_ref=ROOT_REF,
            storage_roots=storage_roots,
        )
        rebuilt_parquet_files += len(artifacts)
    rebuild_duckdb_projection(
        database_path,
        receipts,
        storage_root_ref=ROOT_REF,
        storage_roots=storage_roots,
    )
    return {
        "deleted_database": deleted_database,
        "deleted_parquet_files": deleted_parquet_files,
        "package_read_count": package_read_count,
        "rebuilt_parquet_files": rebuilt_parquet_files,
        "rebuilt_database": database_path.is_file(),
    }


def _projection_logical_hash(connection: Any) -> str:
    rows = connection.execute(
        """
        SELECT row_hash
        FROM warehouse_rows
        ORDER BY export_id, logical_layer, table_code, canonical_primary_key
        """
    ).fetchall()
    return sha256_bytes(b"".join(str(row[0]).encode("ascii") + b"\n" for row in rows))


def _scalar(connection: Any, sql: str, parameters: Sequence[Any] = ()) -> int:
    value = connection.execute(sql, list(parameters)).fetchone()
    if value is None:
        raise AssertionError("warehouse query returned no row")
    return int(value[0])


def query_projection_snapshot(
    database_path: Path,
    bindings: Sequence[WarehouseExportBinding],
) -> dict[str, Any]:
    import duckdb

    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        table_counts = {
            str(table_code): int(count)
            for table_code, count in connection.execute(
                "SELECT table_code, count(*) FROM warehouse_rows GROUP BY table_code"
            ).fetchall()
        }
        current_view_names = (
            "v_parent_claim_current",
            "v_atomic_claim_current",
            "v_claim_split_current",
            "v_machine_verdict_current",
            "v_evidence_current",
            "v_claim_evidence_current",
            "v_gold_current",
            "v_warehouse_projection_lag",
        )
        current_view_counts = {
            view_name: _scalar(connection, f"SELECT count(*) FROM {view_name}")
            for view_name in current_view_names
        }
        expected_current_view_counts = {
            "v_parent_claim_current": 2_004,
            "v_atomic_claim_current": 5_010,
            "v_claim_split_current": 2_004,
            "v_machine_verdict_current": 5_010,
            "v_evidence_current": 7_515,
            "v_claim_evidence_current": 10_020,
            "v_gold_current": 2_510,
            "v_warehouse_projection_lag": 4,
        }
        if current_view_counts != expected_current_view_counts:
            raise AssertionError(
                f"frozen current-view counts drifted: {current_view_counts}"
            )
        current_machine = connection.execute(
            """
            WITH revisions AS (
                SELECT
                    json_extract_string(data_json, '$.assessment.atomic_revision_id')
                        AS atomic_revision_id,
                    'initial_machine' AS phase, 1 AS stage_order,
                    effective_at
                FROM v_machine_verdict_current
                UNION ALL
                SELECT
                    json_extract_string(data_json, '$.atomic_revision_id'),
                    'source_depth', 2, effective_at
                FROM warehouse_rows
                WHERE table_code = 'source_depth_assessment'
            ), ranked AS (
                SELECT atomic_revision_id, phase,
                    row_number() OVER (
                        PARTITION BY atomic_revision_id
                        ORDER BY stage_order DESC, effective_at DESC
                    ) AS position
                FROM revisions
            )
            SELECT count(*),
                   count(*) FILTER (WHERE phase = 'source_depth')
            FROM ranked WHERE position = 1
            """
        ).fetchone()
        as_of_machine = _scalar(
            connection,
            """
            WITH revisions AS (
                SELECT json_extract_string(
                    data_json, '$.assessment.atomic_revision_id'
                ) AS atomic_revision_id, 1 AS stage_order, effective_at
                FROM warehouse_rows_as_of(?)
                WHERE table_code = 'machine_claim_assessment'
                UNION ALL
                SELECT json_extract_string(data_json, '$.atomic_revision_id'),
                       2, effective_at
                FROM warehouse_rows_as_of(?)
                WHERE table_code = 'source_depth_assessment'
            ), ranked AS (
                SELECT row_number() OVER (
                    PARTITION BY atomic_revision_id
                    ORDER BY stage_order DESC, effective_at DESC
                ) AS position
                FROM revisions
            )
            SELECT count(*) FROM ranked WHERE position = 1
            """,
            ["2026-07-20T00:30:00Z", "2026-07-20T00:30:00Z"],
        )
        cross_video = connection.execute(
            """
            WITH machine AS (
                SELECT
                    json_extract_string(data_json, '$.source_id') AS source_id,
                    json_extract_string(
                        data_json, '$.assessment.atomic_revision_id'
                    ) AS atomic_revision_id,
                    json_extract_string(data_json, '$.label_namespace') AS namespace
                FROM warehouse_rows
                WHERE table_code = 'machine_claim_assessment'
            ), gold AS (
                SELECT
                    json_extract_string(data_json, '$.gold.target_revision_id')
                        AS atomic_revision_id,
                    json_extract_string(data_json, '$.gold.gold_label') AS gold_label
                FROM v_gold_current
                WHERE json_extract_string(data_json, '$.gold.target_kind') =
                    'atomic_claim_revision'
            )
            SELECT count(DISTINCT machine.source_id), count(*),
                   count(DISTINCT gold.gold_label),
                   count(DISTINCT machine.namespace)
            FROM machine JOIN gold USING (atomic_revision_id)
            """
        ).fetchone()
        phase_count = _scalar(
            connection,
            """
            WITH phases AS (
                SELECT 'initial_machine' AS phase
                FROM warehouse_rows WHERE table_code = 'machine_claim_assessment'
                UNION ALL
                SELECT 'source_depth'
                FROM warehouse_rows WHERE table_code = 'source_depth_assessment'
            )
            SELECT count(DISTINCT phase) FROM phases
            """,
        )
        observed_counts = {
            "distinct_source_ids": _scalar(
                connection,
                "SELECT count(DISTINCT json_extract_string(data_json, '$.source_id')) "
                "FROM warehouse_rows WHERE table_code = 'source_media'",
            ),
            "temporary_synthetic_contract_runs": table_counts.get("run", 0),
            "s01_machine_export_packages": sum(
                item.logical_layer == "core_provenance" for item in bindings
            ),
            "parent_claims": table_counts.get("parent_claim", 0),
            "atomic_claims": table_counts.get("atomic_claim", 0),
            "evidence_items": table_counts.get("evidence_item", 0),
            "claim_evidence_links": table_counts.get("claim_evidence_link", 0),
            "initial_machine_verdicts": _scalar(
                connection,
                "SELECT count(*) FROM warehouse_rows "
                "WHERE table_code = 'machine_claim_assessment' "
                "AND json_extract_string(data_json, '$.phase') = 'initial_machine'",
            ),
            "source_depth_synthetic_exports": sum(
                item.logical_layer == "source_depth" for item in bindings
            ),
            "rebuilt_verdicts": _scalar(
                connection,
                "SELECT count(*) FROM warehouse_rows "
                "WHERE table_code = 'source_depth_assessment'",
            ),
            "human_annotation_synthetic_exports": sum(
                item.logical_layer == "human_annotation" for item in bindings
            ),
            "human_annotation_revisions": table_counts.get("human_annotation", 0),
            "gold_label_revisions": table_counts.get("gold_label", 0),
            "total_export_packages": _scalar(
                connection, "SELECT count(*) FROM warehouse_loaded_export"
            ),
            "load_batches": _scalar(
                connection, "SELECT count(*) FROM warehouse_load_batch"
            ),
        }
        validate_exact_scale_counts(observed_counts)

        chunk_payloads = [
            json.loads(str(value))
            for (value,) in connection.execute(
                """
                SELECT data_json FROM warehouse_rows
                WHERE table_code = 'parent_claim_text_chunk'
                  AND json_extract_string(data_json, '$.source_id') = ?
                ORDER BY CAST(json_extract(data_json, '$.chunk.chunk_index') AS BIGINT)
                """,
                [_source_id(1)],
            ).fetchall()
        ]
        reassembled = "".join(item["chunk"]["text"] for item in chunk_payloads)
        if reassembled != "🙂" * 65_537:
            raise AssertionError("DuckDB long Claim chunk reassembly mismatch")
        inline_long_claim = connection.execute(
            """
            SELECT
                CAST(json_extract(
                    p.data_json, '$.revision.text.text_char_count'
                ) AS BIGINT),
                CAST(json_array_length(
                    s.data_json, '$.split_set.members'
                ) AS BIGINT)
            FROM v_parent_claim_current p
            JOIN v_claim_split_current s ON
                json_extract_string(p.data_json, '$.revision.parent_revision_id') =
                json_extract_string(s.data_json, '$.split_set.parent_revision_id')
            WHERE json_extract_string(p.data_json, '$.source_id') = ?
              AND CAST(json_extract(
                    p.data_json, '$.revision.display_no'
                  ) AS BIGINT) = 1
            """,
            [_source_id(0)],
        ).fetchone()
        if inline_long_claim != (65_536, 128):
            raise AssertionError("inline long Claim/128-child projection mismatch")

        namespace_violations = _scalar(
            connection,
            """
            SELECT count(*) FROM warehouse_rows
            WHERE (table_code = 'machine_claim_assessment' AND
                   logical_layer <> 'machine_screening')
               OR (table_code = 'source_depth_assessment' AND
                   logical_layer <> 'source_depth')
               OR (table_code IN ('human_annotation', 'gold_label') AND
                   logical_layer <> 'human_annotation')
            """,
        )
        if namespace_violations:
            raise AssertionError("machine/source-depth/human layer isolation failed")
        return {
            "observed_counts": observed_counts,
            "table_row_counts": table_counts,
            "total_projection_rows": sum(table_counts.values()),
            "projection_logical_hash": _projection_logical_hash(connection),
            "current": {
                "machine_claims": int(current_machine[0]),
                "source_depth_current": int(current_machine[1]),
                "parent_claims": current_view_counts["v_parent_claim_current"],
                "atomic_claims": current_view_counts["v_atomic_claim_current"],
                "gold": current_view_counts["v_gold_current"],
            },
            "as_of": {
                "cutoff": "2026-07-20T00:30:00Z",
                "machine_claims": as_of_machine,
            },
            "cross_video_stage_label": {
                "source_count": int(cross_video[0]),
                "claim_count": int(cross_video[1]),
                "gold_label_count": int(cross_video[2]),
                "machine_namespace_count": int(cross_video[3]),
                "machine_phase_count": phase_count,
            },
            "isolation": {
                "namespace_layer_violations": namespace_violations,
                "machine_namespace": "machine_candidate",
                "human_namespace": "human_canonical",
            },
            "schema_query_surfaces": {
                "current_view_counts": current_view_counts,
                "as_of_macro": "warehouse_rows_as_of",
                "as_of_macro_executed": True,
            },
            "long_claim_reassembled_codepoints": len(reassembled),
            "long_claim_inline_codepoints": int(inline_long_claim[0]),
            "long_claim_inline_children": int(inline_long_claim[1]),
            "long_claim_reassembled_sha256": sha256_bytes(
                reassembled.encode("utf-8")
            ),
        }
    finally:
        connection.close()


def _fault_export(
    storage_root: Path,
    stage_index: int,
) -> WarehouseExportResult:
    context = _context("fault", 9_000 + stage_index, CORE_AT)
    rows = [
        _row(
            context,
            logical_layer="core_provenance",
            table_code="source_media",
            canonical_primary_key=context.source_id,
            writer_role="s01_acquisition_writer",
            data={
                "source_id": context.source_id,
                "platform": "youtube",
                "platform_source_key": context.source_id.removeprefix("youtube_"),
                "media_kind": "video",
                "synthetic": True,
            },
        ),
        _row(
            context,
            logical_layer="core_provenance",
            table_code="run",
            canonical_primary_key=context.run_id,
            writer_role="stage5_coordinator",
            data={
                "run_id": context.run_id,
                "source_id": context.source_id,
                "run_created_at": CORE_AT,
                "execution_scope": "gdb1_synthetic_fault_matrix",
                "synthetic": True,
            },
        ),
    ]
    result = _canonical_result(context, rows)
    _publish_and_validate(
        context,
        result,
        storage_roots={ROOT_REF: storage_root},
        source_registry_root=storage_root,
    )
    return result


def execute_loader_fault_matrix(parent_root: Path) -> dict[str, Any]:
    import duckdb

    results: dict[str, Any] = {}
    for stage_index, stage in enumerate(LOADER_STAGES):
        root = _storage_root(parent_root / stage)
        root.mkdir(parents=True, exist_ok=True)
        result = _fault_export(root, stage_index)
        plan = build_load_plan(
            [result.load_binding(logical_layer="core_provenance")],
            created_at=LOAD_DONE_AT,
            storage_root_ref=ROOT_REF,
        )
        loader = WarehouseLoader(
            storage_roots={ROOT_REF: root},
            storage_root_ref=ROOT_REF,
            source_registry_roots={"repository": root},
            cross_run_registry=AppendOnlyRegistry(
                root / "registry" / "claim_warehouse_loader_registry.jsonl",
                scope="cross_run",
            ),
        )
        admission = build_loader_admission_context(
            plan,
            source_registry_roots={"repository": root},
        )

        def inject(observed_stage: str) -> None:
            if observed_stage == stage:
                raise RuntimeError(f"synthetic_fault_after_{stage}")

        failed = False
        try:
            loader.load(
                plan,
                admission=admission,
                attempt_no=1,
                started_at=LOAD_DONE_AT,
                completed_at="2026-07-20T03:01:00Z",
                fault_hook=inject,
            )
        except RuntimeError as exc:
            if str(exc) != f"synthetic_fault_after_{stage}":
                raise
            failed = True
        if not failed:
            raise AssertionError(f"Loader fault was not injected at {stage}")
        recovered = loader.load(
            plan,
            admission=admission,
            attempt_no=2,
            started_at="2026-07-20T03:02:00Z",
            completed_at="2026-07-20T03:03:00Z",
        )
        database_path = root / "duckdb" / "truthfulness_v02.duckdb"
        connection = duckdb.connect(str(database_path), read_only=True)
        try:
            rows_after_recovery = _scalar(
                connection, "SELECT count(*) FROM warehouse_rows"
            )
        finally:
            connection.close()
        replay = loader.load(
            plan,
            admission=admission,
            attempt_no=3,
            started_at="2026-07-20T03:04:00Z",
            completed_at="2026-07-20T03:05:00Z",
        )
        connection = duckdb.connect(str(database_path), read_only=True)
        try:
            rows_after_replay = _scalar(
                connection, "SELECT count(*) FROM warehouse_rows"
            )
        finally:
            connection.close()
        if rows_after_recovery != 2 or rows_after_replay != rows_after_recovery:
            raise AssertionError(f"Loader {stage} replay added business rows")
        source_registry_entries = AppendOnlyRegistry(
            _source_registry_path(root, result.manifest.run_id),
            scope="run",
            expected_run_id=result.manifest.run_id,
        ).read_entries()
        loader_registry_entries = AppendOnlyRegistry(
            root / "registry" / "claim_warehouse_loader_registry.jsonl",
            scope="cross_run",
        ).read_entries()
        results[stage] = {
            "first_attempt": "failed",
            "recovery": "succeeded",
            "second_recovery": "succeeded",
            "business_rows_after_recovery": rows_after_recovery,
            "business_rows_after_second_recovery": rows_after_replay,
            "new_business_rows_on_second_recovery": 0,
            "receipt_hash_stable": (
                recovered.receipt.receipt_hash == replay.receipt.receipt_hash
            ),
            "source_registry_records": len(source_registry_entries),
            "loader_registry_records": len(loader_registry_entries),
            "loader_registry_all_v1_2": all(
                entry.wire_record.registry_schema_version
                == "artifact_record_v1.2.0"
                for entry in loader_registry_entries
            ),
        }
    return {
        "stage_count": len(results),
        "stages": results,
        "all_first_fail_recover_recover": all(
            item["first_attempt"] == "failed"
            and item["recovery"] == "succeeded"
            and item["second_recovery"] == "succeeded"
            and item["new_business_rows_on_second_recovery"] == 0
            and item["receipt_hash_stable"]
            and item["source_registry_records"] == 2
            and item["loader_registry_records"] == 5
            and item["loader_registry_all_v1_2"]
            for item in results.values()
        ),
    }


def _timed_queries(database_path: Path) -> dict[str, Any]:
    import duckdb

    queries = {
        "current": (
            "SELECT sum(row_count) FROM ("
            "SELECT count(*) AS row_count FROM v_parent_claim_current UNION ALL "
            "SELECT count(*) FROM v_atomic_claim_current UNION ALL "
            "SELECT count(*) FROM v_machine_verdict_current UNION ALL "
            "SELECT count(*) FROM v_gold_current)"
        ),
        "as_of": (
            "SELECT count(*) FROM warehouse_rows_as_of("
            "'2026-07-20T00:30:00Z')"
        ),
        "cross_video_label": (
            "SELECT count(DISTINCT json_extract_string(data_json, '$.source_id')) "
            "FROM warehouse_rows WHERE table_code = 'gold_label'"
        ),
    }
    samples: list[float] = []
    per_query: dict[str, list[float]] = {}
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        for name, query in queries.items():
            connection.execute(query).fetchone()  # warm cache, excluded
            durations: list[float] = []
            for _ in range(9):
                started = time.perf_counter()
                connection.execute(query).fetchone()
                durations.append((time.perf_counter() - started) * 1_000)
            per_query[name] = durations
            samples.extend(durations)
    finally:
        connection.close()
    ordered = sorted(samples)
    p95_index = max(0, min(len(ordered) - 1, (95 * len(ordered) + 99) // 100 - 1))
    return {
        "sample_count": len(samples),
        "p50_ms": round(statistics.median(ordered), 6),
        "p95_ms": round(ordered[p95_index], 6),
        "per_query_ms": {
            key: [round(item, 6) for item in values]
            for key, values in per_query.items()
        },
    }


def _disk_usage(root: Path) -> dict[str, int]:
    files = [path for path in root.rglob("*") if path.is_file()]
    return {
        "file_count": len(files),
        "bytes": sum(path.stat().st_size for path in files),
    }


def _registry_metrics(root: Path) -> dict[str, Any]:
    source_registry_paths = sorted(
        (root / "runs" / "V02").glob("run_*/artifact_registry.jsonl")
    )
    source_type_counts: Counter[str] = Counter()
    source_record_count = 0
    source_all_v1_2 = True
    for registry_path in source_registry_paths:
        run_id = registry_path.parent.name
        entries = AppendOnlyRegistry(
            registry_path,
            scope="run",
            expected_run_id=run_id,
        ).read_entries()
        if not entries or entries[0].canonical_view.artifact_type != (
            "warehouse.synthetic_input"
        ):
            raise AssertionError("source Registry does not start at frozen input prefix")
        source_record_count += len(entries)
        source_type_counts.update(
            entry.canonical_view.artifact_type for entry in entries
        )
        source_all_v1_2 = source_all_v1_2 and all(
            entry.wire_record.registry_schema_version == "artifact_record_v1.2.0"
            for entry in entries
        )

    loader_entries = AppendOnlyRegistry(
        root / "registry" / "claim_warehouse_loader_registry.jsonl",
        scope="cross_run",
    ).read_entries()
    loader_registry_path = (
        root / "registry" / "claim_warehouse_loader_registry.jsonl"
    )
    loader_type_counts = Counter(
        entry.canonical_view.artifact_type for entry in loader_entries
    )
    metrics = {
        "source_registry_files": len(source_registry_paths),
        "source_registry_records": source_record_count,
        "source_registry_type_counts": dict(sorted(source_type_counts.items())),
        "source_registry_all_v1_2": source_all_v1_2,
        "source_registry_set_hash": sha256_bytes(
            b"".join(
                sha256_bytes(path.read_bytes()).encode("ascii") + b"\n"
                for path in source_registry_paths
            )
        ),
        "loader_registry_records": len(loader_entries),
        "loader_registry_type_counts": dict(sorted(loader_type_counts.items())),
        "loader_registry_all_v1_2": all(
            entry.wire_record.registry_schema_version == "artifact_record_v1.2.0"
            for entry in loader_entries
        ),
        "loader_registry_hash": sha256_bytes(loader_registry_path.read_bytes()),
    }
    expected = {
        "source_registry_files": 501,
        "source_registry_records": 1_420,
        "source_registry_type_counts": {
            "warehouse.export_batch": 919,
            "warehouse.synthetic_input": 501,
        },
        "source_registry_all_v1_2": True,
        "loader_registry_records": 40,
        "loader_registry_type_counts": {
            "warehouse.load_plan": 10,
            "warehouse.load_receipt": 10,
            "warehouse.projection_attempt": 20,
        },
        "loader_registry_all_v1_2": True,
    }
    if any(metrics[key] != value for key, value in expected.items()):
        raise AssertionError(f"Registry acceptance counts drifted: {metrics}")
    return metrics


def run_synthetic_scale(storage_root: Path) -> dict[str, Any]:
    status = dependency_status()
    if not all(details["exact_match"] for details in status.values()):
        raise RuntimeError(f"exact warehouse dependencies are required: {status}")
    storage_root = _storage_root(storage_root)
    storage_root.mkdir(parents=True, exist_ok=True)
    timings: dict[str, float] = {}
    started_total = time.perf_counter()
    with _PeakRssSampler() as rss:
        started = time.perf_counter()
        corpus = build_and_publish_exact_scale(storage_root)
        timings["canonical_packages_seconds"] = time.perf_counter() - started

        started = time.perf_counter()
        receipts, load_report = load_and_replay_exact_scale(
            storage_root, corpus.bindings
        )
        timings["load_and_replay_seconds"] = time.perf_counter() - started
        database_path = storage_root / "duckdb" / "truthfulness_v02.duckdb"
        before_rebuild = query_projection_snapshot(database_path, corpus.bindings)

        started = time.perf_counter()
        rebuild_report = delete_and_rebuild_projection(
            storage_root,
            corpus.bindings,
            receipts,
        )
        timings["delete_and_rebuild_seconds"] = time.perf_counter() - started
        after_rebuild = query_projection_snapshot(database_path, corpus.bindings)
        comparison_keys = (
            "observed_counts",
            "table_row_counts",
            "total_projection_rows",
            "projection_logical_hash",
            "current",
            "as_of",
            "cross_video_stage_label",
            "isolation",
            "schema_query_surfaces",
            "long_claim_reassembled_codepoints",
            "long_claim_inline_codepoints",
            "long_claim_inline_children",
            "long_claim_reassembled_sha256",
        )
        if any(before_rebuild[key] != after_rebuild[key] for key in comparison_keys):
            raise AssertionError("rebuilt projection differs from first projection")

        started = time.perf_counter()
        fault_matrix = execute_loader_fault_matrix(storage_root / "fault_matrix")
        timings["loader_fault_matrix_seconds"] = time.perf_counter() - started
        registries = _registry_metrics(storage_root)
        query_performance = _timed_queries(database_path)
    timings["total_seconds"] = time.perf_counter() - started_total

    export_directories = list((storage_root / "exports").glob("export_*"))
    manifest_files = list((storage_root / "exports").glob("export_*/manifest.json"))
    rows_files = list((storage_root / "exports").glob("export_*/rows.jsonl"))
    if not (
        len(export_directories)
        == len(manifest_files)
        == len(rows_files)
        == EXACT_SCALE_COUNTS["total_export_packages"]
    ):
        raise AssertionError("canonical manifest/rows package count mismatch")

    real_counters = {
        "real_media": 0,
        "real_claim": 0,
        "real_gold": 0,
        "v01_inputs": 0,
        "real_s01_runs": 0,
        "s02_runs": 0,
    }
    coverage = _gdb1_coverage_matrix()
    if len(coverage) != 16 or any(not item["tests"] for item in coverage.values()):
        raise AssertionError("GDB1 section 14.2 coverage matrix is incomplete")
    return {
        "acceptance_scope": "GDB1 synthetic scale only",
        "synthetic": True,
        "dependencies": status,
        "counts": after_rebuild["observed_counts"],
        "auxiliary_row_counts": corpus.auxiliary_row_counts,
        "canonical_packages": {
            "export_directories": len(export_directories),
            "manifest_files": len(manifest_files),
            "rows_files": len(rows_files),
            "publication_and_read_validation_passes": (
                corpus.package_validation_count
            ),
            "binding_set_hash": sha256_bytes(
                artifact_canonical_json_bytes(
                    [
                        {
                            "export_id": item.export_id,
                            "logical_layer": item.logical_layer,
                            "manifest_hash": item.manifest_hash,
                            "rows_hash": item.rows_hash,
                            "logical_hash": item.logical_hash,
                        }
                        for item in sorted(
                            corpus.bindings,
                            key=lambda binding: (
                                binding.logical_layer,
                                binding.export_id,
                            ),
                        )
                    ]
                )
            ),
        },
        "long_claims": corpus.long_claim_checks,
        "load": load_report,
        "first_projection": before_rebuild,
        "rebuild": rebuild_report,
        "rebuilt_projection": after_rebuild,
        "projection_rebuild_equal": True,
        "registries": registries,
        "loader_fault_matrix": fault_matrix,
        "gdb1_14_2_coverage": {
            "dimension_count": len(coverage),
            "unmapped_dimensions": [],
            "dimensions": coverage,
        },
        "referenced_existing_fault_evidence": {
            "s01_publication_seven_phases": {
                "path": "tests/versions/v02/test_s01_publication.py",
                "executed_by_this_validator": False,
                "status": "read_only_reference_only",
            },
            "s01_finalizer_twelve_phases": {
                "path": "tests/versions/v02/test_s01_finalizer.py",
                "executed_by_this_validator": False,
                "status": "read_only_reference_only",
            },
        },
        "query_performance": query_performance,
        "timings_seconds": {
            key: round(value, 6) for key, value in timings.items()
        },
        "peak_rss_bytes": rss.peak_bytes,
        "disk": _disk_usage(storage_root),
        "real_counters": real_counters,
        "s01_status": "NOT_STARTED_REAL",
        "s02_status": "UNAUTHORIZED_NOT_STARTED",
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--synthetic-scale",
        action="store_true",
        help="explicitly authorize the invented 501-source/919-export acceptance",
    )
    parser.add_argument(
        "--storage-root",
        type=Path,
        help="empty directory for synthetic external storage; defaults to a temp dir",
    )
    parser.add_argument(
        "--report",
        type=Path,
        help="optional JSON report path; must be outside the synthetic storage root",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    if not args.synthetic_scale:
        raise SystemExit("refusing to run: pass --synthetic-scale explicitly")
    if args.storage_root is None and os.name == "nt":
        raise SystemExit(
            "Windows requires an explicit short --storage-root for frozen partition paths"
        )
    root_for_report: Path | None = None
    if args.storage_root is None:
        with tempfile.TemporaryDirectory(prefix="gdb1-warehouse-scale-") as directory:
            report = run_synthetic_scale(Path(directory))
    else:
        root = args.storage_root.resolve()
        root_for_report = root
        if root.exists() and any(root.iterdir()):
            raise SystemExit("--storage-root must be empty")
        root.mkdir(parents=True, exist_ok=True)
        report = run_synthetic_scale(root)
    payload = json.dumps(
        report,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )
    if args.report is not None:
        report_path = args.report.resolve()
        if root_for_report is not None:
            try:
                report_path.relative_to(root_for_report)
            except ValueError:
                pass
            else:
                raise SystemExit("--report must be outside --storage-root")
        report_path.write_text(payload + "\n", encoding="utf-8", newline="\n")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
