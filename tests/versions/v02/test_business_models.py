from __future__ import annotations

import copy
import hashlib
import json
import wave
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from video_truthfulness.core.artifacts.models import to_artifact_record_view
from video_truthfulness.core.artifacts.registry import create_artifact_record
from video_truthfulness.core.execution.hashing import embedded_hash
from video_truthfulness.core.execution.models import ExecutionSchemaError
from video_truthfulness.versions.v02.business_models import (
    BUSINESS_ARTIFACT_MODELS,
    BUSINESS_ARTIFACT_V1_2_MODELS,
    PAYLOAD_MODELS,
    BusinessArtifactV1_1,
    ContractFileRef,
    EvidenceRevisionV1_2,
    ManualInputPolicy,
    NonProductDomainReviewReceiptV1,
    NodeExecutionPolicy,
    PlanReadBinding,
    RegistryHeadBinding,
    SessionControlPlan,
    Stage5ContractError,
    Stage5ContractFiles,
    Stage5ExecutionPlan,
    Stage5ExecutionPlanV1_1,
    Stage5PublicationPlan,
    TelemetryPlan,
    WriteBinding,
    WarehouseExportBatchArtifactV1_2,
    WarehouseExportBatchPayloadV1_2,
    parse_manual_external_input_receipt,
    parse_non_product_domain_review_receipt,
    parse_stage5_execution_plan,
    parse_v02_business_artifact,
    seal_non_product_domain_review_receipt,
    validate_media_audio_wav_bytes,
    validate_ocr_branch_contract,
    validate_source_depth_branch_contract,
)


ROOT = Path(__file__).resolve().parents[3]
U = "01j00000000000000000000000"
H = "1" * 64
H2 = "2" * 64
NOW = "2026-07-19T00:00:00Z"


def _id(prefix: str, suffix: str) -> str:
    return f"{prefix}_{U[:-1]}{suffix}"


TASK = _id("task", "1")
SESSION = _id("session", "1")
RUN = _id("run", "1")
A1 = _id("artifact", "1")
A2 = _id("artifact", "2")
A3 = _id("artifact", "3")
A4 = _id("artifact", "4")
A5 = _id("artifact", "5")
A6 = _id("artifact", "6")
A7 = _id("artifact", "7")
A8 = _id("artifact", "8")
A9 = _id("artifact", "9")
AA = _id("artifact", "a")
AB = _id("artifact", "b")
R1 = _id("record", "1")
R2 = _id("record", "2")
R3 = _id("record", "3")
C1 = _id("claim", "1")
C2 = _id("claim", "2")
E1 = _id("evidence", "1")
E2 = _id("evidence", "2")
V1 = _id("verdict", "1")
V2 = _id("verdict", "2")
S1 = _id("segment", "1")
S2 = _id("segment", "2")
O1 = _id("ocr_entry", "1")
REQ = _id("source_depth_request", "1")
CHECKPOINT = _id("checkpoint", "1")
RECEIPT1 = _id("receipt", "1")
RECEIPT2 = _id("receipt", "2")


def _binding(artifact_id: str = A1) -> dict[str, Any]:
    return {"artifact_id": artifact_id, "record_id": R1, "content_hash": H}


def _file() -> dict[str, Any]:
    return {
        "relative_path": f"runs/V02/source_depth/inbox/{A1}/gemini.md",
        "content_hash_algorithm": "sha256",
        "content_hash": H,
        "size_bytes": 12,
    }


def _locator() -> dict[str, int]:
    return {"start_ms": 0, "end_ms": 1000}


def _claim(claim_id: str = C1) -> dict[str, Any]:
    return {
        "claim_id": claim_id,
        "display_no": 1,
        "raw_context": "原始语境",
        "normalized_claim": "可核查主张",
        "locator": _locator(),
        "namespace": "machine_candidate",
    }


def _evidence(evidence_id: str = E1) -> dict[str, Any]:
    return {
        "evidence_id": evidence_id,
        "claim_ids": [C1],
        "source_type": "official",
        "publisher": "Example authority",
        "published_date": "2026-07-01",
        "retrieved_at": NOW,
        "canonical_url": "https://example.test/source",
        "stable_locator": None,
        "excerpt": "A reviewable excerpt.",
        "relation": "supports",
        "quality": "high",
    }


def _verdict(verdict_id: str = V1, evidence_id: str = E1) -> dict[str, Any]:
    return {
        "verdict_id": verdict_id,
        "claim_id": C1,
        "evidence_ids": [evidence_id],
        "candidate_verdict": "supported",
        "reason": "Evidence supports the candidate claim.",
        "uncertainty": "low",
        "review_status": "machine_pending",
    }


def _target() -> dict[str, Any]:
    return {
        "claim_id": C1,
        "gap": "Need a primary source.",
        "preferred_source_types": ["official"],
    }


def _index(entity_id: str) -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "semantic_hash": H,
        "parent_entity_id": None,
        "upstream_entity_ids": [],
        "locator": _locator(),
    }


def _payloads() -> dict[str, dict[str, Any]]:
    return {
        "acquisition.decision": {
            "source_id": "youtube_abcdefghijk",
            "selected_existing_node_id": "public_no_cookie_download",
            "selected_media": _binding(),
            "redownload_forbidden": True,
            "authorization_source": "G0A_D03_reuse_registered_validated_media",
        },
        "transcript.path_decision": {
            "selected_path": "audio_asr",
            "subtitle_status": "not_registered",
            "parent_media_validation": _binding(),
            "media_artifact_id": A1,
        },
        "media.audio": {
            "codec": "pcm_s16le",
            "container_format": "wav",
            "channels": 1,
            "sample_rate_hz": 16000,
            "duration_ms": 1000,
            "parent_media_content_hash": H,
            "ffmpeg_content_hash": H,
            "extraction_parameters_hash": H,
            "output_content_hash": H2,
        },
        "transcript.raw": {
            "parent_audio_artifact_id": A1,
            "asr_engine": "faster-whisper",
            "asr_engine_version": "1.1.0",
            "asr_model": "large-v3",
            "asr_model_revision": "rev-1",
            "asr_parameters_hash": H,
            "language": "zh",
            "segments": [
                {
                    "segment_id": S1,
                    "locator": _locator(),
                    "text": "示例文本",
                    "words": [
                        {
                            "word": "示例",
                            "start_ms": 0,
                            "end_ms": 500,
                            "probability": 0.9,
                        }
                    ],
                    "uncertainty": "low",
                }
            ],
        },
        "transcript.normalized": {
            "raw_transcript_artifact_id": A1,
            "preserves_raw": True,
            "segments": [
                {
                    "segment_id": S2,
                    "raw_segment_id": S1,
                    "raw_text": "示例文本",
                    "normalized_text": "示例文本",
                    "term_mappings": [],
                    "unresolved_ambiguities": [],
                }
            ],
        },
        "transcript.alignment": {
            "raw_transcript_artifact_id": A1,
            "normalized_transcript_artifact_id": A2,
            "ocr_gate_decision_artifact_id": A3,
            "ocr_result_artifact_id": None,
            "complete": True,
            "alignments": [
                {
                    "raw_segment_id": S1,
                    "normalized_segment_id": S2,
                    "locator": _locator(),
                    "ocr_entry_ids": [],
                }
            ],
        },
        "ocr.gate_decision": {
            "gate_state": "NOT_APPLICABLE",
            "trigger_basis": ["no visual-text dependency"],
            "input_bindings": [_binding()],
            "frame_budget": {
                "run_max_frames": 24,
                "trigger_max_frames": 3,
                "adjacent_min_interval_ms": 2000,
            },
            "adapter_profile_version": None,
        },
        "ocr.result": {
            "gate_decision_artifact_id": A1,
            "engine_name": "synthetic-ocr",
            "engine_revision": "1",
            "profile_version": "ocr-profile-v1",
            "entries": [
                {
                    "ocr_entry_id": O1,
                    "frame_relative_path": "runs/V02/frames/0001.png",
                    "frame_content_hash": H,
                    "timestamp_ms": 100,
                    "raw_text": "画面文字",
                    "confidence": 0.8,
                    "trigger_reason": "synthetic fixture",
                }
            ],
        },
        "claim.collection": {"transcript_artifact_id": A1, "claims": [_claim()]},
        "claim.atomic_collection": {
            "parent_claim_collection_artifact_id": A1,
            "claims": [
                {
                    "claim_id": C1,
                    "parent_claim_id": C2,
                    "split_relation": "atomic_child",
                    "atomic_text": "原子主张",
                    "checkability": "checkable",
                    "source_depth_candidate": True,
                    "locator": _locator(),
                }
            ],
        },
        "claim.entity_index": {
            "container": _binding(),
            "index_revision": "initial",
            "supersedes_artifact_id": None,
            "entries": [_index(C1)],
        },
        "evidence.entity_index": {
            "container": _binding(),
            "claim_collection_artifact_id": A2,
            "entries": [_index(E1)],
        },
        "verdict.entity_index": {
            "container": _binding(),
            "claim_collection_artifact_id": A2,
            "evidence_collection_artifact_id": A3,
            "entries": [_index(V1)],
        },
        "evidence.collection": {
            "claim_collection_artifact_id": A1,
            "evidence": [_evidence()],
        },
        "verdict.collection": {
            "claim_collection_artifact_id": A1,
            "evidence_collection_artifact_id": A2,
            "verdicts": [_verdict()],
        },
        "report.machine": {
            "input_bindings": [_binding(A1), _binding(A2)],
            "summary": "Machine screening report.",
            "claim_refs": [{"claim_id": C1, "verdict_id": V1}],
            "deterministic_template_version": "report-template-v1",
        },
        "source_depth.decision": {"route": "depth", "targets": [_target()]},
        "source_depth.prompt": {
            "source_depth_request_id": REQ,
            "target_claims": [_target()],
            "bounded_context": ["bounded claim context"],
            "current_evidence_ids": [E1],
            "return_contract_version": "source_depth_manual_return_v1.0.0",
            "require_canonical_urls": True,
        },
        "source_depth.result": {
            "capture_mode": "manual_gemini_web",
            "source_depth_request_id": REQ,
            "prompt_artifact_id": A1,
            "source_file": _file(),
            "visible_model": {
                "value": None,
                "status": "unavailable",
                "source": "not_exposed",
            },
            "received_at": NOW,
            "raw_content_hash": H,
            "raw_content": {"sources": []},
        },
        "source_depth.import_validation": {
            "source_depth_result_artifact_id": A1,
            "mapped_claim_ids": [C1],
            "deduplicated_document_count": 1,
            "sources": [
                {
                    "claim_id": C1,
                    "classification": "supports",
                    "canonical_url": "https://example.test/source",
                    "excerpt_verified": True,
                    "lead_status": "evidence",
                    "rejection_reason": None,
                }
            ],
            "conflicts": [],
            "rejected_count": 0,
        },
        "evidence.merged_collection": {
            "base_evidence": _binding(),
            "import_validation_artifact_id": A2,
            "preserved_evidence_ids": [E1],
            "added_evidence": [_evidence(E2)],
            "diff_summary": "Added one reviewed source.",
        },
        "verdict.rebuilt_collection": {
            "base_verdict": _binding(),
            "merged_evidence_artifact_id": A2,
            "verdicts": [_verdict(V2, E2)],
            "before_after_summary": "One verdict rebuilt.",
        },
        "report.rebuilt": {
            "input_bindings": [_binding(A1), _binding(A2)],
            "summary": "Rebuilt report.",
            "claim_refs": [{"claim_id": C1, "verdict_id": V2}],
            "deterministic_template_version": "report-template-v1",
            "before_after_summary": "Source depth evidence included.",
        },
        "screening.sync_record": {
            "selected_report": _binding(),
            "selected_report_kind": "rebuilt",
            "claim_ids": [C1],
            "source_depth_terminal": "IMPORTED",
            "next_stage": "S03",
            "execution_authorized": False,
        },
    }


def _artifact(
    artifact_type: str, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    raw = {
        "artifact_schema_version": "v02_business_artifact_v1.0.0",
        "artifact_id": A1,
        "artifact_type": artifact_type,
        "run_id": RUN,
        "stage_id": "S02" if artifact_type.startswith("source_depth.") else "S01",
        "dag_node_id": "synthetic_node",
        "upstream_artifact_ids": [A2],
        "created_at": NOW,
        "payload": copy.deepcopy(
            payload if payload is not None else _payloads()[artifact_type]
        ),
        "artifact_hash": "0" * 64,
    }
    raw["artifact_hash"] = embedded_hash(raw, "artifact_hash")
    return raw


ARTIFACT_TYPES = tuple(_payloads())
BUSINESS_SCHEMA = json.loads(
    (ROOT / "schemas/versions/v02/v02_business_artifact_v1.schema.json").read_text(
        encoding="utf-8"
    )
)
BUSINESS_VALIDATOR = Draft202012Validator(BUSINESS_SCHEMA)


def _assert_schema_rejects(raw: dict[str, Any]) -> None:
    assert list(BUSINESS_VALIDATOR.iter_errors(raw))


@pytest.mark.parametrize("artifact_type", ARTIFACT_TYPES)
def test_all_24_business_branches_are_schema_runtime_valid(artifact_type: str) -> None:
    raw = _artifact(artifact_type)
    assert not list(BUSINESS_VALIDATOR.iter_errors(raw))
    assert parse_v02_business_artifact(raw).model_dump(mode="json") == raw


def test_exactly_24_runtime_branches_match_schema_discriminator() -> None:
    assert set(BUSINESS_ARTIFACT_MODELS) == set(ARTIFACT_TYPES)
    assert len(BUSINESS_ARTIFACT_MODELS) == 24
    Draft202012Validator.check_schema(BUSINESS_SCHEMA)


@pytest.mark.parametrize("artifact_type", ARTIFACT_TYPES)
def test_each_business_branch_rejects_a_missing_required_payload_field(
    artifact_type: str,
) -> None:
    raw = _artifact(artifact_type)
    del raw["payload"][next(iter(raw["payload"]))]
    raw["artifact_hash"] = embedded_hash(raw, "artifact_hash")
    _assert_schema_rejects(raw)
    with pytest.raises(ExecutionSchemaError):
        parse_v02_business_artifact(raw)


@pytest.mark.parametrize("artifact_type", ARTIFACT_TYPES)
def test_each_business_branch_rejects_extra_payload_fields(artifact_type: str) -> None:
    raw = _artifact(artifact_type)
    raw["payload"]["undeclared_branch_field"] = True
    raw["artifact_hash"] = embedded_hash(raw, "artifact_hash")
    _assert_schema_rejects(raw)
    with pytest.raises(ExecutionSchemaError):
        parse_v02_business_artifact(raw)


@pytest.mark.parametrize("index,artifact_type", tuple(enumerate(ARTIFACT_TYPES)))
def test_each_business_branch_rejects_cross_branch_payload(
    index: int, artifact_type: str
) -> None:
    next_type = ARTIFACT_TYPES[(index + 1) % len(ARTIFACT_TYPES)]
    raw = _artifact(artifact_type, _payloads()[next_type])
    _assert_schema_rejects(raw)
    with pytest.raises(ExecutionSchemaError):
        parse_v02_business_artifact(raw)


@pytest.mark.parametrize("artifact_type", ARTIFACT_TYPES)
def test_each_business_branch_rejects_malformed_required_field_in_both_layers(
    artifact_type: str,
) -> None:
    raw = _artifact(artifact_type)
    raw["payload"][next(iter(raw["payload"]))] = None
    raw["artifact_hash"] = embedded_hash(raw, "artifact_hash")
    _assert_schema_rejects(raw)
    with pytest.raises(ExecutionSchemaError):
        parse_v02_business_artifact(raw)


@pytest.mark.parametrize("invalid_raw_content", [None, 1, 1.5, True])
def test_source_depth_raw_content_schema_runtime_type_parity(
    invalid_raw_content: Any,
) -> None:
    raw = _artifact("source_depth.result")
    raw["payload"]["raw_content"] = invalid_raw_content
    raw["artifact_hash"] = embedded_hash(raw, "artifact_hash")
    _assert_schema_rejects(raw)
    with pytest.raises(ExecutionSchemaError):
        parse_v02_business_artifact(raw)


def test_business_hash_and_duplicate_or_orphan_id_rules_fail_closed() -> None:
    bad_hash = _artifact("claim.collection")
    bad_hash["artifact_hash"] = H2
    with pytest.raises(ExecutionSchemaError, match="artifact_hash mismatch"):
        parse_v02_business_artifact(bad_hash)

    duplicate = _artifact("claim.collection")
    duplicate["payload"]["claims"].append(
        copy.deepcopy(duplicate["payload"]["claims"][0])
    )
    duplicate["artifact_hash"] = embedded_hash(duplicate, "artifact_hash")
    with pytest.raises(ExecutionSchemaError, match="claim IDs must be unique"):
        parse_v02_business_artifact(duplicate)

    orphan = _artifact("source_depth.import_validation")
    orphan["payload"]["sources"][0]["claim_id"] = C2
    orphan["artifact_hash"] = embedded_hash(orphan, "artifact_hash")
    with pytest.raises(ExecutionSchemaError, match="unmapped claim"):
        parse_v02_business_artifact(orphan)

    overlap = _artifact("evidence.merged_collection")
    overlap["payload"]["added_evidence"][0]["evidence_id"] = E1
    overlap["artifact_hash"] = embedded_hash(overlap, "artifact_hash")
    with pytest.raises(ExecutionSchemaError, match="duplicate preserved evidence"):
        parse_v02_business_artifact(overlap)


PLAN_SCHEMA = json.loads(
    (ROOT / "schemas/execution/stage5_execution_plan_v1.schema.json").read_text(
        encoding="utf-8"
    )
)
PLAN_VALIDATOR = Draft202012Validator(PLAN_SCHEMA)
PLAN_V1_1_SCHEMA = json.loads(
    (ROOT / "schemas/execution/stage5_execution_plan_v1_1.schema.json").read_text(
        encoding="utf-8"
    )
)
PLAN_V1_1_VALIDATOR = Draft202012Validator(PLAN_V1_1_SCHEMA)


def _plan_read_binding(
    relative_path: str,
    *,
    binding_mode: str = "frozen_hash",
    content_hash: str | None = H,
    size_bytes: int | None = 12,
) -> dict[str, Any]:
    return {
        "relative_path": relative_path,
        "binding_mode": binding_mode,
        "content_hash_algorithm": "sha256",
        "content_hash": content_hash,
        "size_bytes": size_bytes,
    }


def _write_binding(
    relative_path: str,
    write_mode: str = "create_new",
    expected_content_hash: str | None = None,
) -> dict[str, Any]:
    return {
        "relative_path": relative_path,
        "write_mode": write_mode,
        "expected_content_hash": expected_content_hash,
    }


def _rehash_plan(raw: dict[str, Any]) -> dict[str, Any]:
    raw["plan_hash"] = embedded_hash(raw, "plan_hash")
    return raw


def _plan() -> dict[str, Any]:
    task_dir = f"runs/V02/control/tasks/{TASK}"
    session_dir = f"{task_dir}/sessions/{SESSION}"
    result_path = f"{session_dir}/artifacts/source_depth_result.json"
    ready_path = f"{session_dir}/receipts/result_ready.json"
    materialized_path = f"{session_dir}/receipts/materialization.json"
    publication_receipt_path = f"{session_dir}/receipts/publication.json"
    ledger_path = f"{session_dir}/model_calls.jsonl"
    summary_path = f"{session_dir}/model_usage_summary.json"
    manifest_path = f"{session_dir}/session_manifest.json"
    events_path = f"{session_dir}/events.jsonl"
    observations_path = f"{session_dir}/observations.jsonl"
    registry_path = "runs/V02/artifact_registry.jsonl"
    telemetry_config_path = "configs/observability/model_telemetry_v1.toml"
    dag_source_path = "configs/workflows/youtube_truthfulness_dag_v1_2.yaml"
    workflow_path = "Optmize/workflows/02_深度溯源与结果导入.md"
    prompt_path = "configs/prompts/v02/s02_source_depth_prompt_v1_2.md"
    agent_profile_path = "configs/agents/source_depth_agent_v1_2.toml"
    source_path = f"source_depth/inbox/{A1}/gemini.md"
    registration_manifest_path = (
        f"{task_dir}/registration_manifests/external_depth_action_{R2}.json"
    )
    dag_snapshot_path = f"{task_dir}/dag_snapshots/{CHECKPOINT}.json"
    checkpoint_path = f"{task_dir}/checkpoints/{CHECKPOINT}.json"
    handoff_path = f"{session_dir}/handoff.json"
    handoff_markdown_path = f"{session_dir}/HANDOFF.md"
    read_paths = [
        source_path,
        registry_path,
        telemetry_config_path,
        dag_source_path,
        workflow_path,
        prompt_path,
        agent_profile_path,
    ]
    write_paths = [
        result_path,
        ready_path,
        materialized_path,
        publication_receipt_path,
        registration_manifest_path,
        registry_path,
        dag_snapshot_path,
        checkpoint_path,
        handoff_path,
        handoff_markdown_path,
        ledger_path,
        summary_path,
        manifest_path,
        events_path,
        observations_path,
    ]
    raw = {
        "plan_version": "stage5_execution_plan_v1.0.0",
        "project_version": "v0.2",
        "storage_version": "V02",
        "release_id": "truthfulness_v0.2_youtube_video",
        "task_id": TASK,
        "session_id": SESSION,
        "attempt_no": 1,
        "run_id": RUN,
        "stage_id": "S02",
        "node_id": "external_depth_action",
        "workflow_version": "youtube_truthfulness_workflow_v1.3.0",
        "dag_version": "youtube_truthfulness_dag_v1.2.0",
        "repository_root_ref": "repository",
        "task_directory": task_dir,
        "session_directory": session_dir,
        "read_paths": read_paths,
        "read_bindings": [
            _plan_read_binding(
                path,
                binding_mode="materialize_once"
                if path == source_path
                else "frozen_hash",
                content_hash=None if path == source_path else H,
                size_bytes=None if path == source_path else 12,
            )
            for path in read_paths
        ],
        "write_paths": write_paths,
        "write_bindings": [
            _write_binding(
                path,
                "append_only_expected_head" if path == registry_path else "create_new",
            )
            for path in write_paths
        ],
        "expected_output_paths": [
            result_path,
            ready_path,
            materialized_path,
            publication_receipt_path,
            registration_manifest_path,
            dag_snapshot_path,
            checkpoint_path,
            handoff_path,
            handoff_markdown_path,
        ],
        "expected_registry_head": {
            "relative_path": registry_path,
            "record_count": 1,
            "head_record_id": R1,
            "head_record_hash": H,
            "file_hash": H,
        },
        "granted_gates": ["G0B", "G2"],
        "network_allowed": False,
        "real_media_allowed": False,
        "telemetry": {
            "config_path": telemetry_config_path,
            "config_hash": H,
            "ledger_path": ledger_path,
            "summary_path": summary_path,
            "required": True,
        },
        "node_policy": {
            "execution_kind": "manual_external_capture",
            "required_gate": "G2",
            "expected_artifact_types": ["source_depth.result"],
            "objective": "Capture one exact synthetic external result.",
            "agent_profile_version": "source_depth_agent_v1.2.0",
            "agent_runtime_version": "stage5-runner-v1.0.0",
            "prompt_version": "s02_source_depth_prompt_v1.2.0",
            "contract_files": {
                "workflow": {
                    "relative_path": workflow_path,
                    "version": "youtube_truthfulness_workflow_v1.3.0",
                    "content_hash": H,
                },
                "dag": {
                    "relative_path": dag_source_path,
                    "version": "youtube_truthfulness_dag_v1.2.0",
                    "content_hash": H,
                },
                "prompt": {
                    "relative_path": prompt_path,
                    "version": "s02_source_depth_prompt_v1.2.0",
                    "content_hash": H,
                },
                "agent_profile": {
                    "relative_path": agent_profile_path,
                    "version": "source_depth_agent_v1.2.0",
                    "content_hash": H,
                },
            },
        },
        "session_control": {
            "parent_checkpoint_id": None,
            "bootstrap_refs": [
                {
                    "ref_type": "git_commit",
                    "object_id": "0" * 40,
                    "description": "Frozen synthetic code baseline.",
                },
                {
                    "ref_type": "registry",
                    "relative_path": registry_path,
                    "content_hash_algorithm": "sha256",
                    "content_hash": H,
                    "purpose": "Frozen input Registry head.",
                },
                {
                    "ref_type": "dag_config",
                    "relative_path": dag_source_path,
                    "content_hash_algorithm": "sha256",
                    "content_hash": H,
                    "purpose": "Frozen DAG v1.2 source.",
                },
            ],
            "code_ref": {
                "git_commit": "0" * 40,
                "working_tree_dirty": False,
                "working_tree_manifest_path": None,
                "working_tree_manifest_hash": None,
            },
            "environment_ref": {
                "runtime_name": "python",
                "runtime_version": "3.13",
                "os_family": "windows",
                "architecture": "x86_64",
                "dependency_manifest_path": None,
                "dependency_manifest_hash": None,
            },
            "human_gate_policy": {
                "approval_required": True,
                "gate_node_ids": ["external_depth_action"],
                "decision_artifact_required": True,
                "implicit_approval_allowed": False,
            },
            "session_created_at": "2026-07-19T00:00:00Z",
        },
        "publication": {
            "result_artifact_id": A3,
            "result_record_id": R2,
            "result_ready_receipt_id": RECEIPT1,
            "materialization_receipt_id": RECEIPT2,
            "publication_receipt_path": publication_receipt_path,
            "registration_manifest_path": registration_manifest_path,
            "registry_path": registry_path,
            "dag_source_path": dag_source_path,
            "dag_snapshot_path": dag_snapshot_path,
            "checkpoint_id": CHECKPOINT,
            "checkpoint_path": checkpoint_path,
            "handoff_artifact_id": A4,
            "handoff_record_id": R3,
            "handoff_path": handoff_path,
            "handoff_markdown_path": handoff_markdown_path,
            "recovery_workflow_path": workflow_path,
            "recovery_prompt_path": workflow_path,
            "result_recorded_at": "2026-07-19T00:02:00Z",
            "checkpoint_created_at": "2026-07-19T00:03:00Z",
            "handoff_created_at": "2026-07-19T00:04:00Z",
            "handoff_recorded_at": "2026-07-19T00:05:00Z",
        },
        "manual_input": {
            "source_depth_request_id": REQ,
            "prompt_artifact_id": A1,
            "target_claim_ids": [C1],
            "prompt_created_at": "2026-07-19T00:01:00Z",
            "inbox_directory": f"source_depth/inbox/{A1}",
            "source_input_path": source_path,
            "result_output_path": result_path,
            "result_ready_receipt_path": ready_path,
            "materialization_receipt_path": materialized_path,
            "allowed_extensions": [".json", ".md", ".txt"],
            "max_size_bytes": 20971520,
        },
        "plan_hash": "0" * 64,
    }
    return _rehash_plan(raw)


def _g1a_plan() -> dict[str, Any]:
    raw = _plan()
    task_dir = f"runs/V02/control/tasks/{TASK}"
    session_dir = f"{task_dir}/sessions/{SESSION}"
    registry_path = raw["expected_registry_head"]["relative_path"]
    telemetry_path = raw["telemetry"]["config_path"]
    dag_path = raw["node_policy"]["contract_files"]["dag"]["relative_path"]
    workflow_path = raw["node_policy"]["contract_files"]["workflow"]["relative_path"]
    prompt_path = raw["node_policy"]["contract_files"]["prompt"]["relative_path"]
    profile_path = raw["node_policy"]["contract_files"]["agent_profile"][
        "relative_path"
    ]
    receipt_path = f"runtime/V02/stage5_inputs/{RUN}/input_materialization.json"
    successor_path = f"runtime/V02/stage5_inputs/{RUN}/input_materialization_v1_1.json"
    snapshot_path = f"{task_dir}/dag_snapshots/{CHECKPOINT}.json"
    checkpoint_path = f"{task_dir}/checkpoints/{CHECKPOINT}.json"
    ledger_path = f"{session_dir}/model_calls.jsonl"
    summary_path = f"{session_dir}/model_usage_summary.json"
    observations_path = f"{session_dir}/observations.jsonl"
    manifest_path = f"{session_dir}/session_manifest.json"
    events_path = f"{session_dir}/events.jsonl"
    read_paths = [
        receipt_path,
        registry_path,
        telemetry_path,
        dag_path,
        workflow_path,
        prompt_path,
        profile_path,
    ]
    write_paths = [
        successor_path,
        snapshot_path,
        checkpoint_path,
        ledger_path,
        summary_path,
        observations_path,
        manifest_path,
        events_path,
    ]
    raw.update(
        {
            "plan_version": "stage5_execution_plan_v1.1.0",
            "stage_id": "S01",
            "node_id": "input_binding_control",
            "workflow_version": "youtube_truthfulness_workflow_v1.1.0",
            "read_paths": read_paths,
            "read_bindings": [_plan_read_binding(path) for path in read_paths],
            "write_paths": write_paths,
            "write_bindings": [_write_binding(path) for path in write_paths],
            "expected_output_paths": [successor_path, snapshot_path, checkpoint_path],
            "granted_gates": ["G0B", "G1A"],
            "network_allowed": True,
            "real_media_allowed": True,
            "publication": None,
            "manual_input": None,
        }
    )
    raw["telemetry"].update(ledger_path=ledger_path, summary_path=summary_path)
    raw["node_policy"].update(
        execution_kind="non_model",
        required_gate="G1A",
        expected_artifact_types=[],
        objective="Bind one registered media.video before S01 business execution.",
        agent_profile_version="stage5_v02_agent_v1.0.0",
        prompt_version="contract_auditor_prompt_v1.0.0",
    )
    for name, version in (
        ("workflow", "youtube_truthfulness_workflow_v1.1.0"),
        ("dag", "youtube_truthfulness_dag_v1.2.0"),
        ("prompt", "contract_auditor_prompt_v1.0.0"),
        ("agent_profile", "stage5_v02_agent_v1.0.0"),
    ):
        raw["node_policy"]["contract_files"][name]["version"] = version
    raw["session_control"]["human_gate_policy"].update(
        gate_node_ids=["input_binding_control"],
        decision_artifact_required=False,
    )
    raw["artifact_read_bindings"] = [
        {
            "binding_kind": "receipt_bound_artifact",
            "artifact_ref": {
                "artifact_id": A1,
                "artifact_type": "media.video",
                "record_id": R1,
                "relative_path": "runs/V02/source.mp4",
                "content_hash_algorithm": "sha256",
                "content_hash": H,
                "input_fingerprint": None,
                "validation_status": "passed",
                "lifecycle_state": "validated",
            },
            "receipt_path_ref": {
                "relative_path": receipt_path,
                "content_hash_algorithm": "sha256",
                "content_hash": H,
                "purpose": "bind v1.0 cache receipt for the unique G1A media read",
            },
            "required_receipt_version": "input_materialization_v1.0.0",
            "receipt_semantic_hash": H2,
            "required_storage_root_ref": "ubuntu_native_materialized_v02",
            "access_policy": {
                "content_read": "single_sequential_sha256",
                "decode_allowed": False,
            },
        }
    ]
    raw["control_finalization"] = {
        "mode": "input_binding_no_handoff",
        "successor_receipt_path": successor_path,
        "dag_source_path": dag_path,
        "dag_snapshot_path": snapshot_path,
        "checkpoint_id": CHECKPOINT,
        "checkpoint_path": checkpoint_path,
        "terminal_at": "2026-07-19T00:02:00Z",
        "checkpoint_created_at": "2026-07-19T00:03:00Z",
    }
    return _rehash_plan(raw)


def _assert_plan_runtime_rejects(raw: dict[str, Any], message: str) -> None:
    _rehash_plan(raw)
    with pytest.raises(ExecutionSchemaError, match=message):
        parse_stage5_execution_plan(raw)


def _assert_plan_schema_and_runtime_reject(raw: dict[str, Any], message: str) -> None:
    _rehash_plan(raw)
    assert list(PLAN_VALIDATOR.iter_errors(raw))
    with pytest.raises(ExecutionSchemaError, match=message):
        parse_stage5_execution_plan(raw)


def _remove_read(raw: dict[str, Any], relative_path: str) -> None:
    index = raw["read_paths"].index(relative_path)
    raw["read_paths"].pop(index)
    binding = raw["read_bindings"].pop(index)
    assert binding["relative_path"] == relative_path


def _replace_declared_read(raw: dict[str, Any], old_path: str, new_path: str) -> None:
    index = raw["read_paths"].index(old_path)
    raw["read_paths"][index] = new_path
    assert raw["read_bindings"][index]["relative_path"] == old_path
    raw["read_bindings"][index]["relative_path"] = new_path


def _read_binding_for(raw: dict[str, Any], relative_path: str) -> dict[str, Any]:
    return next(
        binding
        for binding in raw["read_bindings"]
        if binding["relative_path"] == relative_path
    )


def _contract_file_for(raw: dict[str, Any], name: str) -> dict[str, Any]:
    return raw["node_policy"]["contract_files"][name]


def _make_contract_the_only_deferred_read(raw: dict[str, Any], name: str) -> None:
    original_source_path = raw["manual_input"]["source_input_path"]
    original_source = _read_binding_for(raw, original_source_path)
    original_source.update(binding_mode="frozen_hash", content_hash=H, size_bytes=12)
    contract_path = _contract_file_for(raw, name)["relative_path"]
    contract_binding = _read_binding_for(raw, contract_path)
    contract_binding.update(
        binding_mode="materialize_once",
        content_hash=None,
        size_bytes=None,
    )
    raw["manual_input"]["source_input_path"] = contract_path


def _replace_declared_write(raw: dict[str, Any], old_path: str, new_path: str) -> None:
    index = raw["write_paths"].index(old_path)
    raw["write_paths"][index] = new_path
    assert raw["write_bindings"][index]["relative_path"] == old_path
    raw["write_bindings"][index]["relative_path"] = new_path
    raw["expected_output_paths"] = [
        new_path if path == old_path else path for path in raw["expected_output_paths"]
    ]


def test_stage5_plan_schema_runtime_hash_and_g2_scope() -> None:
    raw = _plan()
    assert not list(PLAN_VALIDATOR.iter_errors(raw))
    assert parse_stage5_execution_plan(raw).model_dump(mode="json") == raw

    for mutation, message in (
        (("granted_gates", ["G0B"]), "requires granted G2"),
        (("network_allowed", True), "cannot authorize project network"),
    ):
        changed = copy.deepcopy(raw)
        changed[mutation[0]] = mutation[1]
        _assert_plan_runtime_rejects(changed, message)

    changed = copy.deepcopy(raw)
    changed["plan_hash"] = H2
    with pytest.raises(ExecutionSchemaError, match="plan_hash mismatch"):
        parse_stage5_execution_plan(changed)


def test_g1a_plan_v11_binds_media_receipt_and_no_handoff_finalizer() -> None:
    raw = _g1a_plan()

    assert list(PLAN_VALIDATOR.iter_errors(raw))
    assert not list(PLAN_V1_1_VALIDATOR.iter_errors(raw))
    parsed = parse_stage5_execution_plan(raw)

    assert isinstance(parsed, Stage5ExecutionPlanV1_1)
    assert parsed.model_dump(mode="json") == raw
    assert parsed.control_finalization is not None
    assert parsed.publication is None
    assert parsed.node_policy.expected_artifact_types == []


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda raw: raw["artifact_read_bindings"][0].update(
                {"required_receipt_version": "input_materialization_v1.1.0"}
            ),
            "bind the v1.0 receipt",
        ),
        (
            lambda raw: raw["artifact_read_bindings"][0]["access_policy"].update(
                {"decode_allowed": True}
            ),
            "cannot authorize decode|without decode",
        ),
        (
            lambda raw: raw["artifact_read_bindings"][0]["receipt_path_ref"].update(
                {"content_hash": H2}
            ),
            "must match one exact frozen_hash",
        ),
        (
            lambda raw: raw["write_paths"].append(
                f"runs/V02/control/tasks/{TASK}/HANDOFF.md"
            ),
            "write_bindings must correspond one-to-one|HANDOFF",
        ),
    ],
)
def test_g1a_plan_v11_rejects_receipt_decode_scope_or_handoff_drift(
    mutation: Any,
    message: str,
) -> None:
    raw = _g1a_plan()
    mutation(raw)
    _rehash_plan(raw)

    with pytest.raises(ExecutionSchemaError, match=message):
        parse_stage5_execution_plan(raw)


def test_external_depth_plan_has_one_exact_deferred_source_and_all_other_reads_frozen() -> (
    None
):
    raw = _plan()
    source_path = raw["manual_input"]["source_input_path"]
    materialize_once = [
        binding
        for binding in raw["read_bindings"]
        if binding["binding_mode"] == "materialize_once"
    ]
    assert materialize_once == [
        {
            "relative_path": source_path,
            "binding_mode": "materialize_once",
            "content_hash_algorithm": "sha256",
            "content_hash": None,
            "size_bytes": None,
        }
    ]
    assert all(
        binding["binding_mode"] == "frozen_hash"
        and binding["content_hash"] is not None
        and binding["size_bytes"] is not None
        for binding in raw["read_bindings"]
        if binding["relative_path"] != source_path
    )


def test_all_four_contract_files_are_unique_exact_frozen_reads() -> None:
    raw = _plan()
    contract_files = raw["node_policy"]["contract_files"]
    assert set(contract_files) == {"workflow", "dag", "prompt", "agent_profile"}
    paths = [reference["relative_path"] for reference in contract_files.values()]
    assert len(paths) == len(set(paths)) == 4
    for reference in contract_files.values():
        binding = _read_binding_for(raw, reference["relative_path"])
        assert binding["binding_mode"] == "frozen_hash"
        assert binding["content_hash"] == reference["content_hash"]
        assert binding["size_bytes"] is not None


def test_contract_file_paths_must_be_unique() -> None:
    raw = _plan()
    raw["node_policy"]["contract_files"]["dag"]["relative_path"] = raw["node_policy"][
        "contract_files"
    ]["workflow"]["relative_path"]
    _rehash_plan(raw)
    assert not list(PLAN_VALIDATOR.iter_errors(raw))
    with pytest.raises(
        ExecutionSchemaError, match="contract file paths must be unique"
    ):
        parse_stage5_execution_plan(raw)


@pytest.mark.parametrize("name", ["workflow", "dag", "prompt", "agent_profile"])
def test_each_contract_file_is_required_in_schema_and_runtime(name: str) -> None:
    raw = _plan()
    del raw["node_policy"]["contract_files"][name]
    _assert_plan_schema_and_runtime_reject(raw, "Field required")


@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("workflow", "recovery Workflow must be an exact declared read"),
        ("dag", "DAG snapshot source must be an exact declared read"),
        ("prompt", "prompt contract file must match an exact frozen_hash read"),
        (
            "agent_profile",
            "agent_profile contract file must match an exact frozen_hash read",
        ),
    ],
)
def test_each_contract_file_must_be_an_exact_declared_read(
    name: str, message: str
) -> None:
    raw = _plan()
    _remove_read(raw, _contract_file_for(raw, name)["relative_path"])
    _assert_plan_runtime_rejects(raw, message)


@pytest.mark.parametrize("name", ["workflow", "dag", "prompt", "agent_profile"])
def test_each_contract_file_rejects_deferred_binding(name: str) -> None:
    raw = _plan()
    _make_contract_the_only_deferred_read(raw, name)
    _rehash_plan(raw)
    assert not list(PLAN_VALIDATOR.iter_errors(raw))
    with pytest.raises(
        ExecutionSchemaError,
        match=rf"{name} contract file must match an exact frozen_hash read",
    ):
        parse_stage5_execution_plan(raw)


@pytest.mark.parametrize("name", ["workflow", "dag", "prompt", "agent_profile"])
def test_each_contract_file_hash_must_match_its_frozen_binding(name: str) -> None:
    raw = _plan()
    _contract_file_for(raw, name)["content_hash"] = H2
    _assert_plan_runtime_rejects(
        raw,
        rf"{name} contract file must match an exact frozen_hash read",
    )


@pytest.mark.parametrize(
    ("name", "expected_version"),
    [
        ("workflow", "youtube_truthfulness_workflow_v1.3.0"),
        ("dag", "youtube_truthfulness_dag_v1.2.0"),
        ("prompt", "s02_source_depth_prompt_v1.2.0"),
        ("agent_profile", "source_depth_agent_v1.2.0"),
    ],
)
def test_each_contract_file_version_matches_plan_or_node_policy_identity(
    name: str, expected_version: str
) -> None:
    raw = _plan()
    assert _contract_file_for(raw, name)["version"] == expected_version
    _contract_file_for(raw, name)["version"] = "wrong_contract_v9"
    _assert_plan_runtime_rejects(
        raw,
        rf"{name} contract version differs from plan identity",
    )


@pytest.mark.parametrize(
    ("binding_mode", "content_hash", "size_bytes", "message"),
    [
        ("frozen_hash", None, 12, "frozen_hash read requires exact hash and size"),
        ("frozen_hash", H, None, "frozen_hash read requires exact hash and size"),
        (
            "materialize_once",
            H,
            None,
            "materialize_once read requires null hash and size",
        ),
        (
            "materialize_once",
            None,
            12,
            "materialize_once read requires null hash and size",
        ),
    ],
)
def test_read_binding_mode_hash_and_size_semantics_have_schema_runtime_parity(
    binding_mode: str,
    content_hash: str | None,
    size_bytes: int | None,
    message: str,
) -> None:
    raw = _plan()
    target = (
        raw["telemetry"]["config_path"]
        if binding_mode == "frozen_hash"
        else raw["manual_input"]["source_input_path"]
    )
    binding = _read_binding_for(raw, target)
    binding.update(
        binding_mode=binding_mode,
        content_hash=content_hash,
        size_bytes=size_bytes,
    )
    _assert_plan_schema_and_runtime_reject(raw, message)


@pytest.mark.parametrize(
    "missing_field",
    ["binding_mode", "content_hash_algorithm", "content_hash", "size_bytes"],
)
def test_read_binding_missing_field_is_required_in_schema_and_runtime(
    missing_field: str,
) -> None:
    raw = _plan()
    del raw["read_bindings"][0][missing_field]
    _rehash_plan(raw)
    assert list(PLAN_VALIDATOR.iter_errors(raw))
    with pytest.raises(ExecutionSchemaError):
        parse_stage5_execution_plan(raw)


def test_external_depth_action_requires_exactly_one_materialize_once_read() -> None:
    zero = _plan()
    source = _read_binding_for(zero, zero["manual_input"]["source_input_path"])
    source.update(binding_mode="frozen_hash", content_hash=H, size_bytes=12)
    _assert_plan_runtime_rejects(
        zero, "requires exactly one source_input_path materialize_once read"
    )

    two = _plan()
    prompt = _read_binding_for(two, two["publication"]["recovery_prompt_path"])
    prompt.update(binding_mode="materialize_once", content_hash=None, size_bytes=None)
    _assert_plan_runtime_rejects(
        two, "requires exactly one source_input_path materialize_once read"
    )


def test_materialize_once_read_path_must_equal_manual_source_input_path() -> None:
    raw = _plan()
    raw["manual_input"]["source_input_path"] = f"source_depth/inbox/{A1}/different.md"
    _assert_plan_runtime_rejects(
        raw, "requires exactly one source_input_path materialize_once read"
    )


def test_materialize_once_is_reserved_for_external_depth_action() -> None:
    raw = _plan()
    raw["node_id"] = "claim_generation"
    raw["node_policy"].update(
        execution_kind="interceptable_model",
        required_gate="G0B",
        expected_artifact_types=["claim.collection"],
    )
    raw["manual_input"] = None
    _assert_plan_runtime_rejects(
        raw, "materialize_once read is reserved for external_depth_action"
    )


@pytest.mark.parametrize(
    "protected_path",
    [
        "runs/V02/artifact_registry.jsonl",
        "configs/observability/model_telemetry_v1.toml",
        "configs/workflows/youtube_truthfulness_dag_v1_2.yaml",
        "Optmize/workflows/02_深度溯源与结果导入.md",
        "configs/prompts/v02/s02_source_depth_prompt_v1_2.md",
        "configs/agents/source_depth_agent_v1_2.toml",
    ],
)
def test_bootstrap_registry_telemetry_and_contract_files_cannot_be_deferred(
    protected_path: str,
) -> None:
    raw = _plan()
    protected = _read_binding_for(raw, protected_path)
    protected.update(
        binding_mode="materialize_once", content_hash=None, size_bytes=None
    )
    _rehash_plan(raw)
    assert not list(PLAN_VALIDATOR.iter_errors(raw))
    with pytest.raises(
        ExecutionSchemaError,
        match="requires exactly one source_input_path materialize_once read",
    ):
        parse_stage5_execution_plan(raw)


def test_safe_repo_relative_source_outside_inbox_is_plan_valid() -> None:
    raw = _plan()
    old_path = raw["manual_input"]["source_input_path"]
    new_path = "runs/V02/manual_drop/gemini.md"
    _replace_declared_read(raw, old_path, new_path)
    raw["manual_input"]["source_input_path"] = new_path
    _rehash_plan(raw)
    assert not list(PLAN_VALIDATOR.iter_errors(raw))
    assert parse_stage5_execution_plan(raw).manual_input.source_input_path == new_path


@pytest.mark.parametrize(
    "unsafe_path", ["D:/outside/gemini.md", "../outside/gemini.md"]
)
def test_manual_source_input_path_still_rejects_absolute_and_parent_escape(
    unsafe_path: str,
) -> None:
    raw = _plan()
    raw["manual_input"]["source_input_path"] = unsafe_path
    _assert_plan_schema_and_runtime_reject(raw, "absolute paths are forbidden|escaping")


@pytest.mark.parametrize(
    ("write_mode", "expected_content_hash"),
    [
        ("create_new", None),
        ("append_only_expected_head", None),
        ("atomic_replace_expected_hash", H2),
    ],
)
def test_all_three_write_modes_have_schema_runtime_parity(
    write_mode: str, expected_content_hash: str | None
) -> None:
    raw = _plan()
    target = raw["expected_registry_head"]["relative_path"]
    index = raw["write_paths"].index(target)
    if write_mode != "append_only_expected_head":
        target = raw["telemetry"]["summary_path"]
        index = raw["write_paths"].index(target)
        registry_index = raw["write_paths"].index(
            raw["expected_registry_head"]["relative_path"]
        )
        raw["write_bindings"][registry_index] = _write_binding(
            raw["expected_registry_head"]["relative_path"], "append_only_expected_head"
        )
    raw["write_bindings"][index] = _write_binding(
        target, write_mode, expected_content_hash
    )
    _rehash_plan(raw)
    assert not list(PLAN_VALIDATOR.iter_errors(raw))
    assert parse_stage5_execution_plan(raw).model_dump(mode="json") == raw


@pytest.mark.parametrize(
    ("write_mode", "expected_content_hash", "message"),
    [
        ("create_new", H2, "only atomic replace"),
        ("append_only_expected_head", H2, "only atomic replace"),
        ("atomic_replace_expected_hash", None, "atomic replace requires"),
    ],
)
def test_write_mode_hash_semantics_fail_closed_in_schema_and_runtime(
    write_mode: str, expected_content_hash: str | None, message: str
) -> None:
    raw = _plan()
    target = raw["telemetry"]["summary_path"]
    index = raw["write_paths"].index(target)
    raw["write_bindings"][index] = _write_binding(
        target, write_mode, expected_content_hash
    )
    _rehash_plan(raw)
    assert list(PLAN_VALIDATOR.iter_errors(raw))
    with pytest.raises(ExecutionSchemaError, match=message):
        parse_stage5_execution_plan(raw)


@pytest.mark.parametrize("missing_field", ["write_mode", "expected_content_hash"])
def test_write_binding_missing_field_is_required_in_schema_and_runtime(
    missing_field: str,
) -> None:
    raw = _plan()
    del raw["write_bindings"][0][missing_field]
    _rehash_plan(raw)
    assert list(PLAN_VALIDATOR.iter_errors(raw))
    with pytest.raises(ExecutionSchemaError):
        parse_stage5_execution_plan(raw)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            ("task_directory", "runs/V02/control/tasks/not_the_task"),
            "bind the exact task_id",
        ),
        (
            ("session_directory", f"runs/V02/sessions/{SESSION}"),
            "exact task-level Session",
        ),
        (
            (
                "checkpoint_path",
                f"runs/V02/sessions/{SESSION}/checkpoints/{CHECKPOINT}.json",
            ),
            "task-level",
        ),
        (
            ("handoff_path", f"runs/V02/control/tasks/{TASK}/handoff.json"),
            "Session-local handoff.json",
        ),
        (
            ("handoff_markdown_path", f"runs/V02/control/tasks/{TASK}/HANDOFF.md"),
            "Session-local HANDOFF.md",
        ),
    ],
)
def test_task_session_checkpoint_and_handoff_paths_are_canonical(
    mutation: tuple[str, str], message: str
) -> None:
    raw = _plan()
    field, value = mutation
    if field == "task_directory":
        raw[field] = value
        raw["session_directory"] = f"{value}/sessions/{SESSION}"
    elif field == "session_directory":
        raw[field] = value
    else:
        old_path = raw["publication"][field]
        raw["publication"][field] = value
        _replace_declared_write(raw, old_path, value)
    _assert_plan_runtime_rejects(raw, message)


@pytest.mark.parametrize(
    "expected_types",
    [
        [],
        ["source_depth.result", "source_depth.import_validation"],
        ["source_depth.prompt"],
    ],
)
def test_g2_external_capture_expected_artifact_types_are_exactly_locked(
    expected_types: list[str],
) -> None:
    raw = _plan()
    raw["node_policy"]["expected_artifact_types"] = expected_types
    _assert_plan_runtime_rejects(raw, "produce only source_depth.result")


@pytest.mark.parametrize(
    ("path_getter", "message"),
    [
        (
            lambda raw: raw["expected_registry_head"]["relative_path"],
            "Registry head must match an exact frozen_hash read binding",
        ),
        (lambda raw: raw["telemetry"]["config_path"], "telemetry"),
        (lambda raw: raw["publication"]["dag_source_path"], "DAG snapshot source"),
        (lambda raw: raw["publication"]["recovery_workflow_path"], "recovery Workflow"),
        (lambda raw: raw["publication"]["recovery_prompt_path"], "recovery Workflow"),
    ],
)
def test_registry_telemetry_dag_workflow_and_prompt_are_exact_reads(
    path_getter: Any, message: str
) -> None:
    raw = _plan()
    _remove_read(raw, path_getter(raw))
    _assert_plan_runtime_rejects(raw, message)


@pytest.mark.parametrize(
    ("duplicate_field", "source_field", "message"),
    [
        ("handoff_artifact_id", "result_artifact_id", "Artifact IDs must be distinct"),
        ("handoff_record_id", "result_record_id", "record IDs must be distinct"),
        (
            "materialization_receipt_id",
            "result_ready_receipt_id",
            "receipt IDs must be distinct",
        ),
    ],
)
def test_publication_artifact_record_and_receipt_ids_are_unique(
    duplicate_field: str, source_field: str, message: str
) -> None:
    raw = _plan()
    raw["publication"][duplicate_field] = raw["publication"][source_field]
    _assert_plan_runtime_rejects(raw, message)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "result_recorded_at",
            "2026-07-18T23:59:59Z",
            "cannot predate Session creation",
        ),
        (
            "result_recorded_at",
            "2026-07-19T00:00:30Z",
            "cannot predate the manual prompt",
        ),
        (
            "checkpoint_created_at",
            "2026-07-19T00:01:59Z",
            "monotonically ordered",
        ),
        (
            "handoff_created_at",
            "2026-07-19T00:02:59Z",
            "monotonically ordered",
        ),
        (
            "handoff_recorded_at",
            "2026-07-19T00:03:59Z",
            "monotonically ordered",
        ),
    ],
)
def test_publication_times_obey_causal_order(
    field: str, value: str, message: str
) -> None:
    raw = _plan()
    raw["publication"][field] = value
    _assert_plan_runtime_rejects(raw, message)


def test_attempt_one_and_retry_parent_checkpoint_semantics() -> None:
    first = _plan()
    first["session_control"]["parent_checkpoint_id"] = CHECKPOINT
    _assert_plan_runtime_rejects(first, "attempt 1 cannot bind a parent checkpoint")

    retry_without_parent = _plan()
    retry_without_parent["attempt_no"] = 2
    _assert_plan_runtime_rejects(
        retry_without_parent, "retry attempt requires a parent checkpoint"
    )

    retry = _plan()
    retry["attempt_no"] = 2
    retry["session_control"]["parent_checkpoint_id"] = CHECKPOINT
    _rehash_plan(retry)
    assert not list(PLAN_VALIDATOR.iter_errors(retry))
    assert parse_stage5_execution_plan(retry).attempt_no == 2


@pytest.mark.parametrize(
    ("model", "definition"),
    [
        (Stage5ExecutionPlan, None),
        (PlanReadBinding, "planReadBinding"),
        (WriteBinding, "writeBinding"),
        (RegistryHeadBinding, "registryHead"),
        (TelemetryPlan, "telemetry"),
        (NodeExecutionPolicy, "nodePolicy"),
        (ContractFileRef, "contractFile"),
        (Stage5ContractFiles, "contractFiles"),
        (SessionControlPlan, "sessionControl"),
        (Stage5PublicationPlan, "publication"),
        (ManualInputPolicy, "manualInput"),
    ],
)
def test_stage5_schema_runtime_required_fields_are_synchronized(
    model: Any, definition: str | None
) -> None:
    schema_object = (
        PLAN_SCHEMA if definition is None else PLAN_SCHEMA["$defs"][definition]
    )
    schema_required = set(schema_object.get("required", []))
    runtime_required = {
        name for name, field in model.model_fields.items() if field.is_required()
    }
    assert runtime_required == schema_required


def _domain_review_draft(
    *,
    decision: str = "non_product_verified",
    reviewer_role: str = "authorized_human",
) -> dict[str, Any]:
    return {
        "receipt_version": "non_product_domain_review_receipt_v1.0.0",
        "receipt_id": RECEIPT1,
        "source_id": "youtube_abcdefghijk",
        "input_artifact_id": A1,
        "input_content_hash_algorithm": "sha256",
        "input_content_hash": H,
        "decision": decision,
        "reviewer_id": "reviewer.synthetic_a",
        "reviewer_role": reviewer_role,
        "reviewed_at": NOW,
        "review_scope": "entire_source_material",
        "review_reason": "Synthetic human review for the pre-model gate.",
    }


def test_non_product_domain_review_receipt_seal_parse_and_hash() -> None:
    receipt = seal_non_product_domain_review_receipt(_domain_review_draft())
    assert isinstance(receipt, NonProductDomainReviewReceiptV1)
    raw = receipt.model_dump(mode="json")
    assert raw["receipt_hash"] == embedded_hash(raw, "receipt_hash")
    assert parse_non_product_domain_review_receipt(raw) == receipt

    tampered = copy.deepcopy(raw)
    tampered["input_content_hash"] = H2
    with pytest.raises(ExecutionSchemaError, match="receipt_hash mismatch"):
        parse_non_product_domain_review_receipt(tampered)


def test_non_product_domain_review_receipt_sealer_rejects_supplied_hash() -> None:
    draft = _domain_review_draft()
    draft["receipt_hash"] = H
    with pytest.raises(Stage5ContractError, match="cannot supply a sealed hash"):
        seal_non_product_domain_review_receipt(draft)


def test_interceptable_model_plan_requires_human_decision_artifact() -> None:
    raw = _plan()
    raw["node_id"] = "claim_generation"
    raw["node_policy"].update(
        execution_kind="interceptable_model",
        required_gate="G0B",
        expected_artifact_types=["claim.collection"],
    )
    raw["manual_input"] = None
    source = next(
        binding
        for binding in raw["read_bindings"]
        if binding["binding_mode"] == "materialize_once"
    )
    source.update(binding_mode="frozen_hash", content_hash=H, size_bytes=12)
    raw["session_control"]["human_gate_policy"].update(
        gate_node_ids=["claim_generation"],
        decision_artifact_required=False,
    )
    _assert_plan_runtime_rejects(
        raw, "requires an explicit human decision artifact"
    )


def _receipt(kind: str) -> dict[str, Any]:
    stat = {"device": 1, "inode": 2, "size_bytes": 12, "mtime_ns": 3, "mode": 33206}
    materialized = kind == "materialization"
    raw = {
        "receipt_version": "manual_external_input_receipt_v1.0.0",
        "receipt_kind": kind,
        "receipt_id": _id("receipt", "1" if not materialized else "2"),
        "task_id": TASK,
        "session_id": SESSION,
        "attempt_no": 1,
        "run_id": RUN,
        "source_depth_request_id": REQ,
        "prompt_artifact_id": A1,
        "source_relative_path": f"source_depth/inbox/{A1}/gemini.md",
        "user_signal_at": NOW,
        "signal_semantic_hash": H,
        "result_ready": True,
        "permission": "capture_only",
        "stat_before": copy.deepcopy(stat) if materialized else None,
        "stat_after": copy.deepcopy(stat) if materialized else None,
        "content_hash": H if materialized else None,
        "size_bytes": 12 if materialized else None,
        "media_type": "text/markdown" if materialized else None,
        "receipt_hash": "0" * 64,
    }
    raw["receipt_hash"] = embedded_hash(raw, "receipt_hash")
    return raw


@pytest.mark.parametrize("kind", ["result_ready", "materialization"])
def test_manual_receipt_schema_runtime_parity(kind: str) -> None:
    schema = json.loads(
        (
            ROOT / "schemas/execution/manual_external_input_receipt_v1.schema.json"
        ).read_text(encoding="utf-8")
    )
    raw = _receipt(kind)
    assert not list(Draft202012Validator(schema).iter_errors(raw))
    assert parse_manual_external_input_receipt(raw).model_dump(mode="json") == raw


def test_materialization_receipt_rejects_file_changed_during_read() -> None:
    raw = _receipt("materialization")
    raw["stat_after"]["mtime_ns"] += 1
    raw["receipt_hash"] = embedded_hash(raw, "receipt_hash")
    with pytest.raises(ExecutionSchemaError, match="stable stat-before"):
        parse_manual_external_input_receipt(raw)


def _rehash_business_artifact(raw: dict[str, Any]) -> dict[str, Any]:
    raw["artifact_hash"] = "0" * 64
    raw["artifact_hash"] = embedded_hash(raw, "artifact_hash")
    return raw


def _ocr_gate(artifact_id: str, gate_state: str) -> dict[str, Any]:
    payload = copy.deepcopy(_payloads()["ocr.gate_decision"])
    payload["gate_state"] = gate_state
    payload["adapter_profile_version"] = (
        "ocr-profile-v1" if gate_state == "EXECUTED" else None
    )
    raw = _artifact("ocr.gate_decision", payload)
    raw.update(
        {
            "artifact_id": artifact_id,
            "dag_node_id": "optional_ocr",
            "upstream_artifact_ids": [A2],
        }
    )
    return _rehash_business_artifact(raw)


def _ocr_result(artifact_id: str, gate_id: str) -> dict[str, Any]:
    payload = copy.deepcopy(_payloads()["ocr.result"])
    payload["gate_decision_artifact_id"] = gate_id
    raw = _artifact("ocr.result", payload)
    raw.update(
        {
            "artifact_id": artifact_id,
            "dag_node_id": "optional_ocr",
            "upstream_artifact_ids": [gate_id],
        }
    )
    return _rehash_business_artifact(raw)


def _ocr_alignment(
    artifact_id: str,
    gate_id: str,
    result_id: str | None,
) -> dict[str, Any]:
    payload = copy.deepcopy(_payloads()["transcript.alignment"])
    payload["ocr_gate_decision_artifact_id"] = gate_id
    payload["ocr_result_artifact_id"] = result_id
    upstream = [A1, A2, gate_id]
    if result_id is not None:
        upstream.append(result_id)
    raw = _artifact("transcript.alignment", payload)
    raw.update(
        {
            "artifact_id": artifact_id,
            "dag_node_id": "transcript_normalize_and_align",
            "upstream_artifact_ids": upstream,
        }
    )
    return _rehash_business_artifact(raw)


def _ocr_registry_record(
    artifact: dict[str, Any],
    *,
    supersedes: list[str] | None = None,
    lifecycle: str = "validated",
):
    return to_artifact_record_view(
        create_artifact_record(
            artifact_id=artifact["artifact_id"],
            artifact_type=artifact["artifact_type"],
            logical_name=f"synthetic-{artifact['artifact_type']}",
            container_kind="file",
            project_version="v0.2",
            storage_version="V02",
            source_platform="youtube",
            source_id="youtube_synth3tic01",
            run_id=RUN,
            stage_id=artifact["stage_id"],
            dag_node_id=artifact["dag_node_id"],
            relative_path=f"runs/V02/{RUN}/{artifact['artifact_id']}.json",
            storage_scope="run",
            media_type="application/json",
            size_bytes=len(json.dumps(artifact, ensure_ascii=False).encode("utf-8")),
            content_hash=artifact["artifact_hash"],
            producer_type="workflow",
            schema_versions=[
                "artifact_record_v1.1.0",
                "v02_business_artifact_v1.0.0",
            ],
            tool_versions={"synthetic": "1"},
            upstream_artifact_ids=artifact["upstream_artifact_ids"],
            authority_level="machine_derived",
            lifecycle_state=lifecycle,
            validation_status="passed",
            privacy_class="public_synthetic",
            access_scope="public",
            retention_policy="test only",
            created_at=datetime(2026, 7, 19, tzinfo=timezone.utc),
            supersedes=supersedes or [],
        )
    )


def _validate_ocr_fixture(
    *artifacts: dict[str, Any],
    supersedes: dict[str, list[str]] | None = None,
    lifecycles: dict[str, str] | None = None,
):
    supersedes = supersedes or {}
    lifecycles = lifecycles or {}
    records = [
        _ocr_registry_record(
            artifact,
            supersedes=supersedes.get(artifact["artifact_id"]),
            lifecycle=lifecycles.get(artifact["artifact_id"], "validated"),
        )
        for artifact in artifacts
    ]
    return validate_ocr_branch_contract(
        run_id=RUN,
        artifacts=artifacts,
        registry_records=records,
    )


def test_ocr_not_applicable_allows_alignment_with_null_result_ref() -> None:
    gate = _ocr_gate(A3, "NOT_APPLICABLE")
    before_alignment = _validate_ocr_fixture(gate)
    assert before_alignment.alignment_allowed is True
    assert before_alignment.result_artifact_id is None
    assert before_alignment.alignment_artifact_ids == []

    alignment = _ocr_alignment(A4, A3, None)
    after_alignment = _validate_ocr_fixture(gate, alignment)
    assert after_alignment.gate_state == "NOT_APPLICABLE"
    assert after_alignment.alignment_artifact_ids == [A4]


def test_ocr_required_blocked_stops_without_result_or_alignment() -> None:
    validation = _validate_ocr_fixture(_ocr_gate(A3, "REQUIRED_BLOCKED"))
    assert validation.gate_state == "REQUIRED_BLOCKED"
    assert validation.alignment_allowed is False
    assert validation.result_artifact_id is None
    assert validation.alignment_artifact_ids == []


def test_ocr_executed_requires_matching_result_and_alignment_refs() -> None:
    gate = _ocr_gate(A3, "EXECUTED")
    result = _ocr_result(A4, A3)
    alignment = _ocr_alignment(A5, A3, A4)
    validation = _validate_ocr_fixture(gate, result, alignment)
    assert validation.gate_state == "EXECUTED"
    assert validation.result_artifact_id == A4
    assert validation.alignment_artifact_ids == [A5]
    assert validation.alignment_allowed is True


def test_ocr_superseded_blocked_gate_does_not_compete_with_executed_gate() -> None:
    blocked = _ocr_gate(A3, "REQUIRED_BLOCKED")
    executed = _ocr_gate(A4, "EXECUTED")
    result = _ocr_result(A5, A4)
    alignment = _ocr_alignment(A6, A4, A5)
    validation = _validate_ocr_fixture(
        blocked,
        executed,
        result,
        alignment,
        supersedes={A4: [A3]},
    )
    assert validation.gate_decision_artifact_id == A4
    assert validation.result_artifact_id == A5


def test_ocr_multiple_unsuperseded_gates_are_rejected() -> None:
    with pytest.raises(Stage5ContractError, match="exactly one current valid gate"):
        _validate_ocr_fixture(
            _ocr_gate(A3, "NOT_APPLICABLE"),
            _ocr_gate(A4, "NOT_APPLICABLE"),
        )


def test_ocr_not_applicable_rejects_result() -> None:
    with pytest.raises(Stage5ContractError, match="forbids current OCR results"):
        _validate_ocr_fixture(
            _ocr_gate(A3, "NOT_APPLICABLE"),
            _ocr_result(A4, A3),
        )


def test_ocr_not_applicable_alignment_requires_null_result_ref() -> None:
    with pytest.raises(Stage5ContractError, match="must bind null"):
        _validate_ocr_fixture(
            _ocr_gate(A3, "NOT_APPLICABLE"),
            _ocr_alignment(A4, A3, A5),
        )


def test_ocr_required_blocked_rejects_result() -> None:
    with pytest.raises(Stage5ContractError, match="forbids current OCR results"):
        _validate_ocr_fixture(
            _ocr_gate(A3, "REQUIRED_BLOCKED"),
            _ocr_result(A4, A3),
        )


def test_ocr_required_blocked_rejects_alignment() -> None:
    with pytest.raises(Stage5ContractError, match="forbids transcript alignment"):
        _validate_ocr_fixture(
            _ocr_gate(A3, "REQUIRED_BLOCKED"),
            _ocr_alignment(A4, A3, None),
        )


def test_ocr_executed_rejects_missing_result() -> None:
    with pytest.raises(
        Stage5ContractError, match="exactly one current valid OCR result"
    ):
        _validate_ocr_fixture(_ocr_gate(A3, "EXECUTED"))


def test_ocr_executed_rejects_invalid_result_record() -> None:
    with pytest.raises(
        Stage5ContractError, match="exactly one current valid OCR result"
    ):
        _validate_ocr_fixture(
            _ocr_gate(A3, "EXECUTED"),
            _ocr_result(A4, A3),
            lifecycles={A4: "invalid"},
        )


def test_ocr_executed_rejects_multiple_current_results() -> None:
    with pytest.raises(
        Stage5ContractError, match="exactly one current valid OCR result"
    ):
        _validate_ocr_fixture(
            _ocr_gate(A3, "EXECUTED"),
            _ocr_result(A4, A3),
            _ocr_result(A5, A3),
        )


def test_ocr_executed_rejects_result_bound_to_another_gate() -> None:
    with pytest.raises(Stage5ContractError, match="must bind the current OCR gate"):
        _validate_ocr_fixture(
            _ocr_gate(A3, "EXECUTED"),
            _ocr_result(A4, A6),
        )


@pytest.mark.parametrize(
    ("alignment_gate_id", "alignment_result_id", "message"),
    [
        (A6, A4, "alignment must bind the current OCR gate"),
        (A3, A6, "must bind the current OCR result"),
    ],
)
def test_ocr_executed_rejects_wrong_alignment_bindings(
    alignment_gate_id: str,
    alignment_result_id: str,
    message: str,
) -> None:
    with pytest.raises(Stage5ContractError, match=message):
        _validate_ocr_fixture(
            _ocr_gate(A3, "EXECUTED"),
            _ocr_result(A4, A3),
            _ocr_alignment(A5, alignment_gate_id, alignment_result_id),
        )


@pytest.mark.parametrize(
    ("route", "targets"),
    [("no_depth", []), ("depth", [_target()])],
)
def test_source_depth_decision_route_target_pairs_are_accepted(
    route: str,
    targets: list[dict[str, Any]],
) -> None:
    raw = _artifact("source_depth.decision")
    raw["payload"].update({"route": route, "targets": targets})
    _rehash_business_artifact(raw)
    assert parse_v02_business_artifact(raw).payload.route == route


@pytest.mark.parametrize(
    ("route", "targets", "message"),
    [
        ("no_depth", [_target()], "no_depth route cannot contain targets"),
        ("depth", [], "depth route requires at least one target claim"),
    ],
)
def test_source_depth_decision_rejects_route_target_mismatch(
    route: str,
    targets: list[dict[str, Any]],
    message: str,
) -> None:
    raw = _artifact("source_depth.decision")
    raw["payload"].update({"route": route, "targets": targets})
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match=message):
        parse_v02_business_artifact(raw)


@pytest.mark.parametrize(
    ("terminal", "report_kind"),
    [("NO_DEPTH", "machine"), ("IMPORTED", "rebuilt")],
)
def test_screening_sync_terminal_report_pairs_are_accepted(
    terminal: str,
    report_kind: str,
) -> None:
    raw = _artifact("screening.sync_record")
    raw["payload"].update(
        {"source_depth_terminal": terminal, "selected_report_kind": report_kind}
    )
    _rehash_business_artifact(raw)
    assert parse_v02_business_artifact(raw).payload.selected_report_kind == report_kind


@pytest.mark.parametrize(
    ("terminal", "report_kind", "message"),
    [
        ("NO_DEPTH", "rebuilt", "NO_DEPTH screening sync requires the machine report"),
        ("IMPORTED", "machine", "IMPORTED screening sync requires the rebuilt report"),
    ],
)
def test_screening_sync_rejects_terminal_report_mismatch(
    terminal: str,
    report_kind: str,
    message: str,
) -> None:
    raw = _artifact("screening.sync_record")
    raw["payload"].update(
        {"source_depth_terminal": terminal, "selected_report_kind": report_kind}
    )
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match=message):
        parse_v02_business_artifact(raw)


def _wav_bytes(
    *,
    channels: int = 1,
    sample_rate_hz: int = 16000,
    sample_width: int = 2,
    frame_count: int = 30 * 16000,
) -> bytes:
    buffer = BytesIO()
    with wave.open(buffer, "wb") as writer:
        writer.setnchannels(channels)
        writer.setsampwidth(sample_width)
        writer.setframerate(sample_rate_hz)
        writer.writeframes(b"\x00" * frame_count * channels * sample_width)
    return buffer.getvalue()


def _media_audio_payload(data: bytes, *, duration_ms: int = 30000) -> dict[str, Any]:
    payload = copy.deepcopy(_payloads()["media.audio"])
    payload["duration_ms"] = duration_ms
    payload["output_content_hash"] = hashlib.sha256(data).hexdigest()
    return payload


@pytest.mark.parametrize("frame_count", [30 * 16000, 30 * 16000 + 1])
def test_media_audio_wav_bytes_accept_exact_and_one_frame_rounding(
    frame_count: int,
) -> None:
    data = _wav_bytes(frame_count=frame_count)
    payload = _media_audio_payload(data)
    assert validate_media_audio_wav_bytes(data, payload).duration_ms == 30000


def test_media_audio_wav_bytes_rejects_non_riff_wave() -> None:
    data = b"NOPE" + _wav_bytes()[4:]
    with pytest.raises(Stage5ContractError, match="RIFF/WAVE"):
        validate_media_audio_wav_bytes(data, _media_audio_payload(data))


def test_media_audio_wav_bytes_rejects_truncation() -> None:
    data = _wav_bytes()[:-2]
    with pytest.raises(Stage5ContractError, match="RIFF size"):
        validate_media_audio_wav_bytes(data, _media_audio_payload(data))


@pytest.mark.parametrize(
    ("wav_options", "message"),
    [
        ({"channels": 2}, "channel count"),
        ({"sample_rate_hz": 8000, "frame_count": 30 * 8000}, "sample rate"),
        ({"sample_width": 1}, "PCM s16le"),
    ],
)
def test_media_audio_wav_bytes_rejects_noncanonical_pcm(
    wav_options: dict[str, int],
    message: str,
) -> None:
    data = _wav_bytes(**wav_options)
    with pytest.raises(Stage5ContractError, match=message):
        validate_media_audio_wav_bytes(data, _media_audio_payload(data))


def test_media_audio_wav_bytes_rejects_duration_mismatch() -> None:
    data = _wav_bytes()
    with pytest.raises(Stage5ContractError, match="more than one frame"):
        validate_media_audio_wav_bytes(
            data,
            _media_audio_payload(data, duration_ms=29999),
        )


def test_media_audio_wav_bytes_rejects_output_hash_mismatch() -> None:
    data = _wav_bytes()
    payload = _media_audio_payload(data)
    payload["output_content_hash"] = "0" * 64
    with pytest.raises(Stage5ContractError, match="output_content_hash mismatch"):
        validate_media_audio_wav_bytes(data, payload)


def _source_depth_artifact(
    artifact_type: str,
    artifact_id: str,
    *,
    upstream: list[str],
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    node_by_type = {
        "report.machine": "machine_report",
        "evidence.collection": "evidence_aggregate",
        "verdict.collection": "machine_verdict",
        "source_depth.decision": "source_depth_decision",
        "source_depth.prompt": "source_depth_prompt",
        "source_depth.result": "external_depth_action",
        "source_depth.import_validation": "source_depth_import_validation",
        "evidence.merged_collection": "evidence_merge",
        "verdict.rebuilt_collection": "verdict_rebuild",
        "report.rebuilt": "report_rebuild",
        "screening.sync_record": "screening_sync",
    }
    raw = _artifact(artifact_type, payload)
    raw.update(
        {
            "artifact_id": artifact_id,
            "stage_id": "S01"
            if artifact_type
            in {
                "report.machine",
                "evidence.collection",
                "verdict.collection",
                "source_depth.decision",
            }
            else "S02",
            "dag_node_id": node_by_type[artifact_type],
            "upstream_artifact_ids": upstream,
        }
    )
    return _rehash_business_artifact(raw)


def _source_depth_chain(state: str) -> list[dict[str, Any]]:
    machine = _source_depth_artifact("report.machine", A1, upstream=[])
    decision_payload = copy.deepcopy(_payloads()["source_depth.decision"])
    decision_payload.update(
        {
            "route": "no_depth" if state == "NO_DEPTH_COMPLETED" else "depth",
            "targets": [] if state == "NO_DEPTH_COMPLETED" else [_target()],
        }
    )
    decision = _source_depth_artifact(
        "source_depth.decision", A2, upstream=[A1], payload=decision_payload
    )
    if state == "NO_DEPTH_COMPLETED":
        sync_payload = copy.deepcopy(_payloads()["screening.sync_record"])
        sync_payload.update(
            {
                "selected_report": _binding(A1),
                "selected_report_kind": "machine",
                "source_depth_terminal": "NO_DEPTH",
            }
        )
        sync = _source_depth_artifact(
            "screening.sync_record", AB, upstream=[A2, A1], payload=sync_payload
        )
        return [machine, decision, sync]

    prompt_payload = copy.deepcopy(_payloads()["source_depth.prompt"])
    prompt = _source_depth_artifact(
        "source_depth.prompt", A3, upstream=[A2], payload=prompt_payload
    )
    if state in {"DEPTH_WAITING", "DEPTH_EXTERNAL_EMPTY_FAILED"}:
        return [machine, decision, prompt]

    result_payload = copy.deepcopy(_payloads()["source_depth.result"])
    result_payload["prompt_artifact_id"] = A3
    result = _source_depth_artifact(
        "source_depth.result", A4, upstream=[A3], payload=result_payload
    )
    if state == "DEPTH_CAPTURED_WAITING_G3":
        return [machine, decision, prompt, result]

    import_payload = copy.deepcopy(_payloads()["source_depth.import_validation"])
    import_payload["source_depth_result_artifact_id"] = A4
    imported = _source_depth_artifact(
        "source_depth.import_validation",
        A5,
        upstream=[A4],
        payload=import_payload,
    )
    base_evidence = _source_depth_artifact("evidence.collection", A6, upstream=[])
    merged_payload = copy.deepcopy(_payloads()["evidence.merged_collection"])
    merged_payload.update(
        {
            "base_evidence": _binding(A6),
            "import_validation_artifact_id": A5,
        }
    )
    merged = _source_depth_artifact(
        "evidence.merged_collection",
        A7,
        upstream=[A6, A5],
        payload=merged_payload,
    )
    base_verdict = _source_depth_artifact("verdict.collection", A8, upstream=[])
    verdict_payload = copy.deepcopy(_payloads()["verdict.rebuilt_collection"])
    verdict_payload.update(
        {"base_verdict": _binding(A8), "merged_evidence_artifact_id": A7}
    )
    rebuilt_verdict = _source_depth_artifact(
        "verdict.rebuilt_collection",
        A9,
        upstream=[A8, A7],
        payload=verdict_payload,
    )
    report_payload = copy.deepcopy(_payloads()["report.rebuilt"])
    report_payload["input_bindings"] = [_binding(A9), _binding(A7)]
    rebuilt_report = _source_depth_artifact(
        "report.rebuilt", AA, upstream=[A9], payload=report_payload
    )
    sync_payload = copy.deepcopy(_payloads()["screening.sync_record"])
    sync_payload.update(
        {
            "selected_report": _binding(AA),
            "selected_report_kind": "rebuilt",
            "source_depth_terminal": "IMPORTED",
        }
    )
    sync = _source_depth_artifact(
        "screening.sync_record", AB, upstream=[A2, AA], payload=sync_payload
    )
    return [
        machine,
        decision,
        prompt,
        result,
        imported,
        base_evidence,
        merged,
        base_verdict,
        rebuilt_verdict,
        rebuilt_report,
        sync,
    ]


@pytest.mark.parametrize(
    ("state", "terminal", "action", "target_stage"),
    [
        ("NO_DEPTH_COMPLETED", "COMPLETED", "next_stage", "S03"),
        ("DEPTH_WAITING", "WAITING_FOR_HUMAN", "wait_for_human", None),
        (
            "DEPTH_CAPTURED_WAITING_G3",
            "COMPLETED",
            "return_to_stage",
            "S02",
        ),
        ("DEPTH_IMPORTED_COMPLETED", "COMPLETED", "next_stage", "S03"),
        ("DEPTH_EXTERNAL_EMPTY_FAILED", "FAILED", "terminate", None),
    ],
)
def test_source_depth_branch_five_states_are_mutually_valid(
    state: str,
    terminal: str,
    action: str,
    target_stage: str | None,
) -> None:
    validation = validate_source_depth_branch_contract(
        artifacts=_source_depth_chain(state),
        control_terminal=terminal,
        control_action=action,
        target_stage=target_stage,
    )
    assert validation.state == state


def test_source_depth_no_depth_rejects_depth_prompt_mixing() -> None:
    artifacts = _source_depth_chain("NO_DEPTH_COMPLETED")
    prompt = _source_depth_chain("DEPTH_WAITING")[-1]
    with pytest.raises(Stage5ContractError, match="forbidden Artifacts"):
        validate_source_depth_branch_contract(
            artifacts=[*artifacts, prompt],
            control_terminal="COMPLETED",
            control_action="next_stage",
            target_stage="S03",
        )


def test_source_depth_waiting_rejects_captured_result_mixing() -> None:
    with pytest.raises(Stage5ContractError, match="forbidden Artifacts"):
        validate_source_depth_branch_contract(
            artifacts=_source_depth_chain("DEPTH_CAPTURED_WAITING_G3"),
            control_terminal="WAITING_FOR_HUMAN",
            control_action="wait_for_human",
            target_stage=None,
        )


def test_source_depth_captured_rejects_missing_result() -> None:
    with pytest.raises(
        Stage5ContractError, match="missing required source_depth.result"
    ):
        validate_source_depth_branch_contract(
            artifacts=_source_depth_chain("DEPTH_WAITING"),
            control_terminal="COMPLETED",
            control_action="return_to_stage",
            target_stage="S02",
        )


def test_source_depth_imported_rejects_missing_chain_member() -> None:
    artifacts = [
        artifact
        for artifact in _source_depth_chain("DEPTH_IMPORTED_COMPLETED")
        if artifact["artifact_type"] != "evidence.merged_collection"
    ]
    with pytest.raises(
        Stage5ContractError, match="missing required evidence.merged_collection"
    ):
        validate_source_depth_branch_contract(
            artifacts=artifacts,
            control_terminal="COMPLETED",
            control_action="next_stage",
            target_stage="S03",
        )


def test_source_depth_rejects_wrong_control_action() -> None:
    with pytest.raises(Stage5ContractError, match="does not identify a legal state"):
        validate_source_depth_branch_contract(
            artifacts=_source_depth_chain("DEPTH_WAITING"),
            control_terminal="COMPLETED",
            control_action="wait_for_human",
            target_stage=None,
        )


def test_source_depth_captured_rejects_wrong_prompt_binding() -> None:
    artifacts = _source_depth_chain("DEPTH_CAPTURED_WAITING_G3")
    result = artifacts[-1]
    result["payload"]["prompt_artifact_id"] = A6
    _rehash_business_artifact(result)
    with pytest.raises(Stage5ContractError, match="bind the exact prompt and request"):
        validate_source_depth_branch_contract(
            artifacts=artifacts,
            control_terminal="COMPLETED",
            control_action="return_to_stage",
            target_stage="S02",
        )


def test_source_depth_imported_rejects_missing_required_upstream() -> None:
    artifacts = _source_depth_chain("DEPTH_IMPORTED_COMPLETED")
    merged = next(
        artifact
        for artifact in artifacts
        if artifact["artifact_type"] == "evidence.merged_collection"
    )
    merged["upstream_artifact_ids"].remove(A5)
    _rehash_business_artifact(merged)
    with pytest.raises(Stage5ContractError, match="missing required upstream"):
        validate_source_depth_branch_contract(
            artifacts=artifacts,
            control_terminal="COMPLETED",
            control_action="next_stage",
            target_stage="S03",
        )


def _warehouse_export_artifact_v12() -> dict[str, Any]:
    export_id = _id("export", "1")
    package_dir = f"runs/V02/warehouse/exports/{export_id}"
    raw = {
        "artifact_schema_version": "v02_business_artifact_v1.2.0",
        "artifact_id": A1,
        "artifact_type": "warehouse.export_batch",
        "run_id": RUN,
        "stage_id": "S01",
        "dag_node_id": "warehouse_export",
        "upstream_artifact_ids": [A2],
        "created_at": NOW,
        "payload": {
            "export_id": export_id,
            "run_id": RUN,
            "storage_root_ref": "ubuntu_v02_claim_warehouse",
            "manifest_relative_path": f"{package_dir}/manifest.json",
            "manifest_hash": H,
            "rows_relative_path": f"{package_dir}/rows.jsonl",
            "rows_hash": H2,
            "logical_hash": "3" * 64,
            "row_count": 7,
            "row_counts": {"parent_claim": 2, "atomic_claim": 5},
            "schema_versions": {
                "business_artifact": "v02_business_artifact_v1.2.0",
                "warehouse_export": "warehouse_export_schema_v1.0.0",
            },
            "taxonomy_versions": {
                "label_taxonomy_version": "truthfulness_taxonomy_v02.1.0",
            },
            "exporter_versions": {"warehouse_exporter": "v1.0.0"},
            "projection_status": "pending",
        },
        "artifact_hash": "0" * 64,
    }
    return _rehash_business_artifact(raw)


def test_v11_business_artifact_remains_strictly_readable() -> None:
    raw = _artifact("claim.collection")
    raw["artifact_schema_version"] = "v02_business_artifact_v1.1.0"
    _rehash_business_artifact(raw)
    parsed = parse_v02_business_artifact(raw)
    assert isinstance(parsed, BusinessArtifactV1_1)
    assert parsed.model_dump(mode="json") == raw


def test_warehouse_export_v12_payload_mapping_schema_and_runtime() -> None:
    raw = _warehouse_export_artifact_v12()
    schema = json.loads(
        (
            ROOT / "schemas/versions/v02/v02_business_artifact_v1_2.schema.json"
        ).read_text(encoding="utf-8")
    )
    assert not list(Draft202012Validator(schema).iter_errors(raw))
    parsed = parse_v02_business_artifact(raw)
    assert isinstance(parsed, WarehouseExportBatchArtifactV1_2)
    assert parsed.payload.projection_status == "pending"
    assert parsed.model_dump(mode="json") == raw
    assert PAYLOAD_MODELS["warehouse.export_batch"] is WarehouseExportBatchPayloadV1_2
    assert (
        BUSINESS_ARTIFACT_V1_2_MODELS["warehouse.export_batch"]
        is WarehouseExportBatchArtifactV1_2
    )


def test_warehouse_export_v12_rejects_row_sum_path_and_run_drift() -> None:
    raw = _warehouse_export_artifact_v12()
    raw["payload"]["row_count"] += 1
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match=r"sum\(row_counts\)"):
        parse_v02_business_artifact(raw)

    raw = _warehouse_export_artifact_v12()
    raw["payload"]["rows_relative_path"] = raw["payload"]["manifest_relative_path"]
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match="paths must be distinct"):
        parse_v02_business_artifact(raw)

    raw = _warehouse_export_artifact_v12()
    raw["payload"]["run_id"] = _id("run", "2")
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match="run_id mismatch"):
        parse_v02_business_artifact(raw)


def test_warehouse_export_v12_rejects_zero_rows_and_zero_table_counts() -> None:
    raw = _warehouse_export_artifact_v12()
    raw["payload"]["row_count"] = 0
    raw["payload"]["row_counts"] = {"parent_claim": 0}
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match="greater than or equal to 1"):
        parse_v02_business_artifact(raw)

    raw = _warehouse_export_artifact_v12()
    raw["payload"]["row_counts"] = {"parent_claim": 0, "atomic_claim": 7}
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match="must be positive"):
        parse_v02_business_artifact(raw)

    raw = _warehouse_export_artifact_v12()
    raw["payload"]["storage_root_ref"] = "unknown_warehouse_root"
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError, match="ubuntu_v02_claim_warehouse"):
        parse_v02_business_artifact(raw)


def test_warehouse_export_v12_rejects_product_taxonomy_dependency() -> None:
    raw = _warehouse_export_artifact_v12()
    raw["payload"]["taxonomy_versions"]["product_taxonomy_version"] = (
        "product_review_taxonomy_v02.1.0"
    )
    _rehash_business_artifact(raw)
    with pytest.raises(ExecutionSchemaError):
        parse_v02_business_artifact(raw)


@pytest.mark.parametrize(
    "source_kind",
    [
        "official",
        "primary_report",
        "paper",
        "database",
        "high_quality_secondary",
        "other",
    ],
)
def test_v12_evidence_accepts_only_non_product_source_kinds(source_kind: str) -> None:
    raw = {
        "evidence_id": E1,
        "evidence_revision_id": _id("evidence_revision", "1"),
        "revision_no": 1,
        "supersedes_revision_id": None,
        "source_kind": source_kind,
        "publisher": "Example authority",
        "published_date": "2026-07-01",
        "retrieved_at": NOW,
        "canonical_url": "https://example.test/source",
        "stable_locator": None,
        "excerpt": "A reviewable excerpt.",
        "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
        "writer_role": "machine_evidence_writer",
    }
    assert EvidenceRevisionV1_2.model_validate(raw, strict=True).source_kind == source_kind


@pytest.mark.parametrize(
    "source_kind",
    [
        "manufacturer",
        "retailer_listing",
        "independent_test",
        "buyer_review",
        "seller_or_marketing",
    ],
)
def test_v12_evidence_rejects_deprecated_product_source_kinds(source_kind: str) -> None:
    raw = {
        "evidence_id": E1,
        "evidence_revision_id": _id("evidence_revision", "1"),
        "revision_no": 1,
        "supersedes_revision_id": None,
        "source_kind": source_kind,
        "publisher": "Example authority",
        "published_date": "2026-07-01",
        "retrieved_at": NOW,
        "canonical_url": "https://example.test/source",
        "stable_locator": None,
        "excerpt": "A reviewable excerpt.",
        "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
        "writer_role": "machine_evidence_writer",
    }
    with pytest.raises(ValueError):
        EvidenceRevisionV1_2.model_validate(raw, strict=True)
