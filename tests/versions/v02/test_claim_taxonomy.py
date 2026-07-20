from __future__ import annotations

import hashlib
import json
import math
import tomllib
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from video_truthfulness.versions.v02.business_models import (
    ACCESS_STATUS_CODES,
    ATOMIC_CLAIM_WARNING_CHARS,
    CHECKABILITY_CODES,
    CLAIM_INLINE_UTF8_LIMIT,
    EVIDENCE_AVAILABILITY_CODES,
    EVIDENCE_RELATION_CODES,
    EVIDENCE_STRENGTH_CODES,
    EVIDENCE_USE_STATUS_CODES,
    HUMAN_GOLD_CODES,
    MACHINE_VERDICT_CODES,
    SOURCE_KIND_CODES,
    SOURCE_ROLE_CODES,
    AtomicClaimCollectionPayloadV1_2,
    AtomicClaimRevisionV1_2,
    ClaimTextStorageV1_2,
    EvidenceCollectionPayloadV1_2,
    HumanClaimInclusionDecisionV1_2,
    HumanGoldLabelV1_2,
    MachineClaimAssessmentV1_2,
    ParentClaimRevisionV1_2,
)


ROOT = Path(__file__).resolve().parents[3]
U = "01j00000000000000000000000"
H = "1" * 64
NOW = "2026-07-20T00:00:00Z"


def _id(prefix: str, suffix: str) -> str:
    return f"{prefix}_{U[:-1]}{suffix}"


PARENT = _id("claim", "1")
PARENT_REV = _id("parent_claim_revision", "1")
SPLIT = _id("claim_split_revision", "1")
ATOMIC = _id("claim", "2")
ATOMIC_REV = _id("atomic_claim_revision", "1")
RETRIEVAL = _id("retrieval_batch", "1")
EVIDENCE = _id("evidence", "1")
EVIDENCE_REV = _id("evidence_revision", "1")
LINK = _id("evidence_link", "1")


def _storage(
    text: str,
    *,
    owner_kind: str,
    owner_revision_id: str,
    force_chunks: bool | None = None,
) -> dict[str, Any]:
    encoded = text.encode("utf-8")
    chunked = len(encoded) > CLAIM_INLINE_UTF8_LIMIT
    if force_chunks is not None:
        chunked = force_chunks
    chunks: list[dict[str, Any]] = []
    if chunked:
        step = max(1, math.ceil(len(text) / 3))
        byte_start = 0
        for index, char_start in enumerate(range(0, len(text), step)):
            chunk_text = text[char_start : char_start + step]
            chunk_bytes = chunk_text.encode("utf-8")
            byte_end = byte_start + len(chunk_bytes)
            chunks.append(
                {
                    "chunk_id": _id("claim_text_chunk", "123456789abcdefghjkmnpqrs"[index]),
                    "owner_kind": owner_kind,
                    "owner_revision_id": owner_revision_id,
                    "chunk_index": index,
                    "byte_start": byte_start,
                    "byte_end_exclusive": byte_end,
                    "text": chunk_text,
                    "chunk_sha256": hashlib.sha256(chunk_bytes).hexdigest(),
                }
            )
            byte_start = byte_end
    return {
        "text_char_count": len(text),
        "text_utf8_byte_count": len(encoded),
        "text_sha256": hashlib.sha256(encoded).hexdigest(),
        "inline_text": None if chunked else text,
        "chunks": chunks,
    }


def _atomic_revision(
    *,
    index: int = 0,
    text: str = "可核验原子断言",
    checkability: str = "checkable",
    eligible: bool = True,
    split_revision_id: str = SPLIT,
) -> dict[str, Any]:
    suffixes = "123456789abcdefghjkmnpqrstvwxyz"
    suffix = suffixes[index + 1]
    revision_id = _id("atomic_claim_revision", suffix)
    return {
        "atomic_claim_id": _id("claim", suffix),
        "parent_claim_id": PARENT,
        "atomic_revision_id": revision_id,
        "revision_no": 1,
        "supersedes_revision_id": None,
        "split_revision_id": split_revision_id,
        "text": _storage(
            text,
            owner_kind="atomic_claim_revision",
            owner_revision_id=revision_id,
        ),
        "checkability": checkability,
        "quality_warnings": (
            ["atomic_text_over_5000_chars"]
            if len(text) > ATOMIC_CLAIM_WARNING_CHARS
            else []
        ),
        "machine_verdict_eligible": eligible,
        "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
        "writer_role": "claim_splitter",
    }


def _split_payload(
    atomic_revisions: list[dict[str, Any]],
    *,
    status: str = "resolved_atomic",
    dependencies: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "parent_claim_collection_artifact_id": _id("artifact", "1"),
        "split_sets": [
            {
                "split_revision_id": SPLIT,
                "parent_claim_id": PARENT,
                "parent_revision_id": PARENT_REV,
                "revision_no": 1,
                "supersedes_split_revision_id": None,
                "split_status": status,
                "failure_reason": (
                    None if status == "resolved_atomic" else "复合政策条件无法安全拆分"
                ),
                "members": [
                    {
                        "member_id": (
                            "split_member_"
                            + U[:-2]
                            + "123456789abcdefghjkmnpqrstvwxyz"[
                                index // len("123456789abcdefghjkmnpqrstvwxyz")
                            ]
                            + "123456789abcdefghjkmnpqrstvwxyz"[
                                index % len("123456789abcdefghjkmnpqrstvwxyz")
                            ]
                        ),
                        "atomic_revision_id": item["atomic_revision_id"],
                        "ordinal": index,
                    }
                    for index, item in enumerate(atomic_revisions)
                ],
                "coverage_reviewed": status == "resolved_atomic",
                "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
                "writer_role": "claim_splitter",
            }
        ],
        "atomic_revisions": atomic_revisions,
        "dependencies": dependencies or [],
        "run_gate_state": (
            "READY_FOR_S02" if status == "resolved_atomic" else "WAITING_FOR_HUMAN"
        ),
    }


def _evidence_revision() -> dict[str, Any]:
    return {
        "evidence_id": EVIDENCE,
        "evidence_revision_id": EVIDENCE_REV,
        "revision_no": 1,
        "supersedes_revision_id": None,
        "source_kind": "official",
        "publisher": "Synthetic Public Agency",
        "published_date": "2026-07-20",
        "retrieved_at": NOW,
        "canonical_url": "https://example.invalid/policy",
        "stable_locator": None,
        "excerpt": "合成政策公开说明摘录",
        "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
        "writer_role": "machine_evidence_writer",
    }


def _link(*, use_status: str = "evidence") -> dict[str, Any]:
    return {
        "evidence_link_id": LINK,
        "atomic_revision_id": ATOMIC_REV,
        "evidence_revision_id": EVIDENCE_REV,
        "source_role": "primary_source",
        "use_status": use_status,
        "evidence_strength": "high" if use_status == "evidence" else None,
        "evidence_relation": "supports",
        "rejection_reason": None,
        "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
        "writer_role": "machine_evidence_writer",
    }


def _gold(label: str, **updates: Any) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "gold_revision_id": _id("gold_revision", "1"),
        "target_kind": "atomic_claim_revision",
        "target_revision_id": ATOMIC_REV,
        "annotation_scope": "atomic_truth",
        "claim_checkability": "checkable",
        "gold_label": label,
        "reason": "合成审核理由",
        "evidence_link_ids": [LINK],
        "supported_scope": None,
        "unsupported_scope": None,
        "misleading_mechanism": None,
        "missing_context": None,
        "retrieval_batch_id": None,
        "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
        "approval_status": "approved",
        "writer_role": "authorized_human",
    }
    if label == "gold_partially_supports":
        raw.update(supported_scope="容量", unsupported_scope="价格时点")
    elif label == "gold_misleading":
        raw["misleading_mechanism"] = "把局部统计推广为全部地区"
    elif label == "gold_missing_context":
        raw["missing_context"] = "缺少统计口径与观察时点"
    elif label == "gold_insufficient_evidence":
        raw["retrieval_batch_id"] = RETRIEVAL
        raw["evidence_link_ids"] = []
    elif label == "gold_uncheckable":
        raw["claim_checkability"] = "not_checkable"
        raw["evidence_link_ids"] = []
    raw.update(updates)
    return raw


def test_taxonomy_toml_is_schema_valid_and_exactly_frozen() -> None:
    config = tomllib.loads(
        (ROOT / "configs/versions/v02/claim_taxonomy_v1.toml").read_text(
            encoding="utf-8"
        )
    )
    schema = json.loads(
        (ROOT / "schemas/versions/v02/v02_claim_taxonomy_v1.schema.json").read_text(
            encoding="utf-8"
        )
    )
    assert not list(Draft202012Validator(schema).iter_errors(config))
    assert config["v01_import_status"] == "OUT_OF_SCOPE"
    assert config["v01_mapping_status"] == "DEFERRED"
    assert tuple(config["label_groups"]["checkability"]["codes"]) == CHECKABILITY_CODES
    assert tuple(config["label_groups"]["machine_verdict"]["codes"]) == MACHINE_VERDICT_CODES
    assert tuple(config["label_groups"]["human_gold"]["codes"]) == HUMAN_GOLD_CODES
    assert tuple(config["label_groups"]["source_kind"]["codes"]) == SOURCE_KIND_CODES
    assert tuple(config["label_groups"]["source_role"]["codes"]) == SOURCE_ROLE_CODES
    assert tuple(config["label_groups"]["access_status"]["codes"]) == ACCESS_STATUS_CODES
    assert tuple(config["label_groups"]["use_status"]["codes"]) == EVIDENCE_USE_STATUS_CODES
    assert tuple(config["label_groups"]["evidence_strength"]["codes"]) == EVIDENCE_STRENGTH_CODES
    assert tuple(config["label_groups"]["evidence_relation"]["codes"]) == EVIDENCE_RELATION_CODES
    assert tuple(config["label_groups"]["evidence_availability"]["codes"]) == EVIDENCE_AVAILABILITY_CODES
    assert config["permissions"]["machine_gold_write"] == "forbidden"


def test_65536_mixed_unicode_parent_and_128_atomic_children_are_lossless() -> None:
    pattern = "策🙂e\u0301\"\n"
    parent_text = (pattern * math.ceil(65_536 / len(pattern)))[:65_536]
    parent = ParentClaimRevisionV1_2.model_validate(
        {
            "parent_claim_id": PARENT,
            "parent_revision_id": PARENT_REV,
            "revision_no": 1,
            "supersedes_revision_id": None,
            "display_no": 1,
            "text": _storage(
                parent_text,
                owner_kind="parent_claim_revision",
                owner_revision_id=PARENT_REV,
            ),
            "normalized_text": None,
            "preview": parent_text[:500],
            "source_spans": [{"start_ms": 0, "end_ms": 120000}],
            "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
            "writer_role": "claim_extractor",
        }
    )
    assert parent.text.text_char_count == 65_536
    assert parent.text.reassemble() == parent_text

    revisions: list[dict[str, Any]] = []
    alphabet = "123456789abcdefghjkmnpqrstvwxyz"
    for index in range(128):
        suffix = alphabet[index // len(alphabet)] + alphabet[index % len(alphabet)]
        claim_id = f"claim_{U[:-2]}{suffix}"
        revision_id = f"atomic_claim_revision_{U[:-2]}{suffix}"
        text = f"政策原子断言 {index}"
        revisions.append(
            {
                "atomic_claim_id": claim_id,
                "parent_claim_id": PARENT,
                "atomic_revision_id": revision_id,
                "revision_no": 1,
                "supersedes_revision_id": None,
                "split_revision_id": SPLIT,
                "text": _storage(
                    text,
                    owner_kind="atomic_claim_revision",
                    owner_revision_id=revision_id,
                ),
                "checkability": "checkable",
                "quality_warnings": [],
                "machine_verdict_eligible": True,
                "taxonomy_version": "truthfulness_taxonomy_v02.1.0",
                "writer_role": "claim_splitter",
            }
        )
    payload = _split_payload(revisions)
    for index, member in enumerate(payload["split_sets"][0]["members"]):
        suffix = alphabet[index // len(alphabet)] + alphabet[index % len(alphabet)]
        member["member_id"] = f"split_member_{U[:-2]}{suffix}"
    parsed = AtomicClaimCollectionPayloadV1_2.model_validate(payload)
    assert len(parsed.atomic_revisions) == 128
    assert all(item.text.reassemble().startswith("政策原子断言") for item in parsed.atomic_revisions)


def test_exact_inline_boundary_and_true_four_byte_chunk_boundary() -> None:
    inline_text = "🙂" * 65_536
    inline = ClaimTextStorageV1_2.model_validate(
        _storage(
            inline_text,
            owner_kind="parent_claim_revision",
            owner_revision_id=PARENT_REV,
        )
    )
    assert inline.text_utf8_byte_count == CLAIM_INLINE_UTF8_LIMIT
    assert inline.inline_text == inline_text

    chunked_text = "🙂" * 65_537
    chunked = ClaimTextStorageV1_2.model_validate(
        _storage(
            chunked_text,
            owner_kind="parent_claim_revision",
            owner_revision_id=PARENT_REV,
        )
    )
    assert chunked.text_utf8_byte_count == CLAIM_INLINE_UTF8_LIMIT + 4
    assert chunked.inline_text is None
    assert chunked.reassemble() == chunked_text


def test_chunk_gap_or_hash_corruption_is_rejected() -> None:
    raw = _storage(
        "🙂" * 65_537,
        owner_kind="parent_claim_revision",
        owner_revision_id=PARENT_REV,
    )
    raw["chunks"][1]["byte_start"] += 1
    with pytest.raises(ValidationError, match="byte range|contiguous"):
        ClaimTextStorageV1_2.model_validate(raw)


def test_atomic_over_5000_is_preserved_then_blocks_s02_for_human_split() -> None:
    text = "政策复合断言" * 1001
    atomic = _atomic_revision(text=text, eligible=False)
    parsed = AtomicClaimRevisionV1_2.model_validate(atomic)
    assert parsed.text.reassemble() == text
    assert parsed.text.text_char_count > 5_000
    assert parsed.quality_warnings == ["atomic_text_over_5000_chars"]

    collection = AtomicClaimCollectionPayloadV1_2.model_validate(
        _split_payload([atomic], status="needs_human_split")
    )
    assert collection.run_gate_state == "WAITING_FOR_HUMAN"
    assert collection.split_sets[0].failure_reason


def test_resolved_split_and_context_only_dependency_rules_fail_closed() -> None:
    with pytest.raises(ValidationError, match="at least one child"):
        AtomicClaimCollectionPayloadV1_2.model_validate(_split_payload([]))

    context = _atomic_revision(checkability="context_only", eligible=False)
    with pytest.raises(ValidationError, match="requires a dependency"):
        AtomicClaimCollectionPayloadV1_2.model_validate(_split_payload([context]))


def test_machine_checkability_matrix_is_separate_from_human_gold() -> None:
    base = {
        "assessment_revision_id": _id("assessment_revision", "1"),
        "atomic_revision_id": ATOMIC_REV,
        "claim_checkability": "not_checkable",
        "evidence_link_ids": [],
        "candidate_verdict": "unverifiable",
        "reason": "纯主观偏好",
        "uncertainty": "low",
        "model_version": "synthetic-model-v1",
        "prompt_version": "synthetic-prompt-v1",
        "config_hash": H,
        "review_status": "machine_pending",
        "writer_role": "machine_assessor",
    }
    assert MachineClaimAssessmentV1_2.model_validate(base).candidate_verdict == "unverifiable"
    base["candidate_verdict"] = "supported"
    with pytest.raises(ValidationError, match="requires machine unverifiable"):
        MachineClaimAssessmentV1_2.model_validate(base)
    base.update(claim_checkability="context_only", candidate_verdict="unverifiable")
    with pytest.raises(ValidationError, match="cannot receive a truth verdict"):
        MachineClaimAssessmentV1_2.model_validate(base)


@pytest.mark.parametrize("label", HUMAN_GOLD_CODES)
def test_all_seven_human_gold_values_are_supported(label: str) -> None:
    assert HumanGoldLabelV1_2.model_validate(_gold(label)).gold_label == label


def test_gold_refutes_is_not_mapped_to_misleading_and_machine_cannot_write_gold() -> None:
    parsed = HumanGoldLabelV1_2.model_validate(_gold("gold_refutes"))
    assert parsed.gold_label == "gold_refutes"
    raw = _gold("gold_refutes", writer_role="machine_assessor")
    with pytest.raises(ValidationError):
        HumanGoldLabelV1_2.model_validate(raw)


def test_parent_gold_is_independent_context_only() -> None:
    valid = _gold(
        "gold_missing_context",
        target_kind="parent_claim_revision",
        target_revision_id=PARENT_REV,
        annotation_scope="parent_context",
        claim_checkability=None,
    )
    assert HumanGoldLabelV1_2.model_validate(valid).annotation_scope == "parent_context"
    valid["gold_label"] = "gold_refutes"
    valid["missing_context"] = None
    with pytest.raises(ValidationError, match="parent Gold is limited"):
        HumanGoldLabelV1_2.model_validate(valid)


def test_human_inclusion_dataset_gate_and_pending_excluded_flags() -> None:
    base = {
        "inclusion_revision_id": _id("inclusion_revision", "1"),
        "atomic_revision_id": ATOMIC_REV,
        "status": "pending",
        "reason": None,
        "approved_gold_batch_id": None,
        "training_eligible": False,
        "evaluation_eligible": False,
        "writer_role": "authorized_human",
    }
    assert HumanClaimInclusionDecisionV1_2.model_validate(base).status == "pending"
    base.update(status="excluded", reason="超出数据集范围", training_eligible=True)
    with pytest.raises(ValidationError, match="not train/eval eligible"):
        HumanClaimInclusionDecisionV1_2.model_validate(base)
    base.update(
        status="included",
        reason=None,
        training_eligible=True,
        approved_gold_batch_id=None,
    )
    with pytest.raises(ValidationError, match="approved Gold batch"):
        HumanClaimInclusionDecisionV1_2.model_validate(base)


def test_empty_evidence_collection_and_closed_no_evidence_are_legal() -> None:
    raw = {
        "atomic_claim_collection_artifact_id": _id("artifact", "1"),
        "retrieval_batch_id": RETRIEVAL,
        "evidence_revisions": [],
        "retrieval_attempts": [
            {
                "retrieval_attempt_id": _id("retrieval_attempt", "1"),
                "retrieval_batch_id": RETRIEVAL,
                "attempted_locator": "https://blocked.invalid/spec",
                "access_status": "source_blocked",
                "evidence_revision_id": None,
                "attempted_at": NOW,
                "writer_role": "retriever",
            }
        ],
        "links": [],
        "availability": [
            {
                "atomic_revision_id": ATOMIC_REV,
                "retrieval_batch_id": RETRIEVAL,
                "availability": "no_evidence",
                "batch_closed": True,
                "formal_evidence_link_ids": [],
                "clue_link_ids": [],
                "writer_role": "retrieval_batch_closer",
            }
        ],
    }
    parsed = EvidenceCollectionPayloadV1_2.model_validate(raw)
    assert parsed.evidence_revisions == []
    assert parsed.availability[0].availability == "no_evidence"


def test_no_evidence_can_coexist_with_clue_only_but_not_formal_evidence() -> None:
    clue = _link(use_status="clue_only")
    raw = {
        "atomic_claim_collection_artifact_id": _id("artifact", "1"),
        "retrieval_batch_id": RETRIEVAL,
        "evidence_revisions": [_evidence_revision()],
        "retrieval_attempts": [
            {
                "retrieval_attempt_id": _id("retrieval_attempt", "1"),
                "retrieval_batch_id": RETRIEVAL,
                "attempted_locator": "https://example.invalid/spec",
                "access_status": "accessible",
                "evidence_revision_id": EVIDENCE_REV,
                "attempted_at": NOW,
                "writer_role": "retriever",
            }
        ],
        "links": [clue],
        "availability": [
            {
                "atomic_revision_id": ATOMIC_REV,
                "retrieval_batch_id": RETRIEVAL,
                "availability": "no_evidence",
                "batch_closed": True,
                "formal_evidence_link_ids": [],
                "clue_link_ids": [LINK],
                "writer_role": "retrieval_batch_closer",
            }
        ],
    }
    assert EvidenceCollectionPayloadV1_2.model_validate(raw).links[0].use_status == "clue_only"
    raw["links"] = [_link(use_status="evidence")]
    raw["availability"][0].update(
        formal_evidence_link_ids=[LINK], clue_link_ids=[]
    )
    with pytest.raises(ValidationError, match="no_evidence requires"):
        EvidenceCollectionPayloadV1_2.model_validate(raw)


def test_evidence_axes_reject_strength_on_clue_and_allow_blocked_plus_has_evidence() -> None:
    bad = _link(use_status="clue_only")
    bad["evidence_strength"] = "low"
    from video_truthfulness.versions.v02.business_models import ClaimEvidenceLinkV1_2

    with pytest.raises(ValidationError, match="cannot claim Evidence strength"):
        ClaimEvidenceLinkV1_2.model_validate(bad)

    formal = _link(use_status="evidence")
    raw = {
        "atomic_claim_collection_artifact_id": _id("artifact", "1"),
        "retrieval_batch_id": RETRIEVAL,
        "evidence_revisions": [_evidence_revision()],
        "retrieval_attempts": [
            {
                "retrieval_attempt_id": _id("retrieval_attempt", "1"),
                "retrieval_batch_id": RETRIEVAL,
                "attempted_locator": "https://blocked.invalid/spec",
                "access_status": "source_blocked",
                "evidence_revision_id": None,
                "attempted_at": NOW,
                "writer_role": "retriever",
            },
            {
                "retrieval_attempt_id": _id("retrieval_attempt", "2"),
                "retrieval_batch_id": RETRIEVAL,
                "attempted_locator": "https://example.invalid/spec",
                "access_status": "accessible",
                "evidence_revision_id": EVIDENCE_REV,
                "attempted_at": NOW,
                "writer_role": "retriever",
            },
        ],
        "links": [formal],
        "availability": [
            {
                "atomic_revision_id": ATOMIC_REV,
                "retrieval_batch_id": RETRIEVAL,
                "availability": "has_evidence",
                "batch_closed": True,
                "formal_evidence_link_ids": [LINK],
                "clue_link_ids": [],
                "writer_role": "retrieval_batch_closer",
            }
        ],
    }
    parsed = EvidenceCollectionPayloadV1_2.model_validate(raw)
    assert {item.access_status for item in parsed.retrieval_attempts} == {
        "source_blocked",
        "accessible",
    }
    assert parsed.availability[0].availability == "has_evidence"
