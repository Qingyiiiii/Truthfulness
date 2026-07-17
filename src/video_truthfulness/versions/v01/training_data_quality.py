"""Build the v0.1.1 quality-gated training-data pack.

The pipeline is deliberately downstream of machine screening and human gold
annotation.  It does not rewrite those labels.  Instead it freezes an input
snapshot, computes quality/lineage metadata, applies task-specific admission
rules, and derives controlled synthetic, SFT, and preference artifacts.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import tomllib
from typing import Any, Iterable, Sequence
import unicodedata

from video_truthfulness.versions.v01.training_data_schemas import (
    ChatMessage,
    GateStatus,
    OriginType,
    PreferencePair,
    QualityRecord,
    ReviewStatus,
    SFTExample,
    SyntheticExample,
    TaskGateResult,
    UsageScope,
)


PIPELINE_VERSION = "training_data_quality_v1"
QUALITY_GATE_VERSION = "quality_gate_v1"
DEFAULT_OUTPUT_DATASET_VERSION = "truthfulness_v0.1.1_training_data_pack"
DEFAULT_SEED = 20260717
CORE_OUTPUT_FILES = (
    "quality_records.jsonl",
    "quarantine_records.jsonl",
    "rejected_records.jsonl",
    "sft_examples.jsonl",
    "synthetic_examples.jsonl",
    "preference_pairs.jsonl",
    "dataset_manifest.json",
    "quality_report.json",
    "QUALITY_REPORT.md",
    "PREFERENCE_REVIEW_PACKET.md",
    "HANDOFF.md",
)
EVIDENCE_METADATA_FIELDS = (
    "source_title_evidence",
    "publisher",
    "source_type",
    "retrieved_at",
    "verifiable_excerpt",
)
HUMAN_GOLD_STATUSES = {
    "gold_supports",
    "gold_partially_supports",
    "gold_missing_context",
    "gold_misleading",
    "gold_insufficient_evidence",
    "gold_uncheckable",
    "gold_refutes",
}
TRUTHFULNESS_TASK_STATUSES = {
    "gold_supports",
    "gold_partially_supports",
    "gold_missing_context",
    "gold_misleading",
    "gold_refutes",
}
SYNTHETIC_PARENT_STATUSES = TRUTHFULNESS_TASK_STATUSES
PII_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("email", re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)),
    ("cn_mobile", re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")),
    ("cn_id_candidate", re.compile(r"(?<!\d)\d{17}[\dXx](?!\d)")),
    (
        "secret_assignment",
        re.compile(r"(?i)\b(?:api[_-]?key|access[_-]?token|secret|password)\s*[:=]\s*\S+"),
    ),
)
MOJIBAKE_MARKERS = ("\ufffd", "â€", "Ã", "Â", "√")


@dataclass(frozen=True)
class TrainingDataPackConfig:
    """Configuration for one deterministic quality-pack build."""

    input_jsonl: Path
    split_jsonl: Path | None
    output_dir: Path
    output_dataset_version: str = DEFAULT_OUTPUT_DATASET_VERSION
    usage_scope: UsageScope = UsageScope.PRIVATE_ONLY
    seed: int = DEFAULT_SEED
    near_duplicate_threshold: float = 0.82
    shingle_size: int = 3
    minhash_permutations: int = 64
    lsh_bands: int = 16
    long_text_chars: int = 300
    synthetic_limit: int = 100
    max_synthetic_per_parent: int = 1
    preference_review_count: int = 30
    write_parquet: bool = False
    overwrite: bool = False
    generated_at: str | None = None


@dataclass(frozen=True)
class TrainingDataPackResult:
    """Paths and summary returned after one build."""

    output_dir: Path
    quality_records_path: Path
    quarantine_records_path: Path
    rejected_records_path: Path
    sft_examples_path: Path
    synthetic_examples_path: Path
    preference_pairs_path: Path
    manifest_path: Path
    report_json_path: Path
    report_md_path: Path
    review_packet_path: Path
    handoff_path: Path
    parquet_paths: tuple[Path, ...]
    summary: dict[str, Any]


@dataclass
class _Candidate:
    """Internal normalized record before strict output validation."""

    line_number: int
    raw: dict[str, Any]
    source_dataset_version: str
    source_schema_version: str
    source_id: str
    run_id: str
    claim_id: str
    parent_claim_id: str
    origin: OriginType
    claim_text: str
    normalized_text: str
    status: str
    source_record_id: str
    record_id: str
    input_content_hash: str
    normalized_content_hash: str
    language: str
    pii_flags: list[str]
    quality_flags: list[str]
    evidence_completeness: float
    split: str
    duplicate_cluster_id: str | None = None
    split_group_id: str = ""


class _UnionFind:
    """Small deterministic union-find for duplicate clusters."""

    def __init__(self, values: Iterable[str]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        low, high = sorted((left_root, right_root))
        self.parent[high] = low


def load_training_data_pack_config(path: Path) -> TrainingDataPackConfig:
    """Load a TOML quality-pack configuration."""

    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    dataset = payload.get("dataset", {})
    quality = payload.get("quality", {})
    derived = payload.get("derived", {})
    output = payload.get("output", {})

    required = ("input_jsonl", "output_dir")
    missing = [name for name in required if not str(dataset.get(name, "")).strip()]
    if missing:
        raise ValueError(f"Missing required [dataset] config values: {', '.join(missing)}")

    split_value = str(dataset.get("split_jsonl", "")).strip()
    return TrainingDataPackConfig(
        input_jsonl=Path(str(dataset["input_jsonl"])),
        split_jsonl=Path(split_value) if split_value else None,
        output_dir=Path(str(dataset["output_dir"])),
        output_dataset_version=str(
            dataset.get("output_dataset_version", DEFAULT_OUTPUT_DATASET_VERSION)
        ),
        usage_scope=UsageScope(str(dataset.get("usage_scope", UsageScope.PRIVATE_ONLY.value))),
        seed=int(dataset.get("seed", DEFAULT_SEED)),
        near_duplicate_threshold=float(quality.get("near_duplicate_threshold", 0.82)),
        shingle_size=int(quality.get("shingle_size", 3)),
        minhash_permutations=int(quality.get("minhash_permutations", 64)),
        lsh_bands=int(quality.get("lsh_bands", 16)),
        long_text_chars=int(quality.get("long_text_chars", 300)),
        synthetic_limit=int(derived.get("synthetic_limit", 100)),
        max_synthetic_per_parent=int(derived.get("max_synthetic_per_parent", 1)),
        preference_review_count=int(derived.get("preference_review_count", 30)),
        write_parquet=bool(output.get("write_parquet", False)),
        overwrite=bool(output.get("overwrite", False)),
        generated_at=str(output["generated_at"]) if output.get("generated_at") else None,
    )


def build_training_data_pack_from_toml(path: Path) -> TrainingDataPackResult:
    """Load configuration and build one pack."""

    return build_training_data_pack(load_training_data_pack_config(path))


def build_training_data_pack(config: TrainingDataPackConfig) -> TrainingDataPackResult:
    """Build quality, SFT, synthetic, and preference artifacts."""

    _validate_config(config)
    _prepare_output_dir(config.output_dir, overwrite=config.overwrite)
    generated_at = config.generated_at or _utc_now()
    raw_records = _read_jsonl(config.input_jsonl)
    if not raw_records:
        raise ValueError(f"Input JSONL is empty: {config.input_jsonl}")
    split_map = _load_split_map(config.split_jsonl)
    candidates = [
        _candidate_from_raw(
            raw=raw,
            line_number=index,
            split_map=split_map,
            config=config,
        )
        for index, raw in enumerate(raw_records, start=1)
    ]

    duplicate_summary = _assign_duplicate_clusters(candidates, config)
    _mark_duplicate_and_split_violations(candidates, duplicate_summary)
    quality_records = [
        _build_quality_record(candidate, config=config, generated_at=generated_at)
        for candidate in candidates
    ]

    sft_examples = _derive_sft_examples(quality_records, config)
    synthetic_examples = _derive_synthetic_examples(quality_records, config, generated_at)
    preference_pairs = _derive_preference_pairs(
        quality_records,
        synthetic_examples,
        config,
    )
    quarantine_rows, rejected_rows = _quality_exception_rows(quality_records)

    paths = _output_paths(config.output_dir)
    _write_models_jsonl(paths["quality_records"], quality_records)
    _write_jsonl(paths["quarantine_records"], quarantine_rows)
    _write_jsonl(paths["rejected_records"], rejected_rows)
    _write_models_jsonl(paths["sft_examples"], sft_examples)
    _write_models_jsonl(paths["synthetic_examples"], synthetic_examples)
    _write_models_jsonl(paths["preference_pairs"], preference_pairs)

    summary = _build_summary(
        config=config,
        raw_records=raw_records,
        quality_records=quality_records,
        sft_examples=sft_examples,
        synthetic_examples=synthetic_examples,
        preference_pairs=preference_pairs,
        duplicate_summary=duplicate_summary,
        generated_at=generated_at,
    )
    manifest = _build_manifest(config, summary, generated_at)
    _write_json(paths["manifest"], manifest)
    _write_json(paths["report_json"], summary)
    _write_text(paths["report_md"], _render_quality_report(summary))
    _write_text(
        paths["review_packet"],
        _render_preference_review_packet(preference_pairs, config.preference_review_count),
    )
    _write_text(paths["handoff"], _render_handoff(config, paths, summary))

    parquet_paths: list[Path] = []
    if config.write_parquet:
        for name, models in (
            ("quality_records.parquet", quality_records),
            ("sft_examples.parquet", sft_examples),
            ("synthetic_examples.parquet", synthetic_examples),
            ("preference_pairs.parquet", preference_pairs),
        ):
            parquet_path = config.output_dir / name
            _write_models_parquet(parquet_path, models)
            parquet_paths.append(parquet_path)

    return TrainingDataPackResult(
        output_dir=config.output_dir,
        quality_records_path=paths["quality_records"],
        quarantine_records_path=paths["quarantine_records"],
        rejected_records_path=paths["rejected_records"],
        sft_examples_path=paths["sft_examples"],
        synthetic_examples_path=paths["synthetic_examples"],
        preference_pairs_path=paths["preference_pairs"],
        manifest_path=paths["manifest"],
        report_json_path=paths["report_json"],
        report_md_path=paths["report_md"],
        review_packet_path=paths["review_packet"],
        handoff_path=paths["handoff"],
        parquet_paths=tuple(parquet_paths),
        summary=summary,
    )


def validate_preference_review_file(
    path: Path,
    require_all_reviewed: bool = False,
) -> dict[str, Any]:
    """Validate reviewed/pending preference records without inventing decisions."""

    raw_rows = _read_jsonl(path)
    pairs: list[PreferencePair] = []
    errors: list[str] = []
    seen_pair_ids: set[str] = set()
    for line_number, raw in enumerate(raw_rows, start=1):
        try:
            pair = PreferencePair.model_validate(raw)
        except Exception as exc:
            errors.append(f"line {line_number}: schema validation failed: {exc}")
            continue
        if pair.pair_id in seen_pair_ids:
            errors.append(f"line {line_number}: duplicate pair_id={pair.pair_id}")
        seen_pair_ids.add(pair.pair_id)
        if pair.review_status == ReviewStatus.PENDING:
            if pair.review_decision is not None or pair.reviewed_at is not None:
                errors.append(
                    f"line {line_number}: pending pair must not contain decision/reviewed_at"
                )
            if require_all_reviewed:
                errors.append(f"line {line_number}: pair remains pending")
        else:
            if pair.review_decision is None:
                errors.append(f"line {line_number}: reviewed pair is missing review_decision")
            if not pair.review_reason.strip():
                errors.append(f"line {line_number}: reviewed pair is missing review_reason")
            if not pair.reviewed_at:
                errors.append(f"line {line_number}: reviewed pair is missing reviewed_at")
            if (
                pair.review_decision is not None
                and pair.review_decision.value == "edit"
                and not pair.final_chosen.strip()
            ):
                errors.append(f"line {line_number}: edit decision requires final_chosen")
        pairs.append(pair)
    if errors:
        raise ValueError("Preference review validation failed: " + "; ".join(errors))
    decision_counts = Counter(
        pair.review_decision.value
        for pair in pairs
        if pair.review_decision is not None
    )
    return {
        "status": "pass",
        "path": str(path),
        "records": len(pairs),
        "pending": sum(pair.review_status == ReviewStatus.PENDING for pair in pairs),
        "reviewed": sum(pair.review_status == ReviewStatus.REVIEWED for pair in pairs),
        "decision_counts": dict(sorted(decision_counts.items())),
        "require_all_reviewed": require_all_reviewed,
        "rlhf_completed": False,
    }


def _validate_config(config: TrainingDataPackConfig) -> None:
    if not config.input_jsonl.exists():
        raise FileNotFoundError(config.input_jsonl)
    if config.split_jsonl is not None and not config.split_jsonl.exists():
        raise FileNotFoundError(config.split_jsonl)
    if not 0.0 <= config.near_duplicate_threshold <= 1.0:
        raise ValueError("near_duplicate_threshold must be between 0 and 1.")
    if config.shingle_size < 1:
        raise ValueError("shingle_size must be positive.")
    if config.minhash_permutations < 1:
        raise ValueError("minhash_permutations must be positive.")
    if config.lsh_bands < 1 or config.minhash_permutations % config.lsh_bands:
        raise ValueError("lsh_bands must divide minhash_permutations.")
    if config.synthetic_limit < 0 or config.max_synthetic_per_parent < 0:
        raise ValueError("Synthetic limits must be non-negative.")
    if config.preference_review_count < 0:
        raise ValueError("preference_review_count must be non-negative.")


def _candidate_from_raw(
    raw: dict[str, Any],
    line_number: int,
    split_map: dict[str, str],
    config: TrainingDataPackConfig,
) -> _Candidate:
    source_dataset_version = _text(raw.get("dataset_version")) or "unknown_dataset"
    source_schema_version = _text(raw.get("schema_version")) or "unknown_schema"
    source_id = _text(raw.get("source_id")) or _text(raw.get("run_id"))
    run_id = _text(raw.get("run_id")) or source_id
    claim_id = _text(raw.get("claim_id"))
    parent_claim_id = _text(raw.get("parent_claim_id")) or claim_id
    claim_text = _text(raw.get("claim_text")) or _text(raw.get("text"))
    status = _text(raw.get("status")) or _text(raw.get("gold_status"))
    raw_origin = _text(raw.get("origin"))
    try:
        origin = OriginType(raw_origin) if raw_origin else _origin_for_status(status)
    except ValueError:
        origin = _origin_for_status(status)
    fallback_source = source_id or f"missing_source_line_{line_number}"
    fallback_claim = claim_id or f"missing_claim_line_{line_number}"
    source_record_id = f"{fallback_source}::{fallback_claim}"
    record_id = _stable_id(
        "rec",
        source_dataset_version,
        fallback_source,
        fallback_claim,
        str(line_number),
    )
    normalized_text = normalize_text(claim_text)
    quality_flags: list[str] = []
    if not source_id:
        quality_flags.append("missing_source_id")
    if not run_id:
        quality_flags.append("missing_run_id")
    if not claim_id:
        quality_flags.append("missing_claim_id")
    if not claim_text:
        quality_flags.append("empty_claim_text")
    if not status:
        quality_flags.append("missing_status")
    elif status not in HUMAN_GOLD_STATUSES and status not in {"excluded", "pending", "machine_pending"}:
        quality_flags.append("unknown_status")
    if len(claim_text) > config.long_text_chars:
        quality_flags.append("long_claim_text")
    if any(marker in claim_text for marker in MOJIBAKE_MARKERS):
        quality_flags.append("mojibake_candidate")
    if "???" in claim_text:
        quality_flags.append("question_mark_mojibake_candidate")
    pii_flags = detect_pii_flags(claim_text + "\n" + _text(raw.get("raw_context")))
    evidence_present = sum(bool(_text(raw.get(field))) for field in EVIDENCE_METADATA_FIELDS)
    evidence_completeness = round(evidence_present / len(EVIDENCE_METADATA_FIELDS), 6)
    if evidence_completeness < 1.0:
        quality_flags.append("evidence_metadata_incomplete")
    record_key = f"{fallback_source}::{fallback_claim}"
    split = split_map.get(record_key) or split_map.get(fallback_claim)
    if not split:
        split = _deterministic_split(fallback_source, config.seed)
        quality_flags.append("split_generated")
    if split not in {"train", "dev", "test", "excluded_from_main_tasks"}:
        quality_flags.append("invalid_split")
    return _Candidate(
        line_number=line_number,
        raw=raw,
        source_dataset_version=source_dataset_version,
        source_schema_version=source_schema_version,
        source_id=source_id or fallback_source,
        run_id=run_id or fallback_source,
        claim_id=claim_id or fallback_claim,
        parent_claim_id=parent_claim_id or fallback_claim,
        origin=origin,
        claim_text=claim_text,
        normalized_text=normalized_text,
        status=status,
        source_record_id=source_record_id,
        record_id=record_id,
        input_content_hash=_sha256_text(claim_text),
        normalized_content_hash=_sha256_text(_dedup_text(claim_text)),
        language=detect_language(claim_text),
        pii_flags=pii_flags,
        quality_flags=quality_flags,
        evidence_completeness=evidence_completeness,
        split=split,
    )


def normalize_text(value: str) -> str:
    """Return a stable NFKC and whitespace-normalized string."""

    normalized = unicodedata.normalize("NFKC", value or "")
    return re.sub(r"\s+", " ", normalized).strip()


def detect_language(value: str) -> str:
    """Return a conservative zh/en/mixed/unknown language hint."""

    cjk = len(re.findall(r"[\u3400-\u9fff]", value))
    latin = len(re.findall(r"[A-Za-z]", value))
    total = cjk + latin
    if total == 0:
        return "unknown"
    if cjk / total >= 0.75:
        return "zh"
    if latin / total >= 0.75:
        return "en"
    return "mixed"


def detect_pii_flags(value: str) -> list[str]:
    """Detect PII/secret patterns without copying matched values to reports."""

    return [name for name, pattern in PII_PATTERNS if pattern.search(value)]


def _assign_duplicate_clusters(
    candidates: Sequence[_Candidate],
    config: TrainingDataPackConfig,
) -> dict[str, Any]:
    union_find = _UnionFind(candidate.record_id for candidate in candidates)
    exact_groups: dict[str, list[_Candidate]] = defaultdict(list)
    for candidate in candidates:
        if candidate.normalized_text:
            exact_groups[candidate.normalized_content_hash].append(candidate)
    for group in exact_groups.values():
        if len(group) < 2:
            continue
        first = group[0].record_id
        for candidate in group[1:]:
            union_find.union(first, candidate.record_id)

    shingle_sets = {
        candidate.record_id: _character_shingles(candidate.claim_text, config.shingle_size)
        for candidate in candidates
        if candidate.claim_text
    }
    signatures = {
        record_id: _minhash_signature(shingles, config.minhash_permutations)
        for record_id, shingles in shingle_sets.items()
        if shingles
    }
    candidate_pairs = _lsh_candidate_pairs(signatures, config.lsh_bands)
    near_pairs: list[dict[str, Any]] = []
    candidate_by_id = {candidate.record_id: candidate for candidate in candidates}
    for left_id, right_id in sorted(candidate_pairs):
        if (
            candidate_by_id[left_id].normalized_content_hash
            == candidate_by_id[right_id].normalized_content_hash
        ):
            continue
        left_shingles = shingle_sets[left_id]
        right_shingles = shingle_sets[right_id]
        similarity = _jaccard(left_shingles, right_shingles)
        if similarity < config.near_duplicate_threshold:
            continue
        union_find.union(left_id, right_id)
        near_pairs.append(
            {
                "left_record_id": left_id,
                "right_record_id": right_id,
                "jaccard": round(similarity, 6),
            }
        )

    members_by_root: dict[str, list[str]] = defaultdict(list)
    for candidate in candidates:
        members_by_root[union_find.find(candidate.record_id)].append(candidate.record_id)
    clusters: dict[str, list[str]] = {}
    cluster_by_record: dict[str, str] = {}
    for members in members_by_root.values():
        if len(members) < 2:
            continue
        sorted_members = sorted(members)
        cluster_id = _stable_id("dup", *sorted_members)
        clusters[cluster_id] = sorted_members
        for record_id in sorted_members:
            cluster_by_record[record_id] = cluster_id
    for candidate in candidates:
        candidate.duplicate_cluster_id = cluster_by_record.get(candidate.record_id)
        candidate.split_group_id = candidate.duplicate_cluster_id or _stable_id(
            "group",
            candidate.source_id,
            candidate.parent_claim_id,
        )

    exact_clusters = sum(1 for group in exact_groups.values() if len(group) > 1)
    return {
        "exact_duplicate_cluster_count": exact_clusters,
        "near_duplicate_pair_count": len(near_pairs),
        "duplicate_cluster_count": len(clusters),
        "near_duplicate_pairs": near_pairs,
        "clusters": clusters,
    }


def _mark_duplicate_and_split_violations(
    candidates: Sequence[_Candidate],
    duplicate_summary: dict[str, Any],
) -> None:
    by_record = {candidate.record_id: candidate for candidate in candidates}
    source_key_groups: dict[tuple[str, str], list[_Candidate]] = defaultdict(list)
    hash_groups: dict[str, list[_Candidate]] = defaultdict(list)
    for candidate in candidates:
        source_key_groups[(candidate.source_id, candidate.claim_id)].append(candidate)
        hash_groups[candidate.normalized_content_hash].append(candidate)
    for group in source_key_groups.values():
        if len(group) > 1:
            for candidate in group:
                _add_flag(candidate, "duplicate_source_claim_key")
    for group in hash_groups.values():
        if len(group) < 2 or not group[0].normalized_text:
            continue
        statuses = {candidate.status for candidate in group}
        if len(statuses) > 1:
            for candidate in group:
                _add_flag(candidate, "duplicate_text_label_conflict")
        for candidate in sorted(group, key=lambda item: item.record_id)[1:]:
            _add_flag(candidate, "exact_duplicate_noncanonical")

    for cluster_id, member_ids in duplicate_summary["clusters"].items():
        members = [by_record[record_id] for record_id in member_ids]
        splits = {candidate.split for candidate in members}
        if len(splits) > 1:
            for candidate in members:
                _add_flag(candidate, "cross_split_duplicate_cluster")
        if not any("exact_duplicate_noncanonical" in item.quality_flags for item in members):
            for candidate in members:
                _add_flag(candidate, "near_duplicate_candidate")


def _build_quality_record(
    candidate: _Candidate,
    config: TrainingDataPackConfig,
    generated_at: str,
) -> QualityRecord:
    gate_status_by_task = _task_gate_results(candidate, config)
    return QualityRecord(
        pipeline_version=PIPELINE_VERSION,
        output_dataset_version=config.output_dataset_version,
        source_dataset_version=candidate.source_dataset_version,
        source_schema_version=candidate.source_schema_version,
        record_id=candidate.record_id,
        source_record_id=candidate.source_record_id,
        source_id=candidate.source_id,
        run_id=candidate.run_id,
        claim_id=candidate.claim_id,
        parent_claim_id=candidate.parent_claim_id,
        origin=candidate.origin,
        usage_scope=config.usage_scope,
        claim_text=candidate.claim_text,
        normalized_text=candidate.normalized_text,
        raw_context=_text(candidate.raw.get("raw_context")),
        status=candidate.status,
        evidence_quality=_text(candidate.raw.get("evidence_quality")),
        domain=_text(candidate.raw.get("domain")),
        claim_type=_text(candidate.raw.get("claim_type")),
        checkability=_text(candidate.raw.get("checkability")),
        noise_patterns=_string_list(candidate.raw.get("noise_patterns")),
        recommended_action=_text(candidate.raw.get("recommended_action")),
        include_decision=_text(candidate.raw.get("include_decision")),
        training_use=_text(candidate.raw.get("training_use")),
        input_content_hash=candidate.input_content_hash,
        normalized_content_hash=candidate.normalized_content_hash,
        language=candidate.language,
        pii_flags=sorted(candidate.pii_flags),
        quality_flags=sorted(set(candidate.quality_flags)),
        duplicate_cluster_id=candidate.duplicate_cluster_id,
        split_group_id=candidate.split_group_id,
        split=candidate.split,
        evidence_completeness=candidate.evidence_completeness,
        gate_status_by_task=gate_status_by_task,
        generated_at=generated_at,
    )


def _task_gate_results(
    candidate: _Candidate,
    config: TrainingDataPackConfig,
) -> dict[str, TaskGateResult]:
    hard_flags = {
        "missing_source_id",
        "missing_run_id",
        "missing_claim_id",
        "empty_claim_text",
        "missing_status",
        "unknown_status",
        "invalid_split",
        "duplicate_source_claim_key",
        "duplicate_text_label_conflict",
        "cross_split_duplicate_cluster",
    }.intersection(candidate.quality_flags)
    base_warnings = sorted(
        {
            flag
            for flag in candidate.quality_flags
            if flag
            in {
                "evidence_metadata_incomplete",
                "near_duplicate_candidate",
                "mojibake_candidate",
                "question_mark_mojibake_candidate",
                "split_generated",
            }
        }
    )
    results: dict[str, TaskGateResult] = {}
    if hard_flags:
        rejected = TaskGateResult(
            status=GateStatus.REJECT,
            reasons=sorted(hard_flags),
            warnings=base_warnings,
        )
        return {
            task: rejected.model_copy(deep=True)
            for task in (
                "claim_triage_sft",
                "evidence_grounded_sft",
                "synthetic_parent",
                "truthfulness_eval",
                "public_release",
            )
        }

    triage_reasons: list[str] = []
    target_fields = {
        "claim_type": _text(candidate.raw.get("claim_type")),
        "checkability": _text(candidate.raw.get("checkability")),
        "recommended_action": _text(candidate.raw.get("recommended_action")),
        "include_decision": _text(candidate.raw.get("include_decision")),
    }
    missing_targets = [name for name, value in target_fields.items() if not value]
    if missing_targets:
        triage_reasons.append("missing_triage_targets:" + ",".join(sorted(missing_targets)))
    if "long_claim_text" in candidate.quality_flags:
        triage_reasons.append("long_claim_text")
    if candidate.split == "excluded_from_main_tasks":
        triage_reasons.append("excluded_from_main_tasks")
    if "exact_duplicate_noncanonical" in candidate.quality_flags:
        triage_reasons.append("exact_duplicate_noncanonical")
    if {
        "mojibake_candidate",
        "question_mark_mojibake_candidate",
    }.intersection(candidate.quality_flags):
        triage_reasons.append("text_encoding_or_mojibake_candidate")
    if candidate.pii_flags:
        triage_reasons.append("pii_or_secret_pattern")
    results["claim_triage_sft"] = TaskGateResult(
        status=GateStatus.QUARANTINE if triage_reasons else GateStatus.PASS,
        reasons=triage_reasons,
        warnings=base_warnings,
    )

    evidence_reasons: list[str] = []
    if candidate.status not in TRUTHFULNESS_TASK_STATUSES:
        evidence_reasons.append("status_not_truthfulness_task_eligible")
    if _text(candidate.raw.get("include_decision")) != "included":
        evidence_reasons.append("not_included_by_human")
    if candidate.evidence_completeness < 1.0:
        evidence_reasons.append("evidence_metadata_incomplete")
    if _text(candidate.raw.get("evidence_quality")) in {"", "no_evidence", "clue_only"}:
        evidence_reasons.append("evidence_quality_too_weak")
    if {
        "mojibake_candidate",
        "question_mark_mojibake_candidate",
    }.intersection(candidate.quality_flags):
        evidence_reasons.append("text_encoding_or_mojibake_candidate")
    if candidate.pii_flags:
        evidence_reasons.append("pii_or_secret_pattern")
    results["evidence_grounded_sft"] = TaskGateResult(
        status=GateStatus.QUARANTINE if evidence_reasons else GateStatus.PASS,
        reasons=evidence_reasons,
        warnings=base_warnings,
    )

    synthetic_reasons: list[str] = []
    if candidate.status not in SYNTHETIC_PARENT_STATUSES:
        synthetic_reasons.append("status_not_synthetic_parent_eligible")
    if _text(candidate.raw.get("include_decision")) != "included":
        synthetic_reasons.append("not_included_by_human")
    if candidate.split != "train":
        synthetic_reasons.append("synthetic_generation_train_only")
    if "exact_duplicate_noncanonical" in candidate.quality_flags:
        synthetic_reasons.append("exact_duplicate_noncanonical")
    if {
        "mojibake_candidate",
        "question_mark_mojibake_candidate",
    }.intersection(candidate.quality_flags):
        synthetic_reasons.append("text_encoding_or_mojibake_candidate")
    if candidate.pii_flags:
        synthetic_reasons.append("pii_or_secret_pattern")
    results["synthetic_parent"] = TaskGateResult(
        status=GateStatus.QUARANTINE if synthetic_reasons else GateStatus.PASS,
        reasons=synthetic_reasons,
        warnings=base_warnings,
    )

    eval_reasons: list[str] = []
    if candidate.status not in TRUTHFULNESS_TASK_STATUSES:
        eval_reasons.append("status_not_truthfulness_task_eligible")
    if candidate.split not in {"dev", "test"}:
        eval_reasons.append("not_eval_split")
    if candidate.evidence_completeness < 1.0:
        eval_reasons.append("evidence_metadata_incomplete")
    if {
        "mojibake_candidate",
        "question_mark_mojibake_candidate",
    }.intersection(candidate.quality_flags):
        eval_reasons.append("text_encoding_or_mojibake_candidate")
    if candidate.pii_flags:
        eval_reasons.append("pii_or_secret_pattern")
    results["truthfulness_eval"] = TaskGateResult(
        status=GateStatus.QUARANTINE if eval_reasons else GateStatus.PASS,
        reasons=eval_reasons,
        warnings=base_warnings,
    )

    public_reasons: list[str] = []
    if config.usage_scope not in {UsageScope.PUBLIC_SYNTHETIC, UsageScope.APPROVED_PUBLIC}:
        public_reasons.append("usage_scope_not_public")
    if candidate.pii_flags:
        public_reasons.append("pii_or_secret_pattern")
    if (
        config.usage_scope == UsageScope.PUBLIC_SYNTHETIC
        and candidate.origin != OriginType.SYNTHETIC
    ):
        public_reasons.append("public_synthetic_scope_requires_synthetic_origin")
    results["public_release"] = TaskGateResult(
        status=GateStatus.REJECT if public_reasons else GateStatus.PASS,
        reasons=public_reasons,
        warnings=base_warnings,
    )
    return results


def _derive_sft_examples(
    records: Sequence[QualityRecord],
    config: TrainingDataPackConfig,
) -> list[SFTExample]:
    examples: list[SFTExample] = []
    for record in records:
        gate = record.gate_status_by_task["claim_triage_sft"]
        if gate.status != GateStatus.PASS:
            continue
        user_payload = {
            "claim_text": record.claim_text,
            "domain": record.domain,
            "source_type": "video_claim",
        }
        assistant_payload = {
            "claim_type": record.claim_type,
            "checkability": record.checkability,
            "noise_patterns": record.noise_patterns,
            "recommended_action": record.recommended_action,
            "include_decision": record.include_decision,
            "review_required": record.status
            not in {"gold_supports", "gold_partially_supports"},
        }
        messages = [
            ChatMessage(
                role="system",
                content=(
                    "You are a training-data triage assistant. Return one JSON object only. "
                    "Do not infer truth without evidence."
                ),
            ),
            ChatMessage(
                role="user",
                content=json.dumps(user_payload, ensure_ascii=False, sort_keys=True),
            ),
            ChatMessage(
                role="assistant",
                content=json.dumps(assistant_payload, ensure_ascii=False, sort_keys=True),
            ),
        ]
        example_id = _stable_id(
            "sft",
            config.output_dataset_version,
            record.record_id,
            "claim_triage_v1",
        )
        content_hash = _sha256_text(
            "\n".join(message.content for message in messages)
        )
        examples.append(
            SFTExample(
                pipeline_version=PIPELINE_VERSION,
                output_dataset_version=config.output_dataset_version,
                sft_example_id=example_id,
                source_record_id=record.record_id,
                source_dataset_version=record.source_dataset_version,
                task_name="claim_triage_v1",
                messages=messages,
                target_schema_version="claim_triage_target_v1",
                split=record.split,
                quality_gate_version=QUALITY_GATE_VERSION,
                eligibility_reason="claim_triage_sft=pass",
                content_hash=content_hash,
            )
        )
    return sorted(examples, key=lambda item: item.sft_example_id)


def _derive_synthetic_examples(
    records: Sequence[QualityRecord],
    config: TrainingDataPackConfig,
    generated_at: str,
) -> list[SyntheticExample]:
    if config.synthetic_limit == 0 or config.max_synthetic_per_parent == 0:
        return []
    eligible = [
        record
        for record in records
        if record.gate_status_by_task["synthetic_parent"].status == GateStatus.PASS
    ]
    eligible.sort(key=lambda item: _stable_sort_key(item.record_id, config.seed))
    examples: list[SyntheticExample] = []
    for record in eligible:
        if len(examples) >= config.synthetic_limit:
            break
        generated_for_parent = 0
        for mutation_index, mutation_type in enumerate(
            _mutation_order(record, config.seed)
        ):
            mutation = _apply_mutation(record.claim_text, mutation_type)
            if mutation is None:
                continue
            after_text, parameters = mutation
            verifier_result, verifier_reasons = _verify_mutation(
                record.claim_text,
                after_text,
                mutation_type,
            )
            if verifier_result != "pass":
                continue
            generation_seed = _stable_int(
                str(config.seed),
                record.record_id,
                mutation_type,
                str(mutation_index),
            ) % (2**31 - 1)
            synthetic_id = _stable_id(
                "syn",
                config.output_dataset_version,
                record.record_id,
                mutation_type,
                str(generation_seed),
            )
            examples.append(
                SyntheticExample(
                    pipeline_version=PIPELINE_VERSION,
                    output_dataset_version=config.output_dataset_version,
                    synthetic_id=synthetic_id,
                    parent_record_id=record.record_id,
                    parent_dataset_version=record.source_dataset_version,
                    mutation_type=mutation_type,
                    mutation_parameters=parameters,
                    before_text=record.claim_text,
                    after_text=after_text,
                    generator_type="rule_based_controlled",
                    generator_version="controlled_mutations_v1",
                    prompt_version="not_applicable",
                    generation_seed=generation_seed,
                    generated_at=generated_at,
                    verifier_result=verifier_result,
                    verifier_reasons=verifier_reasons,
                    human_review_status=ReviewStatus.PENDING,
                    split=record.split,
                    content_hash=_sha256_text(after_text),
                )
            )
            generated_for_parent += 1
            if generated_for_parent >= config.max_synthetic_per_parent:
                break
            if len(examples) >= config.synthetic_limit:
                break
    return sorted(examples, key=lambda item: item.synthetic_id)


def _derive_preference_pairs(
    quality_records: Sequence[QualityRecord],
    synthetic_examples: Sequence[SyntheticExample],
    config: TrainingDataPackConfig,
) -> list[PreferencePair]:
    if config.preference_review_count == 0:
        return []
    record_by_id = {record.record_id: record for record in quality_records}
    candidates = sorted(
        synthetic_examples,
        key=lambda item: _stable_sort_key(item.synthetic_id, config.seed),
    )[: config.preference_review_count]
    pairs: list[PreferencePair] = []
    for synthetic in candidates:
        parent = record_by_id[synthetic.parent_record_id]
        prompt = json.dumps(
            {
                "claim_text": parent.claim_text,
                "task": "Return a calibrated verdict object and preserve uncertainty.",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        chosen_payload = {
            "verdict": parent.status.removeprefix("gold_"),
            "recommended_action": parent.recommended_action,
            "evidence_boundary": (
                "metadata_complete"
                if parent.evidence_completeness == 1.0
                else "metadata_incomplete"
            ),
            "review_required": True,
        }
        rejected_payload = {
            "verdict": "supports",
            "recommended_action": "pass",
            "evidence_boundary": "certain_without_review",
            "review_required": False,
            "unsupported_reason": "权威原始资料已经完全证实，无需进一步核查。",
        }
        chosen = json.dumps(chosen_payload, ensure_ascii=False, sort_keys=True)
        rejected = json.dumps(rejected_payload, ensure_ascii=False, sort_keys=True)
        pair_id = _stable_id(
            "pref",
            config.output_dataset_version,
            synthetic.synthetic_id,
        )
        pairs.append(
            PreferencePair(
                pipeline_version=PIPELINE_VERSION,
                output_dataset_version=config.output_dataset_version,
                pair_id=pair_id,
                source_record_id=parent.record_id,
                synthetic_id=synthetic.synthetic_id,
                prompt=prompt,
                chosen=chosen,
                rejected=rejected,
                rejection_reason=(
                    f"{synthetic.mutation_type}: rejected output overclaims evidence "
                    "and removes the human-review boundary"
                ),
                mutation_type=synthetic.mutation_type,
                split=synthetic.split,
                quality_flags=["single_human_review_pending"],
                content_hash=_sha256_text(prompt + "\n" + chosen + "\n" + rejected),
            )
        )
    return sorted(pairs, key=lambda item: item.pair_id)


def _mutation_order(record: QualityRecord, seed: int) -> list[str]:
    available = [
        "time_shift",
        "unit_scale_error",
        "context_drop",
        "partial_to_full",
        "opinion_as_fact",
        "prediction_as_fact",
        "source_laundering",
    ]
    offset = _stable_int(record.record_id, str(seed)) % len(available)
    return available[offset:] + available[:offset]


def _apply_mutation(
    text: str,
    mutation_type: str,
) -> tuple[str, dict[str, Any]] | None:
    if mutation_type == "time_shift":
        match = re.search(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)", text)
        if not match:
            return None
        before = match.group(1)
        after = str(int(before) + 1)
        return (
            text[: match.start()] + after + text[match.end() :],
            {"from_year": before, "to_year": after},
        )
    if mutation_type == "unit_scale_error":
        match = re.search(
            r"(?<!\d)(\d+(?:\.\d+)?)\s*(亿元|万元|亿美元|美元|元|%|％|万|亿|倍|人|公里)",
            text,
        )
        if not match:
            return None
        before_number = match.group(1)
        changed = float(before_number) * 10
        after_number = str(int(changed)) if changed.is_integer() else f"{changed:g}"
        replacement = after_number + match.group(2)
        return (
            text[: match.start()] + replacement + text[match.end() :],
            {
                "from_number": before_number,
                "to_number": after_number,
                "unit": match.group(2),
                "factor": 10,
            },
        )
    if mutation_type == "context_drop":
        for marker in ("但是", "不过", "但", "其中", "前提是", "条件是", "截至"):
            index = text.find(marker)
            if index > max(8, len(text) // 4):
                shortened = text[:index].rstrip("，,；;。 ")
                if len(shortened) >= 8:
                    return shortened + "。", {"dropped_from_marker": marker}
        parenthetical = re.search(r"[（(][^（）()]{4,}[）)]", text)
        if parenthetical:
            changed = (text[: parenthetical.start()] + text[parenthetical.end() :]).strip()
            if changed != text:
                return changed, {"dropped_parenthetical": parenthetical.group(0)}
        return None
    if mutation_type == "partial_to_full":
        replacements = (
            ("部分", "全部"),
            ("可能", "必然"),
            ("约", "精确"),
            ("一些", "所有"),
            ("通常", "毫无例外地"),
            ("大致", "完全"),
        )
        for before, after in replacements:
            if before in text:
                return text.replace(before, after, 1), {"from": before, "to": after}
        return "毫无例外地，" + text, {"added_absolute_scope": True}
    if mutation_type == "opinion_as_fact":
        replacements = (("认为", "事实证明"), ("主张", "事实已经证明"), ("或许", "必然"))
        for before, after in replacements:
            if before in text:
                return text.replace(before, after, 1), {"from": before, "to": after}
        return None
    if mutation_type == "prediction_as_fact":
        replacements = (("预计", "已经"), ("预测", "事实显示"), ("可能会", "已经"))
        for before, after in replacements:
            if before in text:
                return text.replace(before, after, 1), {"from": before, "to": after}
        return None
    if mutation_type == "source_laundering":
        return "权威原始资料已经证实：" + text, {"added_false_source_authority": True}
    raise ValueError(f"Unknown mutation_type: {mutation_type}")


def _verify_mutation(
    before_text: str,
    after_text: str,
    mutation_type: str,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if normalize_text(before_text) == normalize_text(after_text):
        reasons.append("mutation_did_not_change_text")
    if not after_text.strip():
        reasons.append("empty_after_text")
    if len(after_text) > max(1000, len(before_text) * 2 + 100):
        reasons.append("unexpected_length_growth")
    added_pii = set(detect_pii_flags(after_text)) - set(detect_pii_flags(before_text))
    if added_pii:
        reasons.append("mutation_added_pii_or_secret_pattern")
    if mutation_type == "source_laundering" and "权威原始资料已经证实" not in after_text:
        reasons.append("source_laundering_marker_missing")
    return ("reject", reasons) if reasons else ("pass", ["deterministic_rule_verified"])


def _quality_exception_rows(
    records: Sequence[QualityRecord],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    quarantine_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []
    for record in records:
        quarantined_tasks = sorted(
            task
            for task, result in record.gate_status_by_task.items()
            if result.status == GateStatus.QUARANTINE
        )
        rejected_tasks = sorted(
            task
            for task, result in record.gate_status_by_task.items()
            if result.status == GateStatus.REJECT
        )
        if quarantined_tasks:
            quarantine_rows.append(
                {
                    "record_id": record.record_id,
                    "source_record_id": record.source_record_id,
                    "quarantined_tasks": quarantined_tasks,
                    "reasons": {
                        task: record.gate_status_by_task[task].reasons
                        for task in quarantined_tasks
                    },
                }
            )
        if rejected_tasks:
            rejected_rows.append(
                {
                    "record_id": record.record_id,
                    "source_record_id": record.source_record_id,
                    "rejected_tasks": rejected_tasks,
                    "reasons": {
                        task: record.gate_status_by_task[task].reasons
                        for task in rejected_tasks
                    },
                }
            )
    return quarantine_rows, rejected_rows


def _build_summary(
    config: TrainingDataPackConfig,
    raw_records: Sequence[dict[str, Any]],
    quality_records: Sequence[QualityRecord],
    sft_examples: Sequence[SFTExample],
    synthetic_examples: Sequence[SyntheticExample],
    preference_pairs: Sequence[PreferencePair],
    duplicate_summary: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    gate_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for record in quality_records:
        for task, result in record.gate_status_by_task.items():
            gate_counts[task][result.status.value] += 1
    missing_evidence_counts = {
        field: sum(not _text(record.get(field)) for record in raw_records)
        for field in EVIDENCE_METADATA_FIELDS
    }
    hard_flag_names = {
        "missing_source_id",
        "missing_run_id",
        "missing_claim_id",
        "empty_claim_text",
        "missing_status",
        "unknown_status",
        "invalid_split",
        "duplicate_source_claim_key",
        "duplicate_text_label_conflict",
        "cross_split_duplicate_cluster",
    }
    hard_error_record_count = sum(
        bool(hard_flag_names.intersection(record.quality_flags))
        for record in quality_records
    )
    preference_reviewed = sum(
        pair.review_status == ReviewStatus.REVIEWED for pair in preference_pairs
    )
    return {
        "pack_status": "pass" if hard_error_record_count == 0 else "pass_with_rejections",
        "formal_training": False,
        "rlhf_completed": False,
        "pipeline_version": PIPELINE_VERSION,
        "quality_gate_version": QUALITY_GATE_VERSION,
        "output_dataset_version": config.output_dataset_version,
        "usage_scope": config.usage_scope.value,
        "generated_at": generated_at,
        "input": {
            "path": str(config.input_jsonl),
            "sha256": _sha256_file(config.input_jsonl),
            "split_path": str(config.split_jsonl) if config.split_jsonl else None,
            "split_sha256": (
                _sha256_file(config.split_jsonl) if config.split_jsonl else None
            ),
            "records": len(raw_records),
        },
        "records": {
            "quality_records": len(quality_records),
            "hard_error_records": hard_error_record_count,
            "sft_examples": len(sft_examples),
            "synthetic_examples": len(synthetic_examples),
            "preference_pairs": len(preference_pairs),
            "preference_pairs_reviewed": preference_reviewed,
            "preference_pairs_pending": len(preference_pairs) - preference_reviewed,
        },
        "distributions": {
            "status": _counter_dict(record.status for record in quality_records),
            "split": _counter_dict(record.split for record in quality_records),
            "domain": _counter_dict(record.domain or "<empty>" for record in quality_records),
            "evidence_quality": _counter_dict(
                record.evidence_quality or "<empty>" for record in quality_records
            ),
            "quality_flags": _multi_counter_dict(
                record.quality_flags for record in quality_records
            ),
            "mutation_type": _counter_dict(
                example.mutation_type for example in synthetic_examples
            ),
        },
        "gate_counts": {
            task: dict(sorted(counts.items()))
            for task, counts in sorted(gate_counts.items())
        },
        "evidence_metadata_missing": missing_evidence_counts,
        "duplicates": {
            key: value
            for key, value in duplicate_summary.items()
            if key != "near_duplicate_pairs" and key != "clusters"
        },
        "duplicate_cluster_details": {
            "near_duplicate_pairs": duplicate_summary["near_duplicate_pairs"],
            "clusters": duplicate_summary["clusters"],
        },
        "boundaries": {
            "machine_screening_schema_changed": False,
            "manual_annotation_schema_changed": False,
            "real_data_public": False,
            "public_examples_must_be_synthetic": True,
            "preference_review_is_single_human_pilot": True,
            "metrics_are_model_quality_claims": False,
        },
        "config": {
            "seed": config.seed,
            "near_duplicate_threshold": config.near_duplicate_threshold,
            "shingle_size": config.shingle_size,
            "minhash_permutations": config.minhash_permutations,
            "lsh_bands": config.lsh_bands,
            "long_text_chars": config.long_text_chars,
            "synthetic_limit": config.synthetic_limit,
            "max_synthetic_per_parent": config.max_synthetic_per_parent,
            "preference_review_count": config.preference_review_count,
            "write_parquet": config.write_parquet,
        },
    }


def _build_manifest(
    config: TrainingDataPackConfig,
    summary: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    return {
        "dataset_version": config.output_dataset_version,
        "schema_versions": [
            "quality_record_v1",
            "synthetic_example_v1",
            "sft_example_v1",
            "preference_pair_v1",
        ],
        "pipeline_version": PIPELINE_VERSION,
        "quality_gate_version": QUALITY_GATE_VERSION,
        "generated_at": generated_at,
        "input_snapshot": summary["input"],
        "counts": summary["records"],
        "gate_counts": summary["gate_counts"],
        "usage_scope": config.usage_scope.value,
        "split_invariant": "derived records inherit parent split",
        "human_review_boundary": (
            "Preference pairs remain pending until the user records accept/edit/reject."
        ),
        "formal_training": False,
    }


def _render_quality_report(summary: dict[str, Any]) -> str:
    records = summary["records"]
    duplicate = summary["duplicates"]
    lines = [
        "# Training Data Quality Report",
        "",
        "## 运行性质",
        "",
        f"- pack_status: {summary['pack_status']}",
        f"- dataset_version: {summary['output_dataset_version']}",
        f"- pipeline_version: {summary['pipeline_version']}",
        f"- usage_scope: {summary['usage_scope']}",
        "- formal_training: false",
        "- rlhf_completed: false",
        "",
        "本报告证明质量门禁和派生数据管道可运行，不证明模型准确率或生产级 RLHF 能力。",
        "",
        "## 输入与输出",
        "",
        f"- input_records: {summary['input']['records']}",
        f"- input_sha256: {summary['input']['sha256']}",
        f"- quality_records: {records['quality_records']}",
        f"- hard_error_records: {records['hard_error_records']}",
        f"- sft_examples: {records['sft_examples']}",
        f"- synthetic_examples: {records['synthetic_examples']}",
        f"- preference_pairs: {records['preference_pairs']}",
        f"- preference_pairs_pending: {records['preference_pairs_pending']}",
        "",
        "## 任务门禁",
        "",
        "| task | pass | quarantine | reject |",
        "| --- | ---: | ---: | ---: |",
    ]
    for task, counts in summary["gate_counts"].items():
        lines.append(
            f"| {task} | {counts.get('pass', 0)} | "
            f"{counts.get('quarantine', 0)} | {counts.get('reject', 0)} |"
        )
    lines.extend(
        [
            "",
            "## 重复与污染",
            "",
            f"- exact_duplicate_clusters: {duplicate['exact_duplicate_cluster_count']}",
            f"- near_duplicate_pairs: {duplicate['near_duplicate_pair_count']}",
            f"- duplicate_clusters: {duplicate['duplicate_cluster_count']}",
            "",
            "## 证据元数据空值",
            "",
            "| field | missing |",
            "| --- | ---: |",
        ]
    )
    for field, count in summary["evidence_metadata_missing"].items():
        lines.append(f"| {field} | {count} |")
    lines.extend(
        [
            "",
            "## 边界",
            "",
            "- Initial-screening 机器候选 schema 未改变。",
            "- Manual-annotation 人工 gold schema 未改变。",
            "- 合成记录与人工 gold 使用不同 origin 和 schema。",
            "- synthetic、SFT 和 preference 继承父记录 split。",
            "- 真实数据保持本地私有；公开样例必须是纯合成内容。",
            "- 偏好对目前只是待人工复核的小型 pilot。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_preference_review_packet(
    pairs: Sequence[PreferencePair],
    requested_count: int,
) -> str:
    lines = [
        "# Preference Review Packet",
        "",
        "## 边界",
        "",
        f"- requested_review_count: {requested_count}",
        f"- generated_pair_count: {len(pairs)}",
        "- reviewer_type: single_human",
        "- initial_review_status: pending",
        "- 本文件不代表已完成 RLHF、DPO 训练或标注员间一致性评估。",
        "",
        "## 复核说明",
        "",
        "逐条选择 accept、edit 或 reject，并填写 review_reason。只有人工写回后，review_status 才能改为 reviewed。",
        "",
        "## 待复核 pair",
        "",
        "| # | pair_id | mutation_type | split | status |",
        "| ---: | --- | --- | --- | --- |",
    ]
    for index, pair in enumerate(pairs, start=1):
        lines.append(
            f"| {index} | {pair.pair_id} | {pair.mutation_type} | "
            f"{pair.split} | {pair.review_status.value} |"
        )
    lines.extend(["", "## 逐条复核内容", ""])
    for index, pair in enumerate(pairs, start=1):
        lines.extend(
            [
                f"### {index}. {pair.pair_id}",
                "",
                f"- mutation_type: {pair.mutation_type}",
                f"- split: {pair.split}",
                f"- review_status: {pair.review_status.value}",
                "",
                "Prompt:",
                "",
                "    " + pair.prompt,
                "",
                "Chosen:",
                "",
                "    " + pair.chosen,
                "",
                "Rejected:",
                "",
                "    " + pair.rejected,
                "",
                "Review fields:",
                "",
                "- review_decision: pending",
                "- review_reason:",
                "- final_chosen:",
                "",
            ]
        )
    return "\n".join(lines)


def _render_handoff(
    config: TrainingDataPackConfig,
    paths: dict[str, Path],
    summary: dict[str, Any],
) -> str:
    records = summary["records"]
    return f"""# HANDOFF

## Scope

This run built a quality-gated training-data pack from an immutable input snapshot.
It did not change Initial-screening or Manual-annotation schemas and did not run formal training.

## Identity

- dataset_version: {config.output_dataset_version}
- pipeline_version: {PIPELINE_VERSION}
- quality_gate_version: {QUALITY_GATE_VERSION}
- usage_scope: {config.usage_scope.value}

## Counts

- input_records: {summary["input"]["records"]}
- quality_records: {records["quality_records"]}
- sft_examples: {records["sft_examples"]}
- synthetic_examples: {records["synthetic_examples"]}
- preference_pairs: {records["preference_pairs"]}
- preference_pairs_pending: {records["preference_pairs_pending"]}

## Artifacts

- quality_records: {paths["quality_records"]}
- quarantine_records: {paths["quarantine_records"]}
- rejected_records: {paths["rejected_records"]}
- sft_examples: {paths["sft_examples"]}
- synthetic_examples: {paths["synthetic_examples"]}
- preference_pairs: {paths["preference_pairs"]}
- quality_report: {paths["report_md"]}
- review_packet: {paths["review_packet"]}

## Required next human action

Preference pairs remain pending. A human must record accept/edit/reject and a reason.
Do not describe the pending packet as completed RLHF or human-preference production.
"""


def _load_split_map(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    split_map: dict[str, str] = {}
    for row in _read_jsonl(path):
        split = _text(row.get("split"))
        if not split:
            continue
        record_key = _text(row.get("record_key"))
        source_id = _text(row.get("source_id")) or _text(row.get("run_id"))
        claim_id = _text(row.get("claim_id"))
        if record_key:
            split_map[record_key] = split
        if source_id and claim_id:
            split_map[f"{source_id}::{claim_id}"] = split
        if claim_id and claim_id not in split_map:
            split_map[claim_id] = split
    return split_map


def _character_shingles(value: str, size: int) -> set[str]:
    normalized = _dedup_text(value)
    if not normalized:
        return set()
    if len(normalized) <= size:
        return {normalized}
    return {normalized[index : index + size] for index in range(len(normalized) - size + 1)}


def _minhash_signature(shingles: set[str], permutations: int) -> tuple[int, ...]:
    if not shingles:
        return tuple()
    signature: list[int] = []
    for seed in range(permutations):
        minimum = min(
            int.from_bytes(
                hashlib.blake2b(
                    f"{seed}:{shingle}".encode("utf-8"),
                    digest_size=8,
                ).digest(),
                "big",
            )
            for shingle in shingles
        )
        signature.append(minimum)
    return tuple(signature)


def _lsh_candidate_pairs(
    signatures: dict[str, tuple[int, ...]],
    bands: int,
) -> set[tuple[str, str]]:
    if not signatures:
        return set()
    signature_length = len(next(iter(signatures.values())))
    rows_per_band = signature_length // bands
    buckets: dict[tuple[int, tuple[int, ...]], list[str]] = defaultdict(list)
    for record_id, signature in signatures.items():
        for band in range(bands):
            start = band * rows_per_band
            band_values = signature[start : start + rows_per_band]
            buckets[(band, band_values)].append(record_id)
    pairs: set[tuple[str, str]] = set()
    for members in buckets.values():
        if len(members) < 2:
            continue
        sorted_members = sorted(set(members))
        for left_index, left in enumerate(sorted_members):
            for right in sorted_members[left_index + 1 :]:
                pairs.add((left, right))
    return pairs


def _jaccard(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    return len(left & right) / len(union) if union else 0.0


def _dedup_text(value: str) -> str:
    normalized = normalize_text(value).casefold()
    return re.sub(r"[^\w\u3400-\u9fff]+", "", normalized)


def _deterministic_split(group_id: str, seed: int) -> str:
    bucket = _stable_int(str(seed), group_id) % 100
    if bucket < 70:
        return "train"
    if bucket < 85:
        return "dev"
    return "test"


def _origin_for_status(status: str) -> OriginType:
    if status in HUMAN_GOLD_STATUSES or status == "excluded":
        return OriginType.HUMAN_GOLD
    return OriginType.MACHINE_CANDIDATE


def _add_flag(candidate: _Candidate, flag: str) -> None:
    if flag not in candidate.quality_flags:
        candidate.quality_flags.append(flag)


def _stable_sort_key(value: str, seed: int) -> str:
    return _sha256_text(f"{seed}:{value}")


def _stable_int(*values: str) -> int:
    return int(_sha256_text(":".join(values))[:16], 16)


def _stable_id(prefix: str, *values: str) -> str:
    return f"{prefix}_{_sha256_text('|'.join(values))[:24]}"


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [part.strip() for part in re.split(r"[,;|]", text) if part.strip()]


def _counter_dict(values: Iterable[str]) -> dict[str, int]:
    return dict(sorted(Counter(values).items()))


def _multi_counter_dict(values: Iterable[Iterable[str]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for items in values:
        counter.update(items)
    return dict(sorted(counter.items()))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid JSON: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"{path}:{line_number}: expected JSON object")
            rows.append(value)
    return rows


def _output_paths(output_dir: Path) -> dict[str, Path]:
    return {
        "quality_records": output_dir / "quality_records.jsonl",
        "quarantine_records": output_dir / "quarantine_records.jsonl",
        "rejected_records": output_dir / "rejected_records.jsonl",
        "sft_examples": output_dir / "sft_examples.jsonl",
        "synthetic_examples": output_dir / "synthetic_examples.jsonl",
        "preference_pairs": output_dir / "preference_pairs.jsonl",
        "manifest": output_dir / "dataset_manifest.json",
        "report_json": output_dir / "quality_report.json",
        "report_md": output_dir / "QUALITY_REPORT.md",
        "review_packet": output_dir / "PREFERENCE_REVIEW_PACKET.md",
        "handoff": output_dir / "HANDOFF.md",
    }


def _prepare_output_dir(path: Path, overwrite: bool) -> None:
    existing = [name for name in CORE_OUTPUT_FILES if (path / name).exists()]
    existing.extend(
        name
        for name in (
            "quality_records.parquet",
            "sft_examples.parquet",
            "synthetic_examples.parquet",
            "preference_pairs.parquet",
        )
        if (path / name).exists()
    )
    if existing and not overwrite:
        raise FileExistsError(
            f"Output directory already contains generated files: {path} "
            f"({', '.join(existing)}). Enable overwrite to replace them."
        )
    if existing and overwrite:
        resolved = path.resolve()
        for name in existing:
            target = (path / name).resolve()
            if target.parent != resolved:
                raise ValueError(f"Refusing to overwrite outside output directory: {target}")
            target.unlink()
    path.mkdir(parents=True, exist_ok=True)


def _write_models_jsonl(path: Path, models: Sequence[Any]) -> None:
    _write_jsonl(path, [model.model_dump(mode="json") for model in models])


def _write_jsonl(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def _write_models_parquet(path: Path, models: Sequence[Any]) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "write_parquet=true requires pyarrow; install the project data extra."
        ) from exc
    rows: list[dict[str, Any]] = []
    for model in models:
        raw = model.model_dump(mode="json")
        rows.append(
            {
                key: (
                    json.dumps(value, ensure_ascii=False, sort_keys=True)
                    if isinstance(value, (dict, list))
                    else value
                )
                for key, value in raw.items()
            }
        )
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
