import json
from pathlib import Path

from video_truthfulness.versions.v01.training_data_quality import (
    TrainingDataPackConfig,
    build_training_data_pack,
    validate_preference_review_file,
)
from video_truthfulness.versions.v01.training_data_schemas import GateStatus, ReviewStatus, UsageScope


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _record(
    claim_id: str,
    claim_text: str,
    *,
    source_id: str = "synthetic_source",
    status: str = "gold_supports",
    split: str = "train",
    complete_evidence: bool = True,
) -> tuple[dict[str, object], dict[str, object]]:
    evidence_value = "synthetic evidence" if complete_evidence else ""
    record = {
        "dataset_version": "test_synthetic_v1",
        "schema_version": "truthfulness_seed_v0.1",
        "source_id": source_id,
        "run_id": source_id,
        "claim_id": claim_id,
        "parent_claim_id": claim_id,
        "claim_text": claim_text,
        "raw_context": "Synthetic context only.",
        "status": status,
        "evidence_quality": "primary_source",
        "source_title_evidence": evidence_value,
        "publisher": evidence_value,
        "source_type": evidence_value,
        "retrieved_at": evidence_value,
        "verifiable_excerpt": evidence_value,
        "domain": "technology",
        "claim_type": "numeric_statistical_claim",
        "checkability": "directly_checkable",
        "noise_patterns": ["unit_or_scale_error"],
        "recommended_action": "pass",
        "include_decision": "included",
        "training_use": "main",
        "origin": "synthetic",
    }
    split_row = {
        "record_key": f"{source_id}::{claim_id}",
        "source_id": source_id,
        "claim_id": claim_id,
        "split": split,
    }
    return record, split_row


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_quality_gate_is_task_specific_and_preserves_gold(tmp_path: Path) -> None:
    record, split = _record(
        "claim_001",
        "合成园区在2025年新增了20台设备。",
        complete_evidence=False,
    )
    input_path = tmp_path / "input.jsonl"
    split_path = tmp_path / "splits.jsonl"
    _write_jsonl(input_path, [record])
    _write_jsonl(split_path, [split])

    result = build_training_data_pack(
        TrainingDataPackConfig(
            input_jsonl=input_path,
            split_jsonl=split_path,
            output_dir=tmp_path / "output",
            usage_scope=UsageScope.PUBLIC_SYNTHETIC,
            synthetic_limit=1,
            preference_review_count=1,
            generated_at="2026-07-17T00:00:00+00:00",
        )
    )

    quality = _read_jsonl(result.quality_records_path)[0]
    assert quality["status"] == "gold_supports"
    assert quality["gate_status_by_task"]["claim_triage_sft"]["status"] == GateStatus.PASS.value
    assert (
        quality["gate_status_by_task"]["evidence_grounded_sft"]["status"]
        == GateStatus.QUARANTINE.value
    )
    assert result.summary["records"]["sft_examples"] == 1
    assert result.summary["records"]["synthetic_examples"] == 1
    assert result.summary["records"]["preference_pairs"] == 1


def test_duplicate_cluster_crossing_splits_is_rejected(tmp_path: Path) -> None:
    first, first_split = _record(
        "claim_001",
        "合成城市在2025年新增120辆公交车。",
        source_id="source_a",
        split="train",
    )
    second, second_split = _record(
        "claim_001",
        "合成城市在2025年新增120辆公交车。",
        source_id="source_b",
        split="test",
    )
    input_path = tmp_path / "input.jsonl"
    split_path = tmp_path / "splits.jsonl"
    _write_jsonl(input_path, [first, second])
    _write_jsonl(split_path, [first_split, second_split])

    result = build_training_data_pack(
        TrainingDataPackConfig(
            input_jsonl=input_path,
            split_jsonl=split_path,
            output_dir=tmp_path / "output",
            usage_scope=UsageScope.PUBLIC_SYNTHETIC,
            synthetic_limit=0,
            preference_review_count=0,
            generated_at="2026-07-17T00:00:00+00:00",
        )
    )

    assert result.summary["records"]["hard_error_records"] == 2
    assert result.summary["duplicates"]["near_duplicate_pair_count"] == 0
    quality_rows = _read_jsonl(result.quality_records_path)
    for row in quality_rows:
        assert "cross_split_duplicate_cluster" in row["quality_flags"]
        assert row["gate_status_by_task"]["claim_triage_sft"]["status"] == "reject"


def test_existing_excluded_split_is_quarantined_not_rejected(tmp_path: Path) -> None:
    record, split = _record(
        "claim_001",
        "这是一条超过主任务边界后被显式隔离的纯合成长文本。",
    )
    record["training_use"] = "excluded_from_main_tasks"
    split["split"] = "excluded_from_main_tasks"
    input_path = tmp_path / "input.jsonl"
    split_path = tmp_path / "splits.jsonl"
    _write_jsonl(input_path, [record])
    _write_jsonl(split_path, [split])

    result = build_training_data_pack(
        TrainingDataPackConfig(
            input_jsonl=input_path,
            split_jsonl=split_path,
            output_dir=tmp_path / "output",
            usage_scope=UsageScope.PUBLIC_SYNTHETIC,
            synthetic_limit=0,
            preference_review_count=0,
            generated_at="2026-07-17T00:00:00+00:00",
        )
    )

    quality = _read_jsonl(result.quality_records_path)[0]
    assert result.summary["records"]["hard_error_records"] == 0
    assert "invalid_split" not in quality["quality_flags"]
    assert quality["gate_status_by_task"]["claim_triage_sft"]["status"] == "quarantine"
    assert "excluded_from_main_tasks" in quality["gate_status_by_task"]["claim_triage_sft"]["reasons"]


def test_mojibake_candidate_is_quarantined_without_changing_gold(tmp_path: Path) -> None:
    record, split = _record(
        "claim_001",
        "????????????????????",
    )
    input_path = tmp_path / "input.jsonl"
    split_path = tmp_path / "splits.jsonl"
    _write_jsonl(input_path, [record])
    _write_jsonl(split_path, [split])

    result = build_training_data_pack(
        TrainingDataPackConfig(
            input_jsonl=input_path,
            split_jsonl=split_path,
            output_dir=tmp_path / "output",
            usage_scope=UsageScope.PUBLIC_SYNTHETIC,
            synthetic_limit=1,
            preference_review_count=1,
            generated_at="2026-07-17T00:00:00+00:00",
        )
    )

    quality = _read_jsonl(result.quality_records_path)[0]
    assert quality["status"] == "gold_supports"
    assert "question_mark_mojibake_candidate" in quality["quality_flags"]
    assert quality["gate_status_by_task"]["claim_triage_sft"]["status"] == "quarantine"
    assert result.summary["records"]["sft_examples"] == 0
    assert result.summary["records"]["synthetic_examples"] == 0


def test_synthetic_and_preference_children_inherit_train_split(tmp_path: Path) -> None:
    rows: list[dict[str, object]] = []
    split_rows: list[dict[str, object]] = []
    for index in range(1, 6):
        record, split = _record(
            f"claim_{index:03d}",
            f"合成研究在202{index}年记录了{index * 10}个样本。",
            source_id=f"synthetic_source_{index}",
            split="train",
        )
        rows.append(record)
        split_rows.append(split)
    input_path = tmp_path / "input.jsonl"
    split_path = tmp_path / "splits.jsonl"
    _write_jsonl(input_path, rows)
    _write_jsonl(split_path, split_rows)

    result = build_training_data_pack(
        TrainingDataPackConfig(
            input_jsonl=input_path,
            split_jsonl=split_path,
            output_dir=tmp_path / "output",
            usage_scope=UsageScope.PUBLIC_SYNTHETIC,
            synthetic_limit=5,
            preference_review_count=3,
            write_parquet=True,
            generated_at="2026-07-17T00:00:00+00:00",
        )
    )

    synthetic_rows = _read_jsonl(result.synthetic_examples_path)
    preference_rows = _read_jsonl(result.preference_pairs_path)
    assert len(synthetic_rows) == 5
    assert len(preference_rows) == 3
    assert {row["split"] for row in synthetic_rows} == {"train"}
    assert {row["split"] for row in preference_rows} == {"train"}
    assert {row["review_status"] for row in preference_rows} == {ReviewStatus.PENDING.value}
    assert result.summary["records"]["preference_pairs_reviewed"] == 0
    assert {path.name for path in result.parquet_paths} == {
        "quality_records.parquet",
        "sft_examples.parquet",
        "synthetic_examples.parquet",
        "preference_pairs.parquet",
    }
    assert all(path.exists() for path in result.parquet_paths)


def test_public_fixture_builds_without_private_release_claim(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    result = build_training_data_pack(
        TrainingDataPackConfig(
            input_jsonl=repo_root / "examples" / "versions" / "v01" / "training_data" / "input.synthetic.jsonl",
            split_jsonl=repo_root / "examples" / "versions" / "v01" / "training_data" / "splits.synthetic.jsonl",
            output_dir=tmp_path / "public_demo",
            output_dataset_version="public_demo_v1",
            usage_scope=UsageScope.PUBLIC_SYNTHETIC,
            synthetic_limit=8,
            preference_review_count=4,
            generated_at="2026-07-17T00:00:00+00:00",
        )
    )

    assert result.summary["input"]["records"] == 8
    assert result.summary["boundaries"]["real_data_public"] is False
    assert result.summary["records"]["preference_pairs"] == 4
    assert result.report_md_path.exists()
    assert result.review_packet_path.exists()


def test_preference_review_validator_keeps_pending_distinct_from_reviewed(
    tmp_path: Path,
) -> None:
    record, split = _record(
        "claim_001",
        "合成研究在2025年记录了20个样本。",
    )
    input_path = tmp_path / "input.jsonl"
    split_path = tmp_path / "splits.jsonl"
    _write_jsonl(input_path, [record])
    _write_jsonl(split_path, [split])
    result = build_training_data_pack(
        TrainingDataPackConfig(
            input_jsonl=input_path,
            split_jsonl=split_path,
            output_dir=tmp_path / "output",
            usage_scope=UsageScope.PUBLIC_SYNTHETIC,
            synthetic_limit=1,
            preference_review_count=1,
            generated_at="2026-07-17T00:00:00+00:00",
        )
    )

    summary = validate_preference_review_file(result.preference_pairs_path)
    assert summary["pending"] == 1
    assert summary["reviewed"] == 0

    try:
        validate_preference_review_file(
            result.preference_pairs_path,
            require_all_reviewed=True,
        )
    except ValueError as exc:
        assert "pair remains pending" in str(exc)
    else:
        raise AssertionError("require_all_reviewed should reject a pending pair")
