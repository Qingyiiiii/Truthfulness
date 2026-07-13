import json
from pathlib import Path
from uuid import uuid4

import pytest

from video_truthfulness.training import load_gold_jsonl, run_gold_baseline_smoke


BATCH_ID = "test_batch_v1"


def _gold_record(claim_id: str, gold_status: str = "gold_supports") -> dict[str, object]:
    return {
        "schema_version": "claim_gold_v1",
        "batch_id": BATCH_ID,
        "dataset_version": "test",
        "run_id": "test_run",
        "claim_id": claim_id,
        "parent_claim_id": claim_id,
        "gold_status": gold_status,
        "claim_text": f"Test claim {claim_id}",
        "valid_evidence_ids": ["ev_001"],
        "include_in_train": True,
        "include_in_eval": True,
    }


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def _test_dir() -> Path:
    path = Path("tmp") / "test_training" / uuid4().hex
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_load_gold_jsonl_rejects_pending_records() -> None:
    path = _test_dir() / "mixed.jsonl"
    pending = _gold_record("claim_pending")
    pending["gold_status"] = "pending"
    pending["include_in_train"] = False
    pending["include_in_eval"] = False
    _write_jsonl(path, [_gold_record("claim_gold"), pending])

    with pytest.raises(ValueError, match="non-gold status"):
        load_gold_jsonl(path, expected_batch_id=BATCH_ID)


def test_run_gold_baseline_smoke_writes_expected_artifacts() -> None:
    test_dir = _test_dir()
    path = test_dir / "gold.jsonl"
    records = [
        _gold_record("claim_001", "gold_supports"),
        _gold_record("claim_002", "gold_supports"),
        _gold_record("claim_003", "gold_partially_supports"),
        _gold_record("claim_004", "gold_supports"),
    ]
    _write_jsonl(path, records)

    result = run_gold_baseline_smoke(
        gold_jsonl=path,
        batch_id=BATCH_ID,
        exp_id="test_training_smoke",
        experiments_dir=test_dir / "experiments",
    )

    assert result.config_path.exists()
    assert result.log_path.exists()
    assert result.metrics_path.exists()
    assert result.split_path.exists()
    assert result.summary_path.exists()
    assert result.handoff_path.exists()
    assert result.metrics["records_read"] == 4
    assert result.metrics["split_counts"] == {"train": 2, "dev": 1, "test": 1}

    metric_events = [json.loads(line) for line in result.metrics_path.read_text(encoding="utf-8").splitlines()]
    assert [event["event"] for event in metric_events] == [
        "data_read",
        "split_summary",
        "baseline_fit",
        "split_metric",
        "split_metric",
        "split_metric",
        "smoke_summary",
    ]
    assert "不能宣称模型效果" in result.summary_path.read_text(encoding="utf-8")
