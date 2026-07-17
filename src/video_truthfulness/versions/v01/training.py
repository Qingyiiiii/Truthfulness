"""Training smoke-test utilities for gold claim JSONL batches.

This module intentionally implements a tiny baseline flow instead of a real
model trainer. It validates that a gold-only JSONL batch can be read, split
deterministically, and evaluated end-to-end without mixing pending or excluded
records into the training surface.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any


GOLD_STATUS_PREFIX = "gold_"
SPLITS = ("train", "dev", "test")


@dataclass(frozen=True)
class GoldRecord:
    """One validated gold JSONL record."""

    raw: dict[str, Any]
    line_number: int

    @property
    def batch_id(self) -> str:
        return str(self.raw["batch_id"])

    @property
    def claim_id(self) -> str:
        return str(self.raw["claim_id"])

    @property
    def parent_claim_id(self) -> str:
        return str(self.raw.get("parent_claim_id") or self.claim_id)

    @property
    def gold_status(self) -> str:
        return str(self.raw["gold_status"])

    @property
    def target_label(self) -> str:
        return self.gold_status.removeprefix(GOLD_STATUS_PREFIX)

    @property
    def claim_text(self) -> str:
        return str(self.raw["claim_text"])

    @property
    def include_in_train(self) -> bool:
        return bool(self.raw["include_in_train"])

    @property
    def include_in_eval(self) -> bool:
        return bool(self.raw["include_in_eval"])


@dataclass(frozen=True)
class TrainingRunResult:
    """Paths and summary values produced by a training smoke run."""

    exp_id: str
    exp_dir: Path
    config_path: Path
    log_path: Path
    metrics_path: Path
    split_path: Path
    summary_path: Path
    handoff_path: Path
    metrics: dict[str, Any]


def load_gold_jsonl(path: Path, expected_batch_id: str | None = None) -> list[GoldRecord]:
    """Load and validate a gold-only JSONL file.

    The loader is deliberately strict: records that do not carry a `gold_*`
    status, or are not explicitly train/eval eligible, fail the run instead of
    being silently filtered. That protects this stage from accidentally mixing
    pending or excluded samples into a small experiment.
    """

    if not path.exists():
        raise FileNotFoundError(f"Gold JSONL does not exist: {path}")

    records: list[GoldRecord] = []
    invalid_lines: list[str] = []
    duplicate_claim_ids: list[str] = []
    seen_claim_ids: set[str] = set()
    required_fields = {
        "batch_id",
        "claim_id",
        "gold_status",
        "claim_text",
        "include_in_train",
        "include_in_eval",
    }

    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                raw = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number}: {exc}") from exc
            if not isinstance(raw, dict):
                raise ValueError(f"Line {line_number} must be a JSON object.")

            missing = sorted(required_fields.difference(raw))
            if missing:
                invalid_lines.append(f"line {line_number}: missing {', '.join(missing)}")
                continue

            claim_id = str(raw["claim_id"])
            if claim_id in seen_claim_ids:
                duplicate_claim_ids.append(claim_id)
            seen_claim_ids.add(claim_id)

            if expected_batch_id and raw["batch_id"] != expected_batch_id:
                invalid_lines.append(
                    f"line {line_number}: batch_id={raw['batch_id']!r} expected={expected_batch_id!r}"
                )
                continue

            status = str(raw["gold_status"])
            include_in_train = raw["include_in_train"]
            include_in_eval = raw["include_in_eval"]
            if not isinstance(include_in_train, bool) or not isinstance(include_in_eval, bool):
                invalid_lines.append(f"line {line_number}: include flags must be booleans")
                continue
            if not status.startswith(GOLD_STATUS_PREFIX):
                invalid_lines.append(f"line {line_number}: non-gold status {status!r}")
                continue
            if include_in_train is not True or include_in_eval is not True:
                invalid_lines.append(
                    f"line {line_number}: gold record must be train/eval eligible "
                    f"(include_in_train={include_in_train!r}, include_in_eval={include_in_eval!r})"
                )
                continue
            if not str(raw["claim_text"]).strip():
                invalid_lines.append(f"line {line_number}: claim_text is empty")
                continue

            records.append(GoldRecord(raw=raw, line_number=line_number))

    if duplicate_claim_ids:
        duplicates = ", ".join(sorted(set(duplicate_claim_ids)))
        invalid_lines.append(f"duplicate claim_id values: {duplicates}")
    if invalid_lines:
        preview = "; ".join(invalid_lines[:8])
        extra = "" if len(invalid_lines) <= 8 else f"; ... {len(invalid_lines) - 8} more"
        raise ValueError(f"Gold JSONL validation failed: {preview}{extra}")
    if not records:
        raise ValueError(f"Gold JSONL contains no eligible gold records: {path}")

    return records


def split_gold_records(
    records: list[GoldRecord],
    seed: int = 20260701,
    train_ratio: float = 0.5,
    dev_ratio: float = 0.25,
    test_ratio: float = 0.25,
) -> dict[str, str]:
    """Return deterministic claim_id -> split assignments."""

    counts = _allocate_split_counts(len(records), train_ratio, dev_ratio, test_ratio)
    ordered_records = sorted(records, key=lambda record: _stable_record_key(record, seed))
    assignments: dict[str, str] = {}
    cursor = 0
    for split in SPLITS:
        split_count = counts[split]
        for record in ordered_records[cursor : cursor + split_count]:
            assignments[record.claim_id] = split
        cursor += split_count
    return assignments


def run_gold_baseline_smoke(
    gold_jsonl: Path,
    batch_id: str,
    exp_id: str,
    experiments_dir: Path = Path("runtime/V01/reproduction-experiments"),
    seed: int = 20260701,
    train_ratio: float = 0.5,
    dev_ratio: float = 0.25,
    test_ratio: float = 0.25,
    overwrite: bool = False,
) -> TrainingRunResult:
    """Run a minimal gold-data smoke flow and write experiment artifacts."""

    started_at = _utc_now()
    exp_dir = experiments_dir / exp_id
    _prepare_exp_dir(exp_dir, overwrite=overwrite)

    log_lines: list[str] = [
        f"{started_at} start mode=smoke_baseline exp_id={exp_id}",
        f"{_utc_now()} read_gold_jsonl path={gold_jsonl} batch_id={batch_id}",
    ]
    records = load_gold_jsonl(gold_jsonl, expected_batch_id=batch_id)
    assignments = split_gold_records(records, seed, train_ratio, dev_ratio, test_ratio)
    metrics = build_smoke_metrics(records, assignments, batch_id=batch_id, seed=seed)
    metrics.update(
        {
            "exp_id": exp_id,
            "gold_jsonl": str(gold_jsonl),
            "formal_training": False,
            "smoke_test_only": True,
            "warning": "Only data loading, split generation, and a majority-label baseline smoke test were run.",
        }
    )
    log_lines.append(
        f"{_utc_now()} records={metrics['records_read']} "
        f"split_counts={json.dumps(metrics['split_counts'], ensure_ascii=False)}"
    )
    log_lines.append(
        f"{_utc_now()} majority_label={metrics['majority_label']} "
        f"dev_accuracy={metrics['split_metrics']['dev']['accuracy']} "
        f"test_accuracy={metrics['split_metrics']['test']['accuracy']}"
    )
    log_lines.append(f"{_utc_now()} finish status=success formal_training=false")

    config_path = exp_dir / "config.yaml"
    log_path = exp_dir / "train.log"
    metrics_path = exp_dir / "metrics.jsonl"
    split_path = exp_dir / "splits.jsonl"
    summary_path = exp_dir / "RESULT_SUMMARY.md"
    handoff_path = exp_dir / "HANDOFF.md"

    _write_config(
        config_path,
        {
            "exp_id": exp_id,
            "batch_id": batch_id,
            "gold_jsonl": str(gold_jsonl),
            "mode": "smoke_baseline",
            "seed": seed,
            "split_ratios": {
                "train": train_ratio,
                "dev": dev_ratio,
                "test": test_ratio,
            },
            "formal_training": False,
            "smoke_test_only": True,
        },
    )
    _write_log(log_path, log_lines)
    _write_metrics(metrics_path, metrics)
    _write_splits(split_path, records, assignments)
    _write_result_summary(summary_path, metrics)
    _write_handoff(handoff_path, metrics)

    return TrainingRunResult(
        exp_id=exp_id,
        exp_dir=exp_dir,
        config_path=config_path,
        log_path=log_path,
        metrics_path=metrics_path,
        split_path=split_path,
        summary_path=summary_path,
        handoff_path=handoff_path,
        metrics=metrics,
    )


def build_smoke_metrics(
    records: list[GoldRecord],
    assignments: dict[str, str],
    batch_id: str,
    seed: int,
) -> dict[str, Any]:
    """Build data-read, split, and majority-baseline metrics."""

    records_by_split: dict[str, list[GoldRecord]] = {split: [] for split in SPLITS}
    for record in records:
        split = assignments[record.claim_id]
        records_by_split[split].append(record)

    train_labels = [record.target_label for record in records_by_split["train"]]
    if not train_labels:
        raise ValueError("Training split is empty; cannot fit smoke baseline.")
    majority_label = _majority_label(train_labels)

    split_metrics = {
        split: _classification_metrics(
            actual=[record.target_label for record in split_records],
            predicted=[majority_label for _ in split_records],
        )
        for split, split_records in records_by_split.items()
    }
    overall_eval_records = records_by_split["dev"] + records_by_split["test"]
    overall_eval_metrics = _classification_metrics(
        actual=[record.target_label for record in overall_eval_records],
        predicted=[majority_label for _ in overall_eval_records],
    )

    return {
        "batch_id": batch_id,
        "seed": seed,
        "records_read": len(records),
        "eligible_gold_records": len(records),
        "split_counts": {split: len(records_by_split[split]) for split in SPLITS},
        "label_distribution": _label_distribution(records),
        "split_label_distribution": {
            split: _label_distribution(split_records) for split, split_records in records_by_split.items()
        },
        "majority_label": majority_label,
        "split_metrics": split_metrics,
        "overall_eval_metrics": overall_eval_metrics,
        "tiny_dataset_warning": len(records) < 30,
        "metrics_are_quality_claims": False,
    }


def _allocate_split_counts(
    total: int,
    train_ratio: float,
    dev_ratio: float,
    test_ratio: float,
) -> dict[str, int]:
    if total <= 0:
        raise ValueError("Cannot split an empty dataset.")
    if any(ratio < 0 for ratio in (train_ratio, dev_ratio, test_ratio)):
        raise ValueError("Split ratios must be non-negative.")
    ratio_sum = train_ratio + dev_ratio + test_ratio
    if ratio_sum <= 0:
        raise ValueError("At least one split ratio must be positive.")

    if total == 1:
        return {"train": 1, "dev": 0, "test": 0}
    if total == 2:
        return {"train": 1, "dev": 0, "test": 1}
    if total == 3:
        return {"train": 1, "dev": 1, "test": 1}

    ratios = {
        "train": train_ratio / ratio_sum,
        "dev": dev_ratio / ratio_sum,
        "test": test_ratio / ratio_sum,
    }
    quotas = {split: ratios[split] * total for split in SPLITS}
    counts = {split: int(quotas[split]) for split in SPLITS}
    remainder = total - sum(counts.values())
    fractional_order = sorted(SPLITS, key=lambda split: (quotas[split] - counts[split], split), reverse=True)
    for split in fractional_order[:remainder]:
        counts[split] += 1

    for split in SPLITS:
        if ratios[split] > 0 and counts[split] == 0:
            donor = max(SPLITS, key=lambda candidate: counts[candidate])
            if counts[donor] <= 1:
                break
            counts[donor] -= 1
            counts[split] += 1

    if counts["train"] == 0:
        donor = max(("dev", "test"), key=lambda candidate: counts[candidate])
        counts[donor] -= 1
        counts["train"] = 1
    return counts


def _stable_record_key(record: GoldRecord, seed: int) -> str:
    payload = f"{seed}:{record.batch_id}:{record.claim_id}:{record.line_number}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _majority_label(labels: list[str]) -> str:
    counts = Counter(labels)
    return sorted(counts, key=lambda label: (-counts[label], label))[0]


def _classification_metrics(actual: list[str], predicted: list[str]) -> dict[str, Any]:
    if len(actual) != len(predicted):
        raise ValueError("Actual and predicted labels must have the same length.")
    if not actual:
        return {
            "records": 0,
            "accuracy": None,
            "macro_f1": None,
            "correct": 0,
            "per_label": {},
        }

    labels = sorted(set(actual).union(predicted))
    correct = sum(1 for actual_label, predicted_label in zip(actual, predicted) if actual_label == predicted_label)
    per_label: dict[str, dict[str, float | int]] = {}
    f1_values: list[float] = []
    for label in labels:
        true_positive = sum(1 for a, p in zip(actual, predicted) if a == label and p == label)
        false_positive = sum(1 for a, p in zip(actual, predicted) if a != label and p == label)
        false_negative = sum(1 for a, p in zip(actual, predicted) if a == label and p != label)
        precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else 0.0
        recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        f1_values.append(f1)
        per_label[label] = {
            "support": sum(1 for item in actual if item == label),
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
        }

    return {
        "records": len(actual),
        "accuracy": round(correct / len(actual), 6),
        "macro_f1": round(sum(f1_values) / len(f1_values), 6),
        "correct": correct,
        "per_label": per_label,
    }


def _label_distribution(records: list[GoldRecord]) -> dict[str, int]:
    counts = Counter(record.target_label for record in records)
    return dict(sorted(counts.items()))


def _prepare_exp_dir(exp_dir: Path, overwrite: bool) -> None:
    key_files = [
        "config.yaml",
        "train.log",
        "metrics.jsonl",
        "splits.jsonl",
        "RESULT_SUMMARY.md",
        "HANDOFF.md",
    ]
    existing = [name for name in key_files if (exp_dir / name).exists()]
    if existing and not overwrite:
        raise FileExistsError(
            f"Experiment directory already contains generated files: {exp_dir} ({', '.join(existing)}). "
            "Pass --overwrite to replace smoke-test artifacts."
        )
    exp_dir.mkdir(parents=True, exist_ok=True)


def _write_config(path: Path, values: dict[str, Any]) -> None:
    lines = [
        "# Auto-generated by video-truthfulness train-baseline.",
        "# This config records a smoke baseline only; it is not a formal training result.",
    ]
    lines.extend(_yaml_lines(values))
    _write_text(path, "\n".join(lines) + "\n")


def _yaml_lines(value: Any, indent: int = 0, key: str | None = None) -> list[str]:
    prefix = " " * indent
    lines: list[str] = []
    if isinstance(value, dict):
        if key is not None:
            lines.append(f"{prefix}{key}:")
            indent += 2
            prefix = " " * indent
        for item_key, item_value in value.items():
            lines.extend(_yaml_lines(item_value, indent=indent, key=str(item_key)))
    elif isinstance(value, list):
        if key is not None:
            lines.append(f"{prefix}{key}:")
            indent += 2
            prefix = " " * indent
        for item in value:
            if isinstance(item, (dict, list)):
                lines.append(f"{prefix}-")
                lines.extend(_yaml_lines(item, indent=indent + 2))
            else:
                lines.append(f"{prefix}- {_yaml_scalar(item)}")
    else:
        if key is None:
            lines.append(f"{prefix}{_yaml_scalar(value)}")
        else:
            lines.append(f"{prefix}{key}: {_yaml_scalar(value)}")
    return lines


def _yaml_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value), ensure_ascii=False)


def _write_log(path: Path, lines: list[str]) -> None:
    _write_text(path, "\n".join(lines) + "\n")


def _write_metrics(path: Path, metrics: dict[str, Any]) -> None:
    events = [
        {
            "event": "data_read",
            "batch_id": metrics["batch_id"],
            "records_read": metrics["records_read"],
            "eligible_gold_records": metrics["eligible_gold_records"],
            "smoke_test_only": True,
        },
        {
            "event": "split_summary",
            "batch_id": metrics["batch_id"],
            "split_counts": metrics["split_counts"],
            "split_label_distribution": metrics["split_label_distribution"],
        },
        {
            "event": "baseline_fit",
            "batch_id": metrics["batch_id"],
            "majority_label": metrics["majority_label"],
            "formal_training": False,
        },
    ]
    for split in SPLITS:
        events.append(
            {
                "event": "split_metric",
                "batch_id": metrics["batch_id"],
                "split": split,
                **metrics["split_metrics"][split],
            }
        )
    events.append(
        {
            "event": "smoke_summary",
            "batch_id": metrics["batch_id"],
            "overall_eval_metrics": metrics["overall_eval_metrics"],
            "tiny_dataset_warning": metrics["tiny_dataset_warning"],
            "metrics_are_quality_claims": False,
        }
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, ensure_ascii=False))
            handle.write("\n")


def _write_splits(path: Path, records: list[GoldRecord], assignments: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            split_record = {
                "batch_id": record.batch_id,
                "claim_id": record.claim_id,
                "parent_claim_id": record.parent_claim_id,
                "split": assignments[record.claim_id],
                "gold_status": record.gold_status,
                "target_label": record.target_label,
                "valid_evidence_ids": record.raw.get("valid_evidence_ids", []),
                "include_in_train": record.include_in_train,
                "include_in_eval": record.include_in_eval,
            }
            handle.write(json.dumps(split_record, ensure_ascii=False))
            handle.write("\n")


def _write_result_summary(path: Path, metrics: dict[str, Any]) -> None:
    split_counts = metrics["split_counts"]
    dev_metrics = metrics["split_metrics"]["dev"]
    test_metrics = metrics["split_metrics"]["test"]
    content = f"""# RESULT_SUMMARY

## 本次运行性质

- 模式：`smoke_baseline`
- batch_id：`{metrics["batch_id"]}`
- exp_id：`{metrics["exp_id"]}`
- 结论边界：本次只验证 gold JSONL 读取、train/dev/test 划分、metrics/log 输出链路；没有运行正式长训练，不能宣称模型效果。

## 数据读取

- gold_jsonl：`{metrics["gold_jsonl"]}`
- 读取 gold records：`{metrics["records_read"]}`
- eligible gold records：`{metrics["eligible_gold_records"]}`
- label_distribution：`{json.dumps(metrics["label_distribution"], ensure_ascii=False)}`

## 划分结果

| split | records |
|---|---:|
| train | {split_counts["train"]} |
| dev | {split_counts["dev"]} |
| test | {split_counts["test"]} |

## Smoke Metrics

- baseline：训练集 majority label = `{metrics["majority_label"]}`
- dev accuracy：`{dev_metrics["accuracy"]}`
- test accuracy：`{test_metrics["accuracy"]}`
- macro_f1 只用于检查 metrics 管道是否能写出；4 条样本不足以解释模型质量。

## 风险

- 本批次只有 4 条 gold；当前输出是流程连通性证据，不是训练效果证据。
- pending/excluded 没有被读取进 gold 训练面。
- `claim_005a` 使用的 `ev_006` 在原始 manifest 中为 clue_only，后续高质量版本建议补 NVIDIA/OEM 原始规格。
"""
    _write_text(path, content)


def _write_handoff(path: Path, metrics: dict[str, Any]) -> None:
    exp_id = metrics["exp_id"]
    batch_id = metrics["batch_id"]
    gold_jsonl = metrics["gold_jsonl"]
    content = f"""# HANDOFF

## Scope

This smoke run validates a reviewed, gold-only JSONL input, deterministic splits, and metric artifact generation. It is not a formal training run.

## Inputs

- Gold JSONL: `{gold_jsonl}`
- Batch ID: `{batch_id}`
- Entry point: `video-truthfulness train-baseline --smoke-test`

## Generated artifacts

- Config: `runtime/V01/reproduction-experiments/{exp_id}/config.yaml`
- Log: `runtime/V01/reproduction-experiments/{exp_id}/train.log`
- Metrics: `runtime/V01/reproduction-experiments/{exp_id}/metrics.jsonl`
- Splits: `runtime/V01/reproduction-experiments/{exp_id}/splits.jsonl`
- Summary: `runtime/V01/reproduction-experiments/{exp_id}/RESULT_SUMMARY.md`
- This handoff: `runtime/V01/reproduction-experiments/{exp_id}/HANDOFF.md`

## Data summary

| 指标 | 数值 |
|---|---:|
| gold records read | {metrics["records_read"]} |
| eligible gold records | {metrics["eligible_gold_records"]} |
| train records | {metrics["split_counts"]["train"]} |
| dev records | {metrics["split_counts"]["dev"]} |
| test records | {metrics["split_counts"]["test"]} |

标签分布：`{json.dumps(metrics["label_distribution"], ensure_ascii=False)}`

## Boundaries and next steps

- Only `gold_*` records with both eligibility flags set to `true` were accepted; pending and excluded records must remain outside this run.
- The baseline and any dev/test metrics validate plumbing only. They do not establish model quality, calibration, or generalization.
- Before a formal experiment, audit label balance, source independence, annotation agreement, leakage controls, and a frozen holdout set.
- Record dataset, schema, prompt, and split versions together with any future evaluation result.
"""
    _write_text(path, content)


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
