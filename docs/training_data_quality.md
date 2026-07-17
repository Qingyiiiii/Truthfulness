# Training-data quality pack

## Purpose

The v0.1.1 pack adds a downstream training-data contract without changing the
machine-screening or human-gold annotation schemas.

    Initial-screening machine candidates
      -> Manual-annotation human gold
      -> task-specific quality gates
           -> claim-triage SFT
           -> controlled synthetic hard negatives
                -> pending preference-review pilot

The pack is data infrastructure, not a model-training result.

## Public and private boundary

Real videos, transcripts, evidence snapshots, full annotations, and all
derivatives remain local under ignored data/output directories. Public Git
contains:

- strict Pydantic models and exported JSON Schema;
- quality and lineage code;
- an eight-record, fully invented fixture;
- tests and an example TOML configuration;
- aggregate reports generated from synthetic data.

A public fixture must use usage_scope=public_synthetic and origin=synthetic.
The release gate rejects private-only records.

## Gate layers

| Gate | Main checks |
| --- | --- |
| Source/intake | stable source identity, language, usage scope, secret boundary |
| Normalization | NFKC text, empty/long/mojibake checks, original/normalized hashes |
| Deduplication | exact hashes, MinHash/LSH candidates, Jaccard confirmation, split contamination |
| Gold/evidence | human terminal state, evidence completeness, task-specific eligibility |
| Derived data | parent lineage, generator/seed/version, split inheritance |
| Release | public synthetic-only rule, PII/secret pattern rejection |

Gate results are task-specific. A record may pass claim-triage SFT while being
quarantined from evidence-grounded SFT.

## Generated schemas

- quality_record_v1: canonical quality, lineage, duplicate, and task-gate view.
- synthetic_example_v1: controlled mutation with parent and verifier.
- sft_example_v1: portable chat/instruction derivative for one named task.
- preference_pair_v1: pending chosen/rejected review item.

Export schemas with:

    PYTHONPATH=src python scripts/export_training_data_schemas.py

## Run the public synthetic demo

From the repository root:

    PYTHONPATH=src python -m video_truthfulness.cli training-data-pack \
      --config configs/training_data_quality.example.toml

The ignored output directory contains:

- quality_records.jsonl
- quarantine_records.jsonl
- rejected_records.jsonl
- sft_examples.jsonl
- synthetic_examples.jsonl
- preference_pairs.jsonl
- dataset_manifest.json
- quality_report.json and QUALITY_REPORT.md
- PREFERENCE_REVIEW_PACKET.md
- HANDOFF.md

If write_parquet=true, install the data extra and matching Parquet copies are
created. Nested fields are serialized as JSON strings in the Parquet view.

## Synthetic-data boundary

Controlled mutations currently cover:

- time shifts;
- unit or scale errors;
- context deletion;
- partial-to-full overclaiming;
- opinion-as-fact;
- prediction-as-fact;
- source laundering.

Each child inherits the parent split. Eval parents never create train children.
Synthetic records never enter the human-gold namespace.

## Preference pilot boundary

Generated preference pairs start with review_status=pending. A human must record
accept, edit, or reject plus a reason. Thirty pending pairs are evidence of a
review packet, not completed RLHF, DPO training, reward modeling, or
inter-annotator agreement.

Validate a pending packet without claiming completion:

    PYTHONPATH=src python -m video_truthfulness.cli validate-preference-reviews \
      --preference-jsonl path/to/preference_pairs.jsonl

After every record has a real human decision, add --require-all-reviewed. The
validator fails reviewed rows that omit a decision, reason, timestamp, or the
edited final answer required by an edit decision.

## Reproducibility

The manifest records input and split SHA-256 values, dataset/schema/pipeline
versions, gate counts, seed, duplicate settings, and output counts. Stable IDs,
hashes, split inheritance, and gate decisions are deterministic for the same
input and configuration. generated_at remains run metadata.
