# Training-data quality public fixture

This directory contains only invented records. No row is copied or lightly
rewritten from a private video, transcript, evidence snapshot, or human
annotation.

Files:

- input.synthetic.jsonl: eight synthetic claim records with traceable labels.
- splits.synthetic.jsonl: fixed source-level train/dev/test assignments.
- PUBLIC_DEMO_SUMMARY.md: aggregate-only reproducibility result for the fixture.

Run from the repository root:

    python -m video_truthfulness.cli v01-training-data-pack \
      --config configs/versions/v01/training_data_quality.example.toml \
      --allow-frozen-v01-write

Generated artifacts are written to the ignored tmp directory. The demo covers
quality gates, MinHash/LSH candidate generation, task-specific admission,
controlled hard negatives, claim-triage SFT output, and pending preference
pairs. It does not train a model or complete RLHF.
