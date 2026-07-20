# docs

Public engineering documentation lives here.

Current documents:

- `artifact_registry_and_dag.md`: v0.2 append-only Artifact authority, hashing,
  rebuildable SQLite, logical DAG state, CLI, and privacy boundaries.
- `handoff_events_checkpoints.md`: Stage 4 Session/event/checkpoint/HANDOFF authority,
  the pre-Stage-5 Event compatibility and input-cache receipt contracts, publication order,
  hash domains, exact isolated recovery scope, Stage 5 additive successors, and phase boundaries.
- `model_telemetry.md`: Stage 5 project-hook, host-receipt, local-model, and manual Gemini
  observability semantics, including unavailable/not-applicable Token handling.
- `claim_warehouse.md`: GDB1 Claim/evidence/product warehouse authority,
  deterministic export, Parquet/DuckDB projection, recovery and synthetic-only
  acceptance boundary.
- `file_layout.md`: repository, Stage 5 control-plane, and run-artifact classification.
- `interfaces.md`: Demo1 module contracts and extension points.
- `versions/v01/training_data_quality.md`: frozen v0.1.1 quality gates, lineage, controlled
  synthetic data, SFT derivation, and preference-review boundaries.

Docs should describe public architecture and boundaries only. Personal learning material stays outside Git.
