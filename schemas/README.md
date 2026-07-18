# schemas

This directory is reserved for exported JSON Schema files and schema documentation.

The source of truth is the Pydantic implementation in:

- src/video_truthfulness/core/schemas.py for cross-version runtime contracts;
- src/video_truthfulness/core/artifacts/models.py for v0.2 Artifact Registry and logical DAG
  runtime invariants, kept in parity with `schemas/artifact_registry/` and `schemas/dag/` by tests;
- src/video_truthfulness/versions/v01/training_data_schemas.py for V01 quality, lineage, SFT,
  synthetic, and preference artifacts.

Training-data schemas are exported by
scripts/versions/v01/export_training_data_schemas.py. Keep generated files derived from code
instead of hand-editing them.

Artifact Registry and DAG Schemas are explicit versioned public contracts. Their runtime history,
path, scope, credential, and graph invariants are stricter than JSON shape alone and are enforced
by the shared Artifact modules.
