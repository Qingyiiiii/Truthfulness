# schemas

This directory is reserved for exported JSON Schema files and schema documentation.

The source of truth is the Pydantic implementation in:

- src/video_truthfulness/core/schemas.py for cross-version runtime contracts;
- src/video_truthfulness/versions/v01/training_data_schemas.py for V01 quality, lineage, SFT,
  synthetic, and preference artifacts.

Training-data schemas are exported by
scripts/versions/v01/export_training_data_schemas.py. Keep generated files derived from code
instead of hand-editing them.
