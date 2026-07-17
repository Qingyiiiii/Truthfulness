# schemas

This directory is reserved for exported JSON Schema files and schema documentation.

The source of truth is the Pydantic implementation in:

- src/video_truthfulness/schemas.py for the Demo1 runtime;
- src/video_truthfulness/training_data_schemas.py for quality, lineage, SFT,
  synthetic, and preference artifacts.

Training-data schemas are exported by
scripts/export_training_data_schemas.py. Keep generated files derived from code
instead of hand-editing them.
