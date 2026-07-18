# Synthetic Artifact Registry example

This directory contains public, invented data for validating the v0.2 Artifact Registry boundary. It does not copy a real run, source title, local path, media file, annotation, credential, or Registry identity.

- `run.json` is a schema-valid synthetic YouTube run identity.
- `valid_artifact_record.json` registers that file as `run.identity`; its content hash matches the bytes in this directory.
- `invalid_absolute_path_record.json` is intentionally invalid because `relative_path` contains an absolute Windows path.

The invalid fixture exists only for negative tests. Neither record is an instruction to create a real run or execute a DAG node.
