# tests

Pytest tests for shared code and explicit version boundaries live here.

Current focus:

- Schema validation.
- Core-to-version dependency direction.
- V01 offline/report compatibility and frozen-write boundaries.
- V02 isolation from V01 and Bilibili/Cookie policy.
- Artifact Registry full-history reference validation and DAG v1.2 stage-scoped compatibility.
- Stage 5 business-payload, collector, and model-telemetry positive/negative contracts.
- GDB1 Registry v1.2 compatibility, parent/atomic long-Claim and product/SKU taxonomy,
  deterministic export, Parquet/DuckDB projection, and no-clobber/idempotency/fault recovery.

Tests must not require network access, browser login, platform downloads, private videos, or local model services.
Stage 5 fixtures are generated in pytest `tmp_path`; tests must not read or write a real V02 run,
Registry, media cache, model cache, or Gemini inbox.
GDB1 warehouse tests use invented rows and isolated temporary storage roots. They must not resolve
the private Ubuntu warehouse mapping, inspect a real Registry, read real Claim/media content, or
create a real S01/S02 Session.
Scale acceptance and the machine-specific S01/OCR runner tests remain local-only and are not part
of the public pytest collection.
