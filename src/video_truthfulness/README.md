# video_truthfulness

Python package split into version-neutral core and explicit version surfaces.

Current layout:

- `core/`: shared schemas, Agent/RAG, provider abstractions, evidence logic,
  process primitives, reporting, safe filenames, and the append-only Artifact Registry/logical
  DAG control surface; it never imports a version package.
- `core/execution/`: shared Stage 4 Session, append-only event, rebuildable state, immutable
  checkpoint, machine HANDOFF, deterministic Markdown, isolated-recovery validation, and the
  Stage 5 single-writer model telemetry ledger; it is a contract/verification surface, not a
  scheduler or DAG executor.
- `versions/v01/`: frozen Bilibili compatibility paths, title-style legacy run IDs,
  offline Demo1, evaluation fixture binding, training smoke, and v0.1.1 quality gates.
- `versions/v02/`: active YouTube identity/platform boundary plus the Stage 5 strict business
  payloads, observation collector, and model telemetry; GDB1 adds parent/atomic Claim and product
  taxonomy models, deterministic warehouse export, immutable projection facts, and a separately
  gated Parquet/DuckDB Loader; it never imports V01 and never auto-advances the DAG.
- `cli.py`: explicit `v01-*` compatibility routing with frozen writes disabled by default.

The package must keep platform access, browser automation, and real downloads behind explicit adapters so failures are isolated.
The machine-specific OCR freeze and S01 runner/finalizer remain local-only. Importing or validating
the public GDB1 warehouse modules does not start S01, open real media, run the Loader, or authorize
S02. The run Registry/export package remains authoritative; Parquet, DuckDB and SQLite are
rebuildable projections.
