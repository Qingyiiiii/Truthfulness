# video_truthfulness

Python package split into version-neutral core and explicit version surfaces.

Current layout:

- `core/`: shared schemas, Agent/RAG, provider abstractions, evidence logic,
  process primitives, reporting, safe filenames, and the append-only Artifact Registry/logical
  DAG control surface; it never imports a version package.
- `versions/v01/`: frozen Bilibili compatibility paths, title-style legacy run IDs,
  offline Demo1, evaluation fixture binding, training smoke, and v0.1.1 quality gates.
- `versions/v02/`: active YouTube identity and platform-policy boundary; it never imports V01.
- `cli.py`: explicit `v01-*` compatibility routing with frozen writes disabled by default.

The package must keep platform access, browser automation, and real downloads behind explicit adapters so failures are isolated.
