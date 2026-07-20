# examples

Public examples live here.

Allowed:

- Synthetic transcripts.
- Authorized sample evidence metadata.
- Tiny JSON files used by tests and docs.
- Fully invented training-data fixtures with fixed splits.
- Fully invented Artifact Registry identity and positive/negative Schema fixtures under
  `artifact_registry/synthetic_run/`.
- A fully invented Stage 4 execution-contract bundle under
  `execution_contract/synthetic_run/`, including Session, event, checkpoint, HANDOFF, state,
  Registry, DAG, bounded Artifact, and invalid-boundary fixtures used for isolated recovery tests.
- Stage 5 synthetic-contract documentation under `versions/v02/stage5_synthetic/`. Payloads are
  generated deterministically inside pytest `tmp_path`; no real run, media, Gemini response, or
  persistent private fixture is copied into this directory.
- GDB1 Claim warehouse examples under `versions/v02/claim_warehouse/`: one tiny invented
  canonical export/receipt chain used only for schema, hash and Loader documentation. It contains
  no real Claim, observed source/run, Registry file, private path, DuckDB database, Parquet file,
  or product-domain taxonomy/row. The historical 501/919/10 scale run is deferred and is neither
  executed nor approximated by this fixture.

Not allowed:

- Real private videos.
- Cookies or account material.
- Downloaded media.
- Screenshots that cannot be publicly redistributed.

Execution-contract examples must use synthetic identities, repository-relative paths, placeholder
content, and no real run/source IDs, private titles, absolute machine paths, credentials, media, or
account material. The JSON files are the machine authority; generated Markdown is only a
deterministic projection and cannot add facts absent from its JSON source.

The Stage 4 isolated-recovery proof is intentionally limited to the published synthetic
`return_to_stage` action. It reads exactly the nine paths declared in that HANDOFF; it does not read
the referenced Workflow/Prompt document, `current_state.json`, `HANDOFF.md`, chat history, or a real
run. The two projections are rebuilt from machine sources. Other HANDOFF action branches are not a
universal recovery claim and fail closed when the Phase 4 verifier cannot obtain an explicit read
set. See [`execution_contract/synthetic_run/README.md`](execution_contract/synthetic_run/README.md)
for the exact file list and validation command.
