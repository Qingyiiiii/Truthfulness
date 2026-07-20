# configs

Configuration templates live here.

Allowed here:

- Public example configuration files.
- Provider names and default local settings.
- Placeholder values that are safe to commit.
- Smoke-test training config examples that use a caller-provided, reviewed gold JSONL path.

Do not commit API keys, cookies, tokens, account identifiers, local absolute secret paths, or private model paths.

`agent-requirements.txt` contains direct public runtime dependencies for the
Docker API/UI image. Version constraints remain aligned with the `agent` and
`ui` extras in `pyproject.toml`.

Frozen V01 examples are under `configs/versions/v01/`:

- `demo1.local.example.toml`: local Demo1 provider defaults.
- `train_baseline.smoke.example.toml`: gold JSONL data-loading and metrics smoke test only; not a formal training configuration. The public repository deliberately does not include a real gold batch.
- `training_data_quality.example.toml`: runs the public synthetic quality-pack
  demo. It generates task gates, controlled hard negatives, claim-triage SFT,
  and pending preference pairs without exposing real annotations.

Stage 5 V02 configuration is versioned and remains inert until the corresponding user Gate:

- `workflows/youtube_truthfulness_dag_v1_2.yaml`: stage-scoped S01/S02/S03 route contract;
- `agents/stage5_v02_agent_topology_v1.toml`: coordinator/helper permissions and call budgets;
- `agents/source_depth_agent_v1_2.toml`: S02 v1.3 manual-Web capture/import profile;
- `prompts/v02/`: frozen helper and Gemini handoff prompts;
- `observability/model_telemetry_v1.toml`: model identity, timing, Token status, privacy, and
  fail-closed settings.

These files do not authorize network access, model downloads, Gemini automation, real-media reads,
or automatic DAG traversal.

The GDB1 database/label successor configuration is also versioned but remains
synthetic-only until a separate S01 authorization:

- `storage/claim_warehouse_v1.toml`: logical Ubuntu storage root, immutable
  export layout and frozen Parquet/DuckDB writer settings;
- `storage/claim_warehouse_requirements.freeze.txt`: the only GDB1 dependency
  freeze; it does not rewrite the earlier Ubuntu environment baseline;
- `versions/v02/claim_taxonomy_v1.toml`: Claim, evidence, Gold and product/SKU
  taxonomy with explicit cardinality and mutual-exclusion rules;
- `prompts/youtube_claim_*`: parent-Claim preservation, atomic splitting and
  bounded repair prompts for long product-recommendation claims;
- `workflows/youtube_truthfulness_dag_v1_4.yaml`: 13-node S01 successor route
  ending in a local-only immutable warehouse export.
- `agents/stage5_v02_agent_topology_v1_1.toml`: preserves the S01 single
  writer and separates the local export adapter from an independently gated
  single-writer warehouse Loader Session.

The environment variable named by the storage config is a private mapping.
Never place its absolute value in Git, Registry records, examples or reports.
These files do not authorize a real S01 run, a warehouse projection, S02, or
access to real media.
