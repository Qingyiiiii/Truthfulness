# Artifact Registry and logical DAG

The v0.2 YouTube path uses an append-only Artifact Registry for runtime provenance and a declaration-only DAG for workflow visibility. This layer records facts that already exist; it does not download media, run workflow nodes, schedule workers, or create planned outputs.

## Authority boundary

| Layer | Default path | Authority |
|---|---|---|
| Run Registry | `runs/V02/<physical-run-directory>/artifact_registry.jsonl` | Authoritative for one run |
| Cross-run Registry | `registry/V02/artifacts.jsonl` | Authoritative for real batch, dataset, experiment, and evaluation Artifacts |
| SQLite projection | `runtime/V02/artifact_registry.sqlite3` | Non-authoritative and fully rebuildable |
| DAG state | `runs/V02/<physical-run-directory>/dag_state.json` | Non-authoritative and fully rebuildable |
| Schemas, DAG declaration, code | `schemas/`, `configs/`, `src/` | Git-managed control plane; not registered as runtime Artifacts |

An absent cross-run Registry is valid when no real cross-run Artifact exists. JSONL Registries never register themselves, SQLite, or a DAG-state projection.

Real runtime data is private by default and ignored by Git. Only invented fixtures under `examples/artifact_registry/synthetic_run/` are public.

## Identity and revision rules

Every JSONL line is one complete `artifact_record_v1.0.0` metadata snapshot. Runtime validation enforces the JSON Schema plus these history rules:

- one `artifact_id` keeps one immutable content identity;
- content, identity, scope, or storage-location changes require a new `artifact_id`;
- a new content Artifact may point backward with `supersedes`;
- metadata-only changes append a contiguous revision with `previous_record_id` and `previous_record_hash`;
- existing JSONL bytes are never rewritten by Registry APIs;
- every dependency, validation reference, entity-container reference, and superseded Artifact must exist in Registry history;
- run-scoped records require the exact Registry `run_id`; cross-run records require a batch, dataset-build, dataset-version, or experiment identity;
- `relative_path` is always a repository-relative POSIX path, never an absolute machine path.

The record model rejects credential-bearing keys and credential-like strings. Cookies, tokens, browser sessions, account material, and private URLs do not belong in Registry records or public examples.

## Hashes and staleness

`content_hash` is the SHA-256 of raw file bytes. Directory Artifacts hash a canonical manifest sorted by POSIX relative path and containing every member's size and SHA-256. Text and JSON-like data may also carry a semantic hash, but it never replaces the raw-byte hash.

`input_fingerprint` includes the sorted upstream content and entity hashes together with applicable Agent, Workflow, Schema, Prompt, DAG, code, tool, parameter, and configuration versions. A fingerprint mismatch marks a downstream candidate stale; it does not delete or overwrite historical content. Dependency propagation is forward-only and uses entity references when available, falling back to container-level dependencies otherwise.

## Registration manifest

Writes require an explicit local manifest with complete, already-hashed records:

```json
{
  "records": [
    {"registry_schema_version": "artifact_record_v1.0.0", "...": "complete record fields"}
  ]
}
```

`register` accepts only revision 1 records. `append-revision` accepts only revisions greater than 1. Both validate the entire resulting history before appending. The manifest is an input package, not an authority source, and real manifests remain private.

## Command interface

From a source checkout on PowerShell:

```powershell
$env:PYTHONPATH = "src"
python -B -m video_truthfulness.core.artifacts registry validate --registry <run-registry.jsonl> --scope run --run-id <run_id>
python -B -m video_truthfulness.core.artifacts registry list --registry <run-registry.jsonl> --scope run --run-id <run_id>
python -B -m video_truthfulness.core.artifacts registry show <artifact_id> --registry <run-registry.jsonl> --scope run --run-id <run_id>
python -B -m video_truthfulness.core.artifacts registry stale --registry <run-registry.jsonl> --scope run --run-id <run_id>
python -B -m video_truthfulness.core.artifacts registry register --manifest <manifest.json> --registry <run-registry.jsonl> --scope run --run-id <run_id>
python -B -m video_truthfulness.core.artifacts registry append-revision --manifest <manifest.json> --registry <run-registry.jsonl> --scope run --run-id <run_id>
```

Rebuild the disposable SQLite projection from one or more authoritative Registries:

```powershell
python -B -m video_truthfulness.core.artifacts registry rebuild-index --registry <run-registry.jsonl> --scope run --run-id <run_id> --output runtime/V02/artifact_registry.sqlite3
python -B -m video_truthfulness.core.artifacts registry query-index <artifact_id> --index runtime/V02/artifact_registry.sqlite3
```

When a cross-run Registry exists, add a second `--registry` and `--scope cross_run`. Put the run Registry first so its single `--run-id` aligns with the first input. Deleting or manually changing SQLite never changes JSONL; rebuilding discards projection-only changes.

## Logical DAG

`configs/workflows/youtube_truthfulness_dag_v1.yaml` declares 56 nodes across S01–S09. It includes acquisition branches, explicit manual gates, bounded retries, wait/stop terminals, and the training-to-evaluation-to-handoff path. The file uses the dependency-free JSON subset of YAML 1.2 and must validate against `schemas/dag/youtube_truthfulness_dag_v1.schema.json`.

The state projection is derived from the DAG and the latest Registry revision for a run:

- `not_materialized`: terminal condition has no real Artifact;
- `blocked`: one or more required input types are absent;
- `ready`: inputs exist but no output was registered;
- `manual_gate`: inputs exist and explicit human action is still required;
- `satisfied`: a non-invalid declared output exists for this exact DAG node;
- `stale`: a declared output is stale;
- `invalid`: a declared output failed validation or is invalid.

Artifacts of the same type from alternate branches do not satisfy each other. `status` and `explain` calculate state only; neither command executes a node.

```powershell
python -B -m video_truthfulness.core.artifacts dag validate --dag configs/workflows/youtube_truthfulness_dag_v1.yaml
python -B -m video_truthfulness.core.artifacts dag status --dag configs/workflows/youtube_truthfulness_dag_v1.yaml --registry <run-registry.jsonl> --run-id <run_id> --output <dag_state.json>
python -B -m video_truthfulness.core.artifacts dag explain --state <dag_state.json> --node <node_id>
```

## Public verification

Run the phase-specific suite without bytecode or pytest cache writes:

```powershell
python -B -m pytest tests/shared/artifacts -q -p no:cacheprovider
```

The synthetic fixtures include a valid run identity, a valid record whose file size and hash match that identity, and an intentionally invalid absolute-path record. They contain no real run identity or private source data.
