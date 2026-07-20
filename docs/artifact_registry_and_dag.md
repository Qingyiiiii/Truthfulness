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

Registry v1.2 is an additive external-storage successor. Repository records
continue to use `storage_root_ref=repository`; Claim warehouse export records
use `storage_root_ref=ubuntu_v02_claim_warehouse` plus one POSIX relative path.
The wire contract rejects unknown roots, absolute/Windows/UNC paths,
backslashes, `.`/`..` and storage-root-changing metadata revisions. The
environment-aware storage resolver/Loader additionally rejects symlink and
mount escape before any file open. v1.0 and v1.1 wire bytes and record hashes
remain unchanged, while the SQLite projection preserves the canonical
root/path pair and is still disposable.

## Identity and revision rules

Every JSONL line is one complete versioned wire snapshot. Readers support historical `artifact_record_v1.0.0` and canonical `artifact_record_v1.1.0` records in the same Registry. Runtime validation selects the wire model from `registry_schema_version`, verifies the wire hash and revision chain first, and only then creates a version-neutral consumer view. Runtime validation enforces the JSON Schema plus these history rules:

- one `artifact_id` keeps one immutable content identity;
- content, identity, scope, or storage-location changes require a new `artifact_id`;
- a new content Artifact may point backward with `supersedes`;
- metadata-only changes append a contiguous revision with `previous_record_id` and `previous_record_hash`;
- existing JSONL bytes are never rewritten by Registry APIs;
- every dependency, validation reference, entity-container reference, and superseded Artifact must exist in Registry history;
- `supersedes` is strictly backward-looking: self-reference and a reference to an Artifact appearing later in the candidate batch are rejected;
- run-scoped records require the exact Registry `run_id`; cross-run records require a batch, dataset-build, dataset-version, or experiment identity;
- `relative_path` is always a repository-relative POSIX path, never an absolute machine path.

The v1.0 wire format remains immutable. Its compatibility mapping is explicit: `release_version` maps to `release_id`, `experiment_<ulid>` maps to the deterministic alias `exp_<same-ulid>`, and `agent_version` maps only to `agent_runtime_version`; `agent_profile_version` remains null. New records default to v1.1 and reject all three legacy field names. Metadata revisions may upgrade v1.0 to v1.1 while preserving canonical content identity and the previous wire hash; v1.1-to-v1.0 downgrade is forbidden.

The record model rejects credential-bearing keys and credential-like strings. Cookies, tokens, browser sessions, account material, and private URLs do not belong in Registry records or public examples.

## Hashes and staleness

`content_hash` is the SHA-256 of raw file bytes. Directory Artifacts hash a canonical manifest sorted by POSIX relative path and containing every member's size and SHA-256. Text and JSON-like data may also carry a semantic hash, but it never replaces the raw-byte hash.

`input_fingerprint` includes the sorted upstream content and entity hashes together with applicable Agent, Workflow, Schema, Prompt, DAG, code, tool, parameter, and configuration versions. A fingerprint mismatch marks a downstream candidate stale; it does not delete or overwrite historical content. Dependency propagation is forward-only and uses entity references when available, falling back to container-level dependencies otherwise.

## Registration manifest

Writes require an explicit local manifest with complete, already-hashed records:

```json
{
  "records": [
    {"registry_schema_version": "artifact_record_v1.1.0", "release_id": "truthfulness_v0.2_youtube_video", "...": "complete record fields"}
  ]
}
```

`register` accepts only revision 1 records. `append-revision` accepts only revisions greater than 1. Both validate the entire resulting history before appending. The manifest is an input package, not an authority source, and real manifests remain private.

Library callers that must preflight a successor use
`AppendOnlyRegistry.validate_full_history(candidate_records=...)`. It validates stored wire hashes,
revision chains, scope and every upstream, validation, entity-container and supersedes reference
against the proposed append order, while creating or changing no Registry bytes. `append_many()`
uses this same validation path before opening the Registry for append. The older `validate()` summary
remains available for compatibility, but it is not a substitute for full reference validation.

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

When a cross-run Registry exists, add a second `--registry` and `--scope cross_run`. Put the run Registry first so its single `--run-id` aligns with the first input. The projection stores the original wire JSON and exposes canonical `release_id`, `exp_id`, Agent Profile/runtime, and source-schema columns. Deleting or manually changing SQLite never changes JSONL; rebuilding discards projection-only changes.

## Logical DAG

`configs/workflows/youtube_truthfulness_dag_v1.yaml` preserves the historical v1.0 declaration.
`configs/workflows/youtube_truthfulness_dag_v1_1.yaml` preserves the Stage 4 v1.1 control-plane
generation. Both require one uniform Workflow version for all 56 nodes and retain their original
behavior.

`configs/workflows/youtube_truthfulness_dag_v1_2.yaml` is the compatible Stage 5 declaration. It
keeps the same 56 nodes, 60 edges, retry budgets and S01–S09 topology while adding the explicit
`workflow_versions` stage map:

- S01 uses `youtube_truthfulness_workflow_v1.1.0`;
- S02 uses `youtube_truthfulness_workflow_v1.3.0`;
- S03–S09 remain on `youtube_truthfulness_workflow_v1.1.0`.

The top-level v1.2 `workflow_version` is the active v1.3 generation marker; consumers must use
`workflow_version_for_stage(stage_id)` when validating a stage-scoped Session. Only S02 nodes may
carry v1.3 refs. The edge from S02 `screening_sync` to S03 `screening_pool_update` is a routing
target, not execution authority: the Stage 5 HANDOFF must keep S03 `execution_authorized=false`,
and Stage 5 does not create an S03 Session.

The v1.2 OCR declaration always materializes `ocr.gate_decision` from `optional_ocr` and requires
that Artifact before transcript normalization/alignment. `ocr.result` remains optional at the DAG
type level and is materialized only when the validated gate payload has `gate_state=EXECUTED`.
`NOT_APPLICABLE` and `REQUIRED_BLOCKED` never fabricate an OCR result. Gate-payload semantics are
enforced by the Stage 5 business/control validators; the rebuildable DAG state reports Artifact
materialization only.

Each declaration uses the dependency-free JSON subset of YAML 1.2 and validates against its
matching versioned Schema. A DAG declaration remains control data: loading or deriving state never
executes the nine-stage business workflow.

The GDB1 `youtube_truthfulness_dag_v1_4.yaml` successor keeps the prior S01
order and appends `warehouse_export` after `source_depth_decision`: 13 S01
nodes, 17 required Artifact types, with `ocr.result` still conditional.
`warehouse_export` has no network/model/media permission and may read only the
validated current-S01 business objects. It creates one immutable
`warehouse.export_batch`; the S01 finalizer records projection status
`pending`, but never opens Parquet or DuckDB. Loading is a separate Session and
is not authorized by the DAG declaration.

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
python -B -m video_truthfulness.core.artifacts dag validate --dag configs/workflows/youtube_truthfulness_dag_v1_2.yaml
python -B -m video_truthfulness.core.artifacts dag status --dag configs/workflows/youtube_truthfulness_dag_v1.yaml --registry <run-registry.jsonl> --run-id <run_id> --output <dag_state.json>
python -B -m video_truthfulness.core.artifacts dag explain --state <dag_state.json> --node <node_id>
```

## Execution HANDOFF integration

The Stage 4 execution contract registers an immutable machine HANDOFF as a Registry v1.1 Artifact:

- use `handoff.run` with the exact `run_id` for run scope;
- use `handoff.project` with an explicit batch, dataset-build, dataset-version, or experiment
  identity for project/cross-run scope;
- set `authority_level` to `machine_derived`, `lifecycle_state` to `frozen`, and
  `validation_status` to `passed`;
- bind exact `handoff.json` bytes with `content_hash`, HANDOFF semantics with `semantic_hash`, and
  the Registry wire line with `record_hash`.

The HANDOFF records the Registry heads that existed before its own registration, so it cannot cite
itself. Registration appends a new Registry record, and the later `handoff.created` event binds that
record without rewriting the HANDOFF source heads. The final rebuildable state sees both the new
record and the final event.

The DAG remains declarative during recovery. A candidate node in `current_state.json` is a derived
projection, not permission to run it. Recovery validates the HANDOFF's one explicit action and
never schedules or executes a node. See
[HANDOFF, events, checkpoints, and isolated recovery](handoff_events_checkpoints.md) for authority,
publication order, hash domains, and the exact Stage 4 nine-file recovery boundary.

## Public verification

Run the phase-specific suite without bytecode or pytest cache writes:

```powershell
python -B -m pytest tests/shared/artifacts -q -p no:cacheprovider
```

The synthetic fixtures include a valid run identity, a valid record whose file size and hash match that identity, and an intentionally invalid absolute-path record. They contain no real run identity or private source data.
