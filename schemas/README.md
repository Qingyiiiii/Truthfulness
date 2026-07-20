# schemas

This directory is reserved for exported JSON Schema files and schema documentation.

The source of truth is the Pydantic implementation in:

- src/video_truthfulness/core/schemas.py for cross-version runtime contracts;
- src/video_truthfulness/core/artifacts/models.py for v0.2 Artifact Registry and logical DAG
  runtime invariants, kept in parity with `schemas/artifact_registry/` and `schemas/dag/` by tests;
- src/video_truthfulness/core/execution/ for the Stage 4 Session, event, checkpoint, HANDOFF,
  and rebuildable-state runtime invariants, kept in parity with `schemas/execution/` by tests;
- src/video_truthfulness/versions/v01/training_data_schemas.py for V01 quality, lineage, SFT,
  synthetic, and preference artifacts.

Training-data schemas are exported by
scripts/versions/v01/export_training_data_schemas.py. Keep generated files derived from code
instead of hand-editing them.

Artifact Registry and DAG Schemas are explicit versioned public contracts. Their runtime history,
path, scope, credential, and graph invariants are stricter than JSON shape alone and are enforced
by the shared Artifact modules.

Registry `v1.0` is the immutable legacy wire contract; Registry `v1.1` uses canonical release,
experiment, Agent Profile, and Agent runtime fields. DAG `v1.0` and `v1.1` remain separate
immutable single-workflow contracts. Stage 5 adds DAG `v1.2`, whose stage-scoped references
explicitly permit the reviewed `S01 v1.1 -> S02 v1.3 -> S03 v1.1` route without weakening the
older pair rules.

Stage 4 execution contracts and their approved pre-Stage-5 compatibility additions are published
under `schemas/execution/`:

- `session_manifest_v1.schema.json`: immutable Session identity, versions, and declared scope;
- `execution_event_v1.schema.json`: frozen Stage 4 `execution_event_v1.0.0` envelope;
- `execution_event_v1_0_1.schema.json`: compatible writer contract that rejects
  `artifact.read`/`artifact.written` only when both observed-reference arrays are empty;
- `input_materialization_v1.schema.json`: non-authoritative external cache receipt bound to one
  validated source Artifact, immutable Registry snapshot, and content-equivalent relative target;
- `execution_checkpoint_v1.schema.json`: immutable recovery boundary and source-head references;
- `handoff_v2.schema.json`: authoritative machine HANDOFF with Artifact, checkpoint, scope, and
  single-next-action references;
- `current_state_v1.schema.json`: deterministic, disposable state projection rebuilt from
  authoritative inputs.

The machine JSON objects are authoritative. `HANDOFF.md` is a deterministic human-readable
projection of `handoff.json` and must not introduce independent machine facts.

Stage 5 successor contracts are additive and live beside the frozen versions:

- `versions/v02/v02_business_artifact_v1.schema.json`: strict discriminated V02 business payloads;
- `execution/stage5_execution_plan_v1.schema.json`,
  `execution/stage5_execution_plan_v1_1.schema.json`, and `stage5_observation_v1.schema.json`:
  frozen v1.0 single-node plans, receipt-bound/control-finalization v1.1 plans, and non-model observations;
- `execution/input_materialization_v1_1.schema.json`: prefix-bound receipt successor;
- `execution/session_manifest_v1_1.schema.json`, `execution_checkpoint_v1_1.schema.json`,
  `current_state_v1_1.schema.json`, and `handoff_v2_1.schema.json`: cross-version recovery
  successors;
- `execution/manual_external_input_receipt_v1.schema.json`: one-file G2 capture receipt;
- `execution/model_call_event_v1.schema.json` and `model_usage_summary_v1.schema.json`:
  append-only model-call evidence and deterministic per-Session summaries;
- `dag/youtube_truthfulness_dag_v1_2.schema.json`: stage-scoped Stage 5 DAG contract.

JSON shape is not sufficient by itself. Runtime validators enforce exact path scopes, no-clobber,
reference history, single-writer telemetry, Gate transitions, and legacy compatibility.

## GDB1 Claim warehouse and S01 v1.3 successors

GDB1 adds public schemas while leaving every real run and warehouse file private:

- `artifact_registry/artifact_record_v1_2.schema.json`: external-storage
  records using a frozen `storage_root_ref` plus a safe POSIX relative path;
- `versions/v02/v02_claim_taxonomy_v1.schema.json` and
  `v02_business_artifact_v1_2.schema.json`: parent/atomic Claim, evidence,
  verdict, Gold, product/SKU and `warehouse.export_batch` contracts;
- `warehouse/`: logical warehouse DDL plus immutable export, load-plan,
  load-batch, projection-attempt and receipt contracts;
- `dag/youtube_truthfulness_dag_v1_4.schema.json`: the 13-node S01 route
  whose final local node is `warehouse_export`;
- `execution/stage5_s01_execution_plan_v1_3.schema.json`,
  `session_manifest_v1_3.schema.json`, `current_state_v1_3.schema.json`,
  `execution_checkpoint_v1_3.schema.json`, `handoff_v2_3.schema.json`, and
  both fresh-recovery v1.1 schemas: the immutable export identity and
  projection-pending boundary.

Registry v1.0/v1.1 and all earlier execution schemas remain readable and
unchanged. JSON shape alone does not authorize a real S01, warehouse Loader,
S02, media read or network operation.

The public behavior contract, conflict priority, publication order, hash domains, resource limits,
and exact synthetic recovery boundary are documented in
[`docs/handoff_events_checkpoints.md`](../docs/handoff_events_checkpoints.md). Runtime validation
and parity tests enforce constraints that JSON shape alone cannot express; this index does not
duplicate or weaken those invariants.
