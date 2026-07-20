# HANDOFF, events, checkpoints, and isolated recovery

The Stage 4 execution layer is a small, local contract for recording what one Session did and
what a later Session may read to continue. It is not a scheduler and does not execute DAG nodes.
Stage 4 is limited to the public synthetic bundle; real YouTube run integration belongs to Stage 5.

The public Schemas are under [`schemas/execution/`](../schemas/execution/), the shared runtime is
under [`src/video_truthfulness/core/execution/`](../src/video_truthfulness/core/execution/), and the
complete invented example is under
[`examples/execution_contract/synthetic_run/`](../examples/execution_contract/synthetic_run/).

## Frozen contract versions

| Object | Stage 4 version | Role |
|---|---|---|
| Session manifest | `session_manifest_v1.0.0` | Immutable Session identity, versions, and declared scope |
| Execution event | `execution_event_v1.0.0` | Typed append-only event envelope and hash chain |
| Checkpoint | `execution_checkpoint_v1.0.0` | Immutable recovery boundary |
| HANDOFF | `handoff_v2.0.0` | Immutable machine-readable transfer contract |
| Current state | `current_state_v1.0.0` | Disposable projection rebuilt from authoritative inputs |
| Artifact Registry | `artifact_record_v1.1.0` | Canonical Artifact wire record; v1.0 remains readable |
| Workflow | `youtube_truthfulness_workflow_v1.1.0` | Workflow contract bound to HANDOFF v2 |
| DAG | `youtube_truthfulness_dag_v1.1.0` | Logical dependency declaration bound to Workflow v1.1 |
| Markdown renderer | `handoff_markdown_renderer_v1.0.0` | Deterministic human projection profile |

Registry v1.0 compatibility is read-only. New execution records use Registry v1.1, and the v1.0
wire bytes are never rewritten. Workflow and DAG v1.1 preserve the S01-S09 business topology;
the version change binds the shared execution contract and does not add a scheduler.

### Stage 5 compatible successors

The Stage 4 files and runtime paths above remain readable and testable. Stage 5 adds separate,
strict successors rather than rewriting them:

| Object | Successor | Additional invariant |
|---|---|---|
| Session manifest | `session_manifest_v1.1.0` | Stage-level Session has `dag_node_id=null`; DAG v1.2 source Workflow is resolved by `stage_id` |
| Checkpoint | `execution_checkpoint_v1.1.0` | Binds the task-scoped immutable DAG v1.2 snapshot and the source stage Workflow |
| Current state | `current_state_v1.1.0` | Rebuild uses the source stage Workflow, not the DAG's top-level active generation |
| HANDOFF | `handoff_v2.1.0` | Carries an explicit target Workflow and execution authorization for adjacent transitions |
| Input materialization | `input_materialization_v1.1.0` | Binds an immutable Registry prefix while allowing validated append-only growth |

DAG v1.2 permits only the declared stage mapping: S01 and S03-S09 use Workflow v1.1, while S02
uses Workflow v1.3. HANDOFF v2.1 permits only two forward transitions: S01 v1.1 to S02 v1.3 with
`execution_authorized=true`, and S02 v1.3 to S03 v1.1 with
`execution_authorized=false`. The second transition is routing evidence only and cannot create or
start an S03 Session. Undeclared combinations fail Schema/runtime validation.

### Post-seal Event compatibility patch

Stage 4 remains frozen on `execution_event_v1.0.0`. Its Schema, nine-event synthetic stream,
checkpoint, state, HANDOFF, and hashes are not migrated or recalculated. Before the Stage 5 pilot,
`execution_event_v1.0.1` was published as a new compatible writer contract in
`schemas/execution/execution_event_v1_0_1.schema.json`.

One Session manifest must declare exactly one supported Execution Event version, every event in
that Session must use it, and `EventLog.append()` derives its writer version from the manifest.
Existing unfinished `v1.0.0` Sessions therefore remain writable as `v1.0.0`; new Stage 5 Sessions
must declare `execution_event_v1.0.1`.

The only wire-rule change in `v1.0.1` is for `artifact.read` and `artifact.written`: the event must
contain at least one `artifact_refs` entry or at least one `path_refs` entry. Each path reference
already carries a repository-relative path and SHA-256, so path-only observed access remains valid;
Artifact-only and combined references also remain valid. Only the ambiguous state in which both
arrays are empty is rejected. Other event types may still use two empty arrays when their typed
payload does not describe an observed Artifact access.

## Authority and conflict resolution

| Object | Authoritative facts | Mutation rule |
|---|---|---|
| `session_manifest.json` | Session identity, fixed versions, declared read/write scope | Create once |
| `events.jsonl` | Actions that were recorded as having occurred | Append only; freeze after `handoff.created` |
| Artifact Registry JSONL | Artifact identity, exact content, provenance, and lifecycle | Append complete wire revisions only |
| Versioned DAG config | Logical nodes, dependencies, and gates | Immutable by version |
| `current_state.json` | Projection of manifest, event, Registry, and DAG sources | Replaceable and rebuildable |
| `checkpoints/<checkpoint_id>.json` | Fixed event, Registry, state, version, and hash boundary | Create once |
| `handoff.json` | Current Session's machine transfer receipt | Create once |
| `HANDOFF.md` | Human-readable rendering of `handoff.json` | Rebuildable; no independent facts |

Conflicts fail closed and are resolved in this order:

1. Each object's Schema, hash, chain, scope, and immutability rules determine whether it is valid.
2. Artifact content and lifecycle use the latest legal Registry revision.
3. Task facts use the valid event stream.
4. A checkpoint describes only its fixed source heads; it does not override later valid events.
5. A conflicting `current_state.json` is discarded and rebuilt.
6. A conflicting `HANDOFF.md` is discarded and rendered again from `handoff.json`.
7. Chat text, manual summaries, and unpersisted prompts never override valid machine facts.

Git commits, execution checkpoints, and assistant memory are different coordinate systems. A Git
commit identifies source state, while `checkpoint_<ulid>` identifies a task recovery boundary.
Neither value may be substituted for the other.

## Planned private control layout

S01 and S02 are run-scoped. S03-S09 are cross-run scoped. Their private task roots are:

```text
# S01/S02
runs/V02/<physical_directory>/control/tasks/<task_id>/

# S03-S09
runtime/V02/execution/tasks/<task_id>/
```

Both roots use the same internal shape:

```text
<task_root>/
  sessions/
    <session_id>/
      session_manifest.json
      events.jsonl
      current_state.json
      handoff.json
      HANDOFF.md
  checkpoints/
    <checkpoint_id>.json
```

Business Artifacts stay at Workflow-defined paths and are referenced by relative path, Artifact ID,
Registry record, and hash. The control directory does not copy media, datasets, or long logs. There
is no `latest` file, symlink, or implicit newest-Session lookup.

This layout is a contract for later private integration. Stage 4 writes only the public synthetic
fixture and temporary isolated-test directories; it does not write these files into a real run.

## External input cache materialization

An execution environment may need a short native path for bytes whose authoritative Artifact lives
under a different storage root. `input_materialization_v1.0.0` records that physical cache without
creating another business Artifact or changing the source Registry. Its public Schema is
`schemas/execution/input_materialization_v1.schema.json`.

The receipt binds the source run, Artifact ID, record ID/hash, validated content hash/size, complete
Registry file hash/count/HEAD, and a logical `storage_root_ref` plus relative cache path. It also
records equal source stat snapshots, no-clobber copy evidence, the target Unix ownership/mode, and
its own semantic hash. The absolute storage root is an environment parameter and is never persisted
in the receipt.

Validation is read-only and fails closed on a non-canonical receipt, changed Registry, substituted
record, non-passed source, stale lifecycle claim, target symlink/junction, wrong target ownership or
mode, or content mismatch:

```text
python -B -m video_truthfulness.core.execution materialization validate \
  --receipt <repo-relative-receipt> \
  --repository-root <repository-root> \
  --storage-root <environment-resolved-storage-root>
```

A valid receipt has `authority_level=cache`; downstream provenance and input fingerprints continue
to reference the original Artifact and content hash. A later Session may bind the receipt as a
bootstrap document, but the receipt itself does not start a Session, satisfy a business DAG node,
or authorize Stage 5 execution.

`input_materialization_v1.1.0` replaces the complete-file Registry binding with an immutable
prefix hash, byte length, record count, and prefix head. Validation still resolves the exact
revision-1 `media.video` source record and cache bytes, rejects a changed/truncated/reordered
prefix, a later source revision, or a second run-local `media.video`, but accepts unrelated legal
records appended after the bound prefix. The v1.0 receipt and validator behavior remain unchanged.

## Session and event binding

The Session manifest is immutable canonical JSON. Its two hashes deliberately bind different
domains:

- `manifest_hash` is the semantic SHA-256 of canonical JSON with the `manifest_hash` field omitted;
- `session.started.payload.manifest_hash` binds that semantic hash;
- `session.started.path_refs[0].content_hash` binds the exact published file bytes: canonical JSON
  including `manifest_hash`, followed by one LF.

The semantic hash and file hash normally differ and must not be compared as if they were the same
value.

Every event has a typed payload, contiguous sequence number, unique ID, previous event ID/hash, and
its own `event_hash`. The first event is `session.started`. Exactly one task terminal event precedes
checkpoint publication. Only `checkpoint.created` and then `handoff.created` may follow that task
terminal event. After `handoff.created`, the Session event stream is frozen; continuation requires a
new Session.

Declared scope comes from the Session manifest. Actual read/write sets come only from validated
execution events. The runtime rejects unsafe relative paths, broad recursive roots, path traversal,
implicit `latest`, private absolute paths, credential material, and sensitive value markers.

## State and checkpoint boundaries

`current_state.json` is a deterministic convenience view. Its sources are the immutable Session
manifest, a validated event prefix, bounded Registry snapshots, and the matching versioned DAG. A
state hash mismatch or source conflict is repaired by deleting and rebuilding the projection, never
by editing source history.

Checkpoint creation avoids a circular reference:

1. Append the single task terminal event.
2. Rebuild and validate terminal state.
3. Create an immutable checkpoint whose event head is that terminal event.
4. Publish and revalidate the checkpoint.
5. Append `checkpoint.created`, which binds the checkpoint semantic hash and exact file bytes.

The checkpoint event head intentionally excludes the event that announces the checkpoint. The
checkpoint's embedded `checkpoint_hash` binds its semantic object; the event path reference binds
the exact canonical file bytes including the final LF.

## HANDOFF publication order

The full receipt is published in this order:

1. Write, validate, and register the business output.
2. Append the single task terminal event.
3. Rebuild and validate terminal state.
4. Create and validate the immutable checkpoint.
5. Append `checkpoint.created`.
6. Preallocate the HANDOFF Artifact ID.
7. Build, validate, and immutably publish `handoff.json`.
8. Register it as a Registry v1.1 `handoff.run` or `handoff.project` Artifact.
9. Append `handoff.created` with the Handoff Artifact and Registry receipt.
10. Render `HANDOFF.md` from the machine JSON.
11. Rebuild final `current_state.json` from the now-frozen event stream.

No source head is backfilled. The HANDOFF source event head is the `checkpoint.created` event, and
its Registry source heads are the prefixes before the HANDOFF Artifact is registered. The final
Registry adds the HANDOFF record; the final event stream adds `handoff.created`; final state sees
both. If publication fails after an immutable append, the valid evidence already written is
preserved rather than rewritten.

### Four non-interchangeable hash domains

| Domain | Example field | What it binds |
|---|---|---|
| Semantic object | `handoff_hash`, `checkpoint_hash`, `manifest_hash`, `state_hash` | Canonical object with its embedded hash field omitted |
| Exact file bytes | Artifact/path `content_hash` | Canonical published bytes, normally including the embedded hash and final LF |
| Registry wire record | `record_hash` | One complete Registry JSONL wire record with `record_hash` omitted for calculation |
| Event envelope | `event_hash` | One complete event envelope with `event_hash` omitted for calculation |

For the HANDOFF receipt, Registry `content_hash` equals the exact `handoff.json` file hash,
Registry `semantic_hash` equals `handoff_hash`, and Registry `record_hash` binds the wire record.
The `handoff.created` payload carries the semantic and record hashes, its path/Artifact references
carry the file hash, and its own `event_hash` closes the event-chain receipt. A Registry snapshot
also carries a bounded prefix-byte hash; that snapshot hash is not an Artifact semantic hash.

### Registry v1.1 HANDOFF record

A HANDOFF is registered with:

- `artifact_type = handoff.run` for run scope, or `handoff.project` for project/cross-run scope;
- `authority_level = machine_derived`;
- `lifecycle_state = frozen` and `validation_status = passed`;
- `content_hash` for exact file bytes and `semantic_hash` for `handoff_hash`;
- a run identity for run scope, or an explicit batch, dataset, or experiment identity for
  cross-run scope.

The HANDOFF's `source_registry_heads` cannot include its own record. The later
`handoff.created` event is the non-circular receipt for that registration.

## Exact Stage 4 recovery contract

The public synthetic HANDOFF exposes one `return_to_stage` continuation. Its
`required_read_paths` contains exactly these nine repository-relative files:

```text
examples/execution_contract/synthetic_run/artifact_registry.jsonl
examples/execution_contract/synthetic_run/artifacts/input.json
examples/execution_contract/synthetic_run/artifacts/output.json
examples/execution_contract/synthetic_run/checkpoints/checkpoint_01j00000000000000000000000.json
examples/execution_contract/synthetic_run/events.jsonl
examples/execution_contract/synthetic_run/handoff.json
examples/execution_contract/synthetic_run/session_manifest.json
examples/execution_contract/synthetic_run/working_tree_manifest.json
examples/execution_contract/synthetic_run/youtube_truthfulness_dag_v1_1.yaml
```

An isolated recovery test copies only those files while preserving the
`examples/execution_contract/synthetic_run` relative prefix. It supplies no chat history, repository
history, Workflow document, Prompt document, `current_state.json`, or `HANDOFF.md`. The verifier
validates the hashes, event chain, checkpoint, Registry prefix and receipt, identities, versions,
scope, DAG binding, and the single next action. It rebuilds state and Markdown from authoritative
machine sources without treating either projection as a recovery input.

From the repository root, validate the public bundle with:

```powershell
$env:PYTHONPATH = "src"
python -B -m video_truthfulness.core.execution recovery validate --bundle examples/execution_contract/synthetic_run
```

Success writes one canonical JSON summary to stdout and exits `0`. A contract failure writes the
error to stderr and exits `2`. The validator must not execute the returned action.

This Stage 4 proof is intentionally narrow. It proves only the published synthetic
`return_to_stage` action with explicit `required_read_paths`; it does not prove universal recovery
for all four HANDOFF `next_action` variants. In particular, `wait_for_human` and `terminate` do not
carry recovery read paths in HANDOFF v2.0 and must fail closed as unsupported recovery inputs. Real
branch coverage and real-run continuation belong to Stage 5.

### Generic exact HANDOFF recovery

Stage 5 adds a second read-only entry without changing the fixed nine-file proof:

```powershell
$env:PYTHONPATH = "src"
python -B -m video_truthfulness.core.execution recovery validate-handoff `
  --handoff <exact-repository-relative-handoff.json> `
  --repository-root <repository-root>
```

This entry never scans for a task, Session, Registry, checkpoint, or newest file. It starts from
the one supplied machine HANDOFF, opens only its sorted `required_read_paths`, and validates the
manifest, complete Event chain, checkpoint, Registry source prefixes plus HANDOFF registration,
task-scoped DAG snapshot, Artifact records and exact file hashes. The declared package must equal
the mechanically derived minimal recovery set. Media, full transcript/chat/log inputs,
`current_state.json`, `HANDOFF.md`, globbing, and implicit `latest` are rejected. Success reports
the exact read set and `write_count=0`; it does not execute the returned action. A dormant
`wait_for_human` HANDOFF remains non-recoverable until the separately authorized capture flow
creates a new exact recovery boundary.

## Evidence, risk, and metric boundaries

The declared read/write scope is a machine contract. The actual read/write set records validated
application events. It is not OS- or kernel-level file monitoring and cannot prove that no hidden
process or dependency read occurred.

`out_of_scope_detection_count` counts persisted `tool.failed` scope-violation events. Attempts
rejected before a valid event is persisted cannot be presented as a complete system-audit count.
HANDOFF `risks` are declared and Schema-validated statements, not independent observations.
Metrics are deterministic counts and hash comparisons from the synthetic evidence, not claims of
performance, reliability, or business capability. Missing historical baselines remain
`unavailable`; Stage 4 does not invent old recovery time, token use, or real-run volume.

## Resource, serialization, and privacy limits

The machine HANDOFF is bounded before model construction and publication:

- canonical JSON size at most 1 MiB (`1,048,576` bytes);
- at most 1,024 items per array and 1,024 members per object;
- maximum nesting depth 32;
- maximum parsed node count 20,000;
- strict rejection of extra fields;
- UTF-8 canonical JSON with sorted keys and one final LF;
- UTC timestamps serialized with `Z`, canonical repository-relative POSIX paths, and lowercase
  64-character SHA-256 values;
- rejection of credential-like fields/values, private absolute paths, and unsafe text containing
  CR, LF, NUL, U+2028, or U+2029 where a single-line HANDOFF field is required.

The public fixture contains only invented identities and tiny placeholder Artifacts. No Cookie,
account session, authorization material, private URL, absolute machine path, or real media belongs
in a public execution contract.

## GDB1 S01 warehouse successor boundary

The v1.3 S01 plan adds one final local publication without changing the older
v1.2 route. Its single total order is:

1. publish and validate the 13 business nodes, ending with the immutable
   `warehouse.export_batch` seven-phase publication;
2. run the existing 12-phase S01 terminal finalizer;
3. stop with checkpoint/HANDOFF v2.3 recording export ID, manifest/logical
   hashes, per-table row counts and `warehouse_projection_status=pending`;
4. leave S02 unauthorized and leave the independent warehouse Loader
   unstarted.

The v1.1 fresh-recovery receipt preserves the exact-five-file boundary:
HANDOFF, checkpoint, final DAG snapshot, run Registry and static manifest.
The child process performs exactly five opens, zero repository writes and zero
dynamic business/export-row reads. It verifies the export only indirectly via
the checkpoint, HANDOFF and Registry record; DuckDB and Parquet are forbidden.

After a separately authorized Loader Session, a successful immutable receipt
may be appended to the cross-run Registry. It never rewrites the terminal S01
Event log, checkpoint or HANDOFF. Projection failure leaves S01 terminal and
the source Artifact valid but keeps the S03 gate at
`WAITING_WAREHOUSE_PROJECTION`.

## Non-goals and phase boundary

Stage 4 does not provide a scheduler, execute DAG nodes, choose the next node, cross a human gate,
or implement automatic retry, fork, rollback, clone, or concurrent writers. It does not provide OS
monitoring, network access, credential use, media download, ASR, OCR, retrieval, annotation,
training, evaluation, or a real YouTube run. It does not modify V01 and does not start Stage 5.

See [Artifact Registry and logical DAG](artifact_registry_and_dag.md) for Artifact history and DAG
state rules, and [file layout and public policy](file_layout.md) for repository privacy boundaries.
