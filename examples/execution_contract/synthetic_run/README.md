# Synthetic execution-contract recovery bundle

This directory contains invented IDs, tiny JSON Artifacts, a versioned DAG copy, one complete
Session/Event/Checkpoint/HANDOFF flow, deterministic projections, and isolated negative fixtures.
It contains no real run identity, source identity, media, credentials, private path, or business
output. `handoff.json` is authoritative; `HANDOFF.md` and `current_state.json` are rebuildable
projections.

The positive bundle demonstrates the two deliberate time slices:

- the checkpoint fixes the terminal event before `checkpoint.created`;
- HANDOFF fixes the event head after `checkpoint.created` and the Registry head before the
  HANDOFF Artifact is registered;
- the final event stream and final state include `handoff.created` and the registered HANDOFF.

Files under `invalid/` each violate one named boundary and must fail closed.

## Exact isolated-recovery inputs

The authoritative HANDOFF publishes one `return_to_stage` action whose `required_read_paths` are
exactly:

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

An isolated copy preserves that repository-relative prefix and contains only those nine files. The
recovery verifier must not read the Workflow/Prompt reference, chat history, repository history,
`current_state.json`, or `HANDOFF.md`. State and Markdown are deterministic outputs rebuilt from the
machine sources; they are not recovery inputs.

From the repository root:

```powershell
$env:PYTHONPATH = "src"
python -B -m video_truthfulness.core.execution recovery validate --bundle examples/execution_contract/synthetic_run
```

Success emits a canonical JSON summary on stdout and exits `0`. A contract failure emits an error
on stderr and exits `2`; the command never executes the next action.

This proves only this public synthetic `return_to_stage` continuation. It does not prove all four
HANDOFF action variants. `wait_for_human` and `terminate` carry no `required_read_paths` in HANDOFF
v2.0, so the Stage 4 recovery verifier fails closed for them. Real-run and full branch recovery
coverage belong to Stage 5.

The complete authority, hashing, resource, and evidence boundaries are in
[`docs/handoff_events_checkpoints.md`](../../../docs/handoff_events_checkpoints.md).
