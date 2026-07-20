# Execution HANDOFF

> Deterministic projection of the sibling `handoff.json`; the JSON file is authoritative.

## 1. Identity and versions

- Project/storage/release: `v0.2` / `V02` / `truthfulness_v0.2_youtube_video`.
- Task/session/attempt: `task_01j00000000000000000000000` / `session_01j00000000000000000000000` / `1`.
- Run/stage/status: `run_01j00000000000000000000000` / `S01` / `COMPLETED`.
- Workflow/DAG/profile: `youtube_truthfulness_workflow_v1.1.0` / `youtube_truthfulness_dag_v1.1.0` / `video_intake_agent_v1.0.0`.
- HANDOFF Artifact/hash: `artifact_01j00000000000000000000003` / `5c11b98f503effd7919922400d58c1ab0e131a4bab77bd59c6b26fc0fb9fb3ba`.
- Render profile: `handoff_markdown_renderer_v1.0.0`.

## 2. Objective, explicit inputs, and terminal result

- Objective boundary: continue only task `task_01j00000000000000000000000` within stage `S01` and its fixed workflow.
- Terminal result: `COMPLETED`.
- Explicit input Artifact count: `1`.
- Input: `artifact_01j00000000000000000000001` (`synthetic.input`) at `examples/execution_contract/synthetic_run/artifacts/input.json`, SHA-256 `def81af2ea85a2bb3ce0c0e49607aeccceb25734ad5b64cf978b6697d0a56c75`, validation `passed`.

## 3. Completed and remaining actions

- Completed `task_completed`: the bounded synthetic transformation completed and validated.
- Completed `checkpoint_created`: created immutable checkpoint checkpoint\_01j00000000000000000000000.
- Remaining `execute_source_identity`: required inputs exist; node has not been executed.
- Human decisions required: `0`.

## 4. Artifact and validation boundary

- Output Artifact count: `1`.
- Output: `artifact_01j00000000000000000000002` (`synthetic.output`) at `examples/execution_contract/synthetic_run/artifacts/output.json`, SHA-256 `94c42a41e538829a75fa1cab004620b00f7cb36a7c315dba4bb2f93c6856853a`, validation `passed`.
- Validation: `passed` (`1` passed, `0` failed, `0` partial).
- Invalidated Artifact count: `0`.

## 5. Read/write audit summary

- Declared read/write entries: `5` / `1`.
- Actual read/write entries: `1` / `1`.
- Out-of-scope detections: `0`.

## 6. Risk, privacy, evidence, and capability boundary

- Declared risk count: `0`.
- Risks: none.
- Privacy boundary: credentials, private absolute paths, long logs, and raw ASR/OCR bodies are forbidden in this control projection.
- Evidence/capability boundary: this Markdown adds no fact beyond the validated machine HANDOFF.

## 7. Checkpoint and recovery anchor

- Checkpoint: `checkpoint_01j00000000000000000000000`.
- Source event head: `event_01j00000000000000000000008` at sequence `8`, SHA-256 `34f1296e4cfb4cd0a665552ad442118e98f820aeb4c985956317ef04dbdccda1`.
- Pre-registration Registry head count: `1`.
- Registry `run` at `examples/execution_contract/synthetic_run/artifact_registry.jsonl`: records `2`, head `record_01j00000000000000000000002`, prefix SHA-256 `93218772b4d3d0132831738c06e37f70d5b02f16ecfd1a3c219dfb6ce01e27d7`.
- Machine source: the sibling `handoff.json`.
- Rebuildable files: `HANDOFF.md` and `current_state.json` are not recovery facts.

## 8. Unique next action

- Action: return to stage `S01`.
- Workflow: `Optmize/workflows/01_单视频采集与机器初筛.md`.
- Prompt reference: `Optmize/workflows/01_单视频采集与机器初筛.md`.
- Reason: the synthetic Registry has no canonical run.identity; resume at the ready S01 source\_identity boundary.
- Required input Artifacts: none.
- Required read paths: `examples/execution_contract/synthetic_run/artifact_registry.jsonl`, `examples/execution_contract/synthetic_run/artifacts/input.json`, `examples/execution_contract/synthetic_run/artifacts/output.json`, `examples/execution_contract/synthetic_run/checkpoints/checkpoint_01j00000000000000000000000.json`, `examples/execution_contract/synthetic_run/events.jsonl`, and `4` more.

## 9. Minimal execution prompt

Validate the sibling `handoff.json`, its checkpoint, source event head, Registry heads, and only the declared required paths. Then return only to stage `S01` using none; stop at any gate or validation failure.
