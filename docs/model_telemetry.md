# Model telemetry v1

Stage 5 uses `model_telemetry_hook_v1.0.0` to record the model identity, elapsed-time coverage and Token coverage needed for later V02 architecture audits. “Global” means that the same contract covers every S01/S02 model surface; it does not mean one mutable project-wide ledger.

## Files and authority

Each Codex-controlled Session has exactly two telemetry files:

```text
<session_dir>/model_calls.jsonl
<session_dir>/model_usage_summary.json
```

`model_calls.jsonl` is a canonical LF-terminated, append-only hash chain written by `codex_coordinator`. `model_usage_summary.json` is deterministically created after the ledger closes and is never overwritten. Neither file is a business Artifact or a model-quality claim. The execution Event stream binds their exact paths and SHA-256 hashes; `observations.jsonl` may reference telemetry Event IDs but must not duplicate Token facts.

The active implementation and schemas are:

- `src/video_truthfulness/core/execution/model_telemetry.py`
- `schemas/execution/model_call_event_v1.schema.json`
- `schemas/execution/model_usage_summary_v1.schema.json`
- `configs/observability/model_telemetry_v1.toml`

## Call lifecycle

An interceptable project call must append `model_call.started` before invocation and exactly one matching `model_call.finished` afterward. If `started` cannot be durably written, the call is not made. If `finished` cannot be written, derived business output is not published.

Before Session termination, every start must have one finish. The coordinator then appends `model_ledger.closed` with equal started/finished counts and `unmatched_count=0`, freezes the ledger, and creates the summary. A second writer, duplicate finish, head conflict, append after close or summary overwrite fails closed.

The ledger versions are `model_call_event_v1.0.0` and `model_usage_summary_v1.0.0`. Supported call kinds are `llm`, `asr`, `ocr` and `external_research`; supported surfaces are `project_api`, `codex_runtime`, `browser_ui` and `local_runtime`.

## Requested and observed identity

Requested identity and observed identity are separate facts:

```text
requested_model = name + optional revision + optional reasoning
observed_model  = value + status + source + match_status
```

An observed value is legal only when a provider response, runtime metadata, auditable UI label or frozen local manifest actually reports it. A configured/requested name is never copied into `observed_model`. When the runtime does not expose the value, use:

```json
{"value":null,"status":"unavailable","source":"not_exposed","match_status":"unverifiable"}
```

This makes model changes visible without inventing precision.

## Time and Token semantics

Timing fields are `wall_elapsed`, `active_elapsed` and `provider_elapsed`. Each has a value, status and source. Token fields are `input_tokens`, `output_tokens`, `total_tokens`, `cached_tokens` and `reasoning_tokens`, also with value, status and source.

- `measured` requires an authoritative runtime/provider/UI value.
- `derived` is limited to a receipt interval for elapsed time, or the exact sum of measured input and output Token counts.
- `unavailable` always has `value=null` and `source=not_exposed`.
- `not_applicable` always has `value=null` and `source=not_applicable`.
- Numeric `0` is used only when the authoritative source reports zero. It is never a substitute for unavailable.

Token counts are not estimated from prompt length, output length or requested configuration.

## Observation surfaces

| Surface | Instrumentation | Honest boundary |
| --- | --- | --- |
| Project-side LLM/helper call | `project_hook` + `synchronous` | Start before call; finish after call |
| Codex coordinator/helper host execution | `host_receipt` + `retrospective_host_receipt` | Import only facts actually exposed by the host; otherwise observed model/Token are unavailable |
| faster-whisper ASR | `project_hook` + `synchronous` | Model identity from frozen local manifest, elapsed time from monotonic clock, Token fields not applicable |
| OCR | Event only when OCR is actually called | A NOT_APPLICABLE gate creates no model call; executed OCR uses local manifest and Token not applicable |
| User-operated Gemini Web | `external_ui` + `retrospective_manual_receipt` | Imported by S02-B from the frozen S02-A prompt time and G2 result-ready receipt |

Project-hook coverage and host-boundary coverage remain separate. The project must not describe an unavailable Codex host receipt as complete synchronous Hook coverage.

For Gemini Web, the retrospective pair proves only the out-of-band wall interval. It does not prove Gemini active compute time. Observed model may be `reported` only from an auditable UI label/export. Active/provider time and Token usage remain unavailable unless the UI/export explicitly supplies a reviewable usage receipt. There is no polling, timer or background collector during the user-only interval.

## Summary

The deterministic summary groups finished calls by provider, invocation surface, actor role, call kind, requested model and observed model/status. It records call/failure/mismatch counts, covered wall elapsed time and per-field Token coverage. Unavailable and not-applicable counts remain explicit instead of being silently dropped.

Comparisons for a future framework upgrade must read the exact Session ledger/summary paths frozen by its execution plan. They must not scan directories or select `latest`, and must compare coverage gaps as well as numeric totals.

## Privacy

Telemetry contains identifiers, contract hashes, timing and usage facts—not raw prompts or responses. URLs, headers, credentials, cookies, account material and private absolute paths are rejected. Text values are bounded. Gemini result content, transcript text and retrieved excerpts belong in their own authorized business/input contracts, not in telemetry.
