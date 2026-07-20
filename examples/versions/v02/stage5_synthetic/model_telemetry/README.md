# Synthetic model telemetry semantics

Telemetry fixtures are generated deterministically in pytest `tmp_path`; this directory does not store a sample ledger with invented “real” usage.

Each generated Session creates:

```text
model_calls.jsonl
model_usage_summary.json
```

The ledger sequence is one or more `model_call.started` / `model_call.finished` pairs followed by exactly one `model_ledger.closed`. Every row has a contiguous sequence number, one writer, a previous-record hash and its own embedded hash. The summary is created only after close from the frozen ledger hash.

## Synthetic surfaces

- A synchronous `project_hook` LLM pair with measured or unavailable fields supplied explicitly by the fixture.
- A `host_receipt` pair where requested model/profile is present but observed model and Token usage remain unavailable when no host receipt exists.
- A local ASR pair with observed identity from a synthetic local manifest, monotonic elapsed time and every Token field `not_applicable/null`.
- OCR zero-call behavior when the gate is NOT_APPLICABLE, plus an executed synthetic OCR pair when explicitly selected.
- A retrospective Gemini `external_ui` pair whose derived wall interval is bounded by frozen prompt-handoff and result-ready timestamps; active/provider elapsed and Token usage are unavailable unless a synthetic auditable UI receipt is deliberately supplied.

Requested identity never populates observed identity. `0` appears only in cases that explicitly model an authoritative zero.

## Fail-closed cases

Negative fixtures cover a missing start, orphan or duplicate finish, broken hash/sequence, second writer, close with unmatched calls, append after close, non-deterministic summary, invalid measured/derived/null combinations and privacy violations. Raw prompts, responses, URLs, credentials and private absolute paths are rejected.

These fixtures test record semantics only. They do not invoke a model, ASR engine, OCR engine, browser, Gemini, network or real project input, and their numeric values must never be cited as Stage 5 production usage.
