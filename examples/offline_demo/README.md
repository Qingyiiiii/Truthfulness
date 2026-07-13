# Offline Demo Example

This example runs without platform access, browser search, ASR, OCR, or LLM calls.

Inputs:

- `transcript.json`: a small manual transcript.
- `evidence.json`: manual evidence metadata tied to claim IDs.

Expected behavior:

- Extract one factual claim.
- Score the manual evidence.
- Produce a conservative verdict.
- Write a run directory with Markdown and JSON reports.
