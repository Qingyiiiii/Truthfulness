# Interfaces

Demo1 keeps external systems behind explicit interfaces. The offline MVP implements only local transcript and local evidence inputs; platform download, browser search, and LLM providers are reserved behind contracts.

## PlatformAdapter

Responsibility:

- Identify whether a URL belongs to a supported platform.
- Fetch public or user-authorized metadata.
- Hand off media access to `MediaIntake`.

Required behavior:

- Do not bypass login, paywalls, DRM, or platform access controls.
- Run one download at a time.
- Stop and surface the exact failure when access is blocked.

## MediaIntake

Responsibility:

- Try subtitles, audio, video, page text, screenshots, and manual imports in the configured order.
- Save media under `runs/<run_id>/media/`.
- Save keyframes under `runs/<run_id>/frames/`.

Required behavior:

- Use filenames that include platform, video title, and timestamp.
- Record fallback reason in `run_log.jsonl`.
- Return `media_intake_failed` when no compliant input path exists.
- If `yt-dlp` is not installed, return `missing_component` and do not retry repeatedly.
- Download exactly one video at a time; no batch or concurrent download is allowed in Demo1.
- Multi-strategy attempts must write a complete `download_attempts.json` summary.
- If every strategy fails, proceed to browser page-text and screenshot fallback.
- Optional cookie files may be used for user-authorized access, but cookie values and paths must be redacted from public logs and Git. Raw browser cookie headers are converted only as short-lived run-local files and cleaned up after use.
- When a platform such as B站 blocks unauthenticated downloader requests with risk-control errors, the preferred authorized path is: get Cookie from the user or the explicitly authorized logged-in browser page, store it under the ignored `cookie/` directory, run one sequential download, then clear and delete the source Cookie file after success.
- If the authorized Cookie path still fails, stop retrying and return a fallback-ready status instead of repeatedly triggering platform risk controls.

## TranscriptBuilder

Responsibility:

- Convert subtitles, ASR output, page text, or manual text into `Transcript`.
- Preserve segment IDs and timestamps when available.

Required behavior:

- Keep source traceability.
- Do not silently merge unrelated text sources.

## ClaimExtractor

Responsibility:

- Convert transcript segments into atomic `Claim` records.
- Filter opinions, predictions, vague claims, and non-checkable material.

Required behavior:

- Every claim must reference source segment IDs.
- Unclear but potentially factual claims should be marked `needs_context`.

## StanceAnalyzer

Responsibility:

- Identify the author's stance toward a claim or topic.

Required behavior:

- Stance is not a truth verdict.
- Stance evidence must reference transcript segments.

## AuthorEvidenceExtractor

Responsibility:

- Extract screenshots, links, quoted data, experiments, comments, and personal experiences used by the video author.

Required behavior:

- Mark whether the material needs source tracing.
- Do not treat author-provided evidence as verified external evidence by default.

## SearchProvider

Responsibility:

- Produce external evidence candidates for claims.

Provider types:

- `BrowserSearchProvider`
- `SearchAPIProvider`
- `ManualEvidenceProvider`

Required behavior:

- Store query text and retrieved timestamp.
- Save browser screenshots when evidence is selected.
- Separate search failure from evidence insufficiency.

## EvidenceStore

Responsibility:

- Persist evidence records, screenshots, selected text, and source metadata.

Required behavior:

- Every evidence item must be tied to a claim ID.
- Browser evidence must include screenshot path.
- Evidence IDs must be stable within a run.
- Evidence screenshot filenames should follow `<claim_id>_<evidence_id>_<YYYYMMDD_HHMMSS>_<source_type>.png`.

## EvidenceScorer

Responsibility:

- Score relevance, source authority, freshness, independence, completeness, and screenshot availability.

Required behavior:

- Scoring is explanatory, not a total truth score.
- Low quality evidence should keep `review_required` true.

## ReasoningEngine

Responsibility:

- Align claims with evidence.
- Output conservative verdicts.

Required behavior:

- Never output a strong verdict without evidence.
- LLM drafts may only cite existing `evidence_id` values.
- Missing evidence must become `insufficient_evidence`, not a silent pass.

## ReportGenerator

Responsibility:

- Write human-readable Markdown and machine-readable JSON reports.

Required behavior:

- Include claims, evidence, verdicts, review flags, and limitations.
- Preserve uncertainty and missing context.

## ReviewStore

Responsibility:

- Save human review decisions for future training and evaluation.

Required behavior:

- Use JSONL records.
- Do not store private account material or unauthorized source content.

## LLMProvider

Responsibility:

- Provide a stable interface for local Ollama, LM Studio, and OpenAI-compatible services.

Required behavior:

- Do not treat model output as evidence.
- Validate structured output against Pydantic schemas before using it.
- Fail closed when provider output is malformed.
