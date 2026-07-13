# File Layout

This document defines where Demo1 files belong and what may be committed.

## Repository Files

| Category | Path | Commit policy |
| --- | --- | --- |
| Project entry | `README.md`, `项目方案.md` | Commit |
| Python package | `src/video_truthfulness/` | Commit |
| Streamlit entry | `app/` | Commit |
| Public configs | `configs/*.example.toml` | Commit |
| Schema docs | `schemas/` | Commit generated schemas and docs only |
| Engineering docs | `docs/` | Commit |
| Public examples | `examples/` | Commit only synthetic or authorized samples |
| Tests | `tests/` | Commit |
| Runtime outputs | `runs/<run_id>/` | Do not commit real outputs |
| Local model files | `models/` | Do not commit weights or private paths |

## Run Directory Layout

Each single-video run writes to a separate directory:

```text
runs/<run_id>/
  input.json
  metadata.json
  transcript.json
  claims.json
  stance.json
  author_evidence.json
  search_queries.json
  evidence_manifest.json
  evidence_scores.json
  verdicts.json
  download_attempts.json
  page_text.json
  page_fallback.json
  report.md
  report.json
  review.jsonl
  run_log.jsonl
  media/
  frames/
  screenshots/
```

## Media Files

Downloaded video or audio files belong under:

```text
runs/<run_id>/media/
```

Required naming pattern:

```text
<platform>_<safe_video_title>_<YYYYMMDD_HHMMSS>.<extension>
```

Example:

```text
bilibili_sample_video_title_20260628_173015.mp4
```

Rules:

- Include the platform name.
- Include a filesystem-safe video title.
- Include the exact local timestamp used when the file is saved.
- Never run multiple platform downloads at the same time.
- Stop and report the failing step when a platform download is blocked or denied.
- If a required download component is missing, report `missing_component` before attempting platform access.
- Multi-strategy attempts must be sequential and written to `download_attempts.json`.

## Screenshots

Video keyframes belong under:

```text
runs/<run_id>/frames/
```

External evidence screenshots belong under:

```text
runs/<run_id>/screenshots/
```

Evidence screenshot naming pattern:

```text
<claim_id>_<evidence_id>_<YYYYMMDD_HHMMSS>_<source_type>.png
```

Screenshot rules:

- Every browser-collected evidence item must have a screenshot.
- A screenshot is a review artifact, not proof by itself.
- If screenshot capture fails, record the failure separately from evidence insufficiency.

## Download Attempts And Browser Fallback

When direct media download is attempted, write:

```text
runs/<run_id>/download_attempts.json
```

If all download strategies fail, browser fallback writes:

```text
runs/<run_id>/page_text.json
runs/<run_id>/page_fallback.json
runs/<run_id>/screenshots/page_<safe_title>_<YYYYMMDD_HHMMSS>.png
```

Rules:

- Attempt strategies sequentially, never concurrently.
- Keep every strategy result in `download_attempts.json`.
- Do not store cookies, tokens, request headers containing credentials, or account data.
- When platform risk control blocks unauthenticated downloads, use user-authorized cookies before falling back to page text and screenshots.
- Page text and screenshots are fallback inputs, not final evidence by themselves.

## Cookie Files

Cookie files are local-only sensitive inputs.

Rules:

- Store local cookies under ignored paths such as `cookie/`, `cookies/`, or `tmp/`.
- Prefer an ignored project-local directory such as `cookie/` for user-authorized platform cookies used by Demo1 downloads.
- A cookie may come from a user-provided file or, after explicit user authorization, from the currently logged-in browser page.
- Do not commit cookie files.
- Do not print cookie values.
- If a copied browser cookie header is used, convert it to a temporary Netscape cookie file under the ignored run directory and remove it after the downloader exits.
- After a successful download, clear and delete the source cookie file when it is under `cookie/` or `cookies/`.
- If Windows denies immediate deletion, blank the file content first so no cookie value remains.
- Redact `--cookies` paths from saved command records.
- If all authorized download strategies fail, stop repeated downloads and switch to page text, keyframe screenshots, or manual media import.

## Model Files

Model weights and local model caches belong outside Git. If a local model path is needed, put only a safe placeholder in an example config.

Never commit:

- `.gguf`
- `.safetensors`
- `.bin`
- private model directories
- provider API keys

## Sensitive Files

Never commit:

- Cookies
- Tokens
- Account material
- Private video data
- Unauthorized media
- Real sensitive run artifacts
