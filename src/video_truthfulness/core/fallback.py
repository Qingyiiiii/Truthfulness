"""Browser fallback artifact writers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from video_truthfulness.core.json_io import write_json
from video_truthfulness.core.naming import safe_filename_part, timestamp_for_filename
from video_truthfulness.core.schemas import PageFallbackArtifact


def save_page_fallback(
    run_dir: Path,
    page_url: str,
    page_title: str,
    visible_text: str,
    screenshot_bytes: bytes | None = None,
    notes: str = "",
) -> PageFallbackArtifact:
    """Save browser-visible page text and optional screenshot for fallback."""

    # Timestamp the fallback capture independently from the failed download run.
    captured_at = datetime.now(timezone.utc)
    # Browser screenshots belong under the run-local screenshots directory.
    screenshots_dir = run_dir / "screenshots"
    # Create the screenshot directory when fallback is the first artifact writer.
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    # Page text is stored as JSON so it can become transcript/manual evidence later.
    page_text_path = run_dir / "page_text.json"
    # Keep screenshot names readable while preventing invalid path characters.
    safe_title = safe_filename_part(page_title, 60)
    # Screenshot filenames include capture time to avoid overwriting retries.
    screenshot_path = screenshots_dir / f"page_{safe_title}_{timestamp_for_filename(captured_at)}.png"

    # Strip platform tracking query parameters before writing public artifacts.
    public_page_url = strip_tracking_query(page_url)
    # Store only the public URL, page title, timestamp, and visible text.
    write_json(
        page_text_path,
        {
            "page_url": public_page_url,
            "page_title": page_title,
            "captured_at": captured_at.isoformat(),
            "visible_text": visible_text,
        },
    )
    # Screenshot is optional because text fallback can still be useful if capture fails.
    saved_screenshot_path: str | None = None
    # Only write binary image bytes when the browser capture succeeded.
    if screenshot_bytes is not None:
        # Write the PNG/JPEG bytes exactly as returned by the browser.
        screenshot_path.write_bytes(screenshot_bytes)
        # Store the path only after the file write succeeds.
        saved_screenshot_path = str(screenshot_path)

    # Build the machine-readable fallback manifest.
    artifact = PageFallbackArtifact(
        # Use the cleaned public URL, not the browser's tracking URL.
        page_url=public_page_url,
        # Preserve the page title for human review.
        page_title=page_title,
        # Keep the exact fallback capture time.
        captured_at=captured_at,
        # Point reviewers to the saved visible text JSON.
        page_text_path=str(page_text_path),
        # Point reviewers to the screenshot if one exists.
        screenshot_path=saved_screenshot_path,
        # Preserve caller notes such as why direct download failed.
        notes=notes,
    )
    # Save the manifest beside download_attempts.json.
    write_json(run_dir / "page_fallback.json", artifact)
    # Return the same object so callers can display or test paths immediately.
    return artifact


def strip_tracking_query(url: str) -> str:
    """Remove query parameters from a browser URL before storing it."""

    # Split the URL into RFC 3986 components.
    parts = urlsplit(url)
    # Rebuild it without query and fragment to avoid storing tracking state.
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
