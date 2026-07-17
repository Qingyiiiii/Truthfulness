"""Version-neutral yt-dlp command primitives.

Platform policy, run identity, cookie conversion, and output paths belong to a
version package. This module only supplies reusable local-process mechanics.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class DownloadStrategy:
    """One yt-dlp strategy with explicit arguments."""

    name: str
    format_selector: str | None = None
    extra_args: list[str] = field(default_factory=list)
    metadata_only: bool = False


def yt_dlp_available() -> bool:
    """Return whether the optional yt-dlp module is installed."""

    return importlib.util.find_spec("yt_dlp") is not None


def build_yt_dlp_command(
    strategy: DownloadStrategy,
    source_url: str,
    media_path: Path,
    extension: str,
    cookie_args: list[str] | None = None,
    ffmpeg_args: list[str] | None = None,
) -> list[str]:
    """Build a version-neutral yt-dlp command from one supplied strategy."""

    command = [sys.executable, "-m", "yt_dlp", "--no-playlist", "--restrict-filenames"]
    if strategy.format_selector:
        command.extend(["-f", strategy.format_selector])
    if not strategy.metadata_only:
        command.extend(["--merge-output-format", extension])
        command.extend(ffmpeg_location_args() if ffmpeg_args is None else ffmpeg_args)
        command.extend(["-o", str(media_path)])
    if cookie_args:
        command.extend(cookie_args)
    command.extend(strategy.extra_args)
    command.append(source_url)
    return command


def ffmpeg_location_args() -> list[str]:
    """Return yt-dlp ffmpeg arguments when bundled ffmpeg is available."""

    if importlib.util.find_spec("imageio_ffmpeg") is None:
        return []
    import imageio_ffmpeg

    return ["--ffmpeg-location", imageio_ffmpeg.get_ffmpeg_exe()]


def redact_command(command: list[str]) -> list[str]:
    """Remove credential-bearing values from a command before storage."""

    redacted: list[str] = []
    skip_next = False
    for value in command:
        if skip_next:
            redacted.append("[redacted]")
            skip_next = False
            continue
        lowered = value.lower()
        redacted.append(value)
        if lowered in {"--cookies", "--cookies-from-browser", "--username", "--password", "--add-header"}:
            skip_next = True
    return redacted


def redact_sensitive_output(output: str, max_length: int = 1200) -> str:
    """Remove credential-adjacent lines and retain only a bounded log tail."""

    sanitized_lines = []
    for line in output.splitlines():
        if "cookie" in line.lower() or "token" in line.lower():
            sanitized_lines.append("[redacted sensitive downloader line]")
        else:
            sanitized_lines.append(line)
    return "\n".join(sanitized_lines)[-max_length:]
