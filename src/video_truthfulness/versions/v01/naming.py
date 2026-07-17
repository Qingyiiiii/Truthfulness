"""Filename helpers shared by media intake and run creation."""

from __future__ import annotations

from datetime import datetime

from video_truthfulness.core.naming import safe_filename_part, timestamp_for_filename
from video_truthfulness.core.schemas import Platform


def build_media_filename(
    platform: Platform | str,
    video_title: str,
    extension: str,
    moment: datetime | None = None,
) -> str:
    """Build `<platform>_<safe_title>_<YYYYMMDD_HHMMSS>.<extension>`."""

    platform_value = platform.value if isinstance(platform, Platform) else platform
    safe_platform = safe_filename_part(platform_value, max_length=24)
    safe_title = safe_filename_part(video_title, max_length=80)
    safe_extension = extension.lower().lstrip(".")
    return f"{safe_platform}_{safe_title}_{timestamp_for_filename(moment)}.{safe_extension}"


def build_run_id(platform: Platform | str, video_title: str, moment: datetime | None = None) -> str:
    """Build a stable run ID that also remains readable in the filesystem."""

    platform_value = platform.value if isinstance(platform, Platform) else platform
    return f"{safe_filename_part(platform_value, 24)}_{safe_filename_part(video_title, 60)}_{timestamp_for_filename(moment)}"
