"""Filename helpers shared by media intake and run creation."""

from __future__ import annotations

import re
from datetime import datetime

from video_truthfulness.schemas import Platform

SAFE_PART_PATTERN = re.compile(r"[^0-9A-Za-z\u4e00-\u9fff._-]+")


def safe_filename_part(value: str, max_length: int = 80) -> str:
    """Convert a human title into a filesystem-safe filename part."""

    stripped = value.strip()
    replaced = SAFE_PART_PATTERN.sub("_", stripped)
    collapsed = re.sub(r"_+", "_", replaced).strip("._-")
    safe_value = collapsed or "untitled"
    return safe_value[:max_length]


def timestamp_for_filename(moment: datetime | None = None) -> str:
    """Return the timestamp format required by media filenames."""

    current = moment or datetime.now()
    return current.strftime("%Y%m%d_%H%M%S")


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
