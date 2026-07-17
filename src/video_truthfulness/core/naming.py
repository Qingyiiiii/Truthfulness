"""Version-neutral filename helpers."""

from __future__ import annotations

import re
from datetime import datetime


SAFE_PART_PATTERN = re.compile(r"[^0-9A-Za-z\u4e00-\u9fff._-]+")


def safe_filename_part(value: str, max_length: int = 80) -> str:
    """Convert human text into a filesystem-safe filename part."""

    stripped = value.strip()
    replaced = SAFE_PART_PATTERN.sub("_", stripped)
    collapsed = re.sub(r"_+", "_", replaced).strip("._-")
    safe_value = collapsed or "untitled"
    return safe_value[:max_length]


def timestamp_for_filename(moment: datetime | None = None) -> str:
    """Return the legacy timestamp format used by human-facing filenames."""

    current = moment or datetime.now()
    return current.strftime("%Y%m%d_%H%M%S")
