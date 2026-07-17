"""V02 identity constants without media, workflow, or downstream side effects."""

from __future__ import annotations

import re
from pathlib import Path


PROJECT_VERSION = "v0.2"
STORAGE_VERSION = "V02"
RELEASE_ID = "truthfulness_v0.2_youtube_video"
RUN_ID_PATTERN = re.compile(r"^run_[0-9a-hjkmnp-tv-z]{26}$")


def canonical_run_path(run_id: str) -> Path:
    """Return the repository-relative canonical V02 run path."""

    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("V02 run_id must match run_<26-char-lowercase-ulid>.")
    return Path("runs") / STORAGE_VERSION / run_id
