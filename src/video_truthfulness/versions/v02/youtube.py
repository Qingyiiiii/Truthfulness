"""V02 YouTube intake policy boundary without execution side effects."""

from __future__ import annotations

from video_truthfulness.core.media import DownloadStrategy


def no_cookie_strategies() -> list[DownloadStrategy]:
    """Return the bounded, ordinary no-cookie strategy sequence for V02 YouTube."""

    return [
        DownloadStrategy(
            name="youtube_public_av",
            format_selector=(
                "bestvideo[height=480]+bestaudio/"
                "best[height=480]/worstvideo+worstaudio/worst"
            ),
        ),
        DownloadStrategy(
            name="youtube_public_progressive",
            format_selector="best[height=480]/worst",
        ),
        DownloadStrategy(
            name="youtube_metadata_probe",
            extra_args=["--dump-json", "--skip-download"],
            metadata_only=True,
        ),
    ]
